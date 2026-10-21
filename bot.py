"""Research-only Pump.fun Telegram screener. Unknown critical checks never pass."""
import asyncio
import contextlib
import datetime as dt
import json
import logging
import os
import re
import sqlite3
import time
from collections import deque
from pathlib import Path

import feedparser
import httpx
import websockets
from dotenv import load_dotenv
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

load_dotenv()
TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
CHAT_ID = int(os.environ["TELEGRAM_CHAT_ID"])
RPC = os.getenv("SOLANA_RPC_URL", "https://api.mainnet-beta.solana.com")
WS = os.getenv("PUMP_WS_URL", "wss://pumpportal.fun/api/data")
CAP = float(os.getenv("MIN_MARKET_CAP_USD", "10000"))
MAX_AGE = int(os.getenv("MAX_LAUNCH_AGE_SECONDS", "3600"))
MIN_SCORE = float(os.getenv("MIN_SECURITY_SCORE", "5"))
POLL = int(os.getenv("POLL_SECONDS", "15"))
SOL_PRICE_REFRESH_SECONDS = 90
MARKET_REFRESH_SECONDS = 30
# Paid PumpPortal trade streams are intentionally disabled. The AUD amount is
# a planning ceiling, NOT a provider-side payment limit.
PAID_STREAM_ENABLED = False
PAID_STREAM_BUDGET_AUD = 5.0
RSS = [u.strip() for u in os.getenv("RSS_URLS", "").split(",") if u.strip()]
DB = Path("data/bot.db")
DB.parent.mkdir(exist_ok=True)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("screener")

CATEGORIES = {
    "news": "Breaking news", "viral": "Viral events", "phrases": "Words / phrases",
    "original": "Original ideas", "culture": "Cultural narratives", "remix": "Remixes",
}


def conn():
    db = sqlite3.connect(DB)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    return db


def init_db():
    with conn() as db:
        db.executescript("""
        CREATE TABLE IF NOT EXISTS tokens (
          mint TEXT PRIMARY KEY, name TEXT, symbol TEXT, first_seen REAL,
          created_at REAL, cap REAL, security REAL, potential REAL,
          category TEXT, state TEXT, reason TEXT, updated_at REAL,
          launch_cap_sol REAL, cap_source TEXT, cap_observed_at REAL,
          threshold_alerted_at REAL);
        CREATE TABLE IF NOT EXISTS news (
          url TEXT PRIMARY KEY, title TEXT, published REAL, fetched REAL);
        CREATE TABLE IF NOT EXISTS watchlist (handle TEXT PRIMARY KEY);
        """)
        columns = {r[1] for r in db.execute("PRAGMA table_info(tokens)")}
        for name, dtype in (("launch_cap_sol", "REAL"), ("cap_source", "TEXT"), ("cap_observed_at", "REAL"), ("threshold_alerted_at", "REAL")):
            if name not in columns:
                db.execute(f"ALTER TABLE tokens ADD COLUMN {name} {dtype}")


def words(s):
    return set(re.findall(r"[a-z0-9]{3,}", (s or "").lower())) - {
        "the", "and", "with", "for", "coin", "token", "official", "pump"}


def category_and_potential(name, symbol, headlines):
    """Conservative evidence-limited narrative estimate, NOT an AI virality assessment."""
    w = words(name + " " + symbol)
    best = (0, None)
    now = time.time()
    for item in headlines:
        overlap = len(w & words(item["title"]))
        if overlap and overlap > best[0] and now - item["published"] < 86400:
            best = (overlap, item)
    if best[1]:
        # Headline overlap alone cannot establish virality or event authenticity.
        return "news", 4.0, f"Possible news keyword overlap: {best[1]['title'][:90]} | {best[1]['url']}"
    if len(w) <= 2:
        return "phrases", 2.0, "Short name; originality and attention not verified"
    return "original", 2.0, "Narrative unverified; no matching fresh headline"


async def rpc(client, method, params):
    try:
        r = await client.post(RPC, json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params}, timeout=10)
        r.raise_for_status()
        data = r.json()
        return data.get("result") if not data.get("error") else None
    except (httpx.HTTPError, ValueError):
        return None


async def check_authorities(client, mint):
    data = await rpc(client, "getAccountInfo", [mint, {"encoding": "jsonParsed", "commitment": "confirmed"}])
    try:
        account = data["value"]
        parsed = account["data"]["parsed"]
        if parsed["type"] != "mint":
            return None
        info = parsed["info"]
        # Token-2022 extensions need explicit review, even if authorities are null.
        extensions = info.get("extensions", [])
        return {
            "mint_disabled": info.get("mintAuthority") is None,
            "freeze_disabled": info.get("freezeAuthority") is None,
            "extensions": extensions,
            "program": account.get("owner"),
        }
    except (TypeError, KeyError, IndexError):
        return None


async def market(client, mint):
    try:
        r = await client.get(f"https://api.dexscreener.com/token-pairs/v1/solana/{mint}", timeout=10)
        r.raise_for_status()
        pairs = r.json()
        pairs = [p for p in pairs if p.get("baseToken", {}).get("address") == mint]
        return max(pairs, key=lambda p: (p.get("liquidity") or {}).get("usd") or 0) if pairs else None
    except (httpx.HTTPError, ValueError, TypeError):
        return None


async def sol_usd_price(client):
    """Independent spot price; return None rather than guessing on failure."""
    try:
        r = await client.get(
            "https://api.coingecko.com/api/v3/simple/price",
            params={"ids": "solana", "vs_currencies": "usd"}, timeout=10)
        r.raise_for_status()
        price = r.json().get("solana", {}).get("usd")
        if isinstance(price, (int, float)) and not isinstance(price, bool) and 0 < price < 100000:
            return float(price)
    except (httpx.HTTPError, ValueError, TypeError, AttributeError):
        pass
    return None


def provider_cap(pair):
    """Use provider market cap, never FDV as a silent substitute."""
    if not pair:
        return None
    cap = pair.get("marketCap")
    if isinstance(cap, (int, float)) and not isinstance(cap, bool) and cap > 0:
        return float(cap)
    return None


async def largest_accounts(client, mint):
    supply, largest = await asyncio.gather(
        rpc(client, "getTokenSupply", [mint]),
        rpc(client, "getTokenLargestAccounts", [mint]),
    )
    try:
        raw_supply = int(supply["value"]["amount"])
        if raw_supply <= 0:
            return None
        accounts = largest["value"]
        return sum(int(x["amount"]) for x in accounts[:5]) / raw_supply
    except (KeyError, TypeError, ValueError):
        return None


async def inspect(client, mint, record, headlines):
    authority, pair, concentration = await asyncio.gather(
        check_authorities(client, mint), market(client, mint), largest_accounts(client, mint)
    )
    flags = []
    score = 10.0
    critical_unknown = False
    if authority is None:
        critical_unknown = True
        flags.append("Mint authorities unknown")
    else:
        if not authority["mint_disabled"]:
            flags.append("Mint authority active")
            score -= 4
        if not authority["freeze_disabled"]:
            flags.append("Freeze authority active")
            score -= 4
        if authority["extensions"]:
            critical_unknown = True
            flags.append("Token extensions require manual inspection")
    if concentration is None:
        critical_unknown = True
        flags.append("Holder distribution unknown")
    elif concentration >= .5:
        score -= 3
        flags.append(f"Top 5 token accounts control {concentration:.0%} (includes pools)")
    elif concentration >= .3:
        score -= 1.5
        flags.append(f"Top 5 token accounts control {concentration:.0%} (includes pools)")
    if pair is None:
        flags.append("No indexed DEX market yet")
    else:
        liquidity = (pair.get("liquidity") or {}).get("usd")
        if liquidity is not None and liquidity < 3000:
            score -= 2
            flags.append("Low reported DEX liquidity")
    # Do not assert liquidity burn, sellability, holder clusters, or candle safety.
    critical_unknown = True
    flags.append("LP security, sellability and manipulation checks NOT implemented")
    score = max(0.0, min(10.0, score))
    category, potential, why = category_and_potential(record["name"], record["symbol"], headlines)
    state = "HOLD" if critical_unknown else ("PASS" if score >= MIN_SCORE else "REJECT")
    return score, potential, category, state, "; ".join(flags + [why])


async def fetch_headlines(client):
    """RSS supports background narrative context; not social virality verification."""
    result = []
    for url in RSS:
        try:
            response = await client.get(url, timeout=12, follow_redirects=True)
            response.raise_for_status()
            parsed = feedparser.parse(response.text)
            for e in parsed.entries[:35]:
                struct = e.get("published_parsed") or e.get("updated_parsed")
                stamp = time.mktime(struct) if struct else time.time()
                result.append({"title": e.get("title", ""), "url": e.get("link", ""), "published": stamp})
        except (httpx.HTTPError, ValueError) as exc:
            log.warning("RSS unavailable: %s", exc)
    return result


async def launch_stream(queue):
    while True:
        try:
            async with websockets.connect(WS, ping_interval=20, ping_timeout=20) as ws:
                await ws.send(json.dumps({"method": "subscribeNewToken"}))
                log.info("Subscribed to new token launches")
                async for raw in ws:
                    try:
                        event = json.loads(raw)
                        mint = event.get("mint")
                        if mint and event.get("txType") in (None, "create"):
                            await queue.put(event)
                    except (ValueError, TypeError):
                        continue
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            log.warning("Launch stream failed: %s", exc)
            await asyncio.sleep(5)


async def worker(application, queue):
    async with httpx.AsyncClient() as client:
        headlines = []
        last_news = 0.0
        last_sol_check = 0.0
        sol_price = None
        sol_price_at = 0.0
        while True:
            now = time.time()
            if now - last_news > 300:
                headlines = await fetch_headlines(client)
                last_news = now
            if now - last_sol_check >= SOL_PRICE_REFRESH_SECONDS:
                last_sol_check = now
                candidate_price = await sol_usd_price(client)
                if candidate_price is not None:
                    sol_price, sol_price_at = candidate_price, time.time()
            try:
                event = await asyncio.wait_for(queue.get(), timeout=POLL)
            except asyncio.TimeoutError:
                event = None
            if event:
                mint = event["mint"]
                raw_cap = event.get("marketCapSol")
                cap_sol = (float(raw_cap) if isinstance(raw_cap, (int, float))
                           and not isinstance(raw_cap, bool) and 0 < raw_cap < 100000000 else None)
                observed = time.time()
                # The launch snapshot is not a live market cap and not proof of crossing the threshold.
                with conn() as db:
                    db.execute(
                        "INSERT OR IGNORE INTO tokens(mint,name,symbol,first_seen,state,launch_cap_sol) VALUES(?,?,?,?,?,?)",
                        (mint, str(event.get("name", ""))[:100],
                         str(event.get("symbol", ""))[:30], observed, "NEW", cap_sol))
                    if cap_sol is not None:
                        db.execute("UPDATE tokens SET launch_cap_sol=COALESCE(launch_cap_sol, ?) WHERE mint=?",
                                   (cap_sol, mint))
                queue.task_done()
            # Avoid excessive provider polling: cap checks are spaced per token.
            with conn() as db:
                rows = db.execute(
                    "SELECT * FROM tokens WHERE state IN ('NEW','WAIT_CAP') "
                    "AND first_seen>? AND (updated_at IS NULL OR updated_at<?) "
                    "ORDER BY COALESCE(updated_at,0) ASC LIMIT 12",
                    (time.time() - MAX_AGE, time.time() - MARKET_REFRESH_SECONDS)).fetchall()
            for row in rows:
                mint = row["mint"]
                pair = await market(client, mint)
                dex_cap = provider_cap(pair)
                cap, source = None, None
                # Prefer a recently fetched DEX provider observation over the launch snapshot.
                if dex_cap is not None:
                    cap, source = dex_cap, "DEX provider (recent poll)"
                elif row["launch_cap_sol"] is not None and sol_price is not None and time.time() - sol_price_at < 300:
                    cap = float(row["launch_cap_sol"]) * sol_price
                    source = "PumpPortal LAUNCH snapshot x SOL/USD (NOT live)"
                with conn() as db:
                    db.execute(
                        "UPDATE tokens SET cap=?,cap_source=?,cap_observed_at=?,updated_at=?,state='WAIT_CAP' WHERE mint=?",
                        (cap, source, time.time() if cap is not None else None, time.time(), mint))
                # Notify once per mint when a recent DEX market-cap observation
                # first reaches the configured threshold. This is a market-data
                # alert only, NOT a security approval or a trading signal.
                if source == "DEX provider (recent poll)" and cap >= CAP and row["threshold_alerted_at"] is None:
                    with conn() as db:
                        updated = db.execute(
                            "UPDATE tokens SET threshold_alerted_at=? WHERE mint=? AND threshold_alerted_at IS NULL",
                            (time.time(), mint)).rowcount
                    if updated:
                        await application.bot.send_message(
                            CHAT_ID,
                            f"MARKET-CAP WATCH (UNVERIFIED)\n"
                            f"{row['name']} ({row['symbol']})\n"
                            f"Provider-reported cap: US${cap:,.0f} (recent DEX poll)\n"
                            f"Crossing time and launch age NOT confirmed. Security NOT cleared.\n"
                            f"Mint: {mint}")
                # Only a current DEX observation can trigger further screening here.
                # Launch snapshots are displayed for research but never treated as
                # proof that a token has NOW crossed US$10,000.
                if source != "DEX provider (recent poll)" or cap < CAP:
                    continue
                score, potential, category, state, reason = await inspect(client, mint, row, headlines)
                with conn() as db:
                    db.execute(
                        "UPDATE tokens SET security=?,potential=?,category=?,state=?,reason=?,updated_at=? WHERE mint=?",
                        (score, potential, category, state, reason, time.time(), mint))
                if state == "PASS":
                    await application.bot.send_message(
                        CHAT_ID, f"Candidate {row['name']} ({row['symbol']})\n"
                        f"Security {score}/10 | Potential {potential}/10\n{mint}")
            with conn() as db:
                db.execute("UPDATE tokens SET state='EXPIRED' WHERE first_seen<? AND state IN ('NEW','WAIT_CAP')",
                           (time.time() - MAX_AGE,))


def authorized(update):
    return update.effective_chat is not None and update.effective_chat.id == CHAT_ID


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if authorized(update):
        await update.message.reply_text("Pump.fun research bot online. /new (early discoveries; launch cap estimates are NOT live or verified) /status /top /news /viral /phrases /original /culture /remix /check <mint> /accounts /addaccount <handle> /removeaccount <handle>\n\nHOLD means critical security checks are unverified. No trading.")


async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not authorized(update): return
    with conn() as db:
        rows = db.execute("SELECT state,COUNT(*) n FROM tokens GROUP BY state").fetchall()
    await update.message.reply_text("States: " + (", ".join(f"{r['state']}={r['n']}" for r in rows) or "none") + "\nA$100 total risk budget. Research only.")


async def new_discoveries(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Recent feed observations, including unverified/under-threshold tokens."""
    if not authorized(update):
        return
    now = time.time()
    with conn() as db:
        rows = db.execute(
            "SELECT * FROM tokens WHERE first_seen >= ? AND state != 'EXPIRED' "
            "ORDER BY first_seen DESC LIMIT 10", (now - MAX_AGE,)
        ).fetchall()
    if not rows:
        await update.message.reply_text(
            "No recent tokens in the local launch feed yet. Check /status. "
            "This is not evidence that Pump.fun has no launches."
        )
        return
    entries = []
    for index, row in enumerate(rows, 1):
        age_minutes = max(0, int((now - row['first_seen']) / 60))
        cap = row['cap']
        cap_text = f"US${cap:,.0f}" if cap is not None else "unknown"
        source = row["cap_source"] or "No verified market data"
        observed = row["cap_observed_at"]
        cap_age = f"; sampled {int((now-observed)/60)}m ago" if observed else ""
        cap_text += f" [{source}{cap_age}]"
        state = row['state'] or 'NEW'
        if state == 'WAIT_CAP':
            warning = "Threshold not confirmed with current market data; not security-cleared"
        elif state == 'HOLD':
            warning = "Security verification incomplete; NOT cleared"
        elif state == 'REJECT':
            warning = "Failed preliminary screening; NOT cleared"
        elif state == 'PASS':
            warning = "Passed configured checks (review evidence independently)"
        else:
            warning = "Awaiting market data and screening; NOT cleared"
        name = (row['name'] or 'Unnamed').replace('\n', ' ')[:55]
        symbol = (row['symbol'] or '?').replace('\n', ' ')[:18]
        entries.append(
            f"{index}. {name} ({symbol})\n"
            f"Cap: {cap_text} | Seen: {age_minutes}m ago\n"
            f"State: {state} — {warning}\n"
            f"Mint: {row['mint']}"
        )
    await update.message.reply_text(
        "EARLY DISCOVERIES — UNVERIFIED\n"
        "Sorted by time first seen by this bot, NOT verified token age. "
        "A listing is NOT a buy signal.\n\n" + "\n\n".join(entries)
    )


async def ranking(update: Update, context: ContextTypes.DEFAULT_TYPE, category=None):
    if not authorized(update): return
    with conn() as db:
        if category:
            rows = db.execute("SELECT * FROM tokens WHERE state='PASS' AND category=? ORDER BY potential DESC,security DESC LIMIT 10", (category,)).fetchall()
        else:
            rows = db.execute("SELECT * FROM tokens WHERE state='PASS' ORDER BY potential DESC,security DESC LIMIT 10").fetchall()
    if not rows:
        await update.message.reply_text("No fully verified qualifying coins yet. Unverified security checks are held back, not silently passed.")
        return
    await update.message.reply_text("\n\n".join(f"{i}. {r['name']} (${r['symbol']})\nSecurity {r['security']}/10 · Potential {r['potential']}/10\n{r['mint']}" for i,r in enumerate(rows,1)))


async def check(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not authorized(update): return
    if not context.args:
        await update.message.reply_text("Usage: /check <Solana mint address>")
        return
    mint = context.args[0]
    if not re.fullmatch(r"[1-9A-HJ-NP-Za-km-z]{32,44}", mint):
        await update.message.reply_text("Invalid-looking Solana mint address")
        return
    with conn() as db:
        row = db.execute("SELECT * FROM tokens WHERE mint=?", (mint,)).fetchone()
    if not row:
        await update.message.reply_text("Not observed in this bot's Pump.fun launch feed; cannot verify launch age or origin. No rating given.")
    else:
        await update.message.reply_text(f"{row['name']} (${row['symbol']})\nState: {row['state']}\nMarket cap: {row['cap']} USD ({row['cap_source'] or 'unavailable'})\nSecurity: {row['security']}/10\nPotential: {row['potential']}/10\nCategory: {row['category']}\n{row['reason'] or 'Awaiting data'}\nMint: {mint}")


async def accounts(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not authorized(update): return
    with conn() as db:
        handles = [r[0] for r in db.execute("SELECT handle FROM watchlist ORDER BY handle")]
    await update.message.reply_text("Saved X account watchlist (NOT LIVE MONITORED):\n" + ("\n".join("@"+h for h in handles) or "Empty") + "\nX API connection is a future integration.")


async def edit_account(update: Update, context: ContextTypes.DEFAULT_TYPE, remove=False):
    if not authorized(update): return
    if not context.args:
        await update.message.reply_text("Supply an X handle")
        return
    handle = context.args[0].lstrip("@").lower()
    if not re.fullmatch(r"[a-z0-9_]{1,15}", handle):
        await update.message.reply_text("Invalid X handle")
        return
    with conn() as db:
        if remove:
            db.execute("DELETE FROM watchlist WHERE handle=?", (handle,))
        else:
            db.execute("INSERT OR IGNORE INTO watchlist(handle) VALUES(?)", (handle,))
    await update.message.reply_text(("Removed " if remove else "Saved ") + "@" + handle + " (watchlist only; live X monitoring not enabled)")


async def post_init(application):
    queue = asyncio.Queue(maxsize=10000)
    application.bot_data["tasks"] = [
        asyncio.create_task(launch_stream(queue)),
        asyncio.create_task(worker(application, queue)),
    ]


async def post_shutdown(application):
    for task in application.bot_data.get("tasks", []):
        task.cancel()
    await asyncio.gather(*application.bot_data.get("tasks", []), return_exceptions=True)


def main():
    init_db()
    application = Application.builder().token(TOKEN).post_init(post_init).post_shutdown(post_shutdown).build()
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("status", status))
    application.add_handler(CommandHandler("top", ranking))
    application.add_handler(CommandHandler("new", new_discoveries))
    for key in CATEGORIES:
        async def handler(update, context, category=key):
            await ranking(update, context, category)
        application.add_handler(CommandHandler(key, handler))
    application.add_handler(CommandHandler("check", check))
    application.add_handler(CommandHandler("accounts", accounts))
    application.add_handler(CommandHandler("addaccount", lambda u,c: edit_account(u,c,False)))
    application.add_handler(CommandHandler("removeaccount", lambda u,c: edit_account(u,c,True)))
    application.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
