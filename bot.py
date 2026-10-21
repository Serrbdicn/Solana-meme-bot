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
          category TEXT, state TEXT, reason TEXT, updated_at REAL);
        CREATE TABLE IF NOT EXISTS news (
          url TEXT PRIMARY KEY, title TEXT, published REAL, fetched REAL);
        CREATE TABLE IF NOT EXISTS watchlist (handle TEXT PRIMARY KEY);
        """)


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
        last_news = 0
        while True:
            if time.time() - last_news > 300:
                headlines = await fetch_headlines(client)
                last_news = time.time()
            try:
                event = await asyncio.wait_for(queue.get(), timeout=POLL)
            except asyncio.TimeoutError:
                event = None
            if event:
                mint = event["mint"]
                with conn() as db:
                    db.execute("INSERT OR IGNORE INTO tokens(mint,name,symbol,first_seen,state) VALUES(?,?,?,?,?)",
                               (mint, str(event.get("name", ""))[:100], str(event.get("symbol", ""))[:30], time.time(), "NEW"))
                queue.task_done()
            with conn() as db:
                rows = db.execute("SELECT * FROM tokens WHERE state IN ('NEW','WAIT_CAP') AND first_seen>? ORDER BY first_seen DESC LIMIT 25",
                                  (time.time() - MAX_AGE,)).fetchall()
            for row in rows:
                mint = row["mint"]
                pair = await market(client, mint)
                # DexScreener's marketCap may be missing or may reflect a different methodology.
                cap = (pair or {}).get("marketCap")
                if not isinstance(cap, (int, float)) or cap < CAP:
                    with conn() as db:
                        db.execute("UPDATE tokens SET state='WAIT_CAP',cap=?,updated_at=? WHERE mint=?",
                                   (cap, time.time(), mint))
                    continue
                # Verify chain creation time via first launch signature if present; absent = hold.
                # We do not infer creation time from the bot's receipt timestamp.
                score, potential, category, state, reason = await inspect(client, mint, row, headlines)
                with conn() as db:
                    db.execute("UPDATE tokens SET cap=?,security=?,potential=?,category=?,state=?,reason=?,updated_at=? WHERE mint=?",
                               (cap, score, potential, category, state, reason, time.time(), mint))
                if state == "PASS":
                    await application.bot.send_message(CHAT_ID, f"Candidate {row['name']} ({row['symbol']})\nSecurity {score}/10 | Potential {potential}/10\n{mint}")
            with conn() as db:
                db.execute("UPDATE tokens SET state='EXPIRED' WHERE first_seen<? AND state IN ('NEW','WAIT_CAP')",
                           (time.time() - MAX_AGE,))


def authorized(update):
    return update.effective_chat is not None and update.effective_chat.id == CHAT_ID


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if authorized(update):
        await update.message.reply_text("Pump.fun research bot online. /status /top /news /viral /phrases /original /culture /remix /check <mint> /accounts /addaccount <handle> /removeaccount <handle>\n\nHOLD means critical security checks are unverified. No trading.")


async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not authorized(update): return
    with conn() as db:
        rows = db.execute("SELECT state,COUNT(*) n FROM tokens GROUP BY state").fetchall()
    await update.message.reply_text("States: " + (", ".join(f"{r['state']}={r['n']}" for r in rows) or "none") + "\nA$100 total risk budget. Research only.")


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
        await update.message.reply_text(f"{row['name']} (${row['symbol']})\nState: {row['state']}\nMarket cap: {row['cap']} USD (provider reported)\nSecurity: {row['security']}/10\nPotential: {row['potential']}/10\nCategory: {row['category']}\n{row['reason'] or 'Awaiting data'}\nMint: {mint}")


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
