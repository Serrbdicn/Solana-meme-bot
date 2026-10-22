"""Research-only Pump.fun Telegram screener. Unknown critical checks never pass."""
import asyncio
import contextlib
import datetime as dt
import json
import html
import logging
import math
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
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
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
RUGCHECK_CACHE_SECONDS = 300
_rug_cache = {}
# Paid PumpPortal trade streams are intentionally disabled. The AUD amount is
# a planning ceiling, NOT a provider-side payment limit.
PAID_STREAM_ENABLED = False
PAID_STREAM_BUDGET_AUD = 5.0
RSS = [u.strip() for u in os.getenv("RSS_URLS", "").split(",") if u.strip()]
DB = Path(os.getenv("BOT_DB_PATH", "data/bot.db"))
DB.parent.mkdir(parents=True, exist_ok=True)
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
          threshold_alerted_at REAL, creator TEXT,
          early_high INTEGER DEFAULT 0, early_dex_seen INTEGER DEFAULT 0, passed_at REAL,
          liquidity REAL, liquidity_observed_at REAL);
        CREATE TABLE IF NOT EXISTS news (
          url TEXT PRIMARY KEY, title TEXT, published REAL, fetched REAL);
        CREATE TABLE IF NOT EXISTS watchlist (handle TEXT PRIMARY KEY);
        """)
        columns = {r[1] for r in db.execute("PRAGMA table_info(tokens)")}
        for name, dtype in (("launch_cap_sol", "REAL"), ("cap_source", "TEXT"), ("cap_observed_at", "REAL"), ("threshold_alerted_at", "REAL"), ("creator", "TEXT"), ("early_high", "INTEGER DEFAULT 0"), ("early_dex_seen", "INTEGER DEFAULT 0"), ("passed_at", "REAL"), ("liquidity", "REAL"), ("liquidity_observed_at", "REAL")):
            if name not in columns:
                db.execute(f"ALTER TABLE tokens ADD COLUMN {name} {dtype}")


def creator_launch_count(creator, exclude_mint=None):
    """Count launches observed by THIS bot only; not on-chain creator history."""
    if not creator:
        return None
    with conn() as db:
        if exclude_mint:
            return db.execute("SELECT COUNT(*) FROM tokens WHERE creator=? AND mint!=?", (creator, exclude_mint)).fetchone()[0]
        return db.execute("SELECT COUNT(*) FROM tokens WHERE creator=?", (creator,)).fetchone()[0]


def creator_risk_note(creator, mint):
    count = creator_launch_count(creator, mint)
    if count is None:
        return "Creator wallet unknown; creator-history checks unavailable"
    if count >= 5:
        return f"HIGH RISK FLAG: creator has {count} other launches observed by this bot (not proof of fraud)"
    if count >= 2:
        return f"CAUTION: creator has {count} other launches observed by this bot"
    return f"Creator wallet observed; {count} other local launches (external history NOT checked)"


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
    """Return only a Solana pair whose BASE mint matches exactly."""
    try:
        r = await client.get(f"https://api.dexscreener.com/token-pairs/v1/solana/{mint}", timeout=10)
        r.raise_for_status()
        payload = r.json()
        if not isinstance(payload, list):
            return None
        pairs = [p for p in payload if isinstance(p, dict)
                 and p.get("chainId") == "solana"
                 and isinstance(p.get("baseToken"), dict)
                 and p["baseToken"].get("address") == mint]
        return max(pairs, key=lambda p: safe_number((p.get("liquidity") or {}).get("usd")) or 0) if pairs else None
    except (httpx.HTTPError, ValueError, TypeError):
        return None


def safe_number(value):
    """Reject bools, NaN, infinities, and negative provider numbers."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    value = float(value)
    return value if math.isfinite(value) and value >= 0 else None


def market_warnings(pair, cap):
    """Heuristic red flags, not proof of fraud or a complete security audit."""
    flags = []
    if cap is None:
        return flags
    if cap >= 1_000_000_000:
        flags.append("EXTREME valuation (>= US$1B); verify pricing and supply")
    elif cap >= 100_000_000:
        flags.append("Very high valuation (>= US$100M); investigate")
    liquidity = safe_number((pair.get("liquidity") or {}).get("usd")) if pair else None
    if liquidity is None:
        flags.append("DEX liquidity unknown")
    elif liquidity == 0:
        flags.append("Zero reported DEX liquidity")
    elif cap / liquidity >= 1000:
        flags.append(f"EXTREME cap/liquidity ratio ({cap / liquidity:,.0f}x)")
    elif cap / liquidity >= 100:
        flags.append(f"High cap/liquidity ratio ({cap / liquidity:,.0f}x)")
    return flags


def risk_label(flags):
    if any("EXTREME" in flag or "Zero reported" in flag for flag in flags):
        return "HIGH RISK FLAG"
    if flags:
        return "CAUTION"
    return "NO VALUATION FLAGS (security unverified)"


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
    cap = safe_number(cap)
    if cap is not None and cap > 0:
        return cap
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


async def rugcheck_report(client, mint):
    """Read public Rugcheck summary and detailed report; missing data stays unknown."""
    cached = _rug_cache.get(mint)
    if cached and time.time() - cached[0] < RUGCHECK_CACHE_SECONDS:
        return cached[1]
    result = {"summary": None, "full": None, "errors": []}
    for kind, suffix in (("summary", "/report/summary"), ("full", "/report")):
        try:
            response = await client.get(
                f"https://api.rugcheck.xyz/v1/tokens/{mint}{suffix}",
                headers={"Accept": "application/json"}, timeout=9)
            response.raise_for_status()
            payload = response.json()
            if isinstance(payload, dict) and not payload.get("error"):
                result[kind] = payload
            else:
                result["errors"].append(f"{kind}: unexpected response")
        except (httpx.HTTPError, ValueError, TypeError) as exc:
            # Avoid including any request headers or secrets in user-facing output.
            result["errors"].append(f"{kind}: {type(exc).__name__}")
    _rug_cache[mint] = (time.time(), result)
    if len(_rug_cache) > 500:
        _rug_cache.pop(next(iter(_rug_cache)))
    return result


def rugcheck_evidence(report):
    """Provider findings are evidence indicators, never independent verification."""
    summary = report.get("summary") or {}
    full = report.get("full") or {}
    data = summary if summary else full
    lines = []
    if not data:
        lines.append("⚪ Rugcheck: unavailable (public endpoint/rate limit/network); no inference")
        return lines, None
    lines.append("🟡 Rugcheck: provider report retrieved (not an independent audit)")
    lock = data.get("lpLockedPct")
    if lock is None:
        lock = full.get("lpLockedPct")
    if isinstance(lock, (float, int)) and not isinstance(lock, bool) and 0 <= lock <= 100:
        lines.append(f"🟡 Rugcheck-reported LP locked: {lock:g}% (pool coverage and withdrawal rights NOT independently verified)")
    else:
        lines.append("⚪ Rugcheck LP locked percentage: not reported / not applicable")
    risks = data.get("risks")
    if not isinstance(risks, list):
        risks = full.get("risks")
    if isinstance(risks, list):
        if risks:
            for risk in risks[:6]:
                if isinstance(risk, dict):
                    name = str(risk.get("name") or "Unspecified risk")[:65]
                    level = str(risk.get("level") or "unknown").lower()
                    marker = "🔴" if level in ("danger", "error", "critical") else "🟡"
                    lines.append(f"{marker} Rugcheck: {name} (severity: {level[:20]})")
        else:
            lines.append("⚪ Rugcheck: no listed risks in this report (not proof of safety)")
    holders = full.get("topHolders")
    if isinstance(holders, list) and holders:
        # Holder accounts are not necessarily independent beneficial owners.
        percentages = [h.get("pct") for h in holders[:10] if isinstance(h, dict)]
        percentages = [float(p) for p in percentages if isinstance(p, (int, float)) and not isinstance(p, bool) and 0 <= p <= 100]
        if percentages:
            lines.append(f"🟡 Rugcheck top {len(percentages)} reported token accounts: {sum(percentages):.1f}% (may include pools)")
        else:
            lines.append("⚪ Rugcheck holder entries returned, percentages unavailable")
    else:
        lines.append("⚪ Rugcheck detailed holder percentages unavailable")
    lines.append("⚪ Linked-wallet ownership and real sell execution NOT verified")
    return lines, lock


def liquidity_and_sellability_evidence(authority, pair):
    """Conservative, observable indicators; never assert a successful sell or LP lock."""
    findings = []
    critical_unknown = True  # Withdrawal control and executable sell are not verified.
    if pair is None:
        findings.append("⚪ Trading stage: no indexed DEX pair; bonding-curve vs migration NOT verified")
        findings.append("⚪ Liquidity lock: no pool identified; not verifiable")
    else:
        dex = str(pair.get("dexId") or "unknown")[:48]
        address = str(pair.get("pairAddress") or "unknown")[:60]
        findings.append(f"🟡 DEX pool observed: {dex} | pool: {address} (migration NOT proven)")
        findings.append("⚪ Liquidity lock/withdrawal rights: UNVERIFIED; pool-specific on-chain audit required")
        liquidity = safe_number((pair.get("liquidity") or {}).get("usd"))
        if liquidity is None:
            findings.append("⚪ Reported liquidity unavailable")
        elif liquidity == 0:
            findings.append("🔴 Provider reports zero liquidity; trading may be impossible")
        elif liquidity < 3000:
            findings.append(f"🟡 Thin reported liquidity: US${liquidity:,.0f}; price impact risk")
        else:
            findings.append(f"🟡 Provider reports US${liquidity:,.0f} liquidity; NOT evidence of lock")
    if authority is None:
        findings.append("⚪ Sell restrictions: mint account unavailable; cannot inspect authorities/extensions")
    else:
        if not authority["freeze_disabled"]:
            findings.append("🔴 Freeze authority ACTIVE: token accounts may be frozen")
        else:
            findings.append("🟢 Freeze authority disabled (does NOT prove sellability)")
        extensions = authority["extensions"]
        if extensions:
            names = [str(e.get("extension") or e.get("type") or "unknown")[:40] if isinstance(e, dict) else str(e)[:40] for e in extensions]
            findings.append("🟡 Token extensions present: " + ", ".join(names[:12]) + "; transfer rules require audit")
        else:
            findings.append("🟢 No extensions reported by parsed mint (NOT a successful sell test)")
    findings.append("⚪ Actual sell execution: NOT TESTED; no simulation or trade performed")
    return findings, critical_unknown


def assess_checks(authority, pair, concentration, rug, creator, mint):
    """Score only observed evidence. A missing check is not a positive result."""
    checks = []
    warnings = []
    def add(name, value, deduction=0, note=None):
        checks.append({"name": name, "value": value, "deduction": deduction})
        if note:
            warnings.append(note)

    if authority is not None:
        add("Mint authority", "disabled" if authority["mint_disabled"] else "ACTIVE", 0 if authority["mint_disabled"] else 4,
            None if authority["mint_disabled"] else "Mint authority active")
        add("Freeze authority", "disabled" if authority["freeze_disabled"] else "ACTIVE", 0 if authority["freeze_disabled"] else 4,
            None if authority["freeze_disabled"] else "Freeze authority active")
        if authority.get("extensions"):
            warnings.append("Token extensions require separate review")
    if concentration is not None:
        deduction = 3 if concentration >= .5 else 1.5 if concentration >= .3 else 0
        add("Top 5 accounts", f"{concentration:.0%} (pools included)", deduction,
            "High concentration in top 5 token accounts" if deduction else None)
    if pair is not None:
        liq = safe_number((pair.get("liquidity") or {}).get("usd"))
        if liq is not None:
            deduction = 3 if liq == 0 else 2 if liq < 3000 else 0
            add("DEX liquidity", money(liq), deduction, "Low or zero DEX liquidity" if deduction else None)
        cap = provider_cap(pair)
        if cap is not None and liq is not None and liq > 0:
            ratio = cap / liq
            deduction = 3 if ratio >= 1000 else 1 if ratio >= 100 else 0
            add("Cap / liquidity", f"{ratio:.0f}x", deduction,
                "High market cap relative to liquidity" if deduction else None)
    summary = rug.get("summary") or {}
    full = rug.get("full") or {}
    data = summary or full
    lock = data.get("lpLockedPct")
    if lock is None:
        lock = full.get("lpLockedPct")
    if isinstance(lock, (int, float)) and not isinstance(lock, bool) and math.isfinite(lock) and 0 <= lock <= 100:
        deduction = 3 if lock < 50 else 1 if lock < 90 else 0
        add("LP locked (Rugcheck)", f"{lock:g}% reported", deduction,
            "Low provider-reported LP lock" if deduction else None)
    risks = data.get("risks")
    if not isinstance(risks, list):
        risks = full.get("risks")
    if isinstance(risks, list):
        severe = [r for r in risks if isinstance(r, dict) and str(r.get("level", "")).lower() in ("danger", "error", "critical")]
        add("Rugcheck risk report", f"{len(risks)} listed ({len(severe)} severe)", 3 if severe else 0,
            "Rugcheck reported severe risk(s)" if severe else None)
    count = creator_launch_count(creator, mint)
    if count is not None:
        deduction = 3 if count >= 5 else 1 if count >= 2 else 0
        add("Local creator launches", str(count), deduction,
            "Creator has multiple launches observed locally" if deduction else None)
    score = max(0., 10. - sum(x["deduction"] for x in checks)) if checks else None
    # No successful sell test or independent pool-withdrawal verification exists.
    return {"score": score, "completed": len(checks), "total": 8,
            "checks": checks, "warnings": warnings,
            "unverified": ["Actual sell execution", "Pool withdrawal rights", "Linked-wallet ownership"],
            "label": "PROVISIONAL — NOT SAFETY VERIFIED"}


def encode_assessment(assessment):
    return "SAFETY_JSON:" + json.dumps(assessment, ensure_ascii=False, separators=(",", ":"))


def decode_assessment(reason):
    if not reason or not reason.startswith("SAFETY_JSON:"):
        return None
    try:
        return json.loads(reason[len("SAFETY_JSON:"):])
    except (ValueError, TypeError):
        return None


async def inspect(client, mint, record, headlines):
    authority, pair, concentration, rug = await asyncio.gather(
        check_authorities(client, mint), market(client, mint),
        largest_accounts(client, mint), rugcheck_report(client, mint))
    assessment = assess_checks(authority, pair, concentration, rug,
                               record["creator"] if "creator" in record.keys() else None, mint)
    category, potential, why = category_and_potential(record["name"], record["symbol"], headlines)
    assessment["narrative"] = why
    # PRELIMINARY is a research ranking, never a verified PASS.
    state = "PRELIMINARY" if assessment["score"] is not None and assessment["score"] >= MIN_SCORE else "REJECT" if assessment["score"] is not None else "HOLD"
    return assessment["score"], potential, category, state, encode_assessment(assessment)


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
                        "INSERT OR IGNORE INTO tokens(mint,name,symbol,first_seen,state,launch_cap_sol,creator) VALUES(?,?,?,?,?,?,?)",
                        (mint, str(event.get("name", ""))[:100],
                         str(event.get("symbol", ""))[:30], observed, "NEW", cap_sol,
                         event.get("traderPublicKey") if isinstance(event.get("traderPublicKey"), str)
                         and re.fullmatch(r"[1-9A-HJ-NP-Za-km-z]{32,44}", event["traderPublicKey"]) else None))
                    if cap_sol is not None:
                        db.execute("UPDATE tokens SET launch_cap_sol=COALESCE(launch_cap_sol, ?) WHERE mint=?",
                                   (cap_sol, mint))
                    creator = event.get("traderPublicKey")
                    if isinstance(creator, str) and re.fullmatch(r"[1-9A-HJ-NP-Za-km-z]{32,44}", creator):
                        db.execute("UPDATE tokens SET creator=COALESCE(creator, ?) WHERE mint=?", (creator, mint))
                queue.task_done()
            # Avoid excessive provider polling: cap checks are spaced per token.
            with conn() as db:
                rows = db.execute(
                    "SELECT * FROM tokens WHERE state IN ('NEW','WAIT_CAP','HOLD','REJECT','EARLY_PENDING','EARLY_UNVERIFIED','EARLY_EXCLUDED') "
                    "AND first_seen>? AND (updated_at IS NULL OR updated_at<?) "
                    "ORDER BY COALESCE(updated_at,0) ASC LIMIT 12",
                    (time.time() - MAX_AGE, time.time() - MARKET_REFRESH_SECONDS)).fetchall()
            # Poll DEX markets concurrently: sequential 10-second timeouts could
            # consume the entire first-minute window for later launches.
            semaphore = asyncio.Semaphore(8)
            async def fetch_pair(row):
                async with semaphore:
                    return await market(client, row["mint"])
            pairs = await asyncio.gather(*(fetch_pair(row) for row in rows))
            for row, pair in zip(rows, pairs):
                mint = row["mint"]
                dex_cap = provider_cap(pair)
                dex_liquidity = safe_number((pair.get("liquidity") or {}).get("usd")) if pair else None
                cap, source = None, None
                # Prefer a recently fetched DEX provider observation over the launch snapshot.
                if dex_cap is not None:
                    cap, source = dex_cap, "DEX provider (recent poll)"
                elif row["cap"] is None and row["launch_cap_sol"] is not None and sol_price is not None and time.time() - sol_price_at < 300:
                    cap = float(row["launch_cap_sol"]) * sol_price
                    source = "PumpPortal LAUNCH snapshot x SOL/USD (NOT live)"
                # No first-minute cap gate: provider observations determine alerts.
                # Previous EARLY_* rows are eligible for reprocessing.
                with conn() as db:
                    db.execute(
                        "UPDATE tokens SET cap=COALESCE(?,cap),"
                        "cap_source=CASE WHEN ? IS NOT NULL THEN ? ELSE cap_source END,"
                        "cap_observed_at=CASE WHEN ? IS NOT NULL THEN ? ELSE cap_observed_at END,"
                        "liquidity=COALESCE(?,liquidity),"
                        "liquidity_observed_at=CASE WHEN ? IS NOT NULL THEN ? ELSE liquidity_observed_at END,"
                        "updated_at=?,state='WAIT_CAP',early_high=0,early_dex_seen=0 WHERE mint=?",
                        (cap, source, source, cap, time.time() if cap is not None else None,
                         dex_liquidity, dex_liquidity, time.time() if dex_liquidity is not None else None,
                         time.time(), mint))
                valuation_flags = market_warnings(pair, dex_cap) if dex_cap is not None else []
                creator_note = creator_risk_note(row["creator"], mint)
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
                             f"🪙 <b>{esc(row['name'])} ({esc(row['symbol'])})</b>\n"
                             f"💰 Cap: {money(cap)}\n"
                             f"🛡️ Screening pending — not safety verified\n"
                             f"📋 <code>{esc(mint)}</code>",
                             parse_mode="HTML",
                            reply_markup=mint_keyboard(mint))
                # Only a current DEX observation can trigger further screening here.
                # Launch snapshots are displayed for research but never treated as
                # proof that a token has NOW crossed US$10,000.
                if source != "DEX provider (recent poll)" or cap < CAP:
                    continue
                score, potential, category, state, reason = await inspect(client, mint, row, headlines)
                with conn() as db:
                    db.execute(
                        "UPDATE tokens SET security=?,potential=?,category=?,state=?,reason=?,updated_at=?,passed_at=CASE WHEN ?='PASS' THEN COALESCE(passed_at, ?) ELSE passed_at END WHERE mint=?",
                        (score, potential, category, state, reason, time.time(), state, time.time(), mint))
                assessment = decode_assessment(reason) or {}
                result_label = {
                    "PRELIMINARY": "Provisional research result — NOT safety verified",
                    "REJECT": "Below the configured provisional score threshold",
                    "HOLD": "Insufficient verified data to score",
                }.get(state, state)
                score_label = f"{score:g}/10" if score is not None else "Not scored"
                warnings = assessment.get("warnings", [])
                warning_text = "\n".join(f"• {esc(item)}" for item in warnings[:3])
                if not warning_text:
                    warning_text = "No flags recorded; unresolved checks may remain."
                await application.bot.send_message(
                    CHAT_ID,
                    f"🔎 <b>{esc(row['name'])} ({esc(row['symbol'])})</b>\n"
                    f"Result: {esc(result_label)}\n"
                    f"Provisional score: {esc(score_label)}\n"
                    f"Checks recorded: {len(assessment.get('checks', []))}/{assessment.get('total', 8)}\n"
                    f"{warning_text}\n"
                    f"📋 <code>{esc(mint)}</code>\n"
                    "Screening is incomplete by design; no buy/sell test was performed.",
                    parse_mode="HTML", reply_markup=mint_keyboard(mint))
            with conn() as db:
                db.execute("UPDATE tokens SET state='EXPIRED' WHERE first_seen<? AND state IN ('NEW','WAIT_CAP')",
                           (time.time() - MAX_AGE,))




def mint_keyboard(mint):
    """Telegram Bot API copy-text button (Bot API 7.11+).

    api_kwargs supports the wire field even with older python-telegram-bot
    versions, so requirements.txt need not change. Requires a Telegram client
    that supports copy-text buttons.
    """
    button = InlineKeyboardButton(
        "📋 Copy Mint",
        api_kwargs={"copy_text": {"text": mint}},
    )
    return InlineKeyboardMarkup([[button]])


def age_text(seen):
    if not seen:
        return "Unknown"
    seconds = max(0, int(time.time() - seen))
    if seconds < 3600:
        return f"{seconds // 60}m {seconds % 60}s"
    if seconds < 86400:
        return f"{seconds // 3600}h {(seconds % 3600) // 60}m"
    return f"{seconds // 86400}d {(seconds % 86400) // 3600}h"


def money(value):
    return f"US${value:,.0f}" if value is not None else "Unknown"


def metric_note(observed_at, source, live=False):
    if live:
        return "DEX lookup now"
    if source and "LAUNCH" in source:
        return "launch estimate; not live"
    if observed_at:
        return f"last seen {age_text(observed_at)} ago"
    return "not reported"


def esc(value):
    return html.escape(str(value or "Unknown"))


def risk_lines(flags):
    if not flags:
        return "⚪ No valuation flags identified; other security checks may remain unverified"
    return "\n".join(("🔴 " if "EXTREME" in f or "Zero reported" in f else "🟡 ") + esc(f) for f in flags)


def coin_card(row, pair=None, title=None, extra=None, detailed=False):
    """Compact Telegram card; detailed checks only on explicit request."""
    mint = row["mint"]
    assessment = decode_assessment(row["reason"])
    score = row["security"]
    coverage = f"{assessment['completed']}/{assessment['total']}" if assessment else "Not recorded"
    live_cap = provider_cap(pair) if pair else None
    cap = live_cap if live_cap is not None else row["cap"]
    live_liq = safe_number((pair.get("liquidity") or {}).get("usd")) if pair else None
    liq = live_liq if live_liq is not None else row["liquidity"]
    cap_note = metric_note(row["cap_observed_at"], row["cap_source"], live_cap is not None)
    liq_note = metric_note(row["liquidity_observed_at"], None, live_liq is not None)
    lock = next((c["value"] for c in assessment["checks"] if c["name"] == "LP locked (Rugcheck)"), None) if assessment else None
    holders = next((c["value"] for c in assessment["checks"] if c["name"] == "Top 5 accounts"), None) if assessment else None
    status = row["state"] or "NEW"
    heading = title or f"🪙 {row['name'] or 'Unnamed'} ({row['symbol'] or '?'})"
    lines = [f"<b>{esc(heading)}</b>",
             f"🛡️ <b>{esc(status)}</b> | Provisional: {esc(f'{score:g}/10' if score is not None else 'N/A')} | Checks: {esc(coverage)}",
             f"💰 Cap: {esc(money(cap))} · {esc(cap_note)}",
             f"💧 Liquidity: {esc(money(liq))} · {esc(liq_note)}"]
    if lock is not None:
        lines.append(f"🔒 LP locked: {esc(lock)}")
    if holders is not None:
        lines.append(f"👥 Top accounts: {esc(holders)}")
    if assessment:
        for warning in assessment["warnings"][:3]:
            lines.append(f"🔴 {esc(warning)}")
    lines.append("⚪ Not safety verified; missing checks are excluded from score")
    lines.append(f"📋 <code>{esc(mint)}</code>")
    if detailed:
        lines.append("\n<b>🔍 COMPLETED CHECKS</b>")
        if assessment:
            for c in assessment["checks"]:
                lines.append(f"• {esc(c['name'])}: {esc(c['value'])}" + (f" (-{c['deduction']:g})" if c["deduction"] else ""))
            lines.append("<b>NOT VERIFIED:</b> " + esc(", ".join(assessment["unverified"])))
            lines.append("Provider lock data is not independent proof of withdrawal controls.")
        else:
            lines.append("No structured screening recorded yet; older scores are not comparable.")
            lines.append(esc((row["reason"] or "Awaiting screening")[:1000]))
        lines.append(f"⏱️ Observed by bot: {age_text(row['first_seen'])} ago")
        lines.append(f"👤 {esc(creator_risk_note(row['creator'], mint))}")
    return "\n".join(lines)[:3900]


def authorized(update):
    return update.effective_chat is not None and update.effective_chat.id == CHAT_ID


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if authorized(update):
        await update.message.reply_text("Pump.fun research bot online. /new (early discoveries; launch cap estimates are NOT live or verified) /status /top /topday /news /viral /phrases /original /culture /remix /check <mint> /accounts /addaccount <handle> /removeaccount <handle>\n\nHOLD means critical security checks are unverified. No trading.")


async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not authorized(update): return
    with conn() as db:
        rows = db.execute("SELECT state,COUNT(*) n FROM tokens GROUP BY state").fetchall()
    await update.message.reply_text(
        "States: " + (", ".join(f"{r['state']}={r['n']}" for r in rows) or "none")
        + f"\nAlert threshold: {money(CAP)} current DEX market-cap observation; launch snapshots are estimates, not live."
        + "\nResearch only; this bot does not trade.")


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
            "This does not mean Pump.fun has no launches."
        )
        return
    await update.message.reply_text(
        "EARLY DISCOVERIES — UNVERIFIED\n"
        "Sorted by time first seen by this bot, NOT verified token age. "
        "No first-minute cap exclusion. First_seen is bot-observed, "
        "not verified on-chain launch time. A listing is NOT a buy signal. "
        "Tap Copy Mint to copy just the address."
    )
    for index, row in enumerate(rows, 1):
        await update.message.reply_text(
            coin_card(row, title=f"🪙 #{index} {row['name'] or 'Unnamed'} ({row['symbol'] or '?'})"),
            parse_mode="HTML", reply_markup=mint_keyboard(row['mint']))


async def ranking(update: Update, context: ContextTypes.DEFAULT_TYPE, category=None, today=False):
    if not authorized(update):
        return
    with conn() as db:
        if today:
            # Historical first PASS timestamp, retained even if the state later changes.
            rows = db.execute(
                "SELECT * FROM tokens WHERE state='PRELIMINARY' AND updated_at>=? ORDER BY updated_at DESC LIMIT 100",
                (time.time() - 86400,)).fetchall()
        elif category:
            rows = db.execute("SELECT * FROM tokens WHERE state='PRELIMINARY' AND category=? ORDER BY potential DESC,security DESC LIMIT 10", (category,)).fetchall()
        else:
            rows = db.execute("SELECT * FROM tokens WHERE state='PRELIMINARY' ORDER BY potential DESC,security DESC LIMIT 10").fetchall()
    if not rows:
        await update.message.reply_text(
            "No provisional qualifying coins screened in the last 24 hours." if today else
            "No provisional qualifying coins yet. Try /new or /check <mint>.")
        return
    await update.message.reply_text(
        "📅 TOPDAY — provisional scores from the last 24h; NOT verified safe." if today else
        "🏆 TOP — provisional research ranking; NOT verified safe.")
    for row in rows:
        await update.message.reply_text(
            coin_card(row), parse_mode="HTML", reply_markup=mint_keyboard(row['mint']))


async def topday(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await ranking(update, context, today=True)


async def check(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not authorized(update):
        return
    if not context.args:
        await update.message.reply_text("Usage: /check <Solana mint> [details]")
        return
    mint = context.args[0]
    if not re.fullmatch(r"[1-9A-HJ-NP-Za-km-z]{32,44}", mint):
        await update.message.reply_text("Invalid-looking Solana mint address")
        return
    with conn() as db:
        row = db.execute("SELECT * FROM tokens WHERE mint=?", (mint,)).fetchone()
    async with httpx.AsyncClient() as client:
        pair = await market(client, mint)
        if row is None:
            base = (pair or {}).get("baseToken") or {}
            record = {"name": base.get("name") or "Unknown", "symbol": base.get("symbol") or "?", "creator": None}
        else:
            record = row
        score, potential, category, state, reason = await inspect(client, mint, record, [])
    if row is None:
        with conn() as db:
            db.execute("INSERT OR IGNORE INTO tokens(mint,name,symbol,first_seen,state,creator) VALUES(?,?,?,?,?,?)",
                       (mint, record["name"], record["symbol"], time.time(), "NEW", None))
    with conn() as db:
        db.execute("UPDATE tokens SET security=?,potential=?,category=?,state=?,reason=?,updated_at=? WHERE mint=?",
                   (score, potential, category, state, reason, time.time(), mint))
        row = db.execute("SELECT * FROM tokens WHERE mint=?", (mint,)).fetchone()
    await update.message.reply_text(coin_card(row, pair, detailed=len(context.args)>1 and context.args[1].lower() in ("details", "full")),
                                    parse_mode="HTML", reply_markup=mint_keyboard(mint))


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
    application.add_handler(CommandHandler("topday", topday))
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
