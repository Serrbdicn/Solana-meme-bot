# Solana Pump.fun Telegram Research Bot — Starter

This is a **working Telegram integration and a conservative research prototype**, not a finished security auditor or trading recommendation system. It listens to PumpPortal's new-token WebSocket, records launch sightings, checks DexScreener market-cap data, reads Solana token authorities and top token accounts, fetches RSS headlines, and exposes private Telegram commands.

## Critical limitations

- **No token is marked PASS in this starter** because liquidity-lock/burn, sellability, coordinated holders, manipulation patterns, and verified on-chain creation times are not yet fully implemented. Coins with these unknown critical checks remain `HOLD` and are excluded from the six public rankings. This is intentional; do not weaken the fail-closed rule to make rankings look populated.
- Launch age uses first observation for retention, not verified creation age. On-chain creation time validation is a future step.
- DexScreener `marketCap` may be missing or inconsistent with bonding-curve valuations; many pre-migration tokens will remain `WAIT_CAP`.
- RSS headline keyword matching is **not** verified virality or reliable AI classification. This prototype only categorizes news keyword matches and otherwise uses basic name heuristics. The six leaderboards are wired but the richer classifiers are future work.
- `/addaccount` stores an X watchlist only. It **does not monitor X**. Official X API access and a compliant ingestion service must be added.
- The feed and APIs may change or rate-limit requests. PumpPortal's WebSocket URL may require provider-specific access settings.
- Security scores for HOLD tokens are **incomplete diagnostic numbers**, not safety ratings. No score is proof of sellability.
- A$100 is a risk-budget reminder, not an order size or automatic trade instruction. No private keys or trading functions exist.

## Create Telegram bot

1. In Telegram, open official `@BotFather`, send `/newbot`, and securely save the token.
2. Message the new bot once (e.g. `/start`).
3. Visit `https://api.telegram.org/bot<YOUR_TOKEN>/getUpdates` in your own browser and find `message.chat.id`. Do not share the token or the full URL. If you have started another bot poller, stop it first.
4. Copy `.env.example` to `.env` and set `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`, and your reliable `SOLANA_RPC_URL`.

## Local test

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python bot.py
```

In your private Telegram chat: `/start`, `/status`, `/top`, `/news`, `/viral`, `/phrases`, `/original`, `/culture`, `/remix`, `/check <mint>`, `/accounts`, `/addaccount elonmusk`, `/removeaccount elonmusk`.

## VPS deployment

Install Docker and the Docker Compose plugin on an Ubuntu VPS, copy this project securely, create your `.env` (do not commit it), and run:

```bash
docker compose up -d --build
docker compose logs -f bot
```

`./data` persists SQLite across restarts. Restrict server access, protect `.env`, rotate compromised tokens, and monitor logs. Do not expose ports; this bot uses Telegram long polling.

## Recommended next engineering milestones

1. Verify launch transaction signature and block time, plus actual Pump.fun bonding-curve market cap (including token lifecycle).
2. Verify liquidity migration, LP ownership/burn/lock status where applicable, and meaningful sellability checks.
3. Identify actual beneficial holder clusters (not simply largest token accounts), creator-wallet history, and suspicious trading patterns.
4. Build event/virality ingestion from licensed X and other social APIs, validate source timestamps, and implement adaptive six-category AI scoring with evidence/confidence.
5. Add rate-limited parallel jobs, structured audit trails, test fixtures, data freshness limits, and notification rules once the critical security gate is genuinely verifiable.

All numeric thresholds are configurable starting assumptions, not validated predictors of future price movement.
