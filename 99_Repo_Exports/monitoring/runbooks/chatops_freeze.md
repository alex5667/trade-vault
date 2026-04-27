# Runbook: ChatOps Freeze Control (Telegram)

## Goal
Allow a trusted operator to set/clear/status promotion freeze directly from Telegram,
with audit trail to `ops:eventlog`.

## Security model (fail-closed)
- Only messages from **one** chat id are accepted: `TELEGRAM_ALLOWED_CHAT_ID`
- Only users in allowlist are accepted: `TELEGRAM_ADMIN_USER_IDS` (comma-separated Telegram user ids)
- If `TELEGRAM_ADMIN_USER_IDS` is empty → bot exits and no one can run commands.

## Setup
1) Create Telegram bot token.
2) Add bot to the target group (or use direct chat).
3) Disable privacy mode for group command handling if needed (BotFather → /setprivacy).
4) Set env vars:

```bash
export TELEGRAM_BOT_TOKEN="123456:ABC..."
export TELEGRAM_ALLOWED_CHAT_ID="-1001234567890"
export TELEGRAM_ADMIN_USER_IDS="11111111,22222222"
export REDIS_URL="redis://redis-worker-1:6379/0"
```

## Start
```bash
docker compose -f docker-compose-crypto-orderflow.yml up -d chatops-telegram-freeze-bot
docker logs -n 200 chatops-telegram-freeze-bot
```

## Commands
```text
/freeze status
/freeze set 3600 manual investigation
/freeze clear <reason...>
```

## Two-person rule for clear (recommended)
`/freeze clear` requires **2 different admin confirmations** within `cfg:chatops:two_person_window_s`.
Reason is required on the first clear to start the pending request.

## Rate limit
- per admin: `cfg:chatops:rate_limit_per_min` (default 10/min)

## Audit
Tail:
```bash
REDIS_URL="redis://redis-worker-1:6379/0" ./scripts/ops_eventlog_tail.sh
```

Events:
- `chatops_freeze_cmd`
- `chatops_unauthorized`
- `promote_freeze_set`
- `promote_freeze_clear`
