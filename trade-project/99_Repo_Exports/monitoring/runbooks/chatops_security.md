# Runbook: ChatOps security (Telegram)

## Owner
- team: **trade**
- component: **monitoring**

## Alerts
- `ChatOpsUnauthorizedSpike`
- `ChatOpsRateLimitedSpike`

## Unauthorized spike
1) Confirm allowed chat id:
   - `GET cfg:chatops:allowed_chat_id`
2) Confirm admins allowlist:
   - `SMEMBERS cfg:chatops:admins`
3) Review audit:
   - `XREAD COUNT 50 STREAMS ops:eventlog 0-0`
4) If needed: rotate group membership / kick suspicious users.

## Rate-limited spike
1) Check config:
   - `GET cfg:chatops:rate_limit_per_min`
2) If too low for operations, increase, e.g.:
   - `SET cfg:chatops:rate_limit_per_min 20`
3) Investigate abuse:
   - check `ops:eventlog` for repeated user id.

## Notes
- The bot applies per-user per-minute rate limit in allowed chat.
- Clear requires 2-person approval and the first /freeze clear must include a reason.

## Links
- Dashboard: `/d/chatops_security/chatops-security?orgId=1`
- ChatOps runbook: `/chatops_freeze.md`
- Promote freeze runbook: `/promote_freeze.md`
