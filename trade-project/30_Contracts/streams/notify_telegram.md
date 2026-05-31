---
type: stream
stream: notify:telegram
layer: notifications
transport: redis-streams
producer:
  - signal-dispatch
  - ops-services
consumer:
  - telegram-bot
schema_ver: v1
retention: short
idempotency: best effort
tags:
  - contracts
  - streams
  - notifications
updated_at: 2026-04-18
---

# notify:telegram

## Purpose
Оповещения для людей и approval/callback flow.

## Required fields
- `text`

## Optional fields
- `buttons`
- `signal_id`
- `chat_id`
- `severity`
- `event_type`

## Example
```json
{
  "text": "🚀 BREAKOUT BTCUSDT BUY",
  "buttons": [[{"text":"Approve","callback":"approve:123"}]]
}
```

## Invariants
- message compact
- buttons JSON-serializable
- no execution-only fields without user-readable text

## Reason codes
- `notify_format_error`
- `notify_rate_limited`
- `callback_invalid`
- `chat_not_found`

## Links
- [[signal-dispatch]]
