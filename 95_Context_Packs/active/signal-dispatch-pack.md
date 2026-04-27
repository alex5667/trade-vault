---
type: context_pack
tags: [context-pack, generated, llm]
topic: "Signal dispatch review"
source_notes:
  - 20_Services/signal-dispatch.md
  - 30_Contracts/dto/signal_payload.md
  - 30_Contracts/streams/notify_telegram.md
updated_at: auto
---

# Context Pack: Signal dispatch review

## Task
Подготовить compact context pack по signal dispatch, semantic dedup, publish flow и diagnostic trail.

## Summary
Auto-generated pack from selected notes. Review and tighten before sending to an external model.

## Relevant notes
- [[20_Services/signal-dispatch.md]]
- [[30_Contracts/dto/signal_payload.md]]
- [[30_Contracts/streams/notify_telegram.md]]

## Key excerpts

### 20_Services/signal-dispatch.md
```text
# signal-dispatch

## Purpose
Собрать финальный signal payload, защитить систему от дублей и корректно разложить сообщение по downstream consumers.

## Responsibilities
- produce stable `signal_id`
- semantic dedup
- publish to raw streams
- route to execution queue
- route to telegram notify stream
- route diagnostics separately
- do safe xadd with retry path

## Signal payload starter contract
Required fields:
- `signal_id`
- `symbol`
- `kind`
- `side`
- `entry_price`
- `sl_price`
- `tp1_price`
- `confidence`
- `ts_ms`
- `venue`
- `source`
- `meta{}`

## Dedup model
### Cooldown dedup
- by symbol / side / kind
- prevents repeated market idea spam

### Semantic dedup
- stable hash over key fields
- bucketed by time window
- used for replay-safe identity

## Streams
### Tradeable path
- `signals:crypto:raw`
- `orders:queue`
- `orders:queue:mt5`

### Notify path
- `notify:telegram`

### Diagnostic path
- `stream:signals:diagnostics`

## Failure modes
- duplicate orders from missed dedup
- Redis xadd failures
- diagnostics mixed into tradeable path
- telegram spam
- unstable signal_id across equivalent events
- missing payload fields in downstream executor

## Metrics
- published total by stream
- dedup hit total
- publish errors total
- retry queue depth
- notify sent / skipped total
- diagnostic publish total
- raw-to-execution ratio

## Alerts
- orders published without matching raw event
- sudden dedup collapse
- retry queue saturation
- notify stream failures
- missing mandatory payload fields

## Rollout / rollback
### Rollout
- verify signal_id stability on replay
- test semantic dedup under burst
- confirm diagnostics and tradeable streams separated

### Rollback
- reduce execution routing first
- keep raw stream publication for visibility
- disable notify if noisy, but preserve execution path if safe

## Linked notes
- [[pre-publish-gates]]
- [[mt5-executor]]
- [[System Map]]
```
### 30_Contracts/dto/signal_payload.md
```text
# Signal Payload

## Purpose
Единый DTO для публикации tradeable сигнала в dispatch/execution/notifications.

## Core fields
- `signal_id`
- `symbol`
- `kind`
- `side`
- `entry_price`
- `sl_price`
- `tp1_price`
- `confidence`
- `ts_ms`
- `venue`
- `source`

## Meta fields
- `regime`
- `dq_flags`
- `ml_confirm_p`
- `sl_mode`
- `sl_atr_mult`
- `reason_code`

## Invariants
- numeric prices > 0
- stop exists before publish to execution
- `ts_ms` = epoch ms
- `reason_code` persists through pipeline

## Linked streams
- [[signals_of_confirm]]
- [[orders:queue:mt5]]
- [[notify_telegram]]
```
### 30_Contracts/streams/notify_telegram.md
```text
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
```

## Ask for external model
Use only this context pack. Preserve contracts and invariants. Return:
- goal
- facts
- assumptions
- risks
- plan
- tests
- metrics/alerts
- rollout/rollback
