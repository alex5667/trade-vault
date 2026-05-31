---
type: dto
dto: Signal Payload
schema_ver: v1
tags:
  - contracts
  - dto
  - signals
updated_at: 2026-04-18
---

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
