---
type: stream
stream: orders:queue:mt5
layer: execution
transport: redis-streams
producer:
  - signal-dispatch
consumer:
  - mt5-executor
schema_ver: v1
retention: short
idempotency: required
tags:
  - contracts
  - streams
  - execution
updated_at: 2026-04-18
---

# orders:queue:mt5

## Purpose
Очередь приказов в MT5 bridge для реального или paper-like исполнения через советник.

## Required fields
- `signal_id`
- `action`
- `symbol`
- `side`
- `sl_price`

## Recommended fields
- `entry_price`
- `tp1_price`
- `tp2_price`
- `risk_pct`
- `comment`

## Example
```json
{
  "signal_id": "4fac31a",
  "action": "OPEN",
  "symbol": "BTCUSD",
  "side": "BUY",
  "entry_price": 64500.5,
  "sl_price": 64000.0,
  "tp1_price": 65500.0,
  "risk_pct": 1.0
}
```

## Invariants
- idempotency key = `signal_id`
- no order without stop
- broker symbol mapping explicit
- execution reason-code must survive round-trip

## Reason codes
- `duplicate_signal`
- `invalid_symbol`
- `invalid_side`
- `missing_sl`
- `bridge_unavailable`
- `broker_requote`
- `broker_connection_error`

## Links
- [[mt5-executor]]
- [[signal-dispatch]]
