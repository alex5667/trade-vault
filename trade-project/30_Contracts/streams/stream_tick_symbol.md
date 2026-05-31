---
type: stream
stream: stream:tick_<symbol>
layer: ingestion
transport: redis-streams
producer:
  - go-worker-ingestion
consumer:
  - python-crypto-orderflow-service
schema_ver: v1
retention: bounded
idempotency: consumer-side dedupe required
tags:
  - contracts
  - streams
  - ticks
  - data-quality
updated_at: 2026-04-18
---

# stream:tick_<symbol>

## Purpose
Сырые сделки по конкретному инструменту из ingestion-слоя в Python orderflow pipeline.

## Naming
- `stream:tick_BTCUSDT`
- `stream:tick_ETHUSDT`

## Required fields
- `price`
- `qty`
- `side`
- `ts_ms`

## Optional fields
- `trade_id`
- `recv_ts_ms`
- `is_buyer_maker`
- `venue`

## Example
```json
{
  "price": 64000.5,
  "qty": 0.012,
  "side": "B",
  "ts_ms": 1700000000000
}
```

## Invariants
- `ts_ms` = epoch milliseconds
- `price > 0`
- `qty > 0`
- `side ∈ {B,S}`
- duplicates possible → dedupe in consumer
- stale/future/out-of-order ticks are possible → sanitize or quarantine

## Reason codes
- `tick_parse_error`
- `tick_stale`
- `tick_future`
- `tick_duplicate`
- `tick_unknown_side`
- `tick_gap_critical`

## Downstream notes
- [[python-crypto-orderflow-service]]
- [[Time Model]]
- [[Data Quality Model]]
