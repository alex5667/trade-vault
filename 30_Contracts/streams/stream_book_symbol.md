---
type: stream
stream: stream:book_<symbol>
layer: ingestion
transport: redis-streams
producer:
  - go-worker-ingestion
consumer:
  - python-crypto-orderflow-service
schema_ver: v1
retention: bounded
idempotency: last-write-wins snapshot semantics
tags:
  - contracts
  - streams
  - book
updated_at: 2026-04-18
---

# stream:book_<symbol>

## Purpose
Снимки книги ордеров L2/L2-lite для расчёта spread, depth, OBI и book health.

## Naming
- `stream:book_BTCUSDT`
- `stream:book_ETHUSDT`

## Required fields
- `symbol`
- `ts_ms`
- `bids`
- `asks`

## Shape
```json
{
  "symbol": "BTCUSDT",
  "ts_ms": 1700000000000,
  "bids": [{"price": 64000.0, "qty": 1.2}],
  "asks": [{"price": 64001.0, "qty": 0.9}]
}
```

## Invariants
- `ts_ms` = epoch ms
- arrays sorted by price
- best bid < best ask
- negative qty forbidden
- stale books veto downstream

## Derived metrics
- `spread_bps`
- `depth_bid_5`, `depth_ask_5`
- `obi`
- `book_rate_hz`
- `book_health_ok`

## Reason codes
- `book_parse_error`
- `book_stale`
- `book_crossed`
- `book_empty_side`
- `book_bad_price`

## Links
- [[go-worker-ingestion]]
- [[python-crypto-orderflow-service]]
- [[pre-publish-gates]]
