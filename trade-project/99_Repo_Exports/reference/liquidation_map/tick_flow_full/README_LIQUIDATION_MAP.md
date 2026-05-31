# Liquidation Map (Heatmap) — Python service

## What it does

Consumes normalized liquidation events from Redis Streams (`stream:liq_evt`) and maintains rolling *price-bucket* aggregates for multiple time windows (default: `1h / 4h / 24h`).

Publishes snapshots to Redis keys for low-latency UI/API usage:

- `liqmap:snapshot:<SYMBOL>:<WINDOW>` → compact JSON (TTL)

Optionally can also push snapshots to streams (for backend WS fanout):

- `stream:liqmap_snapshot:<SYMBOL>:<WINDOW>` (`XADD`)

## Input contract (Redis Stream DTO)

Required fields (strings where exchanges send strings):

- `ts_event_ms` (int64 ms)
- `ts_ingest_ms` (int64 ms)
- `venue` (string enum): `binance_usdtm` | `bybit_linear`
- `symbol` (UPPER)
- `order_side` (BUY|SELL)
- `liq_side` (long|short)
- `price` (string)
- `qty` (string)
- `notional_usd` (string)

Optional:

- `status` (Binance `X`)
- `raw` (sampled)

Bad/poison messages are sent to DLQ:

- `dlq:<stream:liq_evt>` (same fields + `reason`)

## ENV

### Streams

- `LIQ_EVT_STREAM=stream:liq_evt`
- `LIQMAP_GROUP=liqmap_group`
- `LIQMAP_CONSUMER=<auto>`

### Time / DQ

- `LIQMAP_MAX_FUTURE_MS=30000`
- `LIQMAP_MAX_EVENT_AGE_MS=93600000` (26h)

### Windows / publish

- `LIQMAP_WINDOWS=1h,4h,24h`
- `LIQMAP_PUBLISH_INTERVAL_MS=1000`

### Buckets

- `LIQMAP_BUCKET_MODE=log_bps|log_pct|abs`
- `LIQMAP_BUCKET_BPS=50`
- `LIQMAP_BUCKET_PCT=0.1`
- `LIQMAP_BUCKET_ABS=10` (only for `abs`)

### Snapshot output

- `LIQMAP_SNAPSHOT_KEY_PREFIX=liqmap:snapshot`
- `LIQMAP_SNAPSHOT_TTL_SEC=30`
- `LIQMAP_MAX_LEVELS=250`
- `LIQMAP_RANGE_PCT=5`

### Optional snapshot stream

- `LIQMAP_PUBLISH_STREAM_ENABLED=0|1`
- `LIQMAP_SNAPSHOT_STREAM_PREFIX=stream:liqmap_snapshot`
- `LIQMAP_SNAPSHOT_STREAM_MAXLEN=20000`

### Metrics

- `LIQMAP_METRICS_PORT=9112`

## Metrics (Prometheus)

- `liqmap_evt_read_total`
- `liqmap_evt_ok_total{symbol}`
- `liqmap_evt_drop_total{reason}`
- `liqmap_evt_dlq_total{reason}`
- `liqmap_evt_lag_ms` (histogram)

- `liqmap_levels{symbol,window}`
- `liqmap_snapshot_bytes{symbol,window}`
- `liqmap_snapshot_total{symbol,window}`

- `liqmap_last_publish_ts_ms`
- `liqmap_last_event_ts_ms`

## Debugging / runbook

- **No data in UI**: check TTL keys `liqmap:snapshot:*` and `liqmap_last_publish_ts_ms`.
- **DLQ growth**: inspect `XINFO STREAM dlq:stream:liq_evt` and sample `XRANGE` to see `reason`.
- **High lag**: check `liqmap_evt_lag_ms` + Redis backlog (`XPENDING stream:liq_evt liqmap_group`).

## Notes

- Сервис purposely ACK'ает сообщения даже при ошибке (после DLQ), чтобы не стопорить consumer group.
- Суммирование notional делается через `Decimal` (детерминизм, без float drift).
