# Step 23 — Tick Gate Aggregator via Docker Compose + Prom scrape + self-diagnostics

## 1) Start service

From repo root:

```bash
docker compose -f docker-compose-crypto-orderflow.yml -f docker-compose-timers.yml up -d --build tick-gate-aggregator
```

Or just `make up` (aggregator is included in timers).

Verify endpoints:

```bash
curl -s localhost:9112/health
curl -s localhost:9112/metrics | egrep "tick_gate_(events_total|last_run_ts_seconds|stream_lag_ms|group_pending|consumer_idle_ms|health_ok)"
```

## 2) Prometheus scrape

Add:

`python-worker/infra/observability/prometheus/scrape_tick_gate_aggregator.yml`

under `scrape_configs:` in Prometheus.

## 3) Redis self-diagnostics

Aggregator exports:
- `tick_gate_group_pending` (PEL size)
- `tick_gate_consumer_idle_ms` (idle time for its consumer)
- `tick_gate_stream_lag_ms` (ms since last processed id timestamp)
- `tick_gate_health_ok` (1 if Redis reads succeed recently)

Manual inspection:

```bash
redis-cli XINFO GROUPS ops:tick_quality_gate
redis-cli XPENDING ops:tick_quality_gate tick_gate_agg
redis-cli XINFO CONSUMERS ops:tick_quality_gate tick_gate_agg
```

## 4) Troubleshooting

### No events
- Confirm gate wrapper publishes to Redis:
  - `TICK_GATE_PUBLISH_REDIS=1`
  - `TICK_GATE_REDIS_STREAM=ops:tick_quality_gate`
- Confirm stream has entries:
  - `redis-cli XINFO STREAM ops:tick_quality_gate`

### Lag growing
- `tick_gate_group_pending` rising: consumer can't keep up or is stuck.
- Check container logs and Redis connectivity.
