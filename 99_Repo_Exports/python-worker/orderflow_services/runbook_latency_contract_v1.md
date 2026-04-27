# Runbook: Latency Contract v1

## Overview

The latency contract establishes a unified timestamp chain from exchange event to WebSocket emission.

**Python-covered stages (live):**
| Stage | From → To | Budget |
|---|---|---|
| `redis_to_feature` | `ts_redis_read_ms` → `ts_feature_ms` | 50 ms |
| `feature_to_emit` | `ts_feature_ms` → `ts_emit_ms` | 100 ms |
| `end_to_end_event` | `ts_event_ms` → `ts_emit_ms` | 200 ms |

**Contract placeholders (not yet populated):**
| Stage | Owner | Field pair |
|---|---|---|
| `ingest_to_redis` | Go worker | `ts_ingest_source_ms` → `ts_redis_xadd_ms` |
| `emit_to_ws` | NestJS | `ts_emit_ms` → `ts_ws_emit_ms` |

## Alert: LatencyContractExporterScrapeStale

**Meaning:** The exporter cannot reach Redis or is stopped.
**Check:** `docker ps | grep latency_contract_exporter` and `curl :9830/metrics`
**Fix:** Restart the exporter container; check `REDIS_URL` ENV var.

## Alert: LatencyContractFeatureToEmitBudgetBreach / LatencyContractEndToEndEventBudgetBreach

**Meaning:** p95 latency exceeded the configured budget for 5+ minutes.
**Likely causes:**
- Python worker GIL contention (heavy NumPy or sync call in hot path)
- Redis write latency (check Redis slowlog)
- Burst buffer flush delay (check `CRYPTO_BURST_ENABLE`, `CRYPTO_OF_BURST_FLUSH_INTERVAL_MS`)

**Investigation:**
```bash
# Check recent Redis write latency
redis-cli slowlog get 20

# Check Python worker CPU / GIL
top -p $(pgrep -f crypto_orderflow_service)

# Check Prometheus histogram
histogram_quantile(0.95, rate(latency_contract_stage_ms_bucket{stage="feature_to_emit"}[5m]))
```

## Alert: LatencyContractStateHashStale

**Meaning:** No signal flow for this symbol for > 120s, or stamp_emit_and_observe_async is not being called.
**Check:** Confirm signals are flowing: `redis-cli xlen signals:crypto:raw`
**Normal if:** Symbol trading hours outside of market session, or intentional pause.

## Key Redis state schema

```
HGET metrics:latency_contract:last:python_worker:feature_to_emit:BTCUSDT
  last_duration_ms  -> ms since feature to emit
  last_ts_ms        -> wall clock of last update
  ts_event_ms       -> exchange event timestamp
  ts_redis_read_ms  -> Python worker read from stream
  ts_feature_ms     -> feature/gate computation done
  ts_emit_ms        -> published to outbox
```

## ENV vars for tuning

| Var | Default | Effect |
|---|---|---|
| `LATENCY_BUDGET_REDIS_TO_FEATURE_MS` | 50 | Budget for redis→feature stage |
| `LATENCY_BUDGET_FEATURE_TO_EMIT_MS` | 100 | Budget for feature→emit stage |
| `LATENCY_BUDGET_END_TO_END_EVENT_MS` | 200 | Budget for e2e stage |
| `LATENCY_CONTRACT_TTL_S` | 172800 | Redis hash TTL |
| `LATENCY_CONTRACT_STATE_MIN_UPDATE_MS` | 3000 | Rate-limit Redis writes |
| `LATENCY_CONTRACT_SYMBOL_ALLOWLIST` | BTCUSDT,ETHUSDT | Per-symbol metrics |
| `LATENCY_CONTRACT_EXPORTER_PORT` | 9830 | Exporter port |
