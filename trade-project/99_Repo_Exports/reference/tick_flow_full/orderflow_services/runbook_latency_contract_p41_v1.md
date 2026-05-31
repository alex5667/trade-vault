# P4.1 Latency Contract External Coverage — Runbook

## Goal

Close latency-contract gaps for services upstream/downstream of the Python worker:

- **Go ingest** writer for `ingest_to_redis`
- **NestJS WS gateway** writer for `emit_to_ws` and canonical `end_to_end_event`

## Required owner-stage matrix

| Service | Stage | Writer |
|---|---|---|
| `go_ingest` | `ingest_to_redis` | Go worker (see `integrations/go_ingest_latency_writer_v1.go`) |
| `python_worker` | `redis_to_feature` | Python (existing) |
| `python_worker` | `feature_to_emit` | Python (existing) |
| `nest_gateway` | `emit_to_ws` | NestJS gateway (see `integrations/nest_ws_latency_writer_v1.ts`) |
| `nest_gateway` | `end_to_end_event` | NestJS gateway (canonical owner — only it has `ts_ws_emit_ms`) |

## Redis keys

```
metrics:latency_contract:last:<service>:<stage>:<symbol>
```

## SLO gate summary key

```
metrics:latency_contract:slo:last
```

Fields: `gate_ok`, `missing_total`, `stale_total`, `budget_breach_total`, `required_total`, `present_total`, `last_ts_ms`.

## Alert response

| Alert | Action |
|---|---|
| `OF_LatencyContract_MissingExternalStage_Crit` | Check if Go/NestJS writers are deployed and running. |
| `OF_LatencyContract_StaleExternalStage_Warn` | Check writer health; may be restarting or lag. |
| `OF_LatencyContract_BudgetBreach_Warn` | Review `LATENCY_BUDGET_*` env vars; check network latency. |
| `OF_LatencyContract_SLOGateOpen_Crit` | See gate summary key in Redis for per-stage detail. |

## Rollout

1. Deploy Python P4 + P4.1 SLO gate (`latency_contract_slo_gate_v1.py`) and exporter.
2. Integrate Go writer from `integrations/go_ingest_latency_writer_v1.go` into Go worker.
3. Integrate NestJS writer from `integrations/nest_ws_latency_writer_v1.ts` into WS gateway.
4. The SLO gate will immediately detect missing/stale coverage and fire alerts.

## Manual debug

```bash
# Check SLO gate summary
redis-cli hgetall metrics:latency_contract:slo:last

# Check a specific stage
redis-cli hgetall metrics:latency_contract:last:go_ingest:ingest_to_redis:BTCUSDT
redis-cli hgetall metrics:latency_contract:last:nest_gateway:end_to_end_event:BTCUSDT
```
