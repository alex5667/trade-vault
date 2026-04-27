# ML Control Plane Phase 0.2 — Model Snapshot Compactor

## Scope
Phase 0.2 stays entirely inside `scanner_infra` and builds a compact, per-model snapshot layer for future analysis.

## Inputs
- `ml_model_registry`
- `ml_model_runtime_1m`

## Outputs
- Redis stream: `stream:ml:model_snapshot`
- Redis hash per model: `metrics:ml:model_snapshot:<model_id>`
- Redis summary hash: `metrics:ml:model_snapshot:last`
- Prometheus exporter on `:9846`

## Redis check
```bash
redis-cli HGETALL metrics:ml:model_snapshot:last
redis-cli XREVRANGE stream:ml:model_snapshot + - COUNT 5
redis-cli HGETALL metrics:ml:model_snapshot:<model_id>
```

## Prometheus check
```bash
curl -s localhost:9846/metrics | grep '^ml_model_snapshot_'
```

## Status model
The compactor emits one of:
- `ok`
- `warning`
- `critical`

Reason codes:
- `ARTIFACT_MISSING`
- `NO_RUNTIME`
- `RUNTIME_STALE_WARN`
- `RUNTIME_STALE_CRIT`
- `ERROR_RATE_WARN`
- `ERROR_RATE_CRIT`
- `MISSING_CRITICAL_WARN`
- `MISSING_CRITICAL_CRIT`
- `LAT_P95_WARN`
- `LAT_P95_CRIT`

## Rollback
- stop `scanner-ml-model-snapshot-compactor-v1`
- keep SQL views and Redis keys; they are control-plane only
