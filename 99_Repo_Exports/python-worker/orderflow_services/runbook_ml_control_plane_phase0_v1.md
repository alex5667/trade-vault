# Runbook — ML Control Plane Phase-0

## Scope

Phase-0 adds **inventory + runtime telemetry** only. No LLM providers, no recommendation apply path.

Components:
- `orderflow_services.ml_inventory_exporter_v1`
- `orderflow_services.ml_health_rollup_worker_v1`
- SQL schema: `orderflow_services/sql/ml_control_plane_phase0_v1.sql`
- Alerts: `orderflow_services/prometheus_alerts_ml_control_plane_phase0_v1.yml`

## What it collects

### Inventory
- ML Confirm champion/challenger/candidate configs from Redis
- Meta LR model paths from env
- ML Scorer V2/V3 model paths from env
- artifact path, schema version/hash, promotion state, champion flag, fail policy

### Runtime telemetry
- source: `metrics:ml_confirm`
- per-minute rollups:
  - latency p50/p95/p99 (fixed histogram quantiles)
  - allow/block/abstain/shadow/error rates
  - missing_critical_rate proxy from `missing_n`
- sink:
  - `stream:ml:health_snapshot`
  - `ml_model_runtime_1m`

## Deployment

1. Apply SQL:
```sql
\i orderflow_services/sql/ml_control_plane_phase0_v1.sql
```

2. Add compose fragment:
```bash
cat orderflow_services/docker_compose_fragment_ml_control_plane_phase0_v1.yml
```

3. Load Prometheus rule file:
```bash
cat orderflow_services/prometheus_alerts_ml_control_plane_phase0_v1.yml
```

## Verification

### Inventory stream
```bash
redis-cli XREVRANGE stream:ml:model_inventory + - COUNT 5
redis-cli HGETALL metrics:ml:model_inventory:last
curl -s localhost:9842/metrics | grep '^ml_inventory_'
```

### Health snapshot stream
```bash
redis-cli XREVRANGE stream:ml:health_snapshot + - COUNT 5
redis-cli HGETALL metrics:ml:health_snapshot:last
curl -s localhost:9843/metrics | grep '^ml_health_'
```

### DB rows
```sql
SELECT * FROM ml_model_registry ORDER BY created_at_ms DESC LIMIT 20;
SELECT * FROM ml_model_runtime_1m ORDER BY ts_ms DESC LIMIT 20;
```

## Rollback

Phase-0 is control-plane only. Safe rollback order:
1. stop `scanner-ml-health-rollup-v1`
2. stop `scanner-ml-inventory-exporter-v1`
3. keep tables/streams; do **not** drop data during initial rollback

## Notes

- `ece` / `brier` are placeholders in Phase-0 unless later enriched from dedicated calibration jobs.
- this worker is fail-open and never writes into hot-path keys.
- inventory/export telemetry should be deployed before any LLM analysis phase.
