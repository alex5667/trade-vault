# ML Control Plane Phase 0.1 runbook

## Scope
Scanner-infra only. No NestJS/Next.js/UI wiring.

## Services
- `scanner-ml-training-runs-writer-v1`
- `scanner-ml-health-enricher-v1`

## Purpose
- unify existing training summaries into `ml_training_runs`
- enrich `ml_model_runtime_1m` with drift/calibration context already produced by scanner_infra jobs

## Checks
```bash
redis-cli XREVRANGE stream:ml:training_runs + - COUNT 5
redis-cli HGETALL metrics:ml:training_runs:last
redis-cli HGETALL metrics:ml:health_enriched:last
curl -s localhost:9854/metrics | grep '^ml_training_runs_'
curl -s localhost:9855/metrics | grep '^ml_health_enricher_'
```

## Rollback
Stop both services. Keep tables and streams intact.
