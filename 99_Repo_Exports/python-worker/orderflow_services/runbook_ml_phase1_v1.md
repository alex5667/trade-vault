# ML Phase 1 Runbook (scanner_infra only)

## Scope
Phase 1 adds:
- `ml_analysis_input_pack_builder_v1`
- `ml_vertex_triage_orchestrator_v1`
- `vertex_genai_provider_v1`
- `llm_recommendation_guard_v1`

Advisory-only. No auto-apply.

## Preconditions
- Phase 0 / 0.1 / 0.2 deployed
- `metrics:ml:model_snapshot:*` populated
- `stream:ml:training_runs` populated
- `VERTEX_PROJECT_ID` configured
- `google-genai` package available in python worker image

## Deploy
1. Apply SQL:
   `\i orderflow_services/sql/ml_phase1_v1.sql`
2. Load compose fragment:
   `orderflow_services/docker_compose_fragment_ml_phase1_v1.yml`
3. Load alert rules:
   `orderflow_services/prometheus_alerts_ml_phase1_v1.yml`

## Smoke checks
```bash
redis-cli XREVRANGE stream:ml:analysis_requests + - COUNT 5
redis-cli XREVRANGE stream:ml:analysis_results + - COUNT 5
redis-cli XREVRANGE stream:ml:recommendation_proposals + - COUNT 5
redis-cli HGETALL metrics:ml:analysis_results:last
curl -s localhost:9847/metrics | grep '^ml_analysis_'
curl -s localhost:9848/metrics | grep '^ml_'
```

## Safety invariants
- advisory-only
- structured JSON only
- recommendation guard whitelist only
- no direct config apply
- no impact on hot path

## Rollback
- stop `scanner-ml-vertex-triage-v1`
- stop `scanner-ml-analysis-pack-builder-v1`
- leave streams/tables intact
