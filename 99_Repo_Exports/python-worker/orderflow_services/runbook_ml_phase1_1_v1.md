# ML Phase 1.1 — Vertex hardening / compact packs / prompt versioning

## Scope

- scanner_infra only
- advisory-only triage
- no auto-apply
- no hot-path changes

## What changes

1. `ml_context_pack_compactor_v1`
   - reads `stream:ml:analysis_requests`
   - emits compact deterministic packs to `stream:ml:analysis_requests_compact`
2. `vertex_genai_provider_v1_1`
   - retry/backoff for 429/timeout/503
   - Redis-backed daily budget + hourly call guard
3. `ml_vertex_triage_orchestrator_v1_1`
   - writes prompt/policy version and compact hash to DB/streams

## Rollout

1. Apply SQL:

```sql
\i orderflow_services/sql/ml_phase1_1_v1.sql
```

2. Add compose fragment.
3. Ensure `google-genai` is present in the image.
4. Set env:

```bash
VERTEX_PROJECT_ID=...
VERTEX_LOCATION=global
VERTEX_TRIAGE_MODEL=gemini-2.5-flash-lite
VERTEX_MAX_DAILY_USD=25
VERTEX_MAX_CALLS_PER_HOUR=300
ML_TRIAGE_PROMPT_VERSION=ml_triage_v1
ML_TRIAGE_POLICY_VERSION=policy_v1
```

## Smoke checks

```bash
redis-cli XREVRANGE stream:ml:analysis_requests_compact + - COUNT 5
redis-cli XREVRANGE stream:ml:analysis_results + - COUNT 5
redis-cli XREVRANGE stream:ml:recommendation_proposals + - COUNT 5
redis-cli XREVRANGE stream:ml:analysis_dlq + - COUNT 5
curl -s localhost:9849/metrics | grep '^ml_context_pack_'
curl -s localhost:9850/metrics | grep '^ml_vertex_'
```

## Rollback

- stop `scanner-ml-context-pack-compactor-v1`
- stop `scanner-ml-vertex-triage-v1-1`
- keep tables/streams intact

