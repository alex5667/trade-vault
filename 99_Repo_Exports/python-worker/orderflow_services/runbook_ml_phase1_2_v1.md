# ML Phase 1.2 Runbook (scanner_infra only)

## Scope
Phase 1.2 adds four async/background capabilities inside `scanner_infra` only:

1. Fleet batch triage scheduler
2. Advisory context-cache registry/manager
3. Vertex batch review orchestrator with cost accounting
4. Recommendation feedback loop

No UI. No NestJS/Next.js. No auto-apply. No hot-path changes.

## Streams
- `stream:ml:analysis_batch_requests`
- `stream:ml:analysis_results`
- `stream:ml:recommendation_proposals`
- `stream:ml:analysis_dlq`
- `stream:ml:analysis_cost`
- `stream:ml:recommendation_feedback`

## Redis hashes
- `metrics:ml:context_cache:last`
- `metrics:ml:analysis_cost:last`
- `metrics:ml:recommendation_feedback:last`
- `metrics:ml:recommendation_feedback:summary:<action>`

## Rollout
1. Apply SQL patch `ml_phase1_2_v1.sql`
2. Add compose fragment `docker_compose_fragment_ml_phase1_2_v1.yml`
3. Load alert rules `prometheus_alerts_ml_phase1_2_v1.yml`
4. Verify ports: `9851`, `9852`, `9853`, `9854`
5. Smoke checks:

```bash
redis-cli XREVRANGE stream:ml:analysis_batch_requests + - COUNT 5
redis-cli HGETALL metrics:ml:context_cache:last
redis-cli HGETALL metrics:ml:analysis_cost:last
redis-cli HGETALL metrics:ml:recommendation_feedback:last
curl -s localhost:9851/metrics | grep '^ml_batch_review_'
curl -s localhost:9852/metrics | grep '^ml_context_cache_'
curl -s localhost:9853/metrics | grep '^ml_batch_analysis_'
curl -s localhost:9854/metrics | grep '^ml_recommendation_feedback_'
```

## Key ENV
```bash
VERTEX_PROJECT_ID=...
VERTEX_LOCATION=global
VERTEX_BATCH_TRIAGE_MODEL=gemini-2.5-flash-lite
VERTEX_MAX_DAILY_USD=25
VERTEX_MAX_CALLS_PER_HOUR=300
VERTEX_CONTEXT_CACHE_ENABLE=1
VERTEX_CONTEXT_CACHE_MODE=ADVISORY
VERTEX_CONTEXT_CACHE_MIN_HITS=3
VERTEX_CONTEXT_CACHE_MIN_BYTES=2048
ML_BATCH_REVIEW_EVERY_SEC=3600
ML_BATCH_REVIEW_MAX_ITEMS=10
```

## Notes
- Context-cache support in this phase is **advisory registry + provider hook**. It does not yet provision Vertex cache artifacts directly.
- Cost accounting is estimate-driven unless future provider metadata exposes exact billed usage.
- Feedback loop depends on some upstream actor publishing to `stream:ml:recommendation_feedback`.

## Rollback
Stop these services only:
- `scanner-ml-batch-review-scheduler-v1`
- `scanner-ml-context-cache-manager-v1`
- `scanner-ml-vertex-batch-review-v1`
- `scanner-ml-recommendation-feedback-v1`

Leave schema/tables/streams in place.
