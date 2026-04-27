
# Phase 1.3 — Recommendation Review/Apply Bus (scanner_infra only)

## Scope
Internal `scanner_infra` recommendation review/apply bus:
- review request fan-out
- explicit approval/rejection stream
- replay-required gating
- audit trail
- apply gate in review-only / dry-run / executor-ready modes

## Streams
- `stream:ml:recommendation_proposals`
- `stream:ml:recommendation_review_requests`
- `stream:ml:recommendation_reviews`
- `stream:ml:recommendation_apply_requests`
- `stream:ml:recommendation_apply_results`
- `stream:ml:recommendation_audit`

## Minimal rollout
1. Apply SQL:
   `\i orderflow_services/sql/ml_phase1_3_v1.sql`
2. Load compose fragment:
   `orderflow_services/docker_compose_fragment_ml_phase1_3_v1.yml`
3. Load Prometheus rules:
   `orderflow_services/prometheus_alerts_ml_phase1_3_v1.yml`
4. Set environment:
   - `ML_RECOMMENDATION_MIN_APPROVALS=1`
   - `ML_RECOMMENDATION_APPLY_MODE=REVIEW_ONLY`
   - `ML_RECOMMENDATION_APPLY_DRY_RUN=1`

## Smoke checks
```bash
redis-cli XREVRANGE stream:ml:recommendation_review_requests + - COUNT 5
redis-cli XREVRANGE stream:ml:recommendation_apply_requests + - COUNT 5
redis-cli XREVRANGE stream:ml:recommendation_apply_results + - COUNT 5
redis-cli XREVRANGE stream:ml:recommendation_audit + - COUNT 10
curl -s localhost:9856/metrics | grep '^ml_recommendation_review_'
curl -s localhost:9855/metrics | grep '^ml_recommendation_apply_'
```

## Review event contract
Example payload into `stream:ml:recommendation_reviews`:
```json
{
  "recommendation_id": "abc123",
  "reviewer": "ops_user",
  "decision": "APPROVE",
  "replay_status": "PASS"
}
```

## Operating modes
- `REVIEW_ONLY`: apply gate can approve logically but will never forward for execution
- `DRY_RUN`: returns `DRY_RUN_ALLOWED`
- executor mode (`ML_RECOMMENDATION_APPLY_DRY_RUN=0`, `ML_RECOMMENDATION_APPLY_MODE!=REVIEW_ONLY`) returns `READY_FOR_EXECUTOR`

## Rollback
Stop:
- `scanner-ml-recommendation-review-bus-v1`
- `scanner-ml-recommendation-apply-gate-v1`

Keep tables and streams intact for audit continuity.
