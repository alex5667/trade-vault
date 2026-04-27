# ML Phase 1.6 — Post-Commit Verification + Automatic Rollback Triggers

## Scope
- scanner_infra only
- no UI
- no direct hot-path changes

## Components
- `post_commit_verifier_v1.py`
- `auto_rollback_trigger_engine_v1.py`

## Purpose
- verify bounded low-risk commits after a delay window
- generate rollback requests for hard regressions

## Required sources
- `stream:ml:recommendation_apply_results`
- `stream:ml:recommendation_audit`
- `ml_model_snapshots`
- `DATABASE_URL`

## Verification logic
- default verify delay: 300 sec
- hard rollback triggers:
  - `ERROR_RATE_SPIKE`
  - `LATENCY_P95_REGRESSION`
- soft review trigger:
  - `ALLOW_RATE_DROP`

## Rollout
1. Apply SQL patch.
2. Start verifier.
3. Start auto rollback trigger engine.
4. Observe `DRY_RUN` / `COMMIT` actions separately.

## Smoke
```bash
redis-cli XREVRANGE stream:ml:recommendation_apply_results + - COUNT 5
redis-cli XREVRANGE stream:ml:recommendation_audit + - COUNT 10
redis-cli XREVRANGE stream:ml:recommendation_rollback_requests + - COUNT 5
curl -s localhost:9860/metrics | grep '^ml_post_commit_'
curl -s localhost:9861/metrics | grep '^ml_auto_rollback_'
```

## Rollback
- stop verifier
- stop auto rollback trigger engine
- keep audit tables and streams
