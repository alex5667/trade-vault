# Phase 1.4 — Recommendation Executor Adapters

## Purpose
Limited executor layer for low-risk actions inside scanner_infra.

## Modes
- `DRY_RUN` (default)
- `COMMIT`

## Supported actions
- `propose_threshold_canary`
- `request_calibration_refresh`
- `freeze_candidate`
- `unfreeze_candidate`

## Safety
- review/apply bus still required
- threshold changes are bounded
- replay is mandatory for threshold canary
- rollback journal is written only on commit

## Smoke checks
```bash
redis-cli XREVRANGE stream:ml:recommendation_apply_results + - COUNT 5
redis-cli XREVRANGE stream:ml:recommendation_rollback_journal + - COUNT 5
curl -s localhost:9857/metrics | grep '^ml_recommendation_executor_'
```
