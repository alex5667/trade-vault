# ML Phase 1.7 — Rollback Verification Loop

## Scope
- scanner_infra only
- no UI / Nest / Next
- hot path untouched

## Components
- `rollback_executor_verifier_v1.py`
- `ml_rollback_state_machine_worker_v1.py`

## Streams
- input:
  - `stream:ml:recommendation_rollback_requests`
  - `stream:ml:recommendation_rollback_results`
  - `stream:ml:recommendation_rollback_verification_results`
- output:
  - `stream:ml:recommendation_rollback_state`
  - `stream:ml:recommendation_audit`

## State machine
- `REQUESTED`
- `EXECUTED`
- `VERIFY_PENDING`
- `ROLLBACK_SUCCESS`
- `ROLLBACK_FAILED`
- `MANUAL_REVIEW`

Terminal:
- `ROLLBACK_SUCCESS`
- `ROLLBACK_FAILED`
- `MANUAL_REVIEW`

## Smoke checks
```bash
redis-cli XREVRANGE stream:ml:recommendation_rollback_results + - COUNT 5
redis-cli XREVRANGE stream:ml:recommendation_rollback_verification_results + - COUNT 5
redis-cli XREVRANGE stream:ml:recommendation_rollback_state + - COUNT 10
redis-cli XREVRANGE stream:ml:recommendation_audit + - COUNT 20
curl -s localhost:9862/metrics | grep '^ml_rollback_'
curl -s localhost:9863/metrics | grep '^ml_rollback_'
```

## Rollback
- stop `scanner-ml-rollback-verifier-v1`
- stop `scanner-ml-rollback-state-machine-v1`
- leave SQL tables and streams in place
