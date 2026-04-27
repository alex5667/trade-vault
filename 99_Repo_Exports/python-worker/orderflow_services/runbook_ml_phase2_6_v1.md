# ML Phase 2.6 — operator_rca routing post-apply verification + rollback

## Scope

Только `scanner_infra`. Без UI. Без влияния на hot path.

## Components

- `operator_rca_routing_post_apply_verifier_v2_6.py`
- `operator_rca_routing_rollback_executor_v2_6.py`

## Purpose

- Проверить, что смена default RCA route не ухудшила полезность / error-rate / parse-fail-rate.
- При bounded regressions запросить rollback default route.
- Вести rollback journal.

## Streams

- input: `stream:ml:operator_rca_routing_apply_results`
- output: `stream:ml:operator_rca_routing_verify_results`
- output: `stream:ml:operator_rca_routing_rollback_requests`
- output: `stream:ml:operator_rca_routing_rollback_results`
- output: `stream:ml:operator_rca_routing_rollback_journal`

## Safe startup

```bash
export ML_OPERATOR_RCA_ROUTING_ROLLBACK_MODE=DRY_RUN
```

## Smoke checks

```bash
redis-cli XREVRANGE stream:ml:operator_rca_routing_verify_results + - COUNT 5
redis-cli XREVRANGE stream:ml:operator_rca_routing_rollback_requests + - COUNT 5
redis-cli XREVRANGE stream:ml:operator_rca_routing_rollback_results + - COUNT 5
redis-cli XREVRANGE stream:ml:operator_rca_routing_rollback_journal + - COUNT 5
curl -s localhost:9878/metrics | grep '^ml_operator_rca_routing_verify_'
curl -s localhost:9879/metrics | grep '^ml_operator_rca_routing_rollback_'
```

## Rollback

- stop verifier service
- stop rollback executor service
- keep streams / audit / sql tables for forensics
