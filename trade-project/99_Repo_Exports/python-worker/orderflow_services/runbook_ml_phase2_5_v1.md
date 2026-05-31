# Phase 2.5 — operator_rca winner-aware routing apply controller

## Goal

Take advisory experiment winner decisions from Phase 2.4 and translate them into bounded default RCA routing policy updates.

Scope remains strictly inside `scanner_infra`.

## Streams

- source: `stream:ml:operator_rca_experiment_winner_decisions`
- results: `stream:ml:operator_rca_routing_apply_results`
- audit: `stream:ml:operator_rca_routing_apply_audit`

## Redis policy state

- default routing policy hash: `cfg:ml:operator_rca_routing:default`
- controller state hash: `metrics:ml:operator_rca_routing_apply:last`

## Safety model

Default mode is `DRY_RUN`.

Promotion to `COMMIT` is allowed only when:

- kill-switch is off
- cooldown is not active
- sample floor is reached
- winner uplift is above threshold
- confidence is above threshold
- provider/model/prompt are in allowlists

## Recommended start

```bash
export ML_OPERATOR_RCA_ROUTING_APPLY_ADVISORY_ONLY=1
export ML_OPERATOR_RCA_ROUTING_APPLY_MIN_SAMPLE=12
export ML_OPERATOR_RCA_ROUTING_APPLY_MIN_UPLIFT=0.05
export ML_OPERATOR_RCA_ROUTING_APPLY_MIN_CONFIDENCE=0.60
export ML_OPERATOR_RCA_ROUTING_APPLY_COOLDOWN_SEC=21600
```

## Smoke

```bash
redis-cli XREVRANGE stream:ml:operator_rca_experiment_winner_decisions + - COUNT 5
redis-cli XREVRANGE stream:ml:operator_rca_routing_apply_results + - COUNT 5
redis-cli XREVRANGE stream:ml:operator_rca_routing_apply_audit + - COUNT 10
redis-cli HGETALL cfg:ml:operator_rca_routing:default
curl -s localhost:9877/metrics | grep '^ml_operator_rca_winner_routing_apply_'
```

## Rollback

1. Set `ML_OPERATOR_RCA_ROUTING_APPLY_ADVISORY_ONLY=1`
2. Optionally set `kill_switch=1` in `cfg:ml:operator_rca_routing:default`
3. Revert default policy hash to previous values using audit stream or external config management
