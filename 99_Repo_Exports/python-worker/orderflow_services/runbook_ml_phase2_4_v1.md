# Phase 2.4 — operator RCA experiment harness

## Scope

Только `scanner_infra`. Никакого влияния на hot path.

## Что добавляется

- A/B routing buckets для `operator_rca`
- exposure logging
- winner selection на базе `quality_score + usefulness_score`

## Streams

- input: `stream:ml:operator_rca_requests_routed`
- output: `stream:ml:operator_rca_requests_experimented`
- exposures: `stream:ml:operator_rca_exposures`
- winner decisions: `stream:ml:operator_rca_experiment_winner_decisions`
- audit: `stream:ml:operator_rca_experiment_audit`

## Safe start

1. Включить router и winner selector.
2. Держать только два arms: control/challenger.
3. Не менять provider defaults в routing controller до накопления sample.
4. Использовать winner decisions advisory-only.

## Example rollout

```bash
export ML_OPERATOR_RCA_EXPERIMENT_ENABLE=1
export ML_OPERATOR_RCA_EXPERIMENT_ID=operator_rca_ab_v1
export ML_OPERATOR_RCA_EXPERIMENT_MIN_SAMPLE=8
```

## Smoke checks

```bash
redis-cli XREVRANGE stream:ml:operator_rca_exposures + - COUNT 5
redis-cli XREVRANGE stream:ml:operator_rca_experiment_winner_decisions + - COUNT 5
curl -s localhost:9875/metrics | grep '^ml_operator_rca_experiment_'
curl -s localhost:9876/metrics | grep '^ml_operator_rca_experiment_'
```
