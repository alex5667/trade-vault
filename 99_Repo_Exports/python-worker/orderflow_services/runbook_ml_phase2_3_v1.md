# ML Phase 2.3 — Operator RCA Routing Controller

## Goal

Introduce a bounded routing layer for `operator_rca` that reads governor decisions and chooses
`provider / model_name / prompt_version / policy_version` in a centralized way.

## Guarantees

- `scanner_infra` only
- advisory-safe by default
- `DRY_RUN` by default
- no hot-path changes
- no direct config mutation outside RCA routing state

## Streams

- Input:
  - `stream:ml:operator_rca_governor_decisions`
  - `stream:ml:operator_rca_requests`
- Output:
  - `stream:ml:operator_rca_routing_decisions`
  - `stream:ml:operator_rca_routing_audit`
  - `stream:ml:operator_rca_requests_routed`

## Redis hashes

- `metrics:ml:operator_rca_routing:last`
- `cfg:ml:operator_rca:routing:active`

## Recommended startup

```bash
export ML_OPERATOR_RCA_ROUTING_MODE=DRY_RUN
export ML_OPERATOR_RCA_DEFAULT_PROVIDER=vertex
export ML_OPERATOR_RCA_DEFAULT_MODEL=gemini-2.5-flash-lite
export ML_OPERATOR_RCA_DEFAULT_PROMPT_VERSION=ml_triage_v1
export ML_OPERATOR_RCA_DEFAULT_POLICY_VERSION=policy_v1
```

## Smoke checks

```bash
redis-cli XREVRANGE stream:ml:operator_rca_routing_decisions + - COUNT 5
redis-cli XREVRANGE stream:ml:operator_rca_routing_audit + - COUNT 5
redis-cli XREVRANGE stream:ml:operator_rca_requests_routed + - COUNT 5
redis-cli HGETALL metrics:ml:operator_rca_routing:last
curl -s localhost:9874/metrics | grep '^ml_operator_rca_routing_'
```

## Rollback

Stop `scanner-ml-operator-rca-routing-v2-3`. Existing RCA flow can continue by consuming the
original `stream:ml:operator_rca_requests` directly.
