# ML Phase 2.2 — RCA Usefulness Governor

## Goal

Use `quality_score + usefulness_score` to suppress or promote recurring RCA action patterns and provider/prompt versions.

## Scope

- scanner_infra only
- advisory-only by default
- no direct config mutation in hot path

## Inputs

- `stream:ml:operator_rca_feedback_summary`
- `llm_incident_rca_results`
- `llm_incident_rca_feedback`
- `llm_incident_rca_quality`

## Outputs

- `stream:ml:operator_rca_governor_decisions`
- `stream:ml:operator_rca_governor_audit`
- Redis policy hashes under `cfg:ml:operator_rca_governor:*`

## Decision types

- `SUPPRESS`
- `PROMOTE`
- `HOLD`

## Action pattern keys

`cfg:ml:operator_rca_governor:action:<action_type>:<prompt_version>:<policy_version>`

## Provider/prompt keys

`cfg:ml:operator_rca_governor:provider:<provider>:<model_name>:<prompt_version>`

## Safe starting config

```bash
export ML_OPERATOR_RCA_GOVERNOR_ADVISORY_ONLY=1
export ML_OPERATOR_RCA_GOVERNOR_WINDOW_MIN=1440
export ML_OPERATOR_RCA_GOVERNOR_MIN_SAMPLE=12
export ML_OPERATOR_RCA_GOVERNOR_SUPPRESS_SCORE_MAX=0.45
export ML_OPERATOR_RCA_GOVERNOR_PROMOTE_SCORE_MIN=0.72
```

## Smoke checks

```bash
redis-cli XREVRANGE stream:ml:operator_rca_governor_decisions + - COUNT 5
redis-cli XREVRANGE stream:ml:operator_rca_governor_audit + - COUNT 5
redis-cli HGETALL cfg:ml:operator_rca_governor:action:propose_threshold_canary:ml_triage_v1:policy_v1
curl -s localhost:9873/metrics | grep '^ml_operator_rca_governor_'
```

## Rollback

1. Stop `scanner-ml-operator-rca-usefulness-governor-v2-2`
2. Keep decision history and policy hashes for forensics
3. If needed, archive or delete `cfg:ml:operator_rca_governor:*`

## Notes

- This phase does not auto-apply provider/prompt changes.
- It creates governance signals for later provider/prompt routing phases.
