# Phase 3.48 — Route Incident RCA Mirror RCA Winner-Apply Apply Governance Apply-Flow Experiment Result Consumer + Feedback + Winner Selection

## Цель
Сделать новый isolated experiment contour полноценным A/B loop:
- experiment request
- experiment result
- experiment feedback
- winner scorecards
- winner recommendation

## Что делает
- `experiment_result_consumer`:
  - читает:
    - `stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_vertex_requests`
    - `stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_local_requests`
  - строит bounded result
  - пишет в:
    - `stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_results`

- `experiment_winner_selector`:
  - читает exposures из `3.47`
  - читает experiment results
  - читает experiment feedback
  - строит per-arm scorecards
  - пишет recommendation-only decisions

## Experiment feedback contract
- `request_id`
- `bundle_id`
- `quality_score`
- `usefulness_score`
- `accepted`
- `reason_code`

## Decisions
- `KEEP_VERTEX_PRIMARY`
- `PROMOTE_VERTEX_COMPACT_CANDIDATE`
- `PROMOTE_LOCAL_CANDIDATE`

## Safe behavior
- winner selector recommendation-only
- no apply to bridge / no primary switch yet
- next phase can consume winner recommendation separately

## Safe start
```bash
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_RESULTS_HANDLER_MODE=DETERMINISTIC
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_WINNER_MIN_EXPOSURES=5
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_WINNER_MIN_FEEDBACK=3
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_WINNER_MIN_RESULT_COVERAGE=0.50
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_WINNER_MIN_FEEDBACK_COVERAGE=0.30
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_WINNER_MIN_SCORE_MARGIN=0.05
```

## Smoke checks
```bash
curl -s localhost:9979/metrics | grep '^ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_results_'
curl -s localhost:9980/metrics | grep '^ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_'
redis-cli HGETALL metrics:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_results:last
redis-cli HGETALL metrics:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_winner:last
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_results + - COUNT 5
redis-cli XADD stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_feedback * \
  request_id exp-1 \
  bundle_id b-1 \
  quality_score 0.8 \
  usefulness_score 0.85 \
  accepted 1 \
  reason_code helpful \
  ts_ms $(date +%s%3N)
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_scorecards + - COUNT 5
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_winner_decisions + - COUNT 5
```

## Notes
- scope только `scanner_infra`
- hot path не затронут
- winner selection still does not apply changes
- next phase: winner-aware apply controller for the experiment contour
