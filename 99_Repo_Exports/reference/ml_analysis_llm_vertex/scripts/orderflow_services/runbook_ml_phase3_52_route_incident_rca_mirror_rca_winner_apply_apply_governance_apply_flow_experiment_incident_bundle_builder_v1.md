# Phase 3.52 — Apply-Flow Experiment Forensic Incident Bundle Builder

## Цель
Собрать отдельный forensic bundle уже для нового experiment safety contour,
чтобы не смешивать его с обычным routing incident RCA.

## Что делает
- читает только experiment-safety streams:
  - verification results
  - rollback journal
  - retry results
  - escalations
  - SLO rollups
- строит единый incident bundle
- пишет bundle в dedicated stream:
  - `stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_bundles`

## Bundle contents
- `trigger_type`
- `trigger_reason_code`
- `trigger_severity`
- `summary`
  - verification / rollback / retry / escalation counts
  - reason codes
  - escalation severities
  - verify_keep_rate
  - rollback rates
  - rollback_mttr_p95_sec
  - escalation_rate
- `evidence`
  - latest verification
  - latest rollback
  - latest retry
  - latest escalation
  - latest SLO rollup
- `forensics`
  - recent verification
  - recent rollback
  - recent retry
  - recent escalation

## Trigger logic
- verification rollback path -> usually `critical`
- applied rollback path -> `critical`
- escalation severity is preserved
- degraded SLO path -> `warning` or `critical` depending on thresholds

## Safe behavior
- only dedicated experiment contour
- no bridge apply
- no policy changes
- next phase can use this bundle for RCA bridge / reporting / Vertex-local routing

## Safe start
```bash
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_BUNDLES_MODE=ENABLED
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_BUNDLES_MIN_VERIFICATION_EVENTS=1
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_BUNDLES_VERIFY_KEEP_RATE_CRIT=0.60
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_BUNDLES_ROLLBACK_MTTR_P95_CRIT_SEC=900
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_BUNDLES_ESCALATION_RATE_CRIT=0.20
```

## Smoke checks
```bash
curl -s localhost:9986/metrics | grep '^ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_bundles_'
redis-cli HGETALL metrics:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_bundles:last
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_bundles + - COUNT 5
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_bundles_audit + - COUNT 5
```

## Notes
- scope only `scanner_infra`
- hot path untouched
- designed as separate forensic contour for later RCA bridge/reporting
