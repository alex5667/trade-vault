P74 Grafana dashboard for policy calibration suggestions

Artifacts:
- Grafana dashboard JSON:
  - orderflow_services/grafana/policy_calibration_suggestions_p74.json

What it visualizes (Prometheus metrics):

P74 (suggestions):
- policy_calibration_suggest_staleness_sec
- policy_calibration_suggest_inputs_stale
- policy_calibration_suggest_ok_baseline_present
- policy_calibration_suggest_warn_action_code
- policy_calibration_suggest_warn_severity
- policy_calibration_suggest_warn_share_24h
- policy_calibration_suggest_block_action_code
- policy_calibration_suggest_block_severity
- policy_calibration_suggest_block_share_24h
- policy_calibration_suggest_unknown_share_24h

Dependencies:
- P71 + P72 should be enabled so P74 can compute meaningful suggestions:
  - ENABLE_POLICY_EFFECTIVENESS_REPORT=1
  - ENABLE_POLICY_REGIME_EFFECTIVENESS_REPORT=1
- Enable P74:
  - ENABLE_POLICY_CALIBRATION_SUGGESTER_P74=1

Notes:
- Reports are also written to Redis (see runbook):
  - runbook_policy_calibration_suggester_p74.md
