P73 Grafana dashboard for policy calibration (P71/P72)

Artifacts:
- Grafana dashboard JSON:
  - orderflow_services/grafana/policy_calibration_effectiveness_p73.json

What it visualizes (Prometheus metrics):

P71 (global deltas vs ok, last 24h):
- policy_effectiveness_share_24h{mode="ok|warn|block|unknown"}
- policy_effectiveness_expectancy_r_delta_24h{mode="warn|block|unknown"}
- policy_effectiveness_precision_top5p_delta_24h{mode="warn|block|unknown"}
- policy_effectiveness_ece_delta_24h{mode="warn|block|unknown"}
- policy_effectiveness_baseline_ok_present
- policy_effectiveness_staleness_sec

P72 (within dq_state×drift_state cell deltas vs ok, last 24h):
- policy_regime_effectiveness_cells_total
- policy_regime_effectiveness_cells_ok_baseline
- policy_regime_effectiveness_worst_warn_expectancy_r_delta
- policy_regime_effectiveness_worst_warn_precision_top5p_delta
- policy_regime_effectiveness_worst_warn_ece_delta
- policy_regime_effectiveness_worst_block_expectancy_r_delta
- policy_regime_effectiveness_worst_block_precision_top5p_delta
- policy_regime_effectiveness_worst_block_ece_delta
- policy_regime_effectiveness_staleness_sec

Notes:
- This dashboard assumes P71 and P72 are running (sre_monitor_all_v3.py):
  - ENABLE_POLICY_EFFECTIVENESS_REPORT=1
  - ENABLE_POLICY_REGIME_EFFECTIVENESS_REPORT=1
- If the report workers are disabled or stale, you will see "No data" and/or staleness rising.
- Full reports are also written to Redis by the workers (see runbooks):
  - runbook_policy_effectiveness_p71.md
  - runbook_policy_regime_effectiveness_p72.md
