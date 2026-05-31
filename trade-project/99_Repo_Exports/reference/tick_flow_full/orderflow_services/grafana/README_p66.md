P66 Grafana dashboard + SLO alerts

Artifacts:
- Grafana dashboard JSON:
  - orderflow_services/grafana/tradeoff_coverage_quality_regimes_p66.json
- SLO alert rules:
  - orderflow_services/prometheus_alerts_slo_tradeoff_p66.yml
- Runbook:
  - orderflow_services/runbook_tradeoff_dashboard_p66.md

Notes:
- Dashboard expects the metrics introduced in P63–P65 and P49:
  - decision_regime_share_24h, decision_regime_n_24h, decision_last_ts_ms
  - signal_quality_*_by_regime
  - psi_max_24h, feature_drift_max_z_24h, dq_flag_rate, tick_time_age_p99_ms, book_stale_p99_ms
- If some metrics are absent, panels will show “No data”; adjust queries to your actual metric names.

- `of_gate_ok_rate_health_p76.json` — OF gate ok_rate_strict/soft, eligible rate, and DQ quarantine rate panels.
