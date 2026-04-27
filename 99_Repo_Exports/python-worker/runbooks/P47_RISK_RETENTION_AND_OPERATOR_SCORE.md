# P4.7: Risk retention, mismatch quarantine and operator score

1. Refresh `risk_decision_summary_mv` and verify `latest_risk_decision_summary.json` plus the Prometheus textfile freshness metric (`trade_risk_summary_freshness_seconds`).
2. Run `check_risk_signal_snapshot_consistency.py` and inspect `latest_risk_signal_consistency.json` for repeated mismatch SID values.
3. If repeated mismatches exceed threshold, allow the quarantine policy to add the SID to the quarantine set and investigate signal/risk contract drift.
4. Run `purge_risk_audit_hot_tables.py --dry-run` before enabling nightly retention purge.
5. Verify `/api/operator-score/latest` in the runbook server and use the merged score for canary/prod operator readiness.
