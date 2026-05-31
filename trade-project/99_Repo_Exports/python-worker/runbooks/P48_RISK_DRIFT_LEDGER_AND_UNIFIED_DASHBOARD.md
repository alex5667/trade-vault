# P4.8: Risk drift ledger and unified dashboard

## What this layer adds

- Dedicated SQL ledger for repeated risk mismatch quarantine events (`risk_mismatch_quarantine_ledger`).
- Materialized mismatch summary view (`risk_mismatch_summary_mv`) for 1h/24h/7d windows.
- Compose wiring for auto-refresh (risk summary + mismatch summary) and operator-score refresh timers.
- Unified Grafana dashboard spanning execution, replay, risk, operator score and drift annotations.
- Alertmanager deep-links for risk drift incidents.
