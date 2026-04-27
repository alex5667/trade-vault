P75 Grafana dashboard for confidence calibration live health (ECE/Brier, bad_streak, rollbacks)

Artifacts:
- Grafana dashboard JSON:
  - orderflow_services/grafana/confidence_calibration_live_health_p75.json

What it visualizes (Prometheus metrics):

Exporter base:
- live_ece_raw
- live_ece_cal
- live_brier_raw
- live_brier_cal
- bad_streak
- rollback_total

Exporter diagnostics / guard:
- conf_cal_live_status_age_sec
- conf_cal_live_degrade
- conf_cal_live_rows
- conf_cal_live_rows_cal
- conf_cal_live_rollback_events_total
- conf_cal_live_degrade_reason_total{reason="..."}
- conf_cal_live_skip_reason_total{reason="..."}

Dependencies:
- conf_cal_live_status_exporter_v1.py is running and scraped by Prometheus.
- Live health loop writes status JSON at least hourly.

Notes:
- Dashboard adds an annotation stream for rollback events:
  increase(conf_cal_live_rollback_events_total[5m]) > 0
