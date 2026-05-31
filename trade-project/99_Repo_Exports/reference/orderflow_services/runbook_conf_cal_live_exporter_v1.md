# Runbook: Confidence Cal Live Exporter (v1)

## Purpose
Expose live calibration health status JSON as Prometheus metrics so degradation and rollbacks are observable and alertable.

## Inputs
Exporter reads a status JSON produced by `ml_analysis.tools.confidence_cal_live_health_loop_v1`.

Supported filenames (probe order inside `CONF_CAL_LIVE_REPORTS_DIR`):
- `conf_cal_live_status.json`
- `confidence_calibration_live_status.json`
- `confidence_cal_live_status.json`
- `live_status.json`

Or explicit `CONF_CAL_LIVE_STATUS_PATH`.

## Key metrics
Requested:
- `live_ece_raw`, `live_ece_cal`
- `live_brier_raw`, `live_brier_cal`
- `bad_streak`
- `rollback_total`

Operational:
- `conf_cal_live_degrade`
- `conf_cal_live_status_age_sec`
- `conf_cal_live_rollback_events_total`
- `conf_cal_live_exporter_read_ok`

## How to run
```bash
export CONF_CAL_LIVE_REPORTS_DIR=/var/lib/trade/of_reports/out/conf_cal/live
export CONF_CAL_LIVE_EXPORTER_PORT=9134
python3 orderflow_services/conf_cal_live_status_exporter_v1.py
```

## Prometheus integration
Scrape example:
```yaml
- job_name: conf_cal_live_exporter
  static_configs:
    - targets: ["<host>:9134"]
```

Load alert rules from:
`orderflow_services/prometheus_alerts_conf_cal_live_exporter_v1.yml`
