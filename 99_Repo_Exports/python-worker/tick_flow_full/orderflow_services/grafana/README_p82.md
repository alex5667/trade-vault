# Grafana dashboards (P82)

## OF-gate DLQ
File: `of_gate_dlq_p82.json`

Requires Prometheus metrics from `orderflow_services/of_gate_dlq_exporter_v1.py`:
- `of_gate_dlq_len{stream=...}`
- `of_gate_dlq_age_sec{stream=...}`

Import JSON into Grafana and select datasource `${DS_PROMETHEUS}`.
