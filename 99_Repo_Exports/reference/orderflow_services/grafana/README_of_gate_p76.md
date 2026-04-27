# P76 — OF gate contract smoke-check dashboard

## What it is
This dashboard visualizes the health of the `metrics:of_gate` stream contract:
- `bad_share` (share of rows that fail contract validation)
- `missing_schema_share` (share of rows missing `schema_name/schema_version/reason_code`)
- top `dq_code` (validation failure reasons)
- top `reason_code` (all rows vs bad rows)

It is powered by:
1) `orderflow_services/of_gate_metrics_contract_check_v1.py` (periodic smoke-check job)
2) `orderflow_services/of_gate_contract_smoke_exporter_v1.py` (Prometheus exporter)
3) `orderflow_services/prometheus_alerts_of_gate_ok_rate_v1.yml` (recording rules + alerts)

## Required metrics (Prometheus)
Exporter metrics:
- `of_gate_contract_smoke_bad_share`
- `of_gate_contract_smoke_bad_total`
- `of_gate_contract_smoke_n_total`
- `of_gate_contract_smoke_schema_version_mode`
- `of_gate_contract_smoke_missing_schema_share`
- `of_gate_contract_smoke_last_ts_ms`
- `of_gate_contract_smoke_dq_bad_total{dq_code}`
- `of_gate_contract_smoke_reason_code_total{reason_code}`
- `of_gate_contract_smoke_reason_code_bad_total{reason_code}`

Pipeline counters (for ok_rate / quarantine panels in other dashboards):
- `of_gate_eligible_total{symbol,scenario_v4}`
- `of_gate_ok_hard_total{symbol,scenario_v4}`
- `of_gate_ok_soft_total{symbol,scenario_v4}`
- `of_gate_quarantined_total{why}`

## How to run (minimal)
### 1) Run the exporter
In the same environment that can reach Redis:

```bash
export REDIS_URL='redis://redis-worker-1:6379/0'
export OF_GATE_CONTRACT_SMOKE_OUT_STREAM='sre:of_gate_contract_smoke'
export OF_GATE_CONTRACT_SMOKE_EXPORTER_PORT=9148
python -m orderflow_services.of_gate_contract_smoke_exporter_v1
```

Configure Prometheus to scrape `:9148`.

### 2) Run the smoke-check periodically
Example (manual run):

```bash
export REDIS_URL='redis://redis-worker-1:6379/0'
python -m orderflow_services.of_gate_metrics_contract_check_v1 --notify
echo $?
# 0 = OK, 2 = ALERT
```

In production, run it via systemd timer / docker timers worker.

### 3) Load Prometheus rules
Load `orderflow_services/prometheus_alerts_of_gate_ok_rate_v1.yml` into Prometheus.

### 4) Import the Grafana dashboard
Import JSON:
- `orderflow_services/grafana/of_gate_contract_smoke_p76.json`

Datasource variable is `${DS_PROMETHEUS}`.
