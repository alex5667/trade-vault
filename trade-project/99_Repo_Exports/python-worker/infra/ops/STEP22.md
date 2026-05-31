# Step 22 — Tick Gate Aggregator (Redis stream -> Prometheus /metrics)

## What it does
Consumes gate outcomes from Redis stream (default `ops:tick_quality_gate`) and exposes:
  - tick_gate_events_total{status}
  - tick_gate_fail_reasons_total{reason}  (limited labels)
  - tick_gate_last_run_ts_seconds
  - tick_gate_last_status{status}
  - tick_gate_stream_lag_ms

## Why
Textfile exporter is OK, but a live /metrics endpoint is better for alerting and dashboards.

## Run (manual)
```bash
cd /home/alex/front/trade/scanner_infra/python-worker
export REDIS_URL=redis://redis-worker-1:6379/0
python3 -m tools.tick_gate_metrics_aggregator --metrics-port 9112
```

## Run (systemd)
1) Install env:
```bash
sudo cp python-worker/infra/ops/tick_gate_aggregator.env.example /etc/default/tick-gate-aggregator
sudo nano /etc/default/tick-gate-aggregator
```

2) Install unit:
```bash
sudo cp python-worker/infra/systemd/tick-gate-aggregator.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now tick-gate-aggregator.service
```

3) Verify:
```bash
curl -s localhost:9112/metrics | egrep 'tick_gate_(events_total|last_run_ts_seconds|stream_lag_ms)'
```

## Prometheus rules
Import:
`python-worker/infra/observability/tick_gate_aggregator_alerts.yml`

## Notes / safety
Cardinality controls:
  - `TICK_GATE_REASON_LABEL_MODE` and allowlist
  - `TICK_GATE_SYMBOL_LABEL_MODE` and allowlist (reserved for future extension)

Start position:
  - `TICK_GATE_AGG_START_ID=$` consumes new messages only
  - set to `0` to replay the entire stream (may double-count if you changed group name)
