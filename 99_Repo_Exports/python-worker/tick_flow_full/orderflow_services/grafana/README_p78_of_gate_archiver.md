## OF Gate — Archiver Health (P78)

Dashboard JSON:
- `of_gate_archiver_health_p78.json`

### Required Prometheus metrics
Exported by `orderflow_services/of_gate_archiver_exporter_v1.py`:
- `of_gate_archiver_staleness_sec{kind}`
- `of_gate_archiver_last_run_ts_ms{kind}`
- `of_gate_archiver_inserted_total{kind}`
- `of_gate_archiver_error_total{kind}`

Kinds (labels):
- `metrics`
- `quarantine`
- `rollups_refresh`

### How to run exporter
ENV:
- `REDIS_URL`
- `OF_GATE_ARCHIVER_EXPORTER_PORT` (default 9152)

Command:
```bash
python -m orderflow_services.of_gate_archiver_exporter_v1
```

### Prometheus alerts
Rules file:
- `orderflow_services/prometheus_alerts_of_gate_archiver_p78.yml`
