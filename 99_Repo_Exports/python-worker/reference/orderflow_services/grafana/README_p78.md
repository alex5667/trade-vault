# OF-gate Archiver Health (P78)

Files:
- `of_gate_archiver_health_p78.json` — Grafana dashboard for archiver/exporter metrics.

## Requirements
Prometheus metrics exposed by `orderflow_services.of_gate_archiver_exporter_v1`:
- `of_gate_archiver_last_run_ts_ms{kind}`
- `of_gate_archiver_staleness_sec{kind}`
- `of_gate_archiver_last_stream_ts_ms{kind}`
- `of_gate_archiver_inserted_total{kind}`
- `of_gate_archiver_error_total{kind}`

Kinds (expected): `metrics`, `quarantine`, `rollups_refresh`.

## Exporter
Run:
```bash
REDIS_URL=redis://redis-worker-1:6379/0 \
OF_GATE_ARCHIVER_EXPORTER_PORT=9152 \
python -m orderflow_services.of_gate_archiver_exporter_v1
```

## Alerts
Prometheus rules: `orderflow_services/prometheus_alerts_of_gate_archiver_p78.yml`.

## Notes
- `services/of_timers_worker.py` scheduler runs in **UTC** (uses `datetime.utcnow()`); any "safe windows" should be configured in UTC.

## Rollups Freshness (P80)
Dashboard: `of_gate_rollups_freshness_p80.json`

Requires scheduled probe: `orderflow_services.of_gate_rollups_freshness_probe_v1` writing Redis hash `metrics:of_gate_rollups_freshness` and exporter `orderflow_services.of_gate_archiver_exporter_v1` exporting:
- `of_gate_rollups_bucket_age_sec{view}` — seconds since latest CAGG bucket (view=5m|1h)
- `of_gate_rollups_bucket_ts_ms{view}` — latest bucket timestamp (ms)
- `of_gate_rollups_freshness_ok` — 1 if probe found data in both views

Probe runs hourly at :52 (scheduled by `of_timers_worker`).
Enable via `ENABLE_OF_GATE_ROLLUPS_FRESHNESS_PROBE=1` or inherited from `ENABLE_OF_GATE_ROLLUPS_REFRESH_NIGHTLY=1`.

## Timescale Policies (P81)
Dashboard: `of_gate_timescale_policies_p81.json`

Requires scheduled probe: `orderflow_services.of_gate_timescale_policy_probe_v1` writing Redis hash `metrics:of_gate_timescale_policies` and exporter `orderflow_services.of_gate_archiver_exporter_v1` exporting:
- `of_gate_timescale_present`, `of_gate_timescale_expect`
- `of_gate_timescale_policies_missing`, `of_gate_timescale_policies_disabled`
- `of_gate_timescale_policy_present{policy}`, `of_gate_timescale_policy_disabled{policy}`

Probe runs hourly at :37 (UTC) in `of_timers_worker`. Enable via `ENABLE_OF_GATE_TIMESCALE_POLICY_PROBE=1` or inherited from `ENABLE_OF_GATE_ROLLUPS_REFRESH_NIGHTLY=1`.
