# Runbook: OF-gate Timescale policies probe (P81)

## Purpose
Ensure that TimescaleDB background policies exist and are enabled for OF-gate rollups:
- retention policy for `of_gate_metrics` and `of_gate_metrics_quarantine`
- refresh policy for CAGG `of_gate_ok_rate_5m` and `of_gate_ok_rate_1h`

Probe writes to Redis hash `metrics:of_gate_timescale_policies` and exporter turns it into Prometheus metrics.

## Run
```bash
TRADES_DB_DSN=postgresql://... \
REDIS_URL=redis://redis-worker-1:6379/0 \
python -m orderflow_services.of_gate_timescale_policy_probe_v1
```

If the probe exits with code 2, check:
1) Timescale extension installed (if expected)
2) policies exist & scheduled

## Expected policies
1) Retention:
- `policy_retention` on hypertable `of_gate_metrics`
- `policy_retention` on hypertable `of_gate_metrics_quarantine`

2) CAGG refresh:
- `policy_refresh_continuous_aggregate` for `of_gate_ok_rate_5m`
- `policy_refresh_continuous_aggregate` for `of_gate_ok_rate_1h`

## Troubleshooting SQL
```sql
-- extension
SELECT * FROM pg_extension WHERE extname='timescaledb';

-- jobs
SELECT job_id, proc_name, scheduled, hypertable_name, config
FROM timescaledb_information.jobs
ORDER BY job_id;

-- continuous aggregates
SELECT * FROM timescaledb_information.continuous_aggregates;
```

If `scheduled=false`, re-enable job (depends on TS version; commonly `alter_job(job_id, scheduled => true)`).

## Alerting
Prometheus alerts in `orderflow_services/prometheus_alerts_of_gate_archiver_p78.yml`:
- `OF_Gate_TimescaleMissing`
- `OF_Gate_TimescalePoliciesMissing`
- `OF_Gate_TimescalePoliciesDisabled`
- `OF_Gate_TimescalePolicyProbe_Stale`
