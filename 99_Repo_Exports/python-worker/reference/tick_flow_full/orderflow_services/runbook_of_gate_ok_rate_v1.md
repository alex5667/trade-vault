# OF gate OK-rate + DQ (runbook)

## What this covers
Alerts from `prometheus_alerts_of_gate_ok_rate_v1.yml`:
- eligible_total absent / no eligible rows
- ok_rate_strict low
- soft_share high
- quarantine share/rate high
- P76 contract smoke-check staleness / bad_share

Dashboards:
- OK-rate + DQ: `orderflow_services/grafana/of_gate_ok_rate_and_dq_v1.json`
- Contract smoke-check (P76): `orderflow_services/grafana/of_gate_contract_smoke_p76.json`

Related alerts:
- DLQ (P82): `prometheus_alerts_of_gate_dlq_p82.yml`
- Archiver/rollups (P78/P80/P81): `prometheus_alerts_of_gate_archiver_p78.yml`

## 60-second triage
1) Is OF-gate metrics pipeline alive?
- Check `OF_Gate_EligibleAbsent15m` / `OF_Gate_NoEligible15m`.
- Verify exporter targets respond:
  - `curl -sS http://of-gate-archiver-exporter:9152/metrics | head`
  - `curl -sS http://of-gate-dlq-exporter:9154/metrics | head`

2) If OK-rate dropped:
- Open dashboard `of_gate_ok_rate_and_dq_v1`.
- Look at:
  - `of_gate:ok_rate_strict5m`, `of_gate:ok_rate_soft5m`, `of_gate:soft_share5m`
  - `of_gate:quarantine_share5m`, `of_gate:quarantine_rate5m`

3) Check DLQ immediately:
- If `of_gate_dlq_len > 0`, run:
  - `python -m orderflow_services.of_gate_dlq_drilldown_p83 stats`
  - `python -m orderflow_services.of_gate_dlq_drilldown_p83 top --limit 2000`

## Root cause patterns
### A) Upstream ingest/feed down
Symptoms:
- eligible_total absent or no eligible rows
Actions:
- Check the upstream producer(s) that emit OF-gate metrics.
- Check Redis connectivity + Prometheus scrape targets.

### B) Validation regression / schema break
Symptoms:
- quarantine share/rate high
- P76 contract bad_share high
Actions:
- Open contract dashboard (P76): `of_gate_contract_smoke_p76`.
- Identify top `dq_code` / missing schema markers.
- Roll back the producer deploy or hotfix schema enrichment.

### C) OK-rate strict low, but eligible present
Symptoms:
- ok_rate_strict low while eligible is non-zero
- soft_share may spike
Actions:
- Correlate with recent changes (config, gating thresholds, feature flags).
- Check for time normalization anomalies (ts_ms leaps, monotonicity breaks).
- Inspect recent quarantined samples if available.

### D) Archiver / DB issues
Symptoms:
- DLQ oldest age grows
- Archiver errors in P78
Actions:
- Check archiver logs.
- Verify Postgres/Timescale connectivity and slow queries.
- Consider pausing auto-replay until DB is stable.

## Manual checks / commands
- DLQ drilldown:
  - `python -m orderflow_services.of_gate_dlq_drilldown_p83 stats`
  - `python -m orderflow_services.of_gate_dlq_drilldown_p83 top --limit 5000`
- Safe replay (only after triage):
  - `python -m orderflow_services.of_gate_dlq_fixed_replay_p84 auto --commit --delete-after-replay`

## When to page
- `OF_Gate_OkRateStrictLow` persists >10m.
- `OF_Gate_ContractBadShareHigh` persists >10m.
- `OF_Gate_DLQ_OldestAgeHigh` >1h.
