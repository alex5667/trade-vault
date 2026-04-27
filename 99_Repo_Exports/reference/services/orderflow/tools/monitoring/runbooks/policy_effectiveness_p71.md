# Runbook: Policy effectiveness report (P71)

## What it is

The **policy effectiveness report** compares downstream quality proxies across `policy_effective_mode` buckets (OK / WARN / BLOCK / UNKNOWN) over a rolling **24h** window.

Outputs are written into Redis `settings:dynamic_cfg` by:

- `orderflow_services/policy_effectiveness_report_worker_v1.py`

The Prometheus exporter (`ok_rate_logic/signal_quality_exporter_v3.py`) reads those keys and exposes metrics.

## Primary metrics (Prometheus)

- `policy_effectiveness_last_age_seconds` — staleness of the report.
- `policy_effectiveness_total_n_24h` — total decisions used (must be high enough).
- `policy_effectiveness_share_24h{mode=...}` — traffic split across modes.
- `policy_effectiveness_expectancy_r_delta_24h{mode=...}` — delta vs OK baseline (R units).
- `policy_effectiveness_precision_top5p_delta_24h{mode=...}` — delta vs OK baseline.
- `policy_effectiveness_ece_delta_24h{mode=...}` — calibration delta vs OK baseline (positive = worse).

## Alerts

- `PolicyEffectivenessReportStale`
- `PolicyEffectivenessBaselineMissing`
- `PolicyEffectivenessBlockShareHigh`
- `PolicyEffectivenessWarnExpectancyDrop`
- `PolicyEffectivenessWarnCalibrationWorse`

## Triage checklist

### 1) Report is stale

1. Check Redis key freshness:

```bash
redis-cli -u "$REDIS_URL" HGET settings:dynamic_cfg policy_effectiveness_last_ts_ms
redis-cli -u "$REDIS_URL" HGET settings:dynamic_cfg policy_effectiveness_input_last_ts_ms
```

2. Run the worker manually (from a container that has the code + REDIS_URL env):

```bash
docker compose -f ok_rate_logic/docker-compose-crypto-orderflow.yml exec -T python-worker \
  python -m orderflow_services.policy_effectiveness_report_worker_v1
```

3. Inspect logs around the worker execution window.

### 2) Baseline OK missing

The report uses `policy_effective_mode=ok` as baseline. If there are no OK decisions in 24h:

- Verify `policy_effective_mode` is being set and propagated to downstream streams.
- Check `signal_quality_n_24h_policy_ok` / `signal_quality_policy_mode_last_ts_ms` (inputs consumed by the report worker).
- If the policy is in **freeze** / **fail-closed** state, OK can drop to zero by design.

### 3) Block share is high

- Confirm this matches the intended policy posture for the current regime.
- Check upstream data-quality alerts (bad-time, gaps, WS reconnect storms) — they can inflate block decisions.
- Validate that `policy_mode_effective` is not stuck due to stale config.

### 4) WARN quality drops vs OK

- Check if WARN is acting as a “quarantine” bucket (selection bias): it may capture degraded conditions.
- If the goal is *WARN improving* over OK, re-check routing logic and gate thresholds.
- Verify calibration health and drift metrics; consider per-regime recalibration.

## Dashboard

Grafana: `/grafana/d/policy_eff_p71/policy-effectiveness-p71?orgId=1`
