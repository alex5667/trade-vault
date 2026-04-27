# Runbook: P72 Policy Regime Effectiveness Report

This report is intended for *calibrating P68/P69 policy thresholds* by reducing
confounding from regime mix.

## What it measures

Reads the last 24h of `trades:closed` and groups by:

- `dq_state` (normalized to ok|warn|block|unknown)
- `drift_state` (normalized to ok|warn|block|unknown)
- `policy_effective_mode` (ok|warn|block|unknown)

For each `(dq_state, drift_state)` cell it computes:

- mean R (expectancy)
- precision@top5% by `score`
- ECE based on `score`

Then computes deltas **within the same cell**, using policy `ok` as baseline:

- `Δexpectancy_r = meanR(mode) - meanR(ok)`
- `Δprecision_top5p = precision(mode) - precision(ok)`
- `Δece = ece(mode) - ece(ok)`

## Outputs

### Reports

Stored in Redis keys (JSON + CSV):

- `reports:policy_regime_effectiveness:p72:last_json`
- `reports:policy_regime_effectiveness:p72:last_csv`

### Prometheus gauges (via cfg2 snapshot)

- `policy_regime_effectiveness_last_ts_ms`
- `policy_regime_effectiveness_staleness_sec`
- `policy_regime_effectiveness_cells_total`
- `policy_regime_effectiveness_cells_ok_baseline`
- `policy_regime_effectiveness_worst_warn_expectancy_r_delta`
- `policy_regime_effectiveness_worst_warn_precision_top5p_delta`
- `policy_regime_effectiveness_worst_warn_ece_delta`
- `policy_regime_effectiveness_worst_block_expectancy_r_delta`
- `policy_regime_effectiveness_worst_block_precision_top5p_delta`
- `policy_regime_effectiveness_worst_block_ece_delta`

## How to run

One-shot:

```bash
python3 tools/policy_regime_effectiveness_report_worker_p72.py --once
```

Via SRE monitor loop:

```bash
export ENABLE_POLICY_REGIME_EFFECTIVENESS_REPORT=1
python3 orderflow_services/sre_monitor_all_v3.py --emit-metrics
```

## Interpretation

- **`cells_ok_baseline` low**: insufficient ok baseline per cell -> deltas are not reliable.
- **worst warn deltas negative**: warn mode trades are worse than ok within the same dq/drift cell.
  This may indicate KPI thresholds are over-triggering warn on low-quality signals, or that the
  warn restrictions are not effective.
- **worst warn ECE delta positive**: warn mode calibration is worse than ok within-cell; check
  score pipeline or consider tightening constraints.

## Common failure modes

- Missing fields in `trades:closed` (`policy_effective_mode`, `dq_state`, `drift_state`, `score`, `r_multiple`)
  -> report will fall back to `unknown` states; verify writer contract (P69) and enrichers.
