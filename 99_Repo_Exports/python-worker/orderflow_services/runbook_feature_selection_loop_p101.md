# Runbook: Feature Selection Loop (P101)

## Goal
Nightly **minimal feature selection loop** to quickly detect and prune “noise” additions:

- global importance (permutation-based)
- stability tables by **regime** (trend/range/other) and by **hour buckets**

Output is a report (Markdown + CSVs) plus a low-cardinality metrics hash for Prometheus.

## Components

### Nightly bundle
Module:

- `ml_analysis.tools.nightly_feature_selection_loop_bundle_v1`

Schedule (via `of_timers_worker`):

- **04:02** local time

Guard:

- `FEATURE_SELECTION_LOOP_BUNDLE_ENABLED=1`

### Exporter
Module:

- `ml_analysis.tools.feature_selection_loop_exporter_v1`

Metrics endpoint:

- `/metrics` (default port `9821`, env `FEATURE_SELECTION_LOOP_EXPORTER_PORT`)

### Redis keys

- `metrics:feature_selection_loop:last` (hash)

## Alerts

- **Exporter down**: cannot scrape `feature_selection_loop_exporter_up`
- **Stale**: no update for >36h (`feature_selection_loop_age_seconds`)
- **Failed**: recent failure (`feature_selection_loop_last_success == 0`)
- **Noise high** (warning): `feature_selection_loop_noise_share > 0.60`

## Triage checklist

### 1) Confirm metrics

Prometheus quick queries:

- `feature_selection_loop_last_success`
- `feature_selection_loop_age_seconds`
- `feature_selection_loop_noise_share`

### 2) Inspect Redis summary

```bash
redis-cli HGETALL metrics:feature_selection_loop:last
```

Look at:

- `status`, `reason`, `exit_code`
- `run_dir`, `summary_path`, `report_path`
- `n_rows`, `n_features`, `noise_n`, `auc_val`, `brier_val`

### 3) Open the report

In `run_dir/feature_selection/`:

- `report.md` — human readable summary + interpretation hints
- `importance_global.csv` — top global features
- `stability_table.csv` — group stability (regime/hour)
- `importance_by_regime.csv`, `importance_by_hour.csv`

## Manual rerun

Use the exact run parameters from Redis (`run_dir`) or run fresh:

```bash
python -m ml_analysis.tools.nightly_feature_selection_loop_bundle_v1 \
  --feature_schema_ver v5_of \
  --window_hours 168 \
  --signals_count 250000 \
  --closes_count 250000
```

If you want to run the loop on an existing dataset JSONL:

```bash
python -m ml_analysis.tools.feature_selection_loop_v1 \
  --data_path /path/to/edge_train.jsonl \
  --schema_ver v5_of \
  --out_dir /tmp/fs_loop \
  --model lr
```

## How to interpret “noise”

Noise candidates are typically features that are:

- near-zero importance **globally**, AND/OR
- unstable sign/importance across regimes or hour buckets, AND/OR
- high missing/constant rate (implicit from low signal)

Action rules of thumb:

1) **Pure noise**: global importance ~0 and unstable in groups → remove or move to “experimental” schema.
2) **Regime-only**: low global but strong in one regime/hour → keep, but gate/regularize and validate data quality.
3) **Suspicious instability**: flips in many buckets → check deterministic computation (time alignment, missing legs, late ticks).

## Rollback / Disable

- Disable the job: `FEATURE_SELECTION_LOOP_BUNDLE_ENABLED=0`
- If a recent schema expansion triggered failures, revert the schema commit (registry) and re-run.
