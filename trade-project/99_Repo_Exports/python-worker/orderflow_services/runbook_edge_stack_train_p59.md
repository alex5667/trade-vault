# P59 Runbook — Edge Stack v1 Nightly Train Bundle

## What it is
Nightly job that:
1) builds edge-stack dataset (signals:of:inputs + trades:closed, with archive fallback),
2) validates dataset health,
3) trains `edge_stack_v1` via OOF stacking,
4) publishes candidate (and optionally champion) model artifacts,
5) writes Redis metrics `metrics:edge_stack_train:last` for Prometheus exporter/alerts.

## Key locations
- Bundle tool: `tools.nightly_edge_stack_v1_train_bundle`
- Exporter: `tools.edge_stack_train_exporter_v1`
- Redis metrics: `metrics:edge_stack_train:last`
- Candidate key: `cfg:ml_confirm:edge_stack_v1:candidate`
- Champion key:  `cfg:ml_confirm:edge_stack_v1:champion`
- Artifacts: `$EDGE_STACK_V1_DIR/runs/<run_id>/`

## Common failures and fixes

### 1) fail_build (dataset build tool failed)
**Symptoms**
- `EdgeStackTrainFailed` alert
- bundle status: `fail_build`
- stderr tail mentions Redis / stream read / JSON parse

**Checks**
- Are streams present?
  - `signals:of:inputs` has fresh data for last 72h
  - `trades:closed` has closes for last 72h
- Archive fallback directories mounted?
  - `SIGNALS_ARCHIVE_DIR`, `TRADES_CLOSED_ARCHIVE_DIR`

**Actions**
- Manually re-run dataset build:
  - `python -m ml_analysis.tools.build_edge_stack_dataset_from_redis ...`
- If Redis retention is short: ensure P58 archivers are running and dirs are writable.

### 2) fail_validate (dataset too small or pos_rate out of range)
**Symptoms**
- bundle status: `fail_validate`
- reason: `dataset_too_small` or `pos_rate_out_of_range`

**Actions**
- Increase window: `EDGE_STACK_WINDOW_HOURS=120` temporarily
- Verify joins:
  - `sid` present in payloads
  - `close_ts_ms` sane
- If pos_rate extreme:
  - label threshold `Y_MIN_R` too low/high
  - drift regime / DQ regime blocking many trades → not enough positives

### 3) fail_train (training tool failed)
**Symptoms**
- bundle status: `fail_train`
- stderr tail mentions sklearn/joblib issues or dataset parsing

**Actions**
- Confirm dependencies in image: numpy, sklearn, joblib
- Inspect dataset row schema in run dir
- Re-run training locally with same args

## Promotion policy
Default: candidate-only (no auto promote).
To promote automatically:
- set `EDGE_STACK_AUTO_PROMOTE=1` in timers environment

Manual promotion:
- copy candidate to champion path:
  - `cp $EDGE_STACK_V1_DIR/champions/edge_stack_v1_<run_id>.joblib $EDGE_STACK_V1_DIR/champions/edge_stack_v1_champion.joblib`
- update Redis key `cfg:ml_confirm:edge_stack_v1:champion` with model_path/run_id

## Verification
- `metrics:edge_stack_train:last` has:
  - status=ok
  - joined >= min
  - oof_meta_brier/ece within expected band
- Prometheus exporter exposes gauges on `EDGE_STACK_TRAIN_EXPORTER_PORT` (default 9813)
