# Runbook: Edge Stack Train (P59)

## Owner
- team: **trade**
- component: **edge_stack**
- contact: (add your Telegram/Slack channel here)

## One-command rollback (safe)
- Disable promotions (candidate-only):
  - set `EDGE_STACK_AUTO_PROMOTE=0`
  - restart timers worker / nightly

## How to silence
- Open Alertmanager → Silences → New, match:
  - `team="trade"`, `component="edge_stack"`, (optional) `alertname="EdgeStackTrainFailed"`

## Symptoms
- Alert: `EdgeStackTrainFailed`
- Alert: `EdgeStackTrainStale`
- Alert: `EdgeStackTrainQualityDegraded`

## Immediate checks (5 minutes)
1) Prometheus Targets: ensure `edge-stack-train-exporter-p59` is **UP**.
2) Redis metrics hash:
   - `HGETALL metrics:edge_stack_train:last`
   - look for `success`, `reason`, `schema_hash`, `feature_cols_hash`, `joined`, `pos_rate`.

## Common causes / fixes

### A) Dataset too small / join broke
- `joined` below threshold OR `pos_rate` extreme.

Fix:
- Increase window (`EDGE_STACK_WINDOW_HOURS`) or relax join filters.
- Check streams retention and clock skew.

### B) Feature hash mismatch (column drift)
Symptoms:
- Train tool reports mismatch between Registry and dataset report.

Fix:
- Rebuild dataset with pinned schema (`--feature_schema_ver v3` or `v4_of`).
- Ensure `scenario_prefix` and `include_time_onehot` match between builder/train.

### C) Quality degraded (brier/ece)
Fix:
- Inspect drift on core features: `ofi_z`, `spread_bps`, `mp_mid_bps`.
- Compare candidate vs champion via P60 shadow.
- If candidate is consistently better, enable guarded promote (P60).

## Escalation
- If failures persist for 2+ nights: freeze auto-promote and pin to last known good champion.

## Links
- Grafana dashboard: `/d/edge_stack_overview/edge-stack-overview?orgId=1`
