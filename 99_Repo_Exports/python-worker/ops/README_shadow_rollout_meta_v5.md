# SHADOW Rollout: meta_feat_v5 (DQ hard-features)

Goal: run `meta_feat_v5` in SHADOW for 1-2 nights to validate:
- DQ keys are present in parquet (`dq_present_n` > 0)
- worst-bucket quality is not degraded
- DQ latch (P16) can freeze safely if coverage/quality is bad

## 1) Enable dq_gate in penalty-only mode (runtime)

Apply (example):
```bash
redis-cli HSET settings:dynamic_cfg \
  dq_gate_enable 1 \
  dq_gate_mode penalty \
  dq_pen_max 0.10 \
  dq_data_health_min 0.80 \
  dq_tick_age_ms_max 250 \
  dq_stream_skew_ema_ms_max 200 \
  dq_unknown_side_ema_max 0.05 \
  dq_ts_source_now_ema_ms_max 150 \
  dq_ts_source_stream_id_ema_ms_max 150
```

Rollback:
```bash
redis-cli HSET settings:dynamic_cfg dq_gate_enable 0 dq_gate_mode off
```

## 2) Run SHADOW nightly

Option A (recommended): use the helper script:
```bash
chmod +x python-worker/ops/run_shadow_meta_v5.sh
python-worker/ops/run_shadow_meta_v5.sh \
  --in-parquet /var/lib/trade/of_reports/datasets/nightly_meta_v4.parquet
```

Artifacts:
- `/var/lib/trade/of_reports/models/meta_model_v5.json`
- `/var/lib/trade/of_reports/reports/meta_report_v5.json`
- `/var/lib/trade/of_reports/reports/meta_status_v5.json`
- `/var/lib/trade/of_reports/reports/meta_ramp_state_v5.json`

Prometheus textfile (optional):
- `/var/lib/node_exporter/textfile_collector/meta_quality_v5.prom`
- `/var/lib/node_exporter/textfile_collector/meta_status_v5.prom`

## 3) Morning checklist

Coverage:
- `meta_quality_dq_present_n > 0` (ideally > 500)
- `meta_quality_dq_health_mean` sane (not 0/NaN)

Worst bucket:
- `meta_quality_worst_pr_auc` not worse than v3 by >0.02-0.03
- `meta_quality_worst_ece` not exploding

Correlation:
- `meta_quality_corr_meta_p_dq_health` not strongly negative (>= -0.1)

Ramp decision:
- Should NOT write to Redis (ramp dry-run). Guardrails may set freeze-latch if severe.

## 4) Next step (P19)

If SHADOW looks good:
- run ramp with apply (remove dry-run)
- start with share 0.05-0.10 for `meta_feat_v5`
- keep DQ latch enabled
