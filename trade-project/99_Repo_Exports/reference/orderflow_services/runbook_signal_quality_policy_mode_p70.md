# Runbook: Signal Quality by policy_effective_mode (P70)

## What this adds
P70 extends the **Signal Quality KPIs (P47/P64)** with a low-cardinality breakdown by **policy_effective_mode** (breaker mode):

- `ok`   — normal trading
- `warn` — degraded conditions
- `block` — severe conditions / breaker active
- `unknown` — missing/invalid mode in `trades:closed`

The worker writes per-mode KPIs into `settings:dynamic_cfg`, and the exporter surfaces them as Prometheus metrics.

## Prometheus metrics
Per mode (`mode="ok|warn|block|unknown"`):
- `signal_quality_expectancy_r_24h_by_policy_mode`
- `signal_quality_precision_top5p_24h_by_policy_mode`
- `signal_quality_ece_24h_by_policy_mode`
- `signal_quality_n_24h_by_policy_mode`

Freshness:
- `signal_quality_policy_mode_last_ts_ms`
- `signal_quality_last_ts_ms` + `signal_quality_staleness_sec` (global snapshot)

## Alerts in prometheus_alerts_signal_quality_policy_mode_p70.yml
1) **SignalQualityPolicyModeDataMissing**
- There are trades in last 24h, but ok-mode count is 0.
- Usually means **policy_effective_mode is not copied** into `trades:closed` during join.

2) **SignalQualityPolicyModeWarnMuchWorseThanOk**
- warn-mode expectancy is materially worse than ok.
- Could be genuine (regime worsening), but also a sign of **mislabeling** or **drift/calibration**.

3) **SignalQualityPolicyModeBlockWorseThanOk**
- block-mode expectancy worse than ok.
- If `block` trades exist, verify that the mode is recorded correctly and that the breaker logic matches reality.

4) **SignalQualityPolicyModeEceOkHigh**
- Calibration degraded even in ok-mode.
- Often points to distribution shift or stale calibration.

## Triage checklist
### A) Freshness and pipeline health
1. Check exporter:
   - `signal_quality_staleness_sec` (should be < 5400s unless intentionally slowed)
   - `signal_quality_policy_mode_last_ts_ms` is moving
2. Check worker logs:
   - `of_timers_worker` / the timer that runs `signal_quality_kpi_worker_v1`
3. Check Redis:
   - `XINFO STREAM trades:closed` has recent entries
   - `HGETALL settings:dynamic_cfg` contains `signal_quality_*_policy_*` keys

### B) DataMissing (ok=0)
Likely root causes:
- Joiner/close-writer doesn't propagate `policy_effective_mode` into `trades:closed`.
- Field name changed (e.g., `policy_mode`, `policy_effective_state`).

What to do:
- Inspect a few recent `trades:closed` entries and confirm the presence of `policy_effective_mode`.
- If missing: patch close-writer to carry the field from `decision:{sid}` / `decisions:final`.

### C) Warn/Block worse than ok
1. Validate sample sizes (`signal_quality_n_24h_by_policy_mode`). If small, treat as noise.
2. Compare drift/dq panels:
   - `psi_max_24h`, `feature_drift_max_z_24h`, `dq_flag_rate`, `tick_time_age_p99_ms`, `book_stale_p99_ms`
3. Validate breaker mapping:
   - Ensure `policy_effective_mode` reflects the **final effective** mode (after overrides).

Mitigations:
- Tighten gating in warn/block (reduce leverage/position sizing, require stronger confirmations).
- Recalibrate/retrain model; check champion selection and calibration pipeline.

### D) ECE ok-mode high
- Confirm predicted probability field used by KPI worker is the intended one.
- Check recent model changes and calibration artifacts.
- Trigger recalibration / retraining and compare ECE before/after.

## Notes
- This breakdown is intended to stay **low-cardinality** and safe for Prometheus.
- If your policy modes expand beyond {ok,warn,block}, map them into this set (or explicitly extend but keep cardinality bounded).
