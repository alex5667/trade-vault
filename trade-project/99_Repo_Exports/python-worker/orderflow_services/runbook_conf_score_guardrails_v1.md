# Runbook: Confidence score guardrails (v2 - World Practice)

## What this is

This guardrail closes the loop between **offline drift monitoring** of confidence parts
and **online, low-latency scoring**. It can automatically:

- **Freeze** regime-aware weight shaping (`confidence_score_freeze=1`) when drift is high.
- **Scale down** the resulting confidence (`confidence_score_scale<1.0`) during elevated drift.

**v2 Improvements**:
- **Hysteresis**: Latch freeze for `N` seconds (`--freeze-hold-sec`) to prevent flapping.
- **Recovery Ramp**: Require `N` stable runs (`--recover-runs`) and ramp scale slowly (`--recover-scale-step`).
- **Canary**: Apply only to a subset of symbols (`--canary-share`) for safe rollout.

The goal is to **fail-closed** (become more conservative) without stopping the engine.

## Components

1. Drift report generator: `ml_analysis/tools/confidence_parts_drift_report_v1.py`
2. Guard decision + apply: `orderflow_services/conf_score_guardrails_apply_v1.py`
3. Prometheus exporter: `orderflow_services/conf_score_guard_state_exporter_v1.py`
4. Alert rules: `orderflow_services/prometheus_alerts_conf_score_guardrails_v1.yml`

## Decision policy

- `crit_z` (default 6.0): set `freeze=1`, `scale=0.85` + **Latch** for 1h (default).
- `warn_z` (default 4.0): set `freeze=0`, `scale=0.92` (if not latched).
- Recovery:
  - Must be stable (`max_abs_z <= recover_z`) for `--recover-runs` (def 3) consecutive checks.
  - Scale ramps up from `0.92` by `0.05` steps back to `1.0`.

## Where overrides live

The engine reads per-symbol JSON overrides from Redis:

- Key: `cfg:crypto_of:overrides:{SYMBOL}`
- Allowlisted keys (see `services/orderflow_strategy.py`):
  - `confidence_score_freeze`
  - `confidence_score_scale`
  - plus regime/data-health knobs (optional)

## How to run (manual)

1. Generate drift report (example):

```bash
python -m ml_analysis.tools.confidence_parts_drift_report_v1 \
  --in /path/to/confidence_parts.jsonl \
  --out /tmp/conf_parts_drift.json \
  --group-by symbol
```

2. Decide/apply overrides (Dry Run / Canary):

```bash
export REDIS_URL=redis://localhost:6379/0
python orderflow_services/conf_score_guardrails_apply_v1.py \
  --drift-report /tmp/conf_parts_drift.json \
  --apply 1 \
  --redis-url "$REDIS_URL" \
  --state-path /tmp/conf_score_guard_state.json \
  --canary-share 0.2 \
  --freeze-hold-sec 3600
```

3. Run exporter:

```bash
export CONF_SCORE_GUARD_STATE_PATH=/tmp/conf_score_guard_state.json
export CONF_SCORE_GUARD_EXPORTER_PORT=9135
python orderflow_services/conf_score_guard_state_exporter_v1.py
```

## What to check when alerts fire

### 1) `ConfScoreGuardHighDrift`
- Open the drift report referenced by the latest guard state.
- Identify which **part keys** have the highest `|dz|`.
- Cross-check `data_health` / tick quality metrics: time age, staleness, dq flags.

### 2) `ConfScoreGuardFreezeActive` / `ConfScoreGuardFreezeLatched`
- **Latch** means the symbol is holding freeze even if drift dropped, to prevent flapping.
- Check `conf_score_guard_latch_remaining_sec` metric.
- This is normal behavior after a spike.

### 3) `ConfScoreGuardRecoveryStuck`
- Symbol is stable but not unfreezing.
- Check if `recover-runs` is too high or if the job is running too infrequently.

### 4) Recovery
- If drift returns to normal, the system will auto-recover after latch expires + stable runs.

## Rollback / Emergency

To immediately disable the guard effect:

```bash
# 1. Stop the apply timer (if running)
# 2. Reset Redis keys
redis-cli GET cfg:crypto_of:overrides:BTCUSDT
# edit JSON to set:
#   confidence_score_freeze = 0
#   confidence_score_scale = 1.0
```

Or run the apply script with `--apply 1 --canary-share 0.0` (this will skip everyone, effectively stopping updates, but won't revert existing keys unless logic changes).

To **revert** existing keys, you might need to manually set them or use a cleanup script. The current script does NOT auto-delete keys for skipped symbols (canary=0), it just ignores them.
