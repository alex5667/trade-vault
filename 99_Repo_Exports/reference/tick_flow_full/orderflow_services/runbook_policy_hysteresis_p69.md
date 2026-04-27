# Runbook: Policy Hysteresis (P69)

## Overview
This runbook covers the "Circuit Breaker" policy hysteresis logic (anti-flap).
Policy determines the effective mode (`ok`, `warn`, `block`) based on `dq_state` and `drift_state`.

## Hysteresis
We use a hysteresis mechanism to prevent rapid flapping between modes.
- `CB_MIN_DWELL_S` (default 300s): Minimum time to stay in a mode after switching.
- `CB_MIN_CONSECUTIVE` (default 3): Number of consecutive ticks with new raw mode required to switch.

## Alerts

### PolicyModeBlockShareHigh
- **Condition**: > 20% of decisions in last 24h are `block`.
- **Cause**: Persistent data quality issues or significant drift.
- **Action**:
  1. Check `metrics:dq_state` and `metrics:drift_state`.
  2. Inspect specific symbol logs for `[CB] BLOCK` messages.
  3. If false positive, adjust thresholds or temporarily override via `CB_KEY_PREFIX` overrides (advanced).

### PolicyModeUnknownShareHigh
- **Condition**: > 2% `unknown` mode.
- **Cause**: Missing indicators in `decision:final` or worker parsing errors.
- **Action**:
  1. Check `decision_coverage_kpi_worker` logs.
  2. Verify `tick_processor` is populating `policy_effective_mode`.

## Force Mode / Emergency
To force a specific mode or disable hysteresis:
1. Adjust `CB_MIN_DWELL_S=0` and `CB_MIN_CONSECUTIVE=1` in ENV to disable hysteresis.
2. To force `ok` mode (bypass blocks):
   - Set `CIRCUIT_BREAKER_ENABLE=0` (if P68 supports it)
   - OR fix the underlying DQ/Drift issue.

## Debugging
Redis key for state: `cb:policy:state:{symbol}`.
Inspect:
```bash
redis-cli hgetall cb:policy:state:BTCUSDT
```
look for `mode`, `pending_mode`, `changed_at`.
