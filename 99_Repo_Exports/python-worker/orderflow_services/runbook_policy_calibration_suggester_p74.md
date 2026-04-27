# P74 Runbook — Policy calibration suggester

## What this is
P74 produces **advisory** recommendations to *tighten* or *loosen* policy thresholds for `WARN` and `BLOCK` based on:
- **P71** global deltas vs `ok` (last 24h)
- **P72** worst regime-cell deltas vs `ok` (dq_state × drift_state)

P74 **does not apply** any threshold changes automatically.

## Where to find the latest suggestion
### Redis (reports)
- Markdown:
  - `GET reports:policy_calibration_suggestions:p74:last_md`
- JSON:
  - `GET reports:policy_calibration_suggestions:p74:last_json`

### cfg2 (exported keys)
All keys are written into `HGETALL settings:dynamic_cfg` (or `DYN_CFG_KEY`):
- `policy_calibration_suggest_last_ts_ms`
- `policy_calibration_suggest_staleness_sec`
- `policy_calibration_suggest_inputs_stale`
- `policy_calibration_suggest_ok_baseline_present`
- `policy_calibration_suggest_warn_action_code`, `policy_calibration_suggest_warn_severity`, `policy_calibration_suggest_warn_share_24h`
- `policy_calibration_suggest_block_action_code`, `policy_calibration_suggest_block_severity`, `policy_calibration_suggest_block_share_24h`
- `policy_calibration_suggest_unknown_share_24h`

## How the action codes work
- `action_code = -1` → **loosen** (guardrails likely too sensitive / too much coverage)
- `action_code = 0` → **no action**
- `action_code = +1` → **tighten** (guardrails allow too much harmful coverage)

`severity` is a normalized score (higher = worse impact), computed as max of:
- negative expectancy delta vs `ok`
- negative precision@top5% delta vs `ok`
- positive ECE delta vs `ok`

The score uses both **global** (P71) and **worst-regime** (P72) deltas.

## Preconditions (do not act if any fail)
1. `policy_calibration_suggest_inputs_stale == 0`
2. `policy_calibration_suggest_ok_baseline_present == 1` (enough `ok` samples)
3. `policy_calibration_suggest_unknown_share_24h` is low (target < 2%)
4. Minimum sample sizes in P70/P71 (`signal_quality_n_24h_by_policy_mode`) are satisfied

## What to do on alerts
### 1) `PolicyCalibrationSuggestionStale` / `PolicyCalibrationSuggestionInputsStale`
- Verify P71/P72 workers are enabled:
  - `ENABLE_POLICY_EFFECTIVENESS_REPORT=1`
  - `ENABLE_POLICY_REGIME_EFFECTIVENESS_REPORT=1`
- Verify P74 worker is enabled:
  - `ENABLE_POLICY_CALIBRATION_SUGGESTER_P74=1`
- Check Redis connectivity and stream lag.

### 2) `PolicyCalibrationSuggestionUnknownShareHigh`
- Root cause is almost always missing `policy_effective_mode` propagation into `decision_record`.
- Check producer-side contract fields and recent deployments.

### 3) `PolicyCalibrationSuggestTightenWarn` / `TightenBlock`
Interpretation: the current threshold is likely too permissive, and the modes that should protect you are letting through decisions with large negative deltas.

Actions:
1. Inspect reports:
   - P71: `reports:policy_effectiveness:p71:last_md`
   - P72: `reports:policy_regime_effectiveness:p72:last_md`
   - P74: `reports:policy_calibration_suggestions:p74:last_md`
2. Identify the worst regime cell (P72) and confirm it is not driven by tiny-N artifacts.
3. Tighten *one knob at a time* (small step), prefer:
   - tightening the dq/drift thresholds that define WARN/BLOCK,
   - widening quarantine / fail-closed behavior under bad data,
   - only then tightening policy thresholds.
4. Roll out with canary (small symbol set / short time window).
5. Watch for:
   - `policy_effectiveness_*` deltas improving
   - `signal_quality_*` staying stable
   - `dq_flag_rate` and staleness metrics not exploding

### 4) `PolicyCalibrationSuggestLoosenWarn` / `LoosenBlock`
Interpretation: the guardrails are likely too sensitive (high share, low harm), causing excessive WARN/BLOCK.

Actions:
1. Confirm safety: ensure no hidden worst-regime spikes (P72) and no elevated incident indicators.
2. Loosen in small steps:
   - reduce sensitivity of dq/drift detectors,
   - narrow the conditions that flip into WARN/BLOCK.
3. Verify coverage shifts:
   - expect `policy_effectiveness_share_24h{mode="warn|block"}` to decrease
   - ensure no deterioration in `expectancy` / `precision` / `ece`

## Operational notes
- The P74 worker is **best-effort** (errors are logged, loop continues).
- If P71/P72 are disabled, P74 will still write outputs but `inputs_stale` will be 1 and actions should be treated as no-op.

## Manual verification commands
```bash
# Latest P74 markdown report
redis-cli -u "$REDIS_URL" GET reports:policy_calibration_suggestions:p74:last_md

# Exported cfg2 keys
redis-cli -u "$REDIS_URL" HGETALL settings:dynamic_cfg | egrep 'policy_calibration_suggest|policy_effectiveness_last_ts_ms|policy_regime_effectiveness_last_ts_ms'
```
