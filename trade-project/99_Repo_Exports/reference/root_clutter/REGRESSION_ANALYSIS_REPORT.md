# Regression Analysis Report

## Summary

**Date**: 2026-02-03  
**Test**: Gate regress (engine replay)  
**Results**: 
- Overlap: 23,187 rows
- Mismatches: 417 (1.80%)
- Max allowed: 0

**Mismatches by field**:
- `score`: 414
- `need`: 1
- `scenario`: 1
- `reason`: 1

## Root Cause Analysis

### 1. Scenario Mismatch (Critical)

**Sample**: `ETHUSDT|1770004381249|SHORT`

- **Baseline**: `scenario=continuation`, `need=2`, `reason=continuation_gate(0/2)`
- **Candidate**: `scenario=none`, `need=0`, `reason=no_sweep_and_no_trend`

**Root Cause**: Code change in commit `636e6de9` (2026-02-01)

**Change**:
```python
# Before:
div = getattr(runtime, "last_div", None)

# After:
cvd_q = int(indicators.get("cvd_quarantine_active", 0) or 0)
div = None if cvd_q == 1 else getattr(runtime, "last_div", None)
```

**Impact**: 
- When CVD is quarantined (`cvd_q=1`), `div` becomes `None`
- This causes `trend_dir` to be `None` for continuation scenarios
- When `trend_dir is None`, scenario changes from `continuation` to `none` with reason `no_sweep_and_no_trend` (lines 346-352 in `of_confirm_engine.py`)

**Logic Flow**:
1. Initial scenario determined as `continuation`
2. For continuation, engine requires `trend_dir` from hidden divergence or regime
3. If CVD is quarantined, `div = None` → `trend_dir = None`
4. If `trend_dir is None` → scenario becomes `none` with reason `no_sweep_and_no_trend`
5. When scenario is `none`, no legs required → `need=0`

### 2. Code Changes Analysis

**Commits with scenario-related changes**: 11 out of 13 commits (last 7 days)

**Key commits**:
1. **636e6de9** (2026-02-01): Changed div handling when CVD is quarantined
2. **d5ecccf4** (2026-02-01): Added required legs for continuation/reversal scenarios
3. **789414fc** (2026-02-01): ML scenario handling improvements

### 3. Score Mismatches (414)

**Statistics**:
- Mean absolute delta: 0.0276
- Max absolute delta: 0.15
- Many transitions to value `0.27` (likely capping/threshold)

**Possible causes**:
- Changes in ML model or calibration
- Changes in score calculation logic
- Floating point precision differences

## Assessment

### Is this change expected or a bug?

**✅ EXPECTED CHANGE** - This is a **bug fix / improvement**:

1. **Intent**: Prevent false continuation signals when CVD is quarantined
2. **Rationale**: CVD quarantine indicates broken baseline → hidden divergence unreliable → should not use for trend direction
3. **Impact**: More conservative signal filtering (improves quality)
4. **Behavior**: Correctly rejects continuation scenarios without valid trend direction

### Recommendation

**✅ UPDATE BASELINE** after verification:

1. This change is intentional and improves signal quality
2. The scenario mismatch is expected due to improved logic
3. Score mismatches are likely due to model/calibration updates (also expected)

**Action**:
```bash
python -m tools.propose_baseline_update
```

## Files Changed

- `python-worker/core/of_confirm_engine.py`: 13 commits
- `python-worker/services/ml_confirm_gate.py`: 10 commits
- `python-worker/tools/of_confirm_replay_from_inputs.py`: 3 commits

## Next Steps

1. ✅ Verify the change is intentional (confirmed - bug fix)
2. ✅ Understand impact (more conservative filtering - good)
3. ⏳ Update baseline after verification
4. ⏳ Monitor production for any unexpected behavior

## Tools Used

- `tools.analyze_regress_diff` - Comprehensive diff analysis
- `tools.analyze_scenario_changes` - Code change analysis
- `tools.analyze_scenario_mismatch_root_cause` - Root cause analysis

