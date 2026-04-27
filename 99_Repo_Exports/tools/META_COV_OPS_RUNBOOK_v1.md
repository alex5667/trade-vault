# P36: Meta Coverage Operations Runbook

## Overview
This runbook covers the operation of the nightly meta coverage enforcement bundle, including the new preflight checks (P36).

## Tools
- `tools/nightly_meta_enforce_cov_ops_bundle_v1.py`: Main orchestrator.
- `tools/meta_cov_ops_validate_v1.py`: Preflight validator.
- `tools/meta_cov_rollout_controller_v1.py`: Updates rollout buckets.
- `tools/meta_cov_outcome_auto_apply_v1.py`: Applies outcomes to config.
- `tools/meta_cov_quarantine_monitor_v1.py`: Monitors quarantine stats.

## Preflight Check (P36)
Before any operation, the bundle runs `meta_cov_ops_validate_v1`.
- **RC=0 (OK)**: Proceed normally.
- **RC=2 (Soft Block)**: Insufficient data (e.g., empty streams). Bundle forces `apply_effective=0` (Dry-Run).
- **RC=1 (Hard Fail)**: Infrastructure issue (Redis/Config). Bundle exits with error.

## Manual Operations

### 1. Run Preflight Check Manually
```bash
python3 -m tools.meta_cov_ops_validate_v1
# Check exit code
echo $?
```

### 2. Run Bundle in Dry-Run Mode
```bash
META_COV_BUNDLE_APPLY=0 python3 -m tools.nightly_meta_enforce_cov_ops_bundle_v1 --print-json
```

### 3. Run Bundle in Apply Mode
```bash
META_COV_BUNDLE_APPLY=1 python3 -m tools.nightly_meta_enforce_cov_ops_bundle_v1 --print-json
```
*Note: Will automatically downgrade to dry-run if preflight returns RC=2.*

## Troubleshooting

### "Preflight returned SOFT-BLOCK (rc=2)"
- Check if `metrics:of_gate` (or configured source) has data.
- Check if `events:trades` has closed trades with `r_mult`.
- If new environment, wait for data to accumulate.

### "Preflight FAILED (rc=1)"
- Check Redis connection (`REDIS_URL`).
- Check if `settings:dynamic_cfg` exists in Redis.

## Configuration (ENV)
- `META_COV_SOURCE_STREAM`: Source metric stream (default: `metrics:of_gate`).
- `TRADE_EVENTS_STREAM`: Trade tokens stream (default: `events:trades`).
- `META_COV_PREFLIGHT_MIN_OF_GATE`: Min entries in source (default: 200).
- `META_COV_PREFLIGHT_MIN_TRADES`: Min entries in trades (default: 50).
