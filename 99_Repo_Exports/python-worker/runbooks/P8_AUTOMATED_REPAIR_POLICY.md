# P8 Automated Repair Policy

## Purpose
Run consistency check → SQL mirror repair → re-check → quarantine remaining critical `sid` values.

## Dry-run
```bash
python scripts/automated_execution_repair_policy.py --dry-run
```

## Apply
```bash
python scripts/automated_execution_repair_policy.py
```

## Safety rules
- Never changes live Binance positions.
- Repairs only SQL mirrors.
- Quarantine blocks future publish/execution for matching `sid` values through denylist integration.
- All runs should be mirrored to `execution_repair_runs` and `execution_quarantine_ledger` when DSN is configured.
