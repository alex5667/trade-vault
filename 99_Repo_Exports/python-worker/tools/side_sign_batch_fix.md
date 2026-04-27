# Side-sign batch fix from audit (Step 12)

Goal: remove silent BUY/SELL bias when `side` is missing/UNKNOWN by upgrading `side -> sign` conversions to tri-state
or canonical helper `side_sign_from_tick()`.

## Workflow

From repo root:

1) Run audit and save JSON:
```bash
python -m tools.audit_side_sign_usage --root . --format json > /tmp/side_audit.json
```

2) Generate a unified diff patch (dry-run):
```bash
python -m tools.patch_from_side_audit --root . --audit /tmp/side_audit.json --out /tmp/side_sign_patch.diff
```

3) Apply patch:
```bash
git apply /tmp/side_sign_patch.diff
```

Alternatively, apply in-place with backups:
```bash
python -m tools.patch_from_side_audit --root . --audit /tmp/side_audit.json --apply --backup
```

## What it fixes (conservative)

- `x = 1 if side == "BUY" else -1` -> `x = (1 if side=="BUY" else (-1 if side=="SELL" else 0))`
- `x = -1 if side != "BUY" else 1` -> same tri-state
- `x = 1 if tick.get("side")=="BUY" else -1` -> `x = side_sign_from_tick(tick)[0]` (+ import injection)
- `x = -1 if tick.get("is_buyer_maker") else 1` -> `x = side_sign_from_tick(tick)[0]` (+ import injection)
- `... or "BUY"` fallbacks on side-lines -> `... or "UNKNOWN"`

If a line has changed since the audit run, it will be skipped (safety).

