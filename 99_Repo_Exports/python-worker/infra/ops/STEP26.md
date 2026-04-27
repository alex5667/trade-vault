# Step 26 — integrate tick-gate block into ApplyRunner + anti-flap

## What you got
- `tools.auto_apply_tick_gate_blocker_v2`: daemon that updates Redis block keys using tick gate check (anti-flap)
- `services.orderflow.auto_apply_guard.assert_auto_apply_not_blocked()`: 3-line drop-in for any ApplyRunner
- `tools.run_auto_apply_with_tick_gate_v2`: wrapper that enforces the guard before running a command

## Recommended rollout
1) Run blocker v2 as systemd service:
   - copy `python-worker/infra/ops/auto_apply_tick_gate_blocker_v2.env.example`
     to `/etc/default/auto-apply-tick-gate-blocker-v2` and edit.
   - copy unit `python-worker/infra/systemd/auto-apply-tick-gate-blocker-v2.service`
     to `/etc/systemd/system/`.
   - `systemctl daemon-reload && systemctl enable --now auto-apply-tick-gate-blocker-v2.service`

2) Enforce inside ApplyRunner (preferred):
   At the very beginning of your apply entrypoint (before changing active arms):

       from services.orderflow.auto_apply_guard import assert_auto_apply_not_blocked
       assert_auto_apply_not_blocked()

   This makes the apply self-defensive even if run outside wrappers.

3) Or enforce via wrapper:
   `python -m tools.run_auto_apply_with_tick_gate_v2 -- <your apply command>`

## Redis keys
prefix: `AUTO_APPLY_BLOCK_PREFIX` (default `cfg:suggestions:entry_policy:auto_apply_block`)
- `{prefix}:tick_gate` (string "1", TTL)
- `{prefix}:tick_gate:meta` (JSON, freshest status)
- `{prefix}:tick_gate:ts_ms` (last update)

## Anti-flap knobs
- `AUTO_APPLY_BLOCK_MIN_HOLD_S`: minimum time to stay blocked once a FAIL happens.
- `AUTO_APPLY_UNBLOCK_PASS_STREAK`: consecutive PASS required to unblock.
- `AUTO_APPLY_BLOCK_REASON_PIN_S`: keep the first fail reason for this time window.

## Exit codes
- Guard / wrapper exit code for BLOCKED is `20`.
