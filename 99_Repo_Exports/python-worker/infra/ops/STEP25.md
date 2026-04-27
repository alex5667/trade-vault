# Step 25: Auto-apply block by tick-quality gate

## What it does

`tools.auto_apply_tick_gate_blocker` runs the tick-quality gate periodically and publishes:

Redis keys (default prefix):
- `cfg:suggestions:entry_policy:auto_apply_block:tick_gate` = "1" (when blocked)
- `cfg:suggestions:entry_policy:auto_apply_block:tick_gate:meta` = JSON meta
- `cfg:suggestions:entry_policy:auto_apply_block:tick_gate:ts_ms` = timestamp

Ops stream:
- `ops:auto_apply_tick_gate` (compact events)

Prometheus metrics:
- `auto_apply_tick_gate_blocked`
- `auto_apply_tick_gate_events_total{status}`
- `auto_apply_tick_gate_fail_reasons_total{reason}`

## Integration options

### Option A (recommended): Make ApplyRunner respect the Redis key

At the very beginning of your auto-apply job, check:
`cfg:suggestions:entry_policy:auto_apply_block:tick_gate`
If present => skip applying and log reason from `:meta`.

### Option B: Wrap the auto-apply command

Use `tools.run_auto_apply_with_tick_gate`:

```bash
python -m tools.run_auto_apply_with_tick_gate -- python -m tools.<your_apply_runner>
```

It checks the key and returns exit code 20 if blocked.

## systemd install

```bash
sudo cp python-worker/infra/ops/auto_apply_tick_gate_blocker.env.example /etc/default/auto-apply-tick-gate-blocker
sudo nano /etc/default/auto-apply-tick-gate-blocker

sudo cp python-worker/infra/systemd/auto-apply-tick-gate-blocker.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now auto-apply-tick-gate-blocker.service
```

## Docker compose

If you prefer compose, run the module with:
`python -m tools.auto_apply_tick_gate_blocker`
and expose port 9114 for Prometheus.

## Rollback

Stop the blocker service and delete the key:

```bash
redis-cli DEL cfg:suggestions:entry_policy:auto_apply_block:tick_gate
redis-cli DEL cfg:suggestions:entry_policy:auto_apply_block:tick_gate:meta
redis-cli DEL cfg:suggestions:entry_policy:auto_apply_block:tick_gate:ts_ms
```
