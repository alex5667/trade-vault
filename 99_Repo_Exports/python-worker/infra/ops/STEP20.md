# Step 20: Wire Tick-Quality Gate into orchestration

This step provides ready-to-use templates to run your ramp command only when tick-quality is healthy.

## Option A (recommended): systemd timer + oneshot service

1) Copy templates:
   - `python-worker/infra/systemd/tick-quality-gated-ramp.service`
   - `python-worker/infra/systemd/tick-quality-gated-ramp.timer`

2) Install:
```bash
sudo cp python-worker/infra/systemd/tick-quality-gated-ramp.service /etc/systemd/system/
sudo cp python-worker/infra/systemd/tick-quality-gated-ramp.timer /etc/systemd/system/
sudo systemctl daemon-reload
```

3) Configure environment:
```bash
sudo cp python-worker/infra/ops/tick_quality_gate.env.example /etc/default/tick-quality-gated-ramp
sudo nano /etc/default/tick-quality-gated-ramp
```

4) Enable:
```bash
sudo systemctl enable --now tick-quality-gated-ramp.timer
sudo systemctl start tick-quality-gated-ramp.service
```

## Option B: cron

See `python-worker/infra/ops/cron_tick_quality_gated_ramp.example`.

## Logging / Auditing

If you set:
```
REDIS_URL=redis://redis-worker-1:6379/0
TICK_GATE_PUBLISH_REDIS=1
TICK_GATE_REDIS_STREAM=ops:tick_quality_gate
```

then every run emits a small JSON report to Redis for later debugging.

## Recommended rollout

1) Start with `TICK_GATE_FAIL_MODE=fail_open` and conservative thresholds.
2) Observe for 1-2 days.
3) Switch to `fail_closed` only after you are confident the metrics are stable and always present.
