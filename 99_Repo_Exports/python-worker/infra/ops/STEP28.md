# Step 28 - Auto-Apply Orchestrator (gated by tick-gate block)

## What it does
Runs your auto-apply runner as a scheduled job, but **skips execution** when
`cfg:suggestions:entry_policy:auto_apply_block:tick_gate=1` is set (Step 25/26).

It also logs every decision (skip/run + rc + duration) into Redis Stream:
`ops:auto_apply_runs` (configurable via `AUTO_APPLY_OPS_STREAM`).

## Install (systemd)
1) Copy env example and edit:
```bash
sudo cp python-worker/infra/ops/auto_apply_job.env.example /etc/default/auto-apply-job
sudo nano /etc/default/auto-apply-job
```

2) Install unit+timer:
```bash
sudo cp python-worker/infra/systemd/auto-apply-job.service /etc/systemd/system/
sudo cp python-worker/infra/systemd/auto-apply-job.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now auto-apply-job.timer
```

3) Test a single run:
```bash
sudo systemctl start auto-apply-job.service
journalctl -u auto-apply-job.service -n 50 --no-pager
```

## Exit codes
- 0: apply succeeded
- 10: apply ran but failed
- 20: skipped due to tick-gate block
- 21: skipped due to block-check error (fail-closed)
- 30: misconfiguration

## Observability
Stream `ops:auto_apply_runs` contains JSON lines with:
- status: skipped|ok|fail
- reason/err/meta (for skipped)
- runner_rc, dur_ms, stdout_tail (for executed runs)

Use Step 21 style daily report approach for this stream if needed.
