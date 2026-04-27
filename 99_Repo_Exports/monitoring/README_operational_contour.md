# Operational contour: alerts → Telegram → runbooks

## What changed
1) Alerts include:
- `team` / `component` labels for routing
- `runbook` annotation (short inline text, shown in Telegram)
- `dashboard` annotation (quick PromQL jump)

2) Telegram routing:
- `severity=critical` repeats every 30m
- `severity=warning` is muted during quiet hours (Mon-Fri 23:00–07:00 Europe/Zaporozhye)

3) Inhibition:
- If `critical` is firing for the same scope (`alertname/team/component/job/instance`),
  corresponding `warning/info` alerts are suppressed.

4) Promote freeze operations:
- State in Redis: `cfg:edge_stack:promote_freeze`
- CLI:
  - `python -m ml_analysis.tools.promote_freeze_ctl status|set|clear`
- Audit events stream: `ops:eventlog`

## Runbooks
- `monitoring/runbooks/edge_stack_train_p59.md`
- `monitoring/runbooks/edge_stack_shadow_p60.md`
