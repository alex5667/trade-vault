# Alert inhibition (reduce noise)

## Goal

Avoid notification spam when a `critical` alert is already firing for the same scope.

## Rules

Configured in `monitoring/alertmanager/alertmanager.yml`:

- When `severity=critical` is firing → inhibit `severity=warning` for equal:
  `alertname, team, component, job, instance`
- Same for `severity=info`

## Verify

1) Fire a critical alert (manual test):
```bash
SEVERITY=critical TEAM=trade COMPONENT=edge_stack ./scripts/send_test_alert_to_alertmanager.sh
```

2) Fire a warning alert with the same labels:
```bash
SEVERITY=warning TEAM=trade COMPONENT=edge_stack ./scripts/send_test_alert_to_alertmanager.sh
```

Expected:
- Telegram receives **critical**
- warning is **inhibited** (visible in Alertmanager UI but not notified)

## Tuning

If you want "failed inhibits stale" specifically, use matcher by alertname pairs
instead of severity-wide inhibit (see commented example in alertmanager.yml).
