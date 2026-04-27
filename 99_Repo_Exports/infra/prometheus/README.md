# Prometheus rules & runbooks (go-worker)

This directory contains **drop-in** Prometheus alert rule files and runbooks.

## How to enable
In your Prometheus configuration, add the rules directory:

```yaml
rule_files:
  - /etc/prometheus/rules/*.yml
  - /etc/prometheus/rules/**/*.yml
```

Then mount/copy:
- `infra/prometheus/rules/bybit_dq_alerts.yml` into Prometheus rules path
- Runbook markdown can be hosted in your docs system; alerts reference the repo-relative path.

## Files
- `rules/bybit_dq_alerts.yml`: warn/crit alerts for Bybit DQ metrics
- `runbooks/bybit_dq.md`: runbook for those alerts
