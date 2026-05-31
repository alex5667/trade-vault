# Prometheus rules bundle (v1)

## What
A small, explicit **include list** used for deploying Prometheus rules from this repo.

## Where
- Manifest: `orderflow_services/prometheus_rules_bundle_manifest_v1.yml`
- Rule files live under:
  - `orderflow_services/prometheus_alerts_*.yml`
  - `ok_rate_logic/prometheus_alerts_*.yml`
  - `services/orderflow/prometheus_alerts_*.yml`

## How to deploy
1) Copy the repo (or these subfolders) into your Prometheus rules directory.
2) Add the manifest globs into `prometheus.yml` under `rule_files`.

Example:
```yaml
rule_files:
  - /etc/prometheus/rules/orderflow_services/prometheus_alerts_*.yml
  - /etc/prometheus/rules/ok_rate_logic/prometheus_alerts_*.yml
  - /etc/prometheus/rules/services/orderflow/prometheus_alerts_*.yml
```

## Why this exists
So new alert files (e.g. slippage calibrator health) are included automatically without
editing your Prometheus config every time.
