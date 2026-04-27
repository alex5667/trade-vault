# Prometheus rules bundle (v1) — tick_flow_full mirror

Manifest: `orderflow_services/prometheus_rules_bundle_manifest_v1.yml`

Example `prometheus.yml` snippet (if you mount `tick_flow_full/` as the rules root):
```yaml
rule_files:
  - /etc/prometheus/rules/orderflow_services/prometheus_alerts_*.yml
  - /etc/prometheus/rules/services/orderflow/prometheus_alerts_*.yml
```
