# Prometheus real deploy wiring (P59/P60 + Core Alerts)

## What this adds

- `prometheus` service (port `9090`) added to `docker-compose-crypto-orderflow.yml` on the `trade-network`
- `alertmanager` service (port `9093`) added to `docker-compose-crypto-orderflow.yml` on the `trade-network`
- `monitoring/prometheus/prometheus.yml` dedicated to the core pipeline:
  - rule_files: mounts all `/etc/prometheus/rules/orderflow_services/` configs
  - scrape: edge stack exporters (P59/P60) and self-discovery
- updated `scripts/ci_prometheus_lint.sh` to include `promtool check config` on the new `prometheus.yml`

This setup complements the existing `docker-compose-monitoring.yml` (which runs `scanner-prometheus` on port 19090 on `scanner-network`) by providing a dedicated `trade-prometheus` scoped strictly to the realtime `trade-network` with all domain-specific alerts pre-wired.

## Start

```bash
docker compose -f docker-compose-crypto-orderflow.yml up -d prometheus alertmanager
docker compose -f docker-compose-timers.yml up -d edge-stack-train-exporter-p59 edge-stack-shadow-exporter-p60
```

## Verify

Prometheus UI:
- http://127.0.0.1:9090

Check targets:
- Status → Targets (both exporters should be UP)

Check rules:
- Status → Rules (edge_stack_* alerts and others from `orderflow_services/` should load)

Reload after changing rules/config without restart:
```bash
curl -X POST http://127.0.0.1:9090/-/reload
```

## Rollback

```bash
docker compose -f docker-compose-crypto-orderflow.yml stop prometheus alertmanager
docker compose -f docker-compose-crypto-orderflow.yml rm -f prometheus alertmanager
```
