# Grafana dashboard: Edge Stack (P59 + P60)

## What you get

Provisioned dashboard (no manual clicks):
- UID: `edge_stack_overview`
- Title: `Edge Stack Overview (P59 Train + P60 Shadow)`

Panels:
- P59 train success + staleness + brier/ece
- P60 shadow success + staleness + champion brier

## Start

```bash
docker compose -f docker-compose-crypto-orderflow.yml up -d grafana
```

UI:
- http://127.0.0.1:3000
- login: `${GRAFANA_ADMIN_USER:-admin}` / `${GRAFANA_ADMIN_PASSWORD:-admin}`

Dashboard direct link:
- http://127.0.0.1:3000/d/edge_stack_overview/edge-stack-overview?orgId=1

## Verify

1) Data source exists: **Prometheus** (provisioned)
2) Dashboard loads and shows non-empty metrics
3) Prometheus targets are UP:
   - edge-stack-train-exporter-p59
   - edge-stack-shadow-exporter-p60

## Notes

- If Prometheus is renamed, update datasource URL in:
  `monitoring/grafana/provisioning/datasources/prometheus.yml`
