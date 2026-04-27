# Phase 2.8 — Operator RCA Routing Incident Bundle Builder

## Цель
Построить компактный forensic bundle по `route_change_id` для RCA routing policy.

## Источники
- `stream:ml:operator_rca_routing_apply_results`
- `stream:ml:operator_rca_routing_verify_results`
- `stream:ml:operator_rca_routing_rollback_requests`
- `stream:ml:operator_rca_routing_rollback_results`
- `stream:ml:operator_rca_routing_rollback_journal`
- `stream:ml:operator_rca_routing_retry_requests`
- `stream:ml:operator_rca_routing_escalations`
- `stream:ml:operator_rca_routing_slo_rollups`
- `stream:ml:operator_rca_routing_apply_audit`

## Выходы
- `stream:ml:operator_rca_routing_incident_bundle_results`
- `metrics:ml:operator_rca_routing_incident_bundle:last`
- `metrics:ml:operator_rca_routing_incident_bundle:<route_change_id>`
- `llm_operator_rca_routing_incident_bundles`

## Smoke checks
```bash
redis-cli XADD stream:ml:operator_rca_routing_incident_bundle_requests * route_change_id rc-1 ts_ms $(date +%s%3N)
redis-cli XREVRANGE stream:ml:operator_rca_routing_incident_bundle_results + - COUNT 3
redis-cli HGETALL metrics:ml:operator_rca_routing_incident_bundle:last
curl -s localhost:9883/metrics | grep '^ml_operator_rca_routing_incident_bundle_'
```

## Rollback
- остановить `scanner-operator-rca-routing-incident-bundle-builder-v2-8`
- таблицу и stream results не удалять

## Notes
- scope только `scanner_infra`
- hot path не затронут
- bundle пригоден для operator review и дальнейшего Vertex RCA
