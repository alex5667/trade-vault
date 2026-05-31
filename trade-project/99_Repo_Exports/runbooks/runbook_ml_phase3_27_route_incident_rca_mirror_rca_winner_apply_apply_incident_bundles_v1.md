# Phase 3.27 — Route Incident RCA Mirror RCA Winner Apply Apply Incident Bundles

## Цель
Единый "Forensic Bundle Builder" контур для этапов "Winner Apply Governance".
Отслеживает ситуации применения политик, сбоев роллаутов (rollbacks) и критических эскалаций в слое Governance. При наступлении этих событий, он делает агрегированный срез (bundle) всех метрик (apply slo, retry count, verifications match) и отдаёт на детальный анализ для RCA.

## Smoke checks
```bash
curl -s localhost:9951/metrics | grep '^ml_route_incident_rca_mirror_rca_winner_apply_apply_incident_bundles_'
redis-cli HGETALL metrics:ml:route_incident_rca_mirror_rca_winner_apply_apply_incident_bundles:last
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_incident_bundles + - COUNT 5
```
