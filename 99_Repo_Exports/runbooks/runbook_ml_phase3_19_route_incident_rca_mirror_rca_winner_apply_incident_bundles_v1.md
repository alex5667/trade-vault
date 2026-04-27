# Phase 3.19 — Route Incident RCA Mirror RCA Winner Apply Incident Bundle Builder

## Цель
Создает единый Forensic-пакет для контура `winner-apply`. Слушает триггеры внутри Governance и Winner Apply слоев (повышение статуса эксперимента, срабатывание авто-отката, рост MTTR и эскалации) и собирает контекст из смежных стримов (`journal`, `verification_results`, `retry_results`, `slo_rollups`). В итоге выдает цельный JSON Bundle.

Такой бандл сильно облегчает RCA: можно отдать его LLM (или человеку) со словами "Вот история последних 50 событий по контуру, скажи мне, почему сработал ROLLBACK на 5-ой минуте эксперимента".

## Safe start
```bash
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_INCIDENT_BUNDLES_LOOKBACK_COUNT=50
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_INCIDENT_BUNDLES_ONLY_SEVERITY=warning,critical
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_INCIDENT_BUNDLES_TRIGGER_ON_APPLY_DECISIONS=APPLY_PRIMARY_ARM_SHADOW,APPLY_SINGLE_ARM
```

## Smoke checks
```bash
curl -s localhost:9940/metrics | grep '^ml_route_incident_rca_mirror_rca_winner_apply_incident_bundles_'
redis-cli HGETALL metrics:ml:route_incident_rca_mirror_rca_winner_apply_incident_bundles:last
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_rca_winner_apply_incident_bundles + - COUNT 5
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_rca_winner_apply_incident_bundles_audit + - COUNT 5
```
