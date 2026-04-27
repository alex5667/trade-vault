# Phase 3.24 — Route Incident RCA Mirror RCA Winner Apply Apply Controller

## Цель
Автоматический A/B переход (экспериментальный роллоут).
Controller читает рекомендации от Evaluator (Phase 3.23). Если это `PROMOTE...`, контроллер меняет конфигурацию Harness (Phase 3.22), переключая Primary Arm на модель-победителя.

## Поддерживаемые стратегии (STRATEGY):
- `SHADOW_PRIMARY` — режим остаётся `SHADOW`, инкумбент уходит в тень (становится shadow), победитель становится Primary. Это безопасно, так как мы продолжаем зеркалировать запросы в старую модель.
- `SINGLE_ARM` — жёсткий переход: выключаем рассылку в тени, оставляем победителя единственным Primary ресурсом (экономит compute).

## Safe start
```bash
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_CONTROLLER_ADVISORY_ONLY=1
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_CONTROLLER_EXECUTOR_MODE=DRY_RUN
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_CONTROLLER_STRATEGY=SHADOW_PRIMARY
```
При этих настройках он будет только писать `Will apply: ...` в журнал, но не поменяет конфигурацию в Redis (`DRY_RUN`).

## Smoke checks
```bash
curl -s localhost:9946/metrics | grep '^ml_route_incident_rca_mirror_rca_winner_apply_controller_'
redis-cli HGETALL metrics:ml:route_incident_rca_mirror_rca_winner_apply_controller:last
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_rca_winner_apply_controller_decisions + - COUNT 5
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_rca_winner_apply_controller_journal + - COUNT 5
```
