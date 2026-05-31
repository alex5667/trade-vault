# Phase 3.26 — Route Incident RCA Mirror RCA Winner Apply Governance

## Цель
Поддержка Governance-слоя для контура Winner Apply (отвечает за роллауты победивших моделей).

Он состоит из трёх независимых процессов:
1. `_slo_analytics`: вычисляет агрегаты на основе `verification` и `apply` (MTTR, Apply Rate).
2. `_retry_controller`: отвечает за повторную попытку применить откат в Redis, если первый откат был проигнорирован/перетёрт внешним скриптом.
3. `_auto_escalation_summarizer`: сводит воедино статусы и поднимает PagerDuty Alerts через severity (`info`/`warning`/`critical`).

## Smoke checks
```bash
curl -s localhost:9948/metrics | grep '^ml_route_incident_rca_mirror_rca_winner_apply_apply_'
curl -s localhost:9949/metrics | grep '^ml_route_incident_rca_mirror_rca_winner_apply_apply_retry_'
curl -s localhost:9950/metrics | grep '^ml_route_incident_rca_mirror_rca_winner_apply_apply_escalations_'
redis-cli HGETALL metrics:ml:route_incident_rca_mirror_rca_winner_apply_apply_slo:last
redis-cli HGETALL metrics:ml:route_incident_rca_mirror_rca_winner_apply_apply_retry:last
```
