# Phase 2.14 — Routing Incident RCA Winner Routing Apply Controller

## Цель
Автоматическое применение результатов A/B экспериментов (Phase 2.13). Контроллер применяет строгие проверки (sample size, uplift, cooldown) и в случае успеха переписывает дефолтную routing policy.

## Компоненты 
1. `operator_routing_incident_rca_winner_routing_apply_controller_v2_14.py`

## Потоки
- in: `stream:ml:operator_routing_incident_rca_experiment_winner_decisions`
- out: `stream:ml:operator_routing_incident_rca_routing_apply_results`
- audit: `stream:ml:operator_routing_incident_rca_routing_apply_audit`
- global policy hash: `cfg:ml:operator_routing_incident_rca_routing:default`

## Smoke checks
```bash
# Проверка результатов
redis-cli XREVRANGE stream:ml:operator_routing_incident_rca_routing_apply_results + - COUNT 5

# Прямая проверка policy hash
redis-cli HGETALL cfg:ml:operator_routing_incident_rca_routing:default

# Метрики прометеуса (порт 9893)
curl -s localhost:9893/metrics | grep '^ml_operator_routing_incident_rca_winner_routing_apply_'
```

## Guardrails
- В дефолте `EXECUTOR_MODE=DRY_RUN` и `ADVISORY_ONLY=1`.
- Если `MIN_SAMPLE` < 8, reject с `insufficient_sample`.
- Если `winner_score` < `control_score` + `MIN_UPLIFT`, reject с `insufficient_uplift`.
- Если `kill_switch_active`, reject.
