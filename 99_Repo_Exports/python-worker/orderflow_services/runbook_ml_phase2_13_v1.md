# Phase 2.13 — Routing Incident RCA Experiment Harness

## Цель
А/Б роуминг экспериментов (experiment router) + выбор победителя (winner selector) для Routing Incident RCA пайплайна. Позволяет сравнивать качество control (baseline provider/model) против challenger (новый provider/model/prompt) на основе usefulness_score и quality_score.

## Компоненты 
1. `operator_routing_incident_rca_experiment_router_v2_13.py` (роутер А/Б)
2. `operator_routing_incident_rca_experiment_winner_selector_v2_13.py` (выбор победителя)

## Потоки
- in: `stream:ml:operator_rca_routing_rca_requests_routed` (от маршрутизатора Phase 2.12)
- out: `stream:ml:operator_routing_incident_rca_requests_experimented`
- out: `stream:ml:operator_routing_incident_rca_exposures`
- out: `stream:ml:operator_routing_incident_rca_experiment_winner_decisions`
- audit: `stream:ml:operator_routing_incident_rca_experiment_audit`
- REDIS hash: `cfg:ml:operator_routing_incident_rca_experiment:winner:<id>`

## База данных
- `llm_operator_routing_incident_rca_exposures`

## Smoke checks
```bash
# Проверка отправки exposures
redis-cli XREVRANGE stream:ml:operator_routing_incident_rca_exposures + - COUNT 5

# Проверка побед
redis-cli XREVRANGE stream:ml:operator_routing_incident_rca_experiment_winner_decisions + - COUNT 5

# Audit логи экспериментов
redis-cli XREVRANGE stream:ml:operator_routing_incident_rca_experiment_audit + - COUNT 10

# Метрики прометеуса для router (порт 9891)
curl -s localhost:9891/metrics | grep '^ml_operator_routing_incident_rca_experiment_'

# Метрики прометеуса для winner selector (порт 9892)
curl -s localhost:9892/metrics | grep '^ml_operator_routing_incident_rca_experiment_'
```

## Guardrails
- В дефолте `ML_OPERATOR_ROUTING_INCIDENT_RCA_EXPERIMENT_ENABLE=1` для сбора базовых данных.
- Winner decisions имеют `advisory_only=1`.
- Отбор работает только если собрана минимальная статистика (`MIN_SAMPLE=8` из базы Postgres).
