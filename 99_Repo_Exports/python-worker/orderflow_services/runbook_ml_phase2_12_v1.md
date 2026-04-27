# Phase 2.12 — Routing Incident RCA Routing Controller

## Цель
Централизованный маршрутизатор (controller) для запросов на Routing Incident RCA.
Читает решения usefulness_governor'a (Phase 2.11) и выбирает, какому LLM-провайдеру и какой модели отдавать запрос на построение RCA.

## Компоненты
- `operator_routing_incident_rca_routing_controller_v2_12.py`

## Потоки
- in: `stream:ml:operator_rca_routing_rca_requests`
- out: `stream:ml:operator_rca_routing_rca_requests_routed`
- out: `stream:ml:operator_routing_incident_rca_routing_decisions`
- audit: `stream:ml:operator_routing_incident_rca_routing_audit`
- REDIS hash: `metrics:ml:operator_routing_incident_rca_routing:last`

## База данных
- `llm_operator_routing_incident_rca_routing_decisions`

## Smoke checks
```bash
# Проверка отправки маршрутизированных запросов
redis-cli XREVRANGE stream:ml:operator_rca_routing_rca_requests_routed + - COUNT 5

# Проверка метрик последней маршрутизации
redis-cli HGETALL metrics:ml:operator_routing_incident_rca_routing:last

# Прометеус метрики
curl -s localhost:9890/metrics | grep '^ml_operator_routing_incident_rca_routing_'
```

## Guardrails
- Работает только в `DRY_RUN` по умолчанию (mode default).
