# Phase 2.11 — Routing Incident RCA Usefulness Governor

## Цель
Автоматическая оркестрация политик (SUPPRESS, PROMOTE, HOLD) для routing-incident RCA на основе накопленных баллов качества (quality_score) и полезности (usefulness_score).
Защита hot-path от плохих или зашумленных рекомендаций.

## Компоненты
- `operator_routing_incident_rca_usefulness_governor_v2_11.py`

## Потоки
- decisions: `stream:ml:operator_routing_incident_rca_governor_decisions`
- audit: `stream:ml:operator_routing_incident_rca_governor_audit`
- REDIS keys: `cfg:ml:operator_routing_incident_rca_governor:action:*` & `:provider:*`

## База данных
- `llm_operator_routing_incident_rca_governor_decisions`
- `llm_operator_routing_incident_rca_governor_policy_versions`

## Smoke checks
```bash
# Проверка отправки решений в Redis Stream
redis-cli XREVRANGE stream:ml:operator_routing_incident_rca_governor_decisions + - COUNT 5

# Проверка сохранения state в Hash (укажите валидные ключи из stream)
redis-cli HGETALL cfg:ml:operator_routing_incident_rca_governor:action:...

# Прометеус метрики
curl -s localhost:9889/metrics | grep '^ml_operator_routing_incident_rca_governor_'
```

## Guardrails
- По умолчанию `ADVISORY_ONLY=1`.
- Отделен от обычного `operator_rca` governor (Phase 2.2).
