# Phase 2.10 — Routing Incident RCA Governance

## Цель
Сформировать persistence, quality scoring и operator feedback loop для routing incident RCA, замыкая контур governance.

## Компоненты
- `operator_routing_incident_rca_results_persister_v2_10.py`
- `operator_routing_incident_rca_quality_scorer_v2_10.py`
- `operator_routing_incident_rca_feedback_loop_v2_10.py`

## Потоки
- input: `stream:ml:operator_rca_routing_rca_results`
- quality req: `stream:ml:operator_rca_routing_rca_quality`
- quality res: `stream:ml:operator_rca_routing_rca_quality_results`
- feedback req: `stream:ml:operator_rca_routing_rca_feedback`
- feedback sum: `stream:ml:operator_rca_routing_rca_feedback_summary`

## База данных (сканнер аналитика)
- `llm_operator_routing_incident_rca_results`
- `llm_operator_routing_incident_rca_quality`
- `llm_operator_routing_incident_rca_feedback`

## Smoke checks
```bash
# Проверка, что результаты читаются и записываются в SQL
redis-cli XREVRANGE stream:ml:operator_rca_routing_rca_results + - COUNT 3
redis-cli XREVRANGE stream:ml:operator_rca_routing_rca_quality_results + - COUNT 3
redis-cli XREVRANGE stream:ml:operator_rca_routing_rca_feedback_summary + - COUNT 3

curl -s localhost:9886/metrics | grep '^ml_operator_routing_incident_rca_results_'
curl -s localhost:9887/metrics | grep '^ml_operator_routing_incident_rca_quality_'
curl -s localhost:9888/metrics | grep '^ml_operator_routing_incident_rca_feedback_'
```

## Инструкции для Оператора
Для отправки feedback:
```bash
redis-cli XADD stream:ml:operator_rca_routing_rca_feedback * output_hash "<HASH_ИЗ_DB>" operator_id "<ID>" usefulness "VERY_USEFUL" comments "Great analysis" ts_ms $(date +%s%3N)
```

## Ограничения
Только `scanner_infra`, hot path не затронут.
