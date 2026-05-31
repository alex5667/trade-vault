# Phase 3.21 — Route Incident RCA Mirror RCA Winner Apply Vertex RCA Consumer + Feedback Loop

## Цель
Замыкает контур (петлю) для Forensic Bundles в области Winner Apply. Состоит из двух частей:
1. **Vertex RCA Consumer**: читает задачу из очереди `_vertex_rca_requests`, валидирует контент и формирует Diagnostic RCA Result. В демо/безопасном режиме возвращает детерминированный ответ, в боевом ходит в LLM.
2. **Vertex Feedback Governor**: собирает "оценки качества" (Feedback) за ответы LLM, высчитывает Rolling Quality Score, и может автоматически переключить Bridge-маршрутизацию на отказ от LLM (перевод в режим `LOCAL_ONLY`), если метрики качества провисают ниже ватермарки.

## Safe start
```bash
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_VERTEX_RCA_HANDLER_MODE=DETERMINISTIC
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_VERTEX_RCA_GOVERNANCE_MIN_SAMPLES=10
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_VERTEX_RCA_GOVERNANCE_MIN_AVG_QUALITY=0.55
```

## Smoke checks
```bash
curl -s localhost:9942/metrics | grep '^ml_route_incident_rca_mirror_rca_winner_apply_vertex_rca_'
curl -s localhost:9943/metrics | grep '^ml_route_incident_rca_mirror_rca_winner_apply_vertex_governance_'
redis-cli HGETALL metrics:ml:route_incident_rca_mirror_rca_winner_apply_vertex_rca:last
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_rca_winner_apply_vertex_rca_results + - COUNT 5

# Test feedback evaluation pipeline:
redis-cli XADD stream:ml:route_incident_rca_mirror_rca_winner_apply_vertex_rca_feedback * request_id test-r1 bundle_id test-b1 quality_score 0.8 usefulness_score 0.9 accepted 1 reason_code helpful ts_ms $(date +%s%3N)
```
