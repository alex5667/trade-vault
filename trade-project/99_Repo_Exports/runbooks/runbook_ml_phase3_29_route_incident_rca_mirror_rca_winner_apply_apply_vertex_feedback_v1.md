# Phase 3.29 — Route Incident RCA Mirror RCA Winner Apply Apply Vertex RCA Consumer & Feedback

## Цель
Замыкает RCA цикл специально для контура `apply-governance`. 
- Consumer слушает Vertex-запросы от RCA-моста, прогоняет их через LLM (Vertex AI) и складирует RCA-результаты об откатах/сбоях.
- Governor собирает обратную связь пользователей (Usefulness / Quality) и переключает bridge в режим "Local Only" если Vertex AI несёт чушь относительно сбоев Apply-контроллера.

## Smoke checks
```bash
curl -s localhost:9953/metrics | grep '^ml_route_incident_rca_mirror_rca_winner_apply_apply_vertex_rca_'
curl -s localhost:9954/metrics | grep '^ml_route_incident_rca_mirror_rca_winner_apply_apply_vertex_governance_'
redis-cli HGETALL metrics:ml:route_incident_rca_mirror_rca_winner_apply_apply_vertex_rca:last
redis-cli HGETALL metrics:ml:route_incident_rca_mirror_rca_winner_apply_apply_vertex_rca_governance:last
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_vertex_rca_results + - COUNT 5

# Тестовая отправка фидбека:
redis-cli XADD stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_vertex_rca_feedback * request_id test-r1 bundle_id test-b1 quality_score 0.8 usefulness_score 0.9 accepted 1 reason_code helpful ts_ms $(date +%s%3N)
```
