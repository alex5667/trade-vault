# Phase 3.18 — Route Incident RCA Mirror RCA Winner Apply Governance & Retry Loop

## Цель
Добавляет слой Governance поверх Apply и Verification механизмов.
- **SLO Analytics**: меряет Mean Time To Recovery (MTTR) на rollback и `verify_keep_rate` (процент успешных промоушенов LLM, не откатившихся назад).
- **Retry Controller**: пытается повторно применить Rollback Configuration если система залипла на неудачном Target'е.
- **Auto Escalation Summarizer**: бьет тревогу (warning/critical), если MTTR Rollback-а затягивается выше 120 секунд или число ретраев переходит границы.

## Safe start
```bash
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_RETRY_MAX_ATTEMPTS=2
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_RETRY_BACKOFF_SEC=120
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_ROLLBACK_MTTR_SLO_SEC=120
```

## Smoke checks
```bash
curl -s localhost:9937/metrics | grep '^ml_route_incident_rca_mirror_rca_winner_apply_'
curl -s localhost:9938/metrics | grep '^ml_route_incident_rca_mirror_rca_winner_apply_retry_'
curl -s localhost:9939/metrics | grep '^ml_route_incident_rca_mirror_rca_winner_apply_escalations_'
redis-cli HGETALL metrics:ml:route_incident_rca_mirror_rca_winner_apply_slo:last
redis-cli HGETALL metrics:ml:route_incident_rca_mirror_rca_winner_apply_retry:last
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_rca_winner_apply_slo_rollups + - COUNT 5
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_rca_winner_apply_retry_results + - COUNT 5
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_rca_winner_apply_escalations + - COUNT 5
```
