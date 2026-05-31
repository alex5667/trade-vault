# Phase 3.25 — Route Incident RCA Mirror RCA Winner Apply Verification Loop

## Цель
Автоматический откат (rollback) Winner Apply промоушенов, если после применения они не соблюдают свои собственные политики (низкий Primary Match Rate, слишком много Unexpected Primary и др.).

## Safe start
```bash
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_VERIFICATION_ADVISORY_ONLY=1
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_VERIFICATION_EXECUTOR_MODE=DRY_RUN
```
В этих режимах петля верификации будет писать `"ROLLBACK_DRY_RUN"` и ни в коем случае не тронет реальный Redis Hash `cfg...:global`.

## Smoke checks
```bash
curl -s localhost:9947/metrics | grep '^ml_route_incident_rca_mirror_rca_winner_apply_verification_'
curl -s localhost:9947/metrics | grep '^ml_route_incident_rca_mirror_rca_winner_apply_'
redis-cli HGETALL metrics:ml:route_incident_rca_mirror_rca_winner_apply_verification:last
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_rca_winner_apply_verification_results + - COUNT 5
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_rca_winner_apply_rollback_journal + - COUNT 5
```
