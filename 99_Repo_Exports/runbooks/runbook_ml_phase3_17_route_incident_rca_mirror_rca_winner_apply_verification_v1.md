# Phase 3.17 — Route Incident RCA Mirror RCA Winner Apply Verification Loop

## Цель
Добавляет Verification Loop, который страхует систему после того как winner_apply_controller произвел promotion модели в Primary статус (или изменил Harness Mode). Проверяет, что по факту в Exposure Stream сыплются нужные данные (новые Primary соответствуют ожиданиям; нет Unexpected Primary Rate и т.д.). В случае провала verification (например, если primary_match_rate < 0.8), запускает **Bounded Rollback** на `deterministic` arm.

## Safe start
```bash
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_VERIFICATION_ADVISORY_ONLY=1
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_VERIFICATION_EXECUTOR_MODE=DRY_RUN
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_VERIFICATION_MIN_EXPOSURES=5
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_VERIFICATION_MIN_PRIMARY_MATCH_RATE=0.80
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_VERIFICATION_MAX_UNEXPECTED_PRIMARY_RATE=0.20
export ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_VERIFICATION_MAX_SHADOW_RATE_SINGLE_ARM=0.05
```

## Smoke checks
```bash
curl -s localhost:9936/metrics | grep '^ml_route_incident_rca_mirror_rca_winner_apply_verification_'
curl -s localhost:9936/metrics | grep '^ml_route_incident_rca_mirror_rca_winner_apply_'
redis-cli HGETALL metrics:ml:route_incident_rca_mirror_rca_winner_apply_verification:last
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_rca_winner_apply_verification_results + - COUNT 5
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_rca_winner_apply_rollback_journal + - COUNT 5
redis-cli HGETALL cfg:ml:route_incident_rca_mirror_rca_experiment:global
```
