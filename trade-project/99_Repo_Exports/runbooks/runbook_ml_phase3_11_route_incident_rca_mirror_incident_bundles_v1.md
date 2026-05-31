# Phase 3.11 — Route Incident RCA Mirror Incident Bundles Builder

## Цель
Создание единого forensic ingress для mirror-governance contour.
Автоматически формирует `bundle JSON` при значимых переходах (`AUDIT_TO_MIRROR`, `MIRROR_TO_AUDIT`) или эскалациях (`warning`, `critical`).

## Что делает
Слушает события из `stream:ml:route_incident_rca_mirror_rollout_journal` и `stream:ml:route_incident_rca_mirror_escalations`.
При триггерах собирает недавний контекст из:
- Verification results
- Retry results
- Escalations
- Rollout Journal

Пишет результат в `llm_route_incident_rca_mirror_incident_bundles` (PostgreSQL) и `stream:ml:route_incident_rca_mirror_incident_bundles` (Redis).

## Safe start
```bash
export ML_ROUTE_INCIDENT_RCA_MIRROR_INCIDENT_BUNDLES_LOOKBACK_COUNT=50
export ML_ROUTE_INCIDENT_RCA_MIRROR_INCIDENT_BUNDLES_RECENT_WINDOW_MIN=240
export ML_ROUTE_INCIDENT_RCA_MIRROR_INCIDENT_BUNDLES_ONLY_SEVERITY=warning,critical
export ML_ROUTE_INCIDENT_RCA_MIRROR_INCIDENT_BUNDLES_TRIGGER_ON_TRANSITIONS=AUDIT_TO_MIRROR,MIRROR_TO_AUDIT
```

## Smoke checks
```bash
curl -s localhost:9929/metrics | grep '^ml_route_incident_rca_mirror_incident_bundles_'
redis-cli HGETALL metrics:ml:route_incident_rca_mirror_incident_bundles:last
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_incident_bundles + - COUNT 5
redis-cli XREVRANGE stream:ml:route_incident_rca_mirror_incident_bundles_audit + - COUNT 5
```

## Notes
- Собранный bundle содержит `bundle_id` и `evidence_slices` со всеми необходимыми логами для передачи в RCA/LLM.
