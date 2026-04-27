# Phase 3.5 — Route Incident RCA Shadow / Dual-Audit Ingress

## Цель
Сделать первый осторожный RCA-family step после стабилизации safest contours:
- `route_incident_rca`
- через `shadow / dual-audit ingress`
- без прямого cutover

## Что делает
- читает из промежуточного producer stream:
  - `stream:ml:route_incident_rca_handoff_shadow_source`
- валидирует bounded contract
- строит два нормализованных shadow payload:
  - handoff-style
  - legacy-style
- пишет:
  - `stream:ml:vertex_local_handoff_shadow_requests`
  - `stream:ml:route_incident_rca_legacy_shadow_requests`
- по умолчанию работает в `AUDIT_ONLY`

## Режимы
- `DISABLED`
- `AUDIT_ONLY`: Решения пишутся в DB/Metric, но в output stream'ы ничего не шлется.
- `MIRROR`: Шлет оба shadow payload.
- `HANDOFF_ONLY`: Шлет только в handoff stream.
- `LEGACY_ONLY`: Шлет только в legacy stream.

## Почему это следующий разумный шаг
- это уже RCA-family, но ещё не direct cutover
- можно сравнить contracts и payload shape
- можно включать mirror постепенно
- primary live path остаётся нетронутым

## Streams
- input: `stream:ml:route_incident_rca_handoff_shadow_source`
- handoff shadow out: `stream:ml:vertex_local_handoff_shadow_requests`
- legacy shadow out: `stream:ml:route_incident_rca_legacy_shadow_requests`
- decisions: `stream:ml:route_incident_rca_shadow_handoff_decisions`
- audit: `stream:ml:route_incident_rca_shadow_handoff_audit`

## Safe start
```bash
export ML_ROUTE_INCIDENT_RCA_SHADOW_ENABLED=1
export ML_ROUTE_INCIDENT_RCA_SHADOW_MODE=AUDIT_ONLY
export ML_ROUTE_INCIDENT_RCA_SHADOW_SOURCE_STREAM=stream:ml:route_incident_rca_handoff_shadow_source
export ML_ROUTE_INCIDENT_RCA_HANDOFF_SHADOW_STREAM=stream:ml:vertex_local_handoff_shadow_requests
export ML_ROUTE_INCIDENT_RCA_LEGACY_SHADOW_STREAM=stream:ml:route_incident_rca_legacy_shadow_requests
```

## Smoke checks
```bash
redis-cli XADD stream:ml:route_incident_rca_handoff_shadow_source * \
  request_id rr-1 \
  incident_id route-inc-1 \
  severity warning \
  task_type route_incident_rca \
  summary "Shadow this route incident RCA payload." \
  primary_reason_codes_json '["ROUTE_MISMATCH"]' \
  ts_ms $(date +%s%3N)

redis-cli XREVRANGE stream:ml:route_incident_rca_shadow_handoff_decisions + - COUNT 5
redis-cli XREVRANGE stream:ml:route_incident_rca_shadow_handoff_audit + - COUNT 5
redis-cli XREVRANGE stream:ml:vertex_local_handoff_shadow_requests + - COUNT 5
redis-cli XREVRANGE stream:ml:route_incident_rca_legacy_shadow_requests + - COUNT 5
curl -s localhost:9921/metrics | grep '^ml_route_incident_rca_shadow_handoff_'
```

## Notes
- scope только `scanner_infra`
- hot path не затронут
- default mode = `AUDIT_ONLY`
- direct cutover не делается на этом шаге
