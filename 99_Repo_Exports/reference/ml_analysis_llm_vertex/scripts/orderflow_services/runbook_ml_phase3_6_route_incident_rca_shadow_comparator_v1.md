# Phase 3.6 — Route Incident RCA Shadow Comparator

## Цель
Автоматически сравнивать:
- `handoff-style` shadow payload
- `legacy-style` shadow payload

перед первым `MIRROR` rollout.

## Что делает
- читает:
  - `stream:ml:vertex_local_handoff_shadow_requests`
  - `stream:ml:route_incident_rca_legacy_shadow_requests`
- матчинг по:
  - `incident_id`
  - fallback: `request_id`
  - fallback: `compact_hash`
- сохраняет pending counterpart до прихода второй стороны
- строит comparison result со статусом:
  - `MATCH`
  - `DRIFT`
  - `MISMATCH`

## Что сравнивает
- `incident_id`
- `task_type`
- `severity`
- `compact_hash` (если есть с обеих сторон)
- payload key sets
- `primary_reason_codes`

## Streams
- handoff in: `stream:ml:vertex_local_handoff_shadow_requests`
- legacy in: `stream:ml:route_incident_rca_legacy_shadow_requests`
- results: `stream:ml:route_incident_rca_shadow_comparator_results`
- audit: `stream:ml:route_incident_rca_shadow_comparator_audit`

## Safe start
```bash
export ML_ROUTE_INCIDENT_RCA_HANDOFF_SHADOW_STREAM=stream:ml:vertex_local_handoff_shadow_requests
export ML_ROUTE_INCIDENT_RCA_LEGACY_SHADOW_STREAM=stream:ml:route_incident_rca_legacy_shadow_requests
export ML_ROUTE_INCIDENT_RCA_SHADOW_COMPARATOR_PENDING_TTL_SEC=86400
```

## Smoke checks
```bash
redis-cli XADD stream:ml:vertex_local_handoff_shadow_requests * \
  request_id rr-1 \
  incident_id route-inc-1 \
  task_type route_incident_rca \
  severity warning \
  compact_hash abc \
  payload_json '{"summary":"x","primary_reason_codes":["ROUTE_MISMATCH"]}' \
  ts_ms $(date +%s%3N)

redis-cli XADD stream:ml:route_incident_rca_legacy_shadow_requests * \
  incident_id route-inc-1 \
  task_type route_incident_rca \
  severity warning \
  compact_hash abc \
  payload_json '{"summary":"x","primary_reason_codes":["ROUTE_MISMATCH"]}' \
  ts_ms $(date +%s%3N)

redis-cli XREVRANGE stream:ml:route_incident_rca_shadow_comparator_results + - COUNT 5
redis-cli XREVRANGE stream:ml:route_incident_rca_shadow_comparator_audit + - COUNT 5
curl -s localhost:9922/metrics | grep '^ml_route_incident_rca_shadow_comparator_'
curl -s localhost:9922/metrics | grep '^ml_route_incident_rca_shadow_comparisons_'
```

## Notes
- scope только `scanner_infra`
- hot path не затронут
- `MIRROR` rollout не делать, пока comparator не показывает стабильный `MATCH/DRIFT` без `MISMATCH`
