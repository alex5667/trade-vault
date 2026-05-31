# Phase 3.2 — Local Report Producer → Handoff Ingress Rewire

## Цель
Сделать первый реальный contour-by-contour rewire на **самом безопасном contour**:
- `local_report`

## Что делает
- читает из промежуточного producer stream:
  - `stream:ml:local_report_handoff_source`
- валидирует bounded contract
- шлёт в handoff ingress:
  - `stream:ml:vertex_local_handoff_requests`
- маркирует:
  - `task_family=local_report`

## Почему это safest contour
- не primary RCA
- не fleet-wide analysis
- локально проверяемый результат
- bounded payload

## Важный rollout принцип
Этот шаг **не требует немедленно править producer код**.
Сначала:
1. переведите выбранный producer на output stream `stream:ml:local_report_handoff_source`
2. включите adapter
3. проверьте, что handoff decisions и local fallback / vertex routing корректны

## Streams
- input: `stream:ml:local_report_handoff_source`
- output: `stream:ml:vertex_local_handoff_requests`
- decisions: `stream:ml:local_report_handoff_rewire_decisions`
- audit: `stream:ml:local_report_handoff_rewire_audit`

## Safe start
```bash
export ML_LOCAL_REPORT_HANDOFF_REWIRE_ENABLED=1
export ML_LOCAL_REPORT_HANDOFF_REWIRE_MODE=ENABLED
export ML_LOCAL_REPORT_HANDOFF_SOURCE_STREAM=stream:ml:local_report_handoff_source
export ML_VERTEX_LOCAL_HANDOFF_INPUT_STREAM=stream:ml:vertex_local_handoff_requests
```

## Smoke checks
```bash
redis-cli XADD stream:ml:local_report_handoff_source * \
  request_id lr-1 \
  severity info \
  title "Daily local report" \
  prompt "Summarize scanner state and current degraded paths." \
  ts_ms $(date +%s%3N)

redis-cli XREVRANGE stream:ml:local_report_handoff_rewire_decisions + - COUNT 5
redis-cli XREVRANGE stream:ml:vertex_local_handoff_requests + - COUNT 5
redis-cli XREVRANGE stream:ml:local_report_handoff_rewire_audit + - COUNT 5
curl -s localhost:9918/metrics | grep '^ml_local_report_handoff_rewire_'
```

## Notes
- scope только `scanner_infra`
- hot path не затронут
- adapter reversible
- следующий contour после стабилизации: `offline_debug`
