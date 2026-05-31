# Phase 3.4 — Emergency Summarize Producer → Handoff Ingress Rewire

## Цель
Сделать следующий safest contour для bounded summary path:
- `emergency_summarize`

## Что делает
- читает из промежуточного producer stream:
  - `stream:ml:emergency_summarize_handoff_source`
- валидирует bounded contract + severity gating
- шлёт в handoff ingress:
  - `stream:ml:vertex_local_handoff_requests`
- маркирует:
  - `task_family=emergency_summarize`

## Почему это следующий safest contour
- bounded summary payload
- не primary RCA
- удобен для аварийных report/summarize сценариев
- severity можно жёстко ограничить policy

## Severity Gating
По умолчанию:
- `allow_critical=1` → принимается
- `allow_warning=1` → принимается
- `allow_info=0` → **отклоняется** (SEVERITY_NOT_ALLOWED)

Управляется через Redis hash `cfg:ml:emergency_summarize_handoff_rewire:global`.

## Важный rollout принцип
Этот шаг **не требует немедленно править producer код**.
Сначала:
1. переведите выбранный producer на output stream `stream:ml:emergency_summarize_handoff_source`
2. включите adapter
3. проверьте, что handoff decisions и routing корректны

## Streams
- input: `stream:ml:emergency_summarize_handoff_source`
- output: `stream:ml:vertex_local_handoff_requests`
- decisions: `stream:ml:emergency_summarize_handoff_rewire_decisions`
- audit: `stream:ml:emergency_summarize_handoff_rewire_audit`

## Safe start
```bash
export ML_EMERGENCY_SUMMARIZE_HANDOFF_REWIRE_ENABLED=1
export ML_EMERGENCY_SUMMARIZE_HANDOFF_REWIRE_MODE=ENABLED
export ML_EMERGENCY_SUMMARIZE_HANDOFF_SOURCE_STREAM=stream:ml:emergency_summarize_handoff_source
export ML_VERTEX_LOCAL_HANDOFF_INPUT_STREAM=stream:ml:vertex_local_handoff_requests
export ML_EMERGENCY_SUMMARIZE_HANDOFF_REWIRE_ALLOW_WARNING=1
export ML_EMERGENCY_SUMMARIZE_HANDOFF_REWIRE_ALLOW_CRITICAL=1
export ML_EMERGENCY_SUMMARIZE_HANDOFF_REWIRE_ALLOW_INFO=0
```

## Smoke checks
```bash
redis-cli XADD stream:ml:emergency_summarize_handoff_source * \
  request_id es-1 \
  severity critical \
  title "Emergency summary" \
  prompt "Summarize the current service degradation in bounded form." \
  ts_ms $(date +%s%3N)

redis-cli XREVRANGE stream:ml:emergency_summarize_handoff_rewire_decisions + - COUNT 5
redis-cli XREVRANGE stream:ml:vertex_local_handoff_requests + - COUNT 5
redis-cli XREVRANGE stream:ml:emergency_summarize_handoff_rewire_audit + - COUNT 5
curl -s localhost:9920/metrics | grep '^ml_emergency_summarize_handoff_rewire_'
```

## Notes
- scope только `scanner_infra`
- hot path не затронут
- adapter reversible
- severity gating обязательна
