# Phase 3.3 — Offline Debug Producer → Handoff Ingress Rewire

## Цель
Сделать второй реальный contour-by-contour rewire на следующем safest contour:
- `offline_debug`

## Что делает
- читает из промежуточного producer stream:
  - `stream:ml:offline_debug_handoff_source`
- валидирует bounded contract
- шлёт в handoff ingress:
  - `stream:ml:vertex_local_handoff_requests`
- маркирует:
  - `task_family=offline_debug`

## Почему это второй safest contour
- не primary RCA
- bounded debug payload
- хорошо подходит для локального fallback plane
- результат можно проверять через hypotheses/checks output

## Важный rollout принцип
Этот шаг **не требует немедленно править producer код**.
Сначала:
1. переведите выбранный offline debug producer на output stream `stream:ml:offline_debug_handoff_source`
2. включите adapter
3. проверьте, что handoff decisions и local fallback routing корректны

## Streams
- input: `stream:ml:offline_debug_handoff_source`
- output: `stream:ml:vertex_local_handoff_requests`
- decisions: `stream:ml:offline_debug_handoff_rewire_decisions`
- audit: `stream:ml:offline_debug_handoff_rewire_audit`

## Safe start
```bash
export ML_OFFLINE_DEBUG_HANDOFF_REWIRE_ENABLED=1
export ML_OFFLINE_DEBUG_HANDOFF_REWIRE_MODE=ENABLED
export ML_OFFLINE_DEBUG_HANDOFF_SOURCE_STREAM=stream:ml:offline_debug_handoff_source
export ML_VERTEX_LOCAL_HANDOFF_INPUT_STREAM=stream:ml:vertex_local_handoff_requests
```

## Smoke checks
```bash
redis-cli XADD stream:ml:offline_debug_handoff_source * \
  request_id od-1 \
  severity warning \
  prompt "Analyze replay mismatch and suggest bounded next checks." \
  snapshot_json '{"stream":"events:foo","expected":"A","got":"B"}' \
  ts_ms $(date +%s%3N)

redis-cli XREVRANGE stream:ml:offline_debug_handoff_rewire_decisions + - COUNT 5
redis-cli XREVRANGE stream:ml:vertex_local_handoff_requests + - COUNT 5
redis-cli XREVRANGE stream:ml:offline_debug_handoff_rewire_audit + - COUNT 5
curl -s localhost:9919/metrics | grep '^ml_offline_debug_handoff_rewire_'
```

## Notes
- scope только `scanner_infra`
- hot path не затронут
- adapter reversible
- force_local=1 по умолчанию (debug workloads лучше на local)
- следующий contour после стабилизации: bounded `emergency_summarize`
