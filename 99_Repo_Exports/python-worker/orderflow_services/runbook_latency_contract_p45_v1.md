# P4.5 — Latency Contract Deploy Lint State / Exporter

## Что это делает

Каждый запуск host-side deploy lint теперь публикует состояние в Redis:

- `metrics:latency_contract:deploy_lint:last:<purpose>`
- `cfg:orderflow:latency_contract:deploy_lint_gate:<purpose>:v1`
- `metrics:latency_contract:deploy_lint:summary:last` (через exporter)

Это даёт:
- видимость последних lint verdicts в Prometheus/Grafana
- сигнал о persistent config drift
- отдельный gate per purpose после выдержки `LATENCY_CONTRACT_DEPLOY_LINT_PERSIST_HOLD_S`

## Ключевые поля state

- `ok`
- `errors_count`
- `warnings_count`
- `last_checked_ts_ms`
- `fail_since_ts_ms`
- `fail_age_s`
- `gate_active`
- `gate_reason_code`
- `error_codes`

## Как запускать exporter

```bash
python3 -m orderflow_services.latency_contract_deploy_lint_exporter_v1
```

## Что смотреть

- `latency_contract_deploy_lint_gate_active{purpose}`
- `latency_contract_deploy_lint_fail_age_seconds{purpose}`
- `latency_contract_deploy_lint_summary_gate_active_total`
- `latency_contract_deploy_lint_summary_fail_total`

## Интерпретация

- `ok=0`, `gate_active=0` — transient drift, lint упал, но hold ещё не достигнут
- `ok=0`, `gate_active=1` — persistent drift; recurring jobs должны оставаться заблокированными до исправления конфигурации
- `ok=1` — state/gate очищены после успешного lint
