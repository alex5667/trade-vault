# Rollback Runbook — Execution P5

## Цель
Быстро откатить P5-слой без остановки всего стека и без ручного патча кода.

## Симптомы, когда нужен rollback
- всплеск `execution_emergency_flatten_total`
- рост `execution_reconcile_pending_total`
- частые `trade_execution_journal_write_fail_total`
- аномальный рост `trade_dq_hard_veto_total`
- рост `trade_risk_force_flatten_total`
- maker watchdog fallback происходит чаще ожидаемого baseline

## Быстрый rollback (предпочтительно)
В `.env.execution-p5.example` / prod env переключить:

```env
EXEC_MAKER_TP_ENABLE=0
EXEC_USER_STREAM_ENABLE=0
EXEC_RECONCILE_ENABLE=0
EXEC_JOURNAL_SQL_ENABLE=0
TRADE_DQ_HARD_VETO_ENABLE=0
TRADE_RISK_ENGINE_V2_ENABLE=0
EXEC_FORCE_SAFETY_FIRST=1
```

Затем перезапустить только affected services:

```bash
docker compose \
  -f docker-compose-binance.yml \
  -f docker-compose-crypto-orderflow.yml \
  -f config/docker-compose.execution-p5.override.yml \
  up -d --force-recreate binance-executor crypto-orderflow-service
```

## Жёсткий rollback overlay
Если rollback должен быть максимально простым, убрать P5 overlay:

```bash
docker compose \
  -f docker-compose-binance.yml \
  -f docker-compose-crypto-orderflow.yml \
  up -d --force-recreate
```

## После rollback
Проверить:
- `EXEC_FORCE_SAFETY_FIRST=1`
- нет новых `TP*_WATCHDOG_MARKET_FALLBACK`
- `execution_reconcile_pending_total` перестал расти
- `orders:user_stream` можно оставить отключённым до разбора
