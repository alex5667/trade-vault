# Latency Audit & SLO (P4.1 Contract)

## Обзор
Данный документ определяет стандарты измерения и контроля задержек в торговой системе `scanner_infra`. Мы придерживаемся контракта **P4.1 (Unified Latency Contract)**, который требует сквозного отслеживания таймстампов на каждом этапе обработки.

## Структура стейджей (Stages)

| Stage ID | Описание | Ответственный сервис |
| :--- | :--- | :--- |
| `ingest_source_to_redis` | Время от биржи до записи в Redis Streams | `go-worker` |
| `redis_to_feature` | Чтение из Redis и расчет признаков (ML) | `python-worker` |
| `feature_to_emit` | Время от расчета признаков до публикации сигнала | `python-worker` |
| `end_to_end_event` | Сквозная задержка: от биржи до сигнала | **Весь контур** |

## Целевые показатели (Latency Budgets)

| Метрика | p50 (ms) | p95 (ms) | p99 (ms) | SLO Breach |
| :--- | :--- | :--- | :--- | :--- |
| Go Ingestion | < 1.0 | < 3.0 | < 5.0 | > 10ms |
| Python Core | < 10.0 | < 25.0 | < 30.0 | > 100ms |
| **End-to-End** | **< 15.0** | **< 40.0** | **< 50.0** | **> 200ms** |

## Методика измерения
Метрики собираются через Prometheus гистограмму `latency_contract_stage_ms`.
Для каждой стадии рассчитывается:
- `histogram_quantile(0.99, sum(rate(latency_contract_stage_ms_bucket[5m])) by (le, stage))`

## Алертинг
Алерты срабатывают при нарушении p99 SLO на протяжении более 2 минут.
Критические алерты направляются в Telegram через `notify-telegram-v2`.

## Процедура аудита
1. **Baseline**: Оценка в нормальных рыночных условиях.
2. **Stress Test**: Использование `scripts/bench/latency_load_test.py` для имитации 20k+ событий/сек.
3. **Audit Results**: Фиксация в `WALKTHROUGH.md` после каждого релиза.
