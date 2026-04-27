# 🧬 Архитектура обработки тиков (2025-11-26)

> Обновлено для dedicated `redis-ticks`, Signal Performance Tracker и статистики по источникам.  
> Авторы: Senior Go/Python Developer + Senior Trading Systems Analyst.

---

## 📋 Содержание

1. [Источники данных](#источники-данных)
2. [Пайплайн обработки](#пайплайн-обработки)
3. [Redis структуры](#redis-структуры)
4. [Signal Performance Tracker и статистика](#signal-performance-tracker-и-статистика)
5. [Мониторинг и метрики](#мониторинг-и-метрики)
6. [Ссылки на исходники](#ссылки-на-исходники)

---

## 🌐 Источники данных

### Binance WebSocket → Go Workers

- **Директории**: `go-worker/`, `adapters/binance/`, `go-worker/internal/binance/`.
- **Подписки**:
  - **Kline** (`kline_1m`, `kline_5m`, configurable) — свечи для OHLC агрегации
  - **Aggregated trades** (`aggTrade`) — **тики/сделки** для `stream:tick_<symbol>` (Binance Futures)
  - **Order book depth** (`depth@100ms`) — обновления стакана для `stream:book_<symbol>`
- **Реализация тиков**:
  - **Binance Futures**: `FuturesStreamController` (`go-worker/internal/binance/futures_streams.go`) подписывается на `{symbol}@aggTrade` через `wss://fstream.binance.com/stream`
  - **Обработка**: `NormalizeFuturesMessage()` (`futures_normalizer.go`) парсит `aggTrade` → `NormalizedTick` (price, qty, side, trade_id, timestamp)
  - **Публикация**: `TickPublisher.PublishTick()` (`go-worker/internal/redis/publisher.go`) записывает в `redis-ticks:stream:tick_<SYMBOL>`
  - **Источник**: `source: "binance-futures"`, `market: "USDT-M"`
- **Особенности**:
  - Реконнект с экспоненциальной задержкой (`WS_RECONNECT_INTERVAL`).
  - Валидация таймштампов и последовательности.
  - Прямой экспорт метрик (latency, reconnects) в Prometheus.
  - Запись в `redis-ticks` (`stream:tick_<symbol>`, `stream:book_<symbol>`) через `TickPublisher` с fallback на основной Redis.
  - Динамическое управление символами через Redis key `binance:futures:usdtm:symbols` (Set).

### MT5 TickBridge → Tick ingest server v2

- **Файл**: `mt5/TickBridge.mq5`.
- **Потоки**:
  - `OnTick` → POST `/tick` (`services/tick_ingest_server.py`, JSON: `bid`, `ask`, `last`, `volume`, `flags`, `ts`, `symbol`).
  - DOM (по таймеру) → `/book` (обновляет `stream:book_<symbol>`, `book:latest:<symbol>`).
  - Торговые события → `GatewayBaseURL + /events/publish`.
- **Надёжность**:
  - Ретраи до 3 попыток, защита от «затопления» (throttle).
  - DualRedis fallback (`redis-ticks` как primary, основной Redis как резерв).
  - Локальные логи в терминале MT5.

### Дополнительные контуры (опционально)

- **Historical replay**: `analysis/replay_ticks.py`.
- **Paper Trading**: `python-worker/services/paper_trading_test.py`.

---

## ⚙️ Пайплайн обработки

```
Binance WS ──▶ Go Workers ─┬─▶ redis-ticks: stream:tick_<symbol>
                           └─▶ redis-ticks: stream:book_<symbol>

MT5 TickBridge ──HTTP──▶ Tick Ingest Server ─┬─▶ redis-ticks: stream:tick_<symbol>
                                            └─▶ redis-ticks: stream:book_<symbol>

redis-ticks streams ──▶ Aggregated Hub V2 ─┬─▶ signals:{strategy}:{symbol}
                                          ├─▶ stream:manual-signals
                                          └─▶ redis (book analytics, orders)

signals:* ──▶ Signal Performance Tracker ──▶ stats:{strategy}:{symbol}:{tf}
                                          └─▶ stats:{strategy}:{symbol}:{tf}:{source}
```

### Компоненты пайплайна

| Этап                       | Описание                                                                                  |
| -------------------------- | ----------------------------------------------------------------------------------------- |
| Go Workers                 | Поднимаются в нескольких экземплярах (`docker-compose-ultra.yml`), пишут в `redis-ticks`. |
| Tick Ingest Server         | FastAPI (`/tick`, `/book`), DualRedis (`redis-ticks` primary, основной Redis fallback).   |
| redis-ticks                | Выделенный Redis-инстанс для тиков/DOM. Контроль через `make ticks-status/streams/...`.   |
| Book analytics             | `book_analytics_service.py` — OBI/PNG, мониторинг DOM.                                    |
| ohlc-aggregator            | Собирает свечи для downstream (Heiken-Ashi, Renko при необходимости).                     |
| Aggregated Hub V2          | Объединяет Binance + MT5 потоки, `core/ticks_redis_client`, cluster-score, Parquet sink.  |
| Signal pipeline            | `filtered_signal_writer` → `stream:manual-signals` → ордера/уведомления.                  |
| Signal Performance Tracker | Читает `signals:*`, синхронизирует trailing, обновляет статистику и отчёты.               |
| Stats Aggregator           | Аггрегирует `stats:{strategy}:{symbol}:{tf}` и по источникам `stats:*:{source}`.          |

---

## 🗃️ Redis структуры

| Ключ/шаблон                               | Тип       | Описание                                            |
| ----------------------------------------- | --------- | --------------------------------------------------- |
| `stream:tick_<SYMBOL>`                    | Stream    | Live тики (Binance + MT5), хранятся в `redis-ticks` |
| `stream:tick_mt5_<SYMBOL>`                | Stream    | Legacy поток MT5 (для обратной совместимости)       |
| `tick:mt5:<SYMBOL>`                       | Hash      | Последний тик MT5                                   |
| `stream:book_<SYMBOL>`                    | Stream    | DOM обновления (`/book`)                            |
| `book:latest:<SYMBOL>`                    | Hash      | Последний снапшот стакана                           |
| `candles:<SYMBOL>:<TF>`                   | Hash/JSON | OHLC/Heiken-Ashi ряды                               |
| `tick:latency:<SOURCE>`                   | ZSet      | Замеры задержек                                     |
| `tick:stats:<SYMBOL>`                     | Hash      | Счётчики, последние цены                            |
| `events:trades`                           | Stream    | Торговые события (используются TP Event Listener)   |
| `profiles:trailing:*`                     | Hash      | Профили трейлинга (доступ к ATR/point size)         |
| `stats:{strategy}:{symbol}:{tf}`          | Hash      | Итоговая статистика Signal Performance Tracker      |
| `stats:{strategy}:{symbol}:{tf}:{source}` | Hash      | Статистика по источникам (OrderFlow, AggregatedHub) |
| `stats:sources:{strategy}:{symbol}:{tf}`  | Set       | Индекс источников статистики                        |
| `signals:crypto:raw`                      | Stream    | Сырые сигналы крипто-конвейера (до фильтрации)      |
| `crypto:symbols`                          | Set       | Динамический список символов для hub/tracker        |

**TTL**: для MT5 тиков — 24 часа, для Binance — 12 часов (настраивается).  
**Eviction**: `volatile-lru` для ключей, привязанных к потокам.

> ⚙️ **Стандарт потока тиков**: все продюсеры и консьюмеры работают только с `stream:tick_<symbol>` / `stream:book_<symbol>`.

---

## 🧮 Signal Performance Tracker и статистика

- **Источники**: читает `signals:orderflow:<symbol>`, `signals:ta:<symbol>`, `signals:aggregated:<symbol>` и `signals:crypto:raw`. Для каждого потока создаёт consumer group `signal-tracker-group` (ticks → `signal-tracker-group-ticks`).
- **Динамические символы**: базовый список расширяется множеством `crypto:symbols`. Нормализация и алиасы (`XAUUSD.m`, `BTC-PERP`) ведутся через `SignalPerformanceTracker._register_alias`.
- **Trailing**: `TP1TrailingOrchestrator` синхронизирует профили `profiles:trailing:*` и обновляет trailing/ATR контекст.
- **Статистика**: закрытия позиций обрабатываются `StatsAggregator.update_stats`, обновляя `stats:{strategy}:{symbol}:{tf}` и `stats:{strategy}:{symbol}:{tf}:{source}`; множеством `stats:sources:{strategy}:{symbol}:{tf}` индексируются доступные источники.
- **Отчётность**: `PeriodicReporter` автоматически отправляет отчёты каждые 100 сделок (настраивается через `REPORT_TRIGGER_COUNT`) и ежедневно в заданный час UTC, публикуя их в `notify:telegram` с `type=report`.
- **Запуск**: `python -m services.signal_performance_tracker` (env `REDIS_TICKS_URL`, `SYMBOLS_REDIS_KEY`, `STREAMS__STRATEGIES`). Docker-секция `signal-performance-tracker` использует те же переменные.

---

## 📊 Мониторинг и метрики

### Prometheus / Redis метрики

- `tick_ingest_requests_total`, `tick_ingest_latency_ms`.
- `ws_messages_total`, `ws_reconnects_total`.
- `tick_gap_seconds` — задержки между тик-обновлениями.
- `mt5_http_failures_total` — ошибки HTTP с MT5.
- `stats:{strategy}:{symbol}:{tf}` — статистика Signal Performance Tracker (`make tracker-stats`).
- `stats:{strategy}:{symbol}:{tf}:{source}` — мониторинг эффективности по источникам.

### Grafana

- Dashboard `grafana_dashboard_websocket.json` — состояние WebSocket соединений.
- Dashboard `grafana_multi_symbol_dashboard.json` — мульти-символьная аналитика (тик + trailing).
- Рекомендуется добавить алерты на `tick_gap_seconds > 5` секунд и `ws_reconnects_total` > 10/час.

### Makefile

```bash
make ticks-status        # Контейнер redis-ticks и здоровье ingestion
make ticks-streams       # Длины stream:tick_* / stream:book_* (по умолчанию redis-worker-1, меняется TICKS_REDIS_CONTAINER)
make ticks-groups        # Consumer группы и лаги по stream:tick_*
make ticks-consumers     # Активные консьюмеры для каждой группы
make ticks-logs          # Логи redis-ticks
make ticks-test          # Smoke-тест записи/чтения в redis-ticks
make tracker-stats       # Статистика по стратегиям и источникам сигналов
```

---

## 🔗 Ссылки на исходники

- **Go ingestion тиков**:
  - `go-worker/internal/binance/futures_streams.go` — FuturesStreamController, подписки на `aggTrade` и `depth@100ms`
  - `go-worker/internal/binance/futures_normalizer.go` — нормализация `aggTrade` → `NormalizedTick`
  - `go-worker/internal/redis/publisher.go` — TickPublisher для записи в `stream:tick_<symbol>`
  - `go-worker/binance/` — общие компоненты Binance WebSocket
- **Python ingest**: `python-worker/services/tick_ingest_server.py`, `book_analytics_service.py`, `ohlc_aggregator.py`.
- **Конфиги**: `config/services/go-worker.env`, `config/services/python-worker.env`.
- **MT5**: `mt5/TickBridge.mq5`, `mt5/doc/`.
- **Тесты**: `python-worker/tests/integration/test_tick_ingest.py`.

---

## ✅ Контроль качества

- Документ обновлён 2025-11-26. Последняя проверка: 2025-11-13 (`go-worker`, `tick_ingest_server`, `aggregated_signal_hub_v2`, `signal_performance_tracker`).
- Проверено: `make ticks-status`, `make ticks-streams`, `make tracker-stats`, `redis-cli --scan --pattern 'stats:*'` (redis, redis-ticks).
- Следующий аудит — при добавлении нового источника данных, изменении схемы статистики или обновлении consumer-групп.
