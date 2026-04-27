# 🌊 Поток тиков и событий (2025-11-26)

> Сквозное описание ingestion-траектории: Binance + MT5 → Redis → сигналы → трейлинг. Обновлено после внедрения tick_ingest_server v2 и интеграции Signal Performance Tracker.

---

## 📡 Источники и адаптеры

| Источник          | Компонент                          | Протокол  | Назначение                           |
| ----------------- | ---------------------------------- | --------- | ------------------------------------ |
| Binance WS        | `go-worker/` + `adapters/binance/` | WebSocket | Тики, стакан, свечи                  |
| MT5 TickBridge    | `mt5/TickBridge.mq5`               | HTTP      | Тики из MT5, события сделок          |
| Historical Replay | `analysis/replay_ticks.py`         | File      | Прогон исторических тиков            |
| Paper Trading     | `services/paper_trading_test.py`   | Redis     | Симуляция сигналов без реального MT5 |

---

## 🔄 Обработка шаг за шагом

1. **Go Workers (Binance)**

   - Подписываются на WS каналы, нормализуют payload.
   - Публикуют в Redis (`stream:tick_<symbol>`, `candles:<symbol>:<tf>`).
   - Экспортируют метрики (`ws_messages_total`, `ws_reconnects_total`).

2. **Tick ingest server v2 (MT5)**

   - FastAPI (`services/tick_ingest_server.py`): принимает `/tick` и `/book`, валидирует, пишет в Redis (`stream:tick_<symbol>`, `stream:book_<symbol>`).
   - Использует DualRedis (`REDIS_URL` + `REDIS_TICKS_URL`) и аннотирует снапшоты (`book:latest:<symbol>`).
   - События от MT5 (`TP1_HIT`, `TRAILING_MOVE`) направляются в `events:trades` через Gateway.

3. **Book analytics / OHLC**

   - `book_analytics_service.py` обновляет DOM (`stream:book_<symbol>`), считает OBI, формирует PNG.
   - `ohlc_aggregator.py` строит свечи, обновляет ATR/volatility.

4. **Aggregated Hub V2**

   - Потребляет оба потока, выравнивает таймлайны, добавляет контекст (`atr_cache`, `regime_score`).
   - Выбирает trailing profile (`rocket_v1`, `lock_and_trail`, ...).

5. **Filtered Signal Writer**

   - Совмещает результат с режимами рынка, сохраняет сигнал и публикует в очередь ордеров.

6. **TP Event Listener**
   - Реакция на `TP1_HIT`: извлекает исходный сигнал и инициирует трейлинг.

---

## 🗃️ Ключи и схемы Redis

| Ключ/шаблон                      | Описание                                                   |
| -------------------------------- | ---------------------------------------------------------- |
| `stream:tick_<symbol>`           | Live поток тиков (Binance + MT5), хранится в `redis-ticks` |
| `stream:tick_mt5_<symbol>`       | Legacy поток MT5 (оставлен для обратной совместимости)     |
| `tick:mt5:<symbol>`              | Последний тик MT5 (Hash)                                   |
| `stream:book_<symbol>`           | DOM обновления, формируются через `/book`                  |
| `book:latest:<symbol>`           | Последний снапшот стакана (используется book analytics)    |
| `candles:<symbol>:<tf>`          | OHLC/Heiken-Ashi                                           |
| `events:trades`                  | События MT5 (`TP1_HIT`, `TRAILING_MOVE`, ...)              |
| `signals:{sid}`                  | Финальный сигнал                                           |
| `trade:state:{sid}`              | Статус сделки                                              |
| `stats:{strategy}:{symbol}:{tf}` | Метрики Signal Performance Tracker (hash)                  |

TTL / eviction:

- Тики в `redis-ticks` — 24 часа (`redis-ticks.conf`, политика `volatile-lru`).
- Binance / worker Redis — 12 часов (конфиги `redis-worker-*.conf`).
- События торговли — 7 дней (`trade:timeline`), задаётся в `redis-optimized-for-trade-back.conf`.
- Хэш статистики (`stats:*`) — без TTL (вручную чистится скриптами аналитики).

Consumer группы:

- Префикс `ticks-` для сервисов ingest/analytics (`ticks-hub-v2-XAUUSD`, `ticks-tracker-group`).
- Создаются при старте сервисов (`AggregatedSignalHubV2`, `SignalPerformanceTracker`, `book_analytics_service`).

---

## 📊 Метрики и мониторинг

| Показатель                 | Где посмотреть                         |
| -------------------------- | -------------------------------------- |
| Задержка тиков             | `tick_gap_seconds` (Prometheus)        |
| Кол-во reconnect           | `ws_reconnects_total`                  |
| Ошибки MT5 HTTP            | `mt5_http_failures_total`              |
| Скорость сигналов          | `signals_generated_total`              |
| Лаг стрима `events:trades` | `make trailing-stats` → `consumer_lag` |

Makefile цели:

```bash
make tick-status
make tick-streams
make tick-groups
make trailing-status
make tracker-stats
make tracker-logs
```

Grafana:

- Dashboard `Websocket Streams` — кривая задержек.
- Dashboard `TP1 Trailing` — связь с трейлингом.

---

## 🧪 Тестовые сценарии

1. **Binance → сигнал**

   - `go run cmd/worker/main.go --config ...`
   - `python -m services.signal_generator --symbol XAUUSD --debug`.
   - Проверить `signals:{sid}`.

2. **MT5 → Trailing**

   - `python -m services.tp_event_emulator --event TP1_HIT`.
   - Убедиться, что `events:trades` пополнился, `make trailing-stats` показывает рост.

3. **Stress test**
   - `python -m services.tick_emulator --rate 100/s`.
   - Наблюдать `tick_gap_seconds`, проверять отсутствие лагов.

---

## 🆘 Troubleshooting

- **Нет MT5 тиков** — проверить `TickBridge` (логи в MT5) и `tick_ingest_server` (`make tick-logs`).
- **Сигналы без trailing** — убедиться в `trail_after_tp1=true`, проверить `TP Event Listener`.
- **Высокий лаг Redis стримов** — добавить consumer воркеры, проверить `XINFO GROUPS`.
- **Несогласованность ATR** — убедиться, что `aggregated_signal_hub_v2` обновляет `atr_cache`.
- **Пустые stats:** — выполнить `make tracker-stats`, убедиться что Signal Performance Tracker читает `signals:*`.

---

## ✅ Контроль качества

- Документ обновлён 2025-11-26. Последнее обновление: 2025-11-08 (tick_ingest_server v2 и Signal Performance Tracker).
- Ссылки и команды верифицированы командами Market Data и Trading.
- Изменения в ingestion требуют обновления этого файла и `ticks/TICKS_ARCHITECTURE.md`.
