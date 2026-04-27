# 🧠 Философия модульности торгового контура (2025-11-26)

> Конвенции взаимодействия команд и сервисов. Обновлено после интеграции tick_ingest_server v2 и Signal Performance Tracker.

---

## 🎯 Цели архитектуры

1. **Чёткие границы ответственности** — ingestion, сигналы, исполнение, трейлинг, аналитика.
2. **Наблюдаемость** — каждый модуль экспортирует метрики и логирует ключевые события.
3. **Масштабируемость** — возможность масштабировать слои независимо.
4. **Fail-safe** — деградация одной подсистемы не должна ломать всю цепочку.

---

## 🧩 Модули и ответственность

| Модуль                         | Ответственная команда   | Роль                                                            | Протокол взаимодействия                                                |
| ------------------------------ | ----------------------- | --------------------------------------------------------------- | ---------------------------------------------------------------------- |
| **Tick Ingestion**             | Market Data             | Сбор тиков Binance/MT5, публикация в Redis                      | Redis Streams (`stream:tick_*`), HTTP                                  |
| **Signal Intelligence**        | Python Core + Quant     | Aggregated Hub V2, фильтры, enriched сигналы                    | Redis (`signals:{sid}`), Pub/Sub                                       |
| **Regime & Risk**              | Quant Research          | Определение режимов, риск-фильтры                               | Redis Hashes (`regime:*`), gRPC (опционально)                          |
| **Order Queue**                | Go Team                 | Очередь ордеров, приоритеты, подтверждения                      | Redis Lists, HTTP API                                                  |
| **TP1 Trailing**               | Python Core + Trading   | Обработка TP событий, выбор профилей, команды трейлинга         | Redis Streams, HTTP (`/orders/push`)                                   |
| **MT5 Executor**               | Trading Integration     | Получение команд, отправка событий                              | HTTP (`/orders/poll`, `/events/mt5`)                                   |
| **MT5 Trailing Telemetry**     | Trading Integration     | Логирование `TRAILING_MOVE`, дистанций, telemetry в timeline    | Redis (`trade:timeline:*`, `trade:events:*`), HTTP                     |
| **TradeMonitor**               | Python Core             | Виртуальные позиции, фиксация TP/SL, публикация `trades:closed` | Redis (`events:trades`, `trades:closed`, `signals:*`)                  |
| **Stats Aggregator**           | Python Core + Analytics | Агрегация метрик (`stats:{strategy}:{symbol}:{tf}`)             | Redis Hashes, pipelines                                                |
| **Reporting Service**          | Analytics Ops           | Формирование отчётов, публикация в `notify:telegram`            | Redis (`stats:*`, `notify:telegram`), HTTP (Telegram API через proxy)  |
| **Signal Performance Tracker** | Python Core + Analytics | Оркестратор аналитики: трейлинг, отчёты, health-check           | Redis Streams (`signals:*`, `stream:tick_*`, `events:trades`), threads |
| **Monitoring**                 | DevOps/SRE              | Prometheus, Grafana, алерты                                     | HTTP `/metrics`, Slack/Telegram уведомления                            |

---

## 🔁 Принципы взаимодействия

1. **Писать — в свою область**, читать можно соседям через чётко описанные ключи.
2. **Без прямых вызовов между слоями**, только через очереди/стримы или публичные API.
3. **Идемпотентность команд** — `order_id`/`sid` используются как ключи.
4. **Every event is logged** — каждая стадия торгового процесса оставляет запись в Redis.
5. **Feature flags** — новые функции включаются через ENV/конфиг (`TRAILING_ENABLED`).
6. **Документация = контракт** — изменения workflow фиксируются в `trading_workflow/*.md`.

---

## 🪢 Потоки ответственности

### Ingestion → Signals

- Market Data обеспечивает чистоту данных, SLA по задержкам.
- Python Core отвечает за корректную агрегацию и enrich данных.
- В случае проблем ingestion ставит флаг `data_quality=degraded`, сигнализация через Prometheus.

### Signals → Execution

- Filtered Writer публикует только валидные сигналы (`state=ready`).
- Order Queue обрабатывает приоритеты (`market`, `modify`, `trail`).
- Gateway валидирует payload и логирует каждый запрос.

### Execution → Analytics

- TP Event Listener и MT5TrailingMoveLogger пишут `trade:timeline`, `trade:events`.
- TradeMonitor фиксирует TP/SL, публикует `trades:closed`, флаги `trailing_started`.
- Stats Aggregator обновляет `stats:{strategy}:{symbol}:{tf}` и `stats:*:{source}`.
- Reporting Service формирует уведомления/отчёты, Signal Performance Tracker координирует цикл.

---

## 🧪 Процесс изменений

1. **Design doc** (если затрагивает более 1 модуля).
2. **Pull request** с обновлениями кода и документа (`trading_workflow/*.md`).
3. **Интеграционный тест** (`make trailing-test` или кастомный сценарий).
4. **Мониторинг**: новые метрики добавить в Prometheus/Grafana.
5. **Post-release**: зафиксировать в `FINAL_COMPLETE_INTEGRATION_*.md`.

---

## ✅ Контроль качества

- Обновлено 2025-11-26. Последняя проверка: 2025-11-13 (команды Go/Python/Trading/Analytics).
- Каждые 2 недели проводится аудит модульных границ (sync meeting). Следующий — не позднее 2025-12-04.
- Нарушения конвенций фиксируются в issue tracker с приоритетом Medium+.

Следуйте этим принципам, чтобы система оставалась стабильной, наблюдаемой и расширяемой.
