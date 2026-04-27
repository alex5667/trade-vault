# 🔁 Trading Workflow & TP1 Trailing (2026-01-01)

> Обновлённое описание сквозного трейдингового процесса: тики → сигналы → ордера → трейлинг → аналитика.  
> Команда: Senior Go/Python Developer + Senior Trading Systems Analyst.

---

## 🗂️ Структура раздела

| Документ                                               | Описание                                                           |
| ------------------------------------------------------ | ------------------------------------------------------------------ |
| **[ticks_ingestion.md](ticks_ingestion.md)**           | Источники маркет-данных, ingestion пайплайн, Redis ключи           |
| **[order_creation.md](order_creation.md)**             | Формирование ордеров, Go Gateway, очередь заказов                  |
| **[tp1_trailing.md](tp1_trailing.md)**                 | Полный цикл TP1 трейлинга, профили, метрики                        |
| **[module_philosophy.md](module_philosophy.md)**       | Принципы модульности, ответственность команд                       |
| **[analytics_pipeline.md](../full_guide/overview.md)** | Signal Performance Tracker, TradeMonitor, Stats Aggregator, отчёты |

---

## 🔀 Рекомендуемый маршрут чтения

1. `ticks_ingestion.md` — понять, откуда берутся данные и как они нормализуются.
2. `module_philosophy.md` — увидеть границы модулей и правила взаимодействия.
3. `order_creation.md` — разобраться в очереди ордеров и Gateway API.
4. `tp1_trailing.md` — изучить автоматический трейлинг и последующие события.
5. `../full_guide/overview.md` — понять, как TradeMonitor/StatsAggregator превращают события в отчёты.

---

## 📌 Основные изменения (2025-11-26)

- **TP1 Trailing Orchestrator** и **MT5TrailingMoveLogger** формируют полную телеметрию трейлинга (`trade:timeline`, `trades:closed`).
- Расширены профили трейлинга: `mode=ATR|POINTS|STEP`, поддержка `hard_min_lock`, конвертация ATR→points через dispatcher.
- Обновлён payload команд: `action="trail"`/`"modify"`, `metadata.trail_*`, единая схема с Go gateway.
- Версия `orders:queue` подтверждена: LPUSH трейлингов, RPOP обработка, без отдельной priority queue.
- Добавлен полный блок аналитики: `TradeMonitor` → `StatsAggregator` → `ReportingService` → `Signal Performance Tracker`.
- Документация синхронизирована с `ARCHITECTURE.md`, `analytics/README.md`, `SIGNAL_TRACKER` руководствами.

---

## ✅ Контроль версий

- 2026-01-21 — обновление дат документации, проверка актуальности.
- 2025-11-21 — обновление документации, синхронизация с текущим состоянием кодовой базы.
- 2025-11-13 — синхронизация workflow с TradeMonitor/StatsAggregator и MT5 trailing telemetry.
- 2025-11-08 — актуализация tick ingest v2, Signal Performance Tracker, обновление очереди ордеров.
- 2025-11-07 — полное переписывание документации trading workflow.
- Перекрёстные ссылки синхронизированы с `ARCHITECTURE.md` и `DEVELOPMENT.md`.
- Ответственные: `@trading-analytics`, `@go-team`, `@python-team`.

Добро пожаловать в обновлённый стек торговых процессов! Если обнаружили рассинхрон — создайте issue в `#scanner_trade_docs`.
