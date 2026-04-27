# Scanner Infrastructure — карта документации (обновлено 2026-04-02)

Эта папка описывает платформу высокочастотного трейдинга: от сбора рыночных данных и генерации сигналов до очереди ордеров, трейлинга, MT5-интеграции и аналитики. Документация рассчитана на разработчиков, аналитиков, DevOps/SRE и пользователей торговых систем.

---

## Как пользоваться документацией

| Если вы...                        | Начните с...                       | Что дальше                                                                  |
| --------------------------------- | ---------------------------------- | --------------------------------------------------------------------------- |
| Новичок в проекте                 | `README.md` (этот файл)            | `ARCHITECTURE.md` → `CONFIGURATION.md` → `DEVELOPMENT.md`                   |
| Python/Go разработчик             | `ARCHITECTURE.md#сервисные-домены` | `DEVELOPMENT.md#python-services`, `DEVELOPMENT.md#go-gateway-и-go-сервисы`  |
| Quants / Trading Operations       | `trading_workflow/README.md`       | `trading_workflow/tp1_trailing.md`, `trading_workflow/order_creation.md`    |
| Trading Analytics / Performance   | `signal_analytics/README.md`       | `signal_analytics/signal_lifecycle.md`, `signal_analytics/pnl_analysis.md`  |
| Market Data / Ingestion engineers | `ticks/README.md`                  | `ticks/TICKS_ARCHITECTURE.md`, `ticks/TICKS_DEVELOPMENT.md`                 |
| DevOps / SRE                      | `CONFIGURATION.md`                 | `ARCHITECTURE.md#инфраструктурный-слой`, `CONFIGURATION.md#troubleshooting` |
| Экспериментальный слой            | `python-worker/EXPERIMENT_LAYER_README.md` | `ARCHITECTURE.md#signal-intelligence-layer`, `CONFIGURATION.md#сервисы-и-ключевые-переменные` |
| Документируете изменения          | `README.md#процесс-актуализации`   | Следуйте чек-листу обновлений и ссылок                                      |

> Совет: используйте встроенный поиск IDE `documentation/` и быстрые ссылки из таблицы «Главные документы».

---

## Главные документы и их назначение

| Документ / раздел                                     | Содержание                                                                                     | Основная аудитория                         |
| ----------------------------------------------------- | ---------------------------------------------------------------------------------------------- | ------------------------------------------ |
| `ARCHITECTURE.md`                                     | Высокоуровневая архитектура, взаимодействие сервисов, потоки данных, интеграции, SLA           | Tech Leads, System Designers, разработчики |
| `CONFIGURATION.md`                                    | Docker Compose профили, ENV-стратегия, Redis конфиги, мониторинг, безопасность                 | DevOps, SRE, инженеры эксплуатации         |
| `DEVELOPMENT.md`                                      | Onboarding, Makefile, тестирование, отладка, code style для Go/Python, интеграционные сценарии | Go/Python инженеры, QA                     |
| `trading_workflow/`                                   | Сквозной торговый сценарий: тики → сигналы → очередь → MT5 → трейлинг → аналитика              | Trading operations, quants, product owners |
| `signal_analytics/`                                   | Аналитика сигналов: полный цикл от формирования до отчета, P&L, трейлинг стопы, отчеты         | Trading analytics, performance analysts    |
| `ticks/`                                              | Инфраструктура тиков: источники, пайплайн, схемы хранения, мониторинг, dev workflow            | Market data team, ingestion разработчики   |
| `documentation/trading_workflow/module_philosophy.md` | Принципы модульности, распределение зон ответственности, процессы изменений                    | Менеджеры, тимлиды, архитекторы            |

---

## Стартовое погружение (дорожная карта на 1–2 дня)

1. **Обзор системы** — прочитайте `ARCHITECTURE.md#обзор` и диаграмму потока данных.
2. **Окружение** — настройте `.env.local`, пройдите раздел `CONFIGURATION.md#bootstrap-сценарии`.
3. **Запуск** — выполните `make up-bg`, затем `make status` и `make diagnose`.
4. **Понимание трейдинга** — изучите `trading_workflow/tp1_trailing.md` с фокусом на событиях `TP1_HIT`.
5. **Тестовый прогон** — воспользуйтесь `DEVELOPMENT.md#интеграционные-сценарии` (`make trailing-test`).
6. **Мониторинг** — откройте Grafana dashboard `TP1 Trailing` и `Websocket Streams`.

---

## Краткая карта доменов

| Домен                          | Ключевые сервисы                                               | Хранилища / ключи                                           | SLA / SLO (P4.1)                             |
| ------------------------------ | -------------------------------------------------------------- | ----------------------------------------------------------- | -------------------------------------------- |
| Data acquisition & ingestion   | `go-worker`, `tick_ingest_server`, `book_analytics_service`    | `stream:tick_*`, `stream:book_*`, `tick:mt5:*`              | **t0-t1** ≤ 1 мс (P99)                       |
| Signal intelligence & processing| `aggregated_signal_hub_v2`, `crypto_orderflow_handler`         | `signals:{sid}`, `trade:state:{sid}`, `orders:exec`         | **t1-t3** ≤ 5 мс (P99)                       |
| Experiment layer (A/B testing)| `experiment_manager`, `experiment_metrics`, `ab_winner_*`      | `experiments:*`, PostgreSQL `experiments` table             | Детерминированное назначение ≤ 5 мс          |
| Risk management & validation  | `risk_position_sizer`, `validate_signals`, `execution_gate_*`  | `risk:*`, `validation:*`, `gates:*`                         | Валидация в рамках **t1-t3** budget          |
| Trade execution (Binance/MT5) | `binance_executor`, `tp_event_listener`, `mt5_executor`        | `events:trades`, `orders:exec`, `orders:history`            | **t4-t5** ≤ 200 мс (P99) (Exchange)          |
| Post-trade analytics & reporting| `signal_performance_tracker`, `projection_worker`, `pnl_math`  | `stats:*`, `trade:timeline:*`, `notify:telegram`            | Материализация стейта ≤ 100 мс               |
| Specialized analytics         | `sl_quantile_aggregator`, `trailing_metrics`, `news_agent`      | `sl:*`, `trailing:*`, `news_reco:*`                         | Аналитика по запросу ≤ 10 мин                |
| Notification & communication  | `telegram_bot_commands`, `telegram_worker`, `notify_bridge`    | `telegram:*`, `notify:*`                                    | Доставка уведомлений ≤ 30 с                  |
| Observability & infrastructure| `health_monitor`, `bootstrap_supervisor`, `redis_janitor`      | `health:*`, `metrics:*`, `prometheus`                       | Метрики P4.1 в реальном времени              |

---

## Что изменилось в выпуске 2026-04-02
- **Синхронизация с P4.1 Latency Contract** — внедрен сквозной контроль задержек (t0-t5) с новыми SLA (1 мс на логику, 200 мс на исполнение).
- **Journal-First Execution** — переход на модель персистентного журнала ордеров (`orders:exec`) перед совершением действий.
- **Signal Gating G0-G15** — полное описание цепочки гейтов в CryptoOrderFlow, включая ML (G10) и News (G14).
- **Инфраструктура Redis** — документирована сегментация на Market Data, Core и Analytics шарды, а также управление ACL.
- **Новые сервисы** — добавлены описания `BinanceExecutor`, `ProjectionWorker`, `BootstrapSupervisor` и `NewsAgent`.
- **docs-lint** — внедрена автоматическая проверка целостности ссылок (`make docs-lint`).

## Что изменилось в выпуске 2026-01-27

- **Обновление инвентаризации сервисов** — актуализация списка сервисов с учетом текущего состояния проекта (93 сервиса вместо 178)
- **Синхронизация с docker-compose** — полное соответствие документации конфигурациям Docker Compose
- **Новые сервисы автопилота** — добавлены `scanner-autopilot`, `scanner-autopilot-guardrail`, `scanner-autopilot-reporter`, `scanner-trailing-autotune` для автоматизированного трейдинга
- **Расширение экспериментального слоя** — обновлены сервисы A/B-тестирования: `ab-policy-suggester-timer`, `ab-winner-apply-runner`, `ab-winner-evaluator`, `ab-winner-lcb-timer`
- **Специализированные сервисы** — добавлены `binance-iceberg-detector`, `calendar-feature-store`, `htf-zones-publisher`, `py-obi-service` для расширенной аналитики
- **Инфраструктурные улучшения** — обновлены сервисы мониторинга: `docker-watchdog`, `redis-cleanup`, `redis-monitor`, `migration-runner`

### Предыдущие изменения (2026-01-10)

- **Добавлен модуль `pnl_math.py`** — новый модуль для корректного расчета P&L с учетом спецификаций символов (тиковая/линейная модель). Устраняет хардкод в расчетах и поддерживает различные модели расчета прибыли/убытков. Документация обновлена в `signal_analytics/pnl_analysis.md`.
- **Обновлена документация по аналитике сигналов** — добавлена информация о модуле `pnl_math.py`, обновлены примеры расчета P&L с использованием нового модуля.
- **Обновлена архитектурная документация** — добавлена информация о новых компонентах: Trade Monitor, P&L Math Module в раздел Post-Trade & Analytics Layer.
- Добавлены подробные инструкции по Bootstrap окружения, профилям Redis и ротации токенов.
- Обновлены схемы потоков данных (ingestion → trailing → analytics) с указанием SLA и ключевых метрик.
- Расширены разделы по Signal Performance Tracker, включая интеграцию с Telegram и отчётами.
- Уточнены сценарии disaster recovery для Redis и MT5 (см. `CONFIGURATION.md#disaster-recovery`).
- Документация trading workflow дополнена чек-листами, матрицей событий и диаграммами последовательности.
- Раздел ticks расширен описанием форматов сообщений, тестовых фикстур, профилирования и alert-политики.
- **Добавлен новый раздел `signal_analytics/`** — полная документация по аналитике сигналов, отслеживанию сделок, трейлинг стопам, расчету P&L и формированию отчетов в Telegram.
- **Упрощен скрипт `scripts/clear_trades_and_signals.sh`** — рефакторинг кода с использованием массивов и циклов, улучшена читаемость и поддержка (~36% сокращение кода). Добавлена документация по использованию скрипта в `DEVELOPMENT.md` и `CONFIGURATION.md`.

Полный changelog хранится в `documentation/CHANGELOG.md` (см. `Процесс актуализации`).

---

## Быстрый справочник по CLI и Makefile

```bash
make up-bg                # старт core-инфраструктуры в фоне
make status               # сводный статус контейнеров
make diagnose             # диагностика: healthchecks, Redis latency, dmesg
make trailing-start       # включить TP Event Listener и трейлинг-связку
make trailing-stats       # метрики трейлинга, lag consumer-групп
make tracker-stats        # агрегированная статистика Signal Performance Tracker
make tick-streams         # лаги и consumer groups для stream:tick_*
make gateway-test         # smoke-тесты HTTP API gateway
make send-real-report     # принудительно запустить генерацию отчёта
make tracker-restart      # перезапуск Signal Performance Tracker
make experiment-status    # статус экспериментального слоя
make postgres-status      # проверка PostgreSQL и миграций
```

См. `DEVELOPMENT.md#makefile-и-полезные-команды` для расширенной таблицы.

---

## Частые задачи и где искать ответы

- **Понять, как работает TP1 трейлинг** → `trading_workflow/tp1_trailing.md#поток-событий`.
- **Добавить новый профиль трейлинга** → `trading_workflow/tp1_trailing.md#профили-трейлинга`.
- **Понять полный цикл сигнала** → `signal_analytics/signal_lifecycle.md`.
- **Разобраться в расчете P&L** → `signal_analytics/pnl_analysis.md` (включая модуль `pnl_math.py`).
- **Настроить отчеты** → `signal_analytics/reporting.md`.
- **Очистить данные по сигналам и сделкам** → `scripts/clear_trades_and_signals.sh` (см. `DEVELOPMENT.md#очистка-данных`).
- **Сконфигурировать новый Redis профиль** → `CONFIGURATION.md#redis-инфраструктура`.
- **Найти описание ключей Redis** → `ticks/TICKS_ARCHITECTURE.md#redis-структуры` и `trading_workflow/order_creation.md#очередь-ордеров`.
- **Запустить Signal Performance Tracker локально** → `DEVELOPMENT.md#python-сервисы`.
- **Понять метрики и алерты** → `CONFIGURATION.md#мониторинг-и-алертинг`.
- **Подготовить MT5 к работе** → `trading_workflow/order_creation.md#mt5-исполнение` + `tp1_trailing.md`.

---

## Процесс актуализации документации

1. Обновляете код → фиксируете изменения в релизном документе `FINAL_COMPLETE_INTEGRATION_<date>.md`.
2. Для каждого затронутого домена:
   - Вносите правки в профильные файлы (`ARCHITECTURE.md`, `trading_workflow/*`, `ticks/*`).
   - Добавляете пункт в таблицу «Что изменилось» в этом README.
   - Обновляете `documentation/CHANGELOG.md` (создайте при необходимости; формат ISO даты).
3. Запускаете `make docs-lint` (проверка Markdown, линк-чекер).
4. Получаете ревью от ответственных команд (см. `Контакты` ниже).

---

## Контакты и владельцы доменов

| Домен                          | Владельцы                            | Канал в Slack/Telegram |
| ------------------------------ | ------------------------------------ | ---------------------- |
| Data Acquisition & Ingestion  | `@market-data-team`, `@go-team`      | `#scanner_ingestion`   |
| Signal Intelligence & Processing| `@python-team`, `@quant-team`        | `#scanner_signals`     |
| Experiment Layer (A/B Testing)| `@python-team`, `@quant-team`        | `#scanner_experiments` |
| Risk Management & Validation | `@trading-ops`, `@quant-team`        | `#scanner_risk`        |
| Trade Execution & MT5        | `@trading-ops`, `@go-team`           | `#scanner_trading`     |
| Post-Trade Analytics & Reporting| `@trading-analytics`, `@python-team` | `#scanner_analytics`   |
| Specialized Analytics        | `@quant-team`, `@trading-analytics`  | `#scanner_specialized` |
| Notification & Communication | `@trading-ops`                       | `#scanner_notifications` |
| Observability & Infrastructure| `@sre-team`, `@devops`               | `#scanner_ops`         |

Если не знаете, куда писать — создайте issue с тегом `documentation` или напишите в `#scanner_docs`.

---

## Глоссарий

- **SID** — уникальный идентификатор сигнала (`signal-<symbol>-<timestamp>`).
- **TP1 / TP2** — первая и вторая цели фиксации прибыли.
- **DualRedis** — стратегия записи в два экземпляра Redis (основной + fallback).
- **Trailing profile** — набор параметров для перевода ATR в размеры трейлинга.
- **Hub V2** — актуальная версия агрегатора сигналов, объединяющая данные из разных источников.
- **Trade timeline** — отсортированные события сделки (`trade:timeline:{sid}`).
- **Notify stream** — Redis stream `notify:telegram`, который считывает Signal Performance Tracker.

---

## Контроль качества

- Документация покрывает все изменения релиза 2026-04-02 (P4.1, Journal-First, G-Gates).
- Все ссылки проверены линк-чекером (`make docs-lint`).
- Ответственные: `@python-team`, `@go-team`, `@trading-analytics`, `@market-data-team`, `@quant-team`, `@trading-ops`, `@sre-team`.
- Последняя ревизия: 2026-04-02. Следующая ревизия: не позднее 2026-05-01 или при изменении P4.1 SLO/схем Redis.

Добро пожаловать в Scanner Infrastructure! Оставляйте комментарии и предложения по улучшению — документация живёт благодаря вам.
