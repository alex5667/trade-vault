# Scanner Infrastructure: Полное Руководство

Добро пожаловать в расширенную базу знаний проекта `scanner_infra`. Этот комплект документов создан как «one-stop hub» для инженеров, аналитиков, трейдеров и DevOps-специалистов. Система эволюционировала в сторону **детерминированной событийной архитектуры** с жёстким контролем задержек (P4.1) и журнальной моделью исполнения (Journal-First).

---

## 1. Что такое Scanner Infrastructure

Scanner Infrastructure — это модульная платформа высокочастотной торговли, построенная вокруг событийной архитектуры и Redis Streams. Система объединяет рыночные данные разных источников (Binance, Bybit, Hyperliquid, MT5), автоматически генерирует пакеты сигналов через многоуровневую систему гейтов (G0-G15), управляет исполнением ордеров через защищённые журналы и собирает глубокую аналитику.

Ключевые особенности:
- ✅ **Низкая задержка**: Полный цикл (t0-t5) с контролем по P4.1 Unified Latency Contract (Hot path P99 < 5мс).
- ✅ **Детерминизм и Journal-First**: Все команды исполнения сначала пишутся в append-only журнал (`orders:exec`), затем материализуются.
- ✅ **ML Governance**: Автоматическое отслеживание дрейфа признаков (PSI/KS) и калибровка моделей (ECE/Brier).
- ✅ **LLM Intelligence**: Интеграция News Agent для извлечения событий из новостных лент и коррекции риска.
- ✅ **Масштабирование**: Шардирование Redis (Market Data vs State/Core) и Docker-профили.

---

## 2. Как устроено полное руководство

| Документ                  | Вопросы, на которые отвечает                                                    |
| ------------------------- | ------------------------------------------------------------------------------- |
| `overview.md` (этот файл) | Обзор экосистемы, доменов, ролей, карта чтения                                  |
| `architecture.md`         | P4.1 Contract, Journal-First, G0-G15 Gates, Redis Sharding, шаблоны надёжности  |
| `setup.md`                | Подготовка окружения, Docker профили, конфигурация секретов, проверка здоровья |
| `operations.md`           | Redis ACLs, калибровка гейтов, мониторинг дрейфа, дежурства, чек-листы          |
| `data_flow.md`            | t0-t5 пайплайны: тики → гейты → журнал → экзекутор → отчёты, форматы v: 1       |
| `services.md`             | Справочник по сервисам: BinanceExecutor, NewsAgent, ProjectionWorker, ML Gov    |
| `troubleshooting.md`      | AOF Corruption, User-Stream liveness, SLO violations, процедуры восстановления  |
| `glossary.md`             | Расширенный глоссарий терминов (P4.1, Journal-First, G-Gates)                   |
| `roadmap.md`              | Планы развития, приоритетные улучшения, запланированные ADR                     |

---

## 3. Карта доменов и вовлечённых команд

| Домен                 | Описание                                                                  | Основные директории / сервисы                                                                                            | Владельцы                     |
| --------------------- | ------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------ | ----------------------------- |
| Market Data Ingestion | Захват тиков/стаканов (Binance/Bybit), нормализация P4.1, DQ (Data Quality) | `go-gateway/internal/binance/`, `go-gateway/internal/bybit/`, `python-worker/services/tick_ingest_server.py`             | `@market-data-team`           |
| Signal Intel (Gates)  | G0-G15 Pipeline, калибровка порогов (DnCalib), ML подтверждение           | `python-worker/handlers/crypto_orderflow/`, `python-worker/services/auto_calibration_service.py`                         | `@python-team`, `@quant-team` |
| News & ML Governance  | LLM News Agent (Reasoning), Drift Detection (PSI/KS), Feedback Loops      | `news-agent/`, `python-worker/services/ml_governance_service.py`, `python-worker/drift/`                                 | `@ml-team`, `@news-ops`       |
| Execution & Journals  | Journal-First (`orders:exec`), BinanceExecutor, ProjectionWorker, Supervisor | `go-gateway/executor/`, `python-worker/services/projection_worker.py`, `python-worker/services/bootstrap_supervisor.py` | `@go-team`, `@trading-ops`    |
| Analytics & Observability | P4.1 SLO Monitoring, Signal Tracker, Telegram Analytics, Grafana P99      | `python-worker/services/signal_performance_tracker.py`, `python-worker/analytics/`, `telegram-worker/`                   | `@sre-team`, `@analytics`     |

---

## 4. Что нужно знать новичку за первые 4 часа

1. **Прочитать `architecture.md#p41-unified-latency-contract`** — понять, как измеряется время в системе.
2. **Изучить пайплайны в `data_flow.md#tick--signal--journal--execution`** — проследить путь от тика до экзекутора.
3. **Настроить окружение** по `setup.md#быстрый-старт-dev` и выполнить `make diagnose`, `make status`.
4. **Изучить систему гейтов `CryptoOrderFlow`** (раздел в `services.md`) — как сигнал проходит фильтрацию G0-G15.
5. **Проверить мониторинг**: открыть дэшборд `P4.1 Latency SLO` и убедиться, что задержки в норме.

---

## 5. Диаграмма верхнего уровня

```
         ┌───────────────────┐        ┌───────────────────┐
         │ Binance/Bybit WS  │        │ News Feed (LLM)   │
         └─────────┬─────────┘        └─────────┬─────────┘
                   │                            │
                   ▼                            ▼
         ┌───────────────────┐        ┌───────────────────┐
         │ Go Gateway (Ingest)│        │ News Agent        │
         │ (t0: Normalization)│        │ (Risk Reco DTO)   │
         └─────────┬─────────┘        └─────────┬─────────┘
                   │                            │
                   ▼                            ▼
         ┌───────────────────────────────────┐  │
         │ Redis Stream: tick_<sym> (t1)      │◀─┘
         └─────────────────┬─────────────────┘
                           │
                           ▼
         ┌───────────────────────────────────┐
         │ Python core: CryptoOrderFlow       │
         │ (G0-G15 Gates pipeline) (t2, t3)   │
         └─────────────────┬─────────────────┘
                           │
                           ▼
         ┌───────────────────────────────────┐
         │ Redis Journal: orders:exec (t4)   │
         └─────────────────┬─────────────────┘
                           │
            ┌──────────────┴──────────────┐
            │                             │
            ▼                             ▼
   ┌───────────────────┐         ┌───────────────────┐
   │ BinanceExecutor   │ (t5)    │ ProjectionWorker  │
   │ (Order Placement) │         │ (State/Metrics)   │
   └─────────┬─────────┘         └─────────┬─────────┘
             │                             │
             ▼                             ▼
   ┌───────────────────┐         ┌───────────────────┐
   │ Analytics V7/V8   │         │ Telegram Reports  │
   │ (ML Benchmarks)   │         │ (Signal Alerts)   │
   └───────────────────┘         └───────────────────┘
```

---

## 6. Навигация по репозиторию

| Путь                                         | Назначение                                                                                                           |
| -------------------------------------------- | -------------------------------------------------------------------------------------------------------------------- |
| `/telegram-worker/`                          | Telegram бот для уведомлений и команд                                                                                |
| `/infra/`                                    | Сетевые настройки, reverse proxy, дополнительные скрипты                                                             |

Дополнительно см. `README.md#краткая-карта-доменов` для связи доменов и ключевых сервисов.

---

## 7. Рекомендованные шаги изучения

1. Прочитать `architecture.md` полностью.
2. Настроить окружение с помощью `setup.md`.
3. Изучить потоки данных (`data_flow.md`) и сервисы (`services.md`).
4. Изучить инструкции по операциям (`operations.md`) и проводить регулярные проверки по чек-листу.
5. Ознакомиться с `troubleshooting.md`, чтобы знать, как реагировать на типовые инциденты.
6. Проработать `faq.md` и `glossary.md`, чтобы исключить пробелы в терминологии и ожиданиях команд.
7. Просмотреть `roadmap.md` и текущие ADR, чтобы понимать направление развития.

---

## 8. Как поддерживать актуальность руководства

- Любое изменение в архитектуре, конфигурации, SLA или бизнес-процессах должно сопровождаться обновлением соответствующих разделов.
- При добавлении новых сервисов обновляйте `services.md`, `data_flow.md` и при необходимости `architecture.md`.
- При изменении пайплайнов или мониторинга корректируйте `operations.md` и `troubleshooting.md`.
- Не забывайте обновлять `roadmap.md`, если приоритизация или сроки изменились.

Линк-чекер запускается командой `make docs-lint`. Перед мёрджем необходимо убедиться, что все проверки пройдены.

---

## 9. Контакты и каналы взаимодействия

| Команда / Контакт      | Область ответственности          | Канал связи              |
| ---------------------- | -------------------------------- | ------------------------ |
| `@market-data-team`    | Go workers, биржевые адаптеры    | Slack `#scanner_ticks`   |
| `@python-team`         | Signal Hub V2, tracker, трейлинг | Slack `#scanner_signals` |
| `@trading-ops`         | MT5, исполнение ордеров          | Slack `#scanner_trading` |
| `@sre-team`, `@devops` | Инфраструктура, мониторинг       | Slack `#scanner_ops`     |
| `#scanner_docs`        | Обсуждение документации          | Slack канал              |

По инцидентам следуйте процедурам из `operations.md#эскалация` и `troubleshooting.md`.

---

## 10. Что дальше

- Перейдите к `architecture.md`, чтобы глубже понять устройство платформы.
- Используйте `setup.md`, чтобы развернуть локальное окружение.
- Следуйте `data_flow.md`, чтобы проследить путь данных и понять логику бэкенда.

Если обнаружите пробелы или неточности, создайте issue с тегом `documentation/full_guide` или напишите в `#scanner_docs`. Обратная связь помогает держать базу знаний в актуальном состоянии.
