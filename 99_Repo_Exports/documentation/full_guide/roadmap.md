# Дорожная карта Scanner Infrastructure (обновлено 2026-04-02)

Документ отражает стратегические направления развития платформы. Он служит для синхронизации команд и планирования релизов. Каждое направление имеет владельца, оценку сложности и ожидаемый эффект.

---

## 1. Структура роадмапа

- **Epic** — крупная инициатива.
- **Milestone** — конкретный этап с дедлайном.
- **Tasks** — набор действий, от которых зависит milestone.
- **Status**: `Planned`, `In Progress`, `Blocked`, `Done`.

---

## 2. Платформа и архитектура

### Epic: Миграция Redis на управляемый кластер

- **Цель**: снизить операционные риски, масштабировать без простоя.
- **Владелец**: `@sre-team`.
- **Статус**: Planned (Q1 2026).
- **Milestones**:
  1. `2025-12`: Proof-of-Concept на stage (Redis Enterprise / AWS Elasticache).
  2. `2026-02`: Настройка репликации и failover.
  3. `2026-03`: Перенос prod с минимальным downtime.
- **Ключевые задачи**:
  - Обновить `config/redis/*.conf`.
  - Мигрировать скрипты бэкапов.
  - Обновить `operations.md`, `troubleshooting.md`.

### Epic: P4.1 Unified Latency Contract [DONE]

- **Цель**: внедрение единой системы измерения задержек t0-t5 и SLO по Hot Path.
- **Статус**: Done (2026-03).
- **Результат**: ✅ t0-t5 tagging, ✅ SLO exporter, ✅ Grafana P99 dashboards.

### Epic: Внедрение OpenTelemetry

- **Цель**: добавить распределённый трейсинг для критичных потоков.
- **Владелец**: `@devops`.
- **Статус**: Planned (Q2 2026).
- **Milestones**:
  1. `2026-01`: Трейсинг в Go gateway.
  2. `2026-02`: Интеграция с Python сервисами.
  3. `2026-04`: Дашборды в Grafana Tempo/Jaeger.
- **Зависимости**: обновление `services.md`, `architecture.md`, `operations.md`.

---

## 3. Market Data & Ingestion

### Epic: Поддержка дополнительных бирж (OKX, Bybit)

- **Цель**: диверсифицировать источники данных и стратегии.
- **Владелец**: `@market-data-team`.
- **Статус**: In Progress.
- **Milestones**:
  - `2025-11`: Завершение адаптера OKX (dev/stage).
  - `2025-12`: Запуск Bybit, унификация конфигов.
  - `2026-01`: Общее тестирование failover.
- **Tasks**:
  - Написать адаптеры (`go-worker/adapters`).
  - Обновить `documentation/ticks/`.
  - Добавить тесты реплея (`fixtures/okx_*`).

### Epic: Smart Replay Engine

- **Цель**: реплей исторических данных в ускоренном режиме с управлением шагом.
- **Владелец**: `@python-team`.
- **Статус**: Planned (Q1 2026).
- **Milestones**:
  - `2025-12`: Дизайн ADR.
  - `2026-02`: Реализация движка.
  - `2026-03`: Интеграция в Makefile.

---

## 4. Signal Intelligence

### Epic: OrderFlow Handlers Expansion

- **Цель**: расширить покрытие символов и улучшить алгоритмы spike detection.
- **Владелец**: `@python-team`.
- **Статус**: In Progress.
- **Milestones**:
  - `2025-12`: Добавить обработчики для XAUUSD, дополнительные crypto pairs.
  - `2026-01`: Улучшить OBI calculation и weak progress detection.
  - `2026-02`: Интеграция с regime classification.
- **Tasks**:
  - Создать `handlers/forex_*_handler.py` для металлов.
  - Оптимизировать `base_orderflow_handler.py`.
  - Обновить `services.md` и метрики.

### Epic: Advanced Analytics & Calibration

- **Цель**: полная система аналитики V2/V3 с GPU acceleration и auto-calibration.
- **Владелец**: `@analytics-team`.
- **Статус**: Done (Q4 2025).
- **Достижения**:
  - ✅ Analytics V2/V3 system с ROC analysis.
  - ✅ GPU compute service для ускорения расчётов.
  - ✅ Auto calibration service с threshold tuning.
  - ✅ Dataset export в Parquet для ML моделей.
- **Следующие шаги**: оптимизация производительности, A/B testing framework.

### Epic: G0-G15 Gate Pipeline [DONE]

- **Цель**: декомпозиция логики сигналов на цепочку независимых гейтов.
- **Статус**: Done (2026-03).
- **Результат**: ✅ G0-G15 architecture, ✅ DnCalib, ✅ Gate Diagnostics.

### Epic: ML-Enhanced Signal Scoring & Calibration [DONE]

- **Цель**: интегрировать ML модели для оценки качества сигналов и автоматической калибровки.
- **Статус**: Done (2026-03).
- **Результат**: ✅ Champion/Challenger logic, ✅ Drift Detection (PSI/KS), ✅ ECE/Brier monitoring.

---

## 5. Execution & MT5

### Epic: REST API v2 для Go gateway

- **Цель**: улучшенная схема авторизации, поддержка дополнительных команд (partial close, hedged positions).
- **Владелец**: `@go-team`.
- **Статус**: Planned (Q3 2026).
- **Milestones**:
  - `2026-04`: ADR и спецификация.
  - `2026-06`: Реализация v2 (параллельно с v1).
  - `2026-08`: Миграция клиентов.
- **Tasks**:
  - Обновить ServeMux маршруты (поддержка версионности).
  - Реализовать backward compatibility.
  - Расширить `faq.md`, `services.md`.

### Epic: Journal-First Execution [DONE]

- **Цель**: переход на журнальную модель исполнения ордеров через `orders:exec`.
- **Статус**: Done (2026-03).
- **Результат**: ✅ BinanceExecutor, ✅ ProjectionWorker, ✅ BootstrapSupervisor.

### Epic: News Agent (LLM Intelligence) [DONE]

- **Цель**: интеграция LLM для анализа новостей и коррекции риска.
- **Статус**: Done (2026-03).
- **Результат**: ✅ Reasoning LLM loop, ✅ NewsTightenRecoDTO, ✅ G14 News Guard.

### Epic: Multi-Exchange Journaling

- **Цель**: поддержка Hyperliquid, Bybit и MT5 в едином Journal-First пайплайне.
- **Статус**: In Progress (Q3 2026).

---

## 6. Analytics & Reporting

### Epic: Real-time Dashboarding

- **Цель**: дашборды с обновлением в реальном времени для трейдинга и аналитики.
- **Владелец**: `@trading-analytics`.
- **Статус**: In Progress.
- **Milestones**:
  - `2025-11`: Веб-сокет слой для отчётов (stage).
  - `2025-12`: Live-дэшборд с ключевыми метриками.
  - `2026-02`: Расширение на кастомные виджеты.
- **Tasks**:
  - Обновить `signal_performance_tracker` (pub/sub).
  - Интегрировать Grafana live features.
  - Обновить документацию (`services.md`, `operations.md`).

### Epic: Unified Reporting Pipeline

- **Цель**: унифицировать формирование CSV/Parquet, автоматизировать выгрузку в S3.
- **Владелец**: `@analytics-ops`.
- **Статус**: Planned (Q1 2026).
- **Milestones**:
  - `2026-01`: Проектирование схем.
  - `2026-02`: Реализация ETL.
  - `2026-03`: Включение в CI/CD.

---

## 6. AI/ML и Продвинутая Аналитика

### Epic: Real-time ML Inference

- **Цель**: интегрировать ML модели в реальном времени для оценки сигналов.
- **Владелец**: `@ml-team`.
- **Статус**: Planned (Q2 2026).
- **Milestones**:
  - `2026-01`: Исследование и прототипирование моделей.
  - `2026-02`: Оптимизация для edge deployment.
  - `2026-03`: Интеграция в Signal Hub с low-latency inference.
- **Зависимости**: GPU compute service, dataset export.

### Epic: Automated Strategy Discovery

- **Цель**: использовать reinforcement learning для поиска новых стратегий.
- **Владелец**: `@quant-team`.
- **Статус**: Research (Q3 2026).
- **Milestones**:
  - `2026-04`: Сбор данных для RL environment.
  - `2026-06`: Прототип RL агента.
  - `2026-08`: Интеграция в calibration pipeline.
- **Tasks**:
  - Разработка reward functions.
  - Создание simulation environment.
  - Валидация на исторических данных.

---

## 7. Observability & DevOps

### Epic: Alerting 2.0

- **Цель**: улучшить сигналы и шум в алертах, внедрить SLO dashboards.
- **Владелец**: `@sre-team`.
- **Статус**: In Progress.
- **Milestones**:
  - `2025-11`: Реорганизация каналов (Slack/Telegram/PagerDuty).
  - `2025-12`: Добавление SLO dashboards.
  - `2026-01`: Автоматизированные постмортемы.

### Epic: CI/CD Hardening

- **Цель**: ускорить сборку, добавить статический анализ и секрет-сканирование.
- **Владелец**: `@devops`.
- **Статус**: Planned (Q1 2026).
- **Milestones**:
  - `2025-12`: Включение `gosec`, `bandit`.
  - `2026-02`: Кэширование артефактов.
  - `2026-03`: Автоматическая проверка секретов (`git-secrets`).

---

## 8. Документация и процессы

### Epic: Документация v3

- **Цель**: консолидация всех документов, создание портала знаний.
- **Владелец**: `@documentation-team`.
- **Статус**: In Progress.
- **Milestones**:
  - `2025-11`: Создание `documentation/full_guide/` (этот релиз).
  - `2025-12`: Интерактивный оглавление и линк-чекер в CI.
  - `2026-01`: Портал на базе Docusaurus (опционально).

### Epic: Автоматизация чек-листов

- **Цель**: перевести operational чек-листы в автоматические проверки.
- **Владелец**: `@ops-automation`.
- **Статус**: Planned (Q2 2026).
- **Milestones**:
  - `2026-03`: Сбор требований.
  - `2026-05`: MVP (Makefile + скрипты).
  - `2026-07`: Полный rollout.

---

## 9. Обновление роадмапа

- Ревизия проводится раз в квартал.
- Ответственный за актуальность: `@system-architect`.
- Изменения фиксируются в этом файле и анонсируются в `#scanner_docs`.
- При переносе сроков обновляйте Milestones и статусы.

---

## 10. Ссылки и материалы

- Jira board: `https://jira.company/projects/SCANNER`.
- ADR: `docs/adr/`.
- Описание релизного процесса: `operations.md#управление-релизами`.
- История изменений: `documentation/README.md#что-изменилось`.

Роадмап — живой документ. Обновляйте его при изменениях планов, чтобы команды работали синхронно.
