# 03. Roadmap на 12 месяцев

## Недельный режим обучения

Оптимальный режим:
- 5 дней в неделю по 60–90 минут
- 1 день практика 2–3 часа
- 1 день отдых / повторение

**Шаблон недели:**
| День | Действие | Выходной артефакт |
|---:|---|---|
| 1 | Чтение главы/статьи/лекции | 10 тезисов |
| 2 | Конспект | 5 терминов + 3 вопроса к проекту |
| 3 | Применение | схема / ADR / contract / checklist |
| 4 | AI-практика | prompt + diff review + тесты |
| 5 | Review | что понял, что изменю в проекте |
| 6 | Мини-проект | dashboard / runbook / failure matrix / schema |
| 7 | Пауза | повторение без новых источников |

---

## Месяц 1 — Инженерное мышление и контроль AI-кода
**Цель:** Перестать мыслить “задача → код”. Начать мыслить: `проблема → контекст → ограничения → варианты → решение → последствия → проверка → rollout → rollback`.
**Читать:**
- `CORE-01` Software Engineering at Google: главы про Software Engineering vs Programming, Code Review, Testing, Large-Scale Changes.
- `ARCH-02` ADR.
- `CORE-02` Twelve-Factor: Config, Logs, Backing Services.
**Результат:** Умение принимать задачу от AI не как готовый diff, а как инженерное изменение с reason, tests и rollback.

## Месяц 2 — Архитектура и границы сервисов
**Цель:** Увидеть систему слоями: кто производит данные, кто потребляет, где состояние, где контракты.
**Читать:**
- `ARCH-01` C4 Model.
- `CLOUD-01` AWS Well-Architected: Operational Excellence, Reliability.
- `BOOK-03` выборочно: architecture characteristics, trade-offs.
**Результат:** Способность объяснить проект без кода: сервисы, ответственность, данные, риски.

## Месяц 3 — Data contracts и streaming foundation
**Цель:** Понять, как данные рождаются, передаются, ломаются, версионируются и восстанавливаются.
**Читать:**
- `DATA-01` DDIA: intro, data models, encoding/evolution.
- `STREAM-01` Redis Streams.
- `TRADE-01` Binance WebSocket Streams.
**Результат:** Понимание event от command, Pub/Sub от Stream, raw market data от derived signal.

## Месяц 4 — PostgreSQL / Timescale / storage thinking
**Цель:** Понимать БД как storage engine + query engine + transaction system, а не просто таблицы.
**Читать:**
- `DB-01` CMU 15-445: storage, indexes, transactions.
- `DB-02` PostgreSQL docs: indexes, WAL, monitoring.
- `TS-01` Timescale docs: hypertables, continuous aggregates, retention.
**Результат:** Умение оценить масштабирование БД при росте данных x10/x100.

## Месяц 5 — Distributed systems и failure matrix
**Цель:** Понять, что несколько сервисов — это новая категория отказов.
**Читать:**
- `DIST-01` MIT 6.5840: introduction, RPC, fault tolerance, replication, Raft overview.
- `DATA-01` DDIA: replication, partitioning, transactions, distributed systems trouble.
- `BOOK-01` Release It!: stability patterns и failure modes.
**Результат:** Понимание at-least-once, duplicate processing, idempotency, retries, timeouts, partial failure.

## Месяц 6 — Testing strategy
**Цель:** Проверять систему не только unit-тестами.
**Читать:**
- `CORE-01` Testing chapters.
- `SRE-01` главы про monitoring/release/postmortems.
- `BOOK-01` Release It! по integration failure.
**Результат:** Умение принять/отклонить AI-generated diff по тестовой стратегии.

## Месяц 7 — Observability и SRE
**Цель:** Видеть production через метрики, логи, трассы и SLO.
**Читать:**
- `SRE-01` Google SRE Books: SLO, monitoring, incident response.
- `OBS-01` OpenTelemetry Docs.
- `DORA-01` DORA Metrics.
**Результат:** Для каждого алерта вы можете сказать: impact, owner, first checks, mitigation, rollback.

## Месяц 8 — DevOps и безопасная доставка изменений
**Цель:** Научиться менять production безопасно.
**Читать:**
- `DORA-01` DORA Metrics.
- `CLOUD-01` AWS Well-Architected: Operational Excellence.
- `CLOUD-02` Google Cloud Architecture Center.
**Результат:** Умение управляемо изменять production (shadow, canary, rollout).

## Месяц 9 — Security engineering
**Цель:** Встроить security в архитектуру, а не проверять “потом”.
**Читать:**
- `SEC-01` OWASP Top Ten.
- `SEC-02` OWASP ASVS.
- `SEC-03` NIST SSDF.
- `SEC-04` SLSA.
**Результат:** Умение провести security review задачи до merge.

## Месяц 10 — ML / AI Engineering
**Цель:** Понимать production ML как систему вокруг модели, а не только модель.
**Читать:**
- `ML-01` Google Rules of ML.
- `ML-02` Production ML Systems.
- `ML-03` MLOps pipelines.
**Результат:** Отличие “красивая offline-метрика” от “модель безопасна для production”.

## Месяц 11 — Управление AI-разработкой
**Цель:** Использовать Codex/Claude/Gemini как контролируемую инженерную команду.
**Практика:** Каждую AI-задачу давать только через spec-first шаблон `ai-task-template.md`.
**Результат:** AI перестаёт быть “магическим программистом” и становится controlled execution tool.

## Месяц 12 — Инженерное управление проектом
**Цель:** Собрать знания в систему управления проектом.
**Создать 5 постоянных документов:**
1. `Architecture Map`
2. `ADR Log`
3. `Risk Register`
4. `Runbooks`
5. `Engineering Metrics Dashboard`
**Результат:** Управление проектом как инженерной системой.

---

## Метрики развития

### Через 3 месяца
- [ ] Нарисовать C4 схему любой системы.
- [ ] Написать ADR.
- [ ] Объяснить service boundaries.
- [ ] Отличать event от command.
- [ ] Составить basic test plan.
- [ ] Понимать ENV/config/secrets/logs.
- [ ] Описать data contract для market event.

### Через 6 месяцев
- [ ] Проектировать data pipeline.
- [ ] Понимать Redis Streams / consumer groups.
- [ ] Понимать retries/timeouts/partial failure.
- [ ] Делать failure matrix.
- [ ] Требовать contract tests.
- [ ] Читать production metrics.
- [ ] Делать Timescale storage plan.

### Через 9 месяцев
- [ ] Делать SLO.
- [ ] Проектировать alerts.
- [ ] Делать rollout/rollback.
- [ ] Проводить security review.
- [ ] Понимать DORA metrics.
- [ ] Управлять CI/CD изменениями.
- [ ] Отличать warning-alert от page-alert.

### Через 12 месяцев
- [ ] Делать architecture review.
- [ ] Управлять AI-generated development.
- [ ] Видеть ML/production risks.
- [ ] Строить roadmap.
- [ ] Контролировать technical debt.
- [ ] Принимать инженерные решения как technical owner.
- [ ] Объяснить, почему система trustworthy.
