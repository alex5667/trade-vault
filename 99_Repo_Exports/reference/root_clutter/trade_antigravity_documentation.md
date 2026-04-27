# Документация по Antigravity-конфигурации проекта `trade`

## 1. Назначение

Этот набор файлов превращает workspace проекта `trade` в специализированную среду Antigravity для production-задач по торговому пайплайну.

Цель конфигурации:
- стандартизировать ответы и архитектурные решения;
- заставить агента учитывать реальную правду репозитория;
- автоматически подмешивать нужные предметные инструкции по смыслу запроса;
- дать повторяемые slash-workflows для типовых сценариев: аудит, новый сигнал, replay, rollout, postmortem.

Базовая истина проекта:

```text
Go (ticks/klines/orderbook ingestion)
-> Redis
-> Python (analysis/signals/gates)
-> NestJS (aggregation/WebSocket/API)
-> Next.js UI
-> Postgres/Timescale (history/metrics)
```

Ключевые цели:
- reliability
- deterministic time semantics
- data quality control
- low latency
- observability
- controlled risk

---

## 2. Структура

Актуальная структура pack:

```text
trade/
  .agents/
    agents.md
    skills/
      trade-project-core/
        SKILL.md
      trade-data-quality-time/
        SKILL.md
      trade-go-redis-ingest/
        SKILL.md
      trade-python-signal-engine/
        SKILL.md
      trade-api-ui-contracts/
        SKILL.md
      trade-timescale-postgres/
        SKILL.md
      trade-observability-rollout/
        SKILL.md
      trade-ml-replay-gating/
        SKILL.md
    workflows/
      trade-audit.md
      trade-new-signal.md
      trade-replay.md
      trade-rollout.md
      trade-postmortem.md
```

Дополнительно в pack есть `README.md` с краткой инструкцией по установке.

---

## 3. Как это работает в Antigravity

### 3.1 `agents.md`

`agents.md` — это единая карта ролей и глобальных правил поведения агента внутри workspace.

В нашем проекте этот файл:
- включает global operating rules;
- фиксирует repository truth;
- описывает роли специалистов;
- задаёт routing hints между ролями и skill-модулями;
- определяет collaboration protocol и quality bar.

### 3.2 `skills/*/SKILL.md`

Каждый skill — это отдельная папка с `SKILL.md`.

Назначение skill:
- не грузить весь контекст всегда;
- подключать узкую экспертизу по смыслу запроса;
- держать отдельные регламенты по data-quality, Go ingest, Python detectors, DB, rollout и replay.

Важный практический момент:
- routing делается прежде всего по YAML frontmatter;
- самое важное поле — `description`;
- чем точнее `description`, тем легче Antigravity автоматически выберет нужный skill.

### 3.3 `workflows/*.md`

Workflow — это reusable slash command.

Назначение workflow:
- запускать не просто ответ, а заранее оформленную последовательность ролей и skill'ов;
- стандартизировать сложные процессы;
- снижать разброс качества между похожими задачами.

Именно workflow удобны для повторяемых сценариев: production audit, safe rollout, RCA, replay.

---

## 4. Документация по `.agents/agents.md`

## 4.1 Роль файла

Файл задаёт основное поведение всей trade-конфигурации.

Когда запрос начинается с `TRADE:` или явно относится к проекту `trade`, агент должен маршрутизировать работу через описанную там команду.

## 4.2 Что зафиксировано в global rules

В `agents.md` закреплены следующие правила:
- язык ответов по умолчанию — русский;
- приоритет implementation-first над общей теорией;
- обязательное разделение на **Facts / Assumptions / Risks**;
- предпочтительный формат ответа:
  1. Goal
  2. What we have
  3. Plan
  4. Details
  5. Tests
  6. Metrics / logs / alerts
  7. Rollout / rollback
  8. Prod checklist
- если данных достаточно — решать сразу;
- если данных не хватает — максимум 3–6 критических вопросов, но по возможности продолжать через явные assumptions;
- время и данные должны быть формализованы: unit, timezone, monotonicity, late/out-of-order handling;
- bad data path строго: `detect -> sanitize -> quarantine -> metrics`;
- для noisy market data — robust statistics, например median/MAD;
- для оптимизаций обязательна схема `measure -> change -> re-measure`;
- любые production-affecting изменения должны включать tests, observability, rollout/rollback.

## 4.3 Repository truth

Файл фиксирует правду репозитория:
- Go отвечает за ingestion;
- Redis — центральная транспортная шина;
- Python — detectors / signals / gates;
- NestJS — aggregation / API / WebSocket;
- Next.js — UI;
- Postgres/Timescale — history / metrics.

Также задано правило сохранять backward compatibility для Redis, WebSocket и storage contracts, если пользователь явно не разрешил breaking change.

## 4.4 Роли агентов

### `@trade-lead`
Главный orchestrator.

Отвечает за:
- формулировку плана;
- выбор специалистов;
- согласованность cross-service решений;
- полноту deliverables: contracts, tests, metrics, rollout.

### `@microstructure-analyst`
Роль для рыночной логики и risk-analysis.

Отвечает за:
- валидность сигнала;
- regime sensitivity;
- spread/slippage/latency risk;
- trade-off между FP/FN;
- measurable success criteria.

### `@go-ingest-engineer`
Отвечает за edge ingestion слой.

Фокус:
- exchange WS;
- reconnect / ping-pong / backpressure;
- timestamp normalization;
- sequencing;
- Redis pub/sub и streams contracts;
- graceful shutdown и metrics.

### `@python-signal-engineer`
Отвечает за Python-анализ и replayable signal logic.

Фокус:
- detectors / feature extraction;
- robust thresholds;
- state machines;
- quarantine / degradation;
- replay compatibility.

### `@platform-api-ui-engineer`
Отвечает за transport contracts и UI delivery.

Фокус:
- Redis -> NestJS ingestion;
- DTO и schema versioning;
- WebSocket payloads;
- Next.js rendering;
- backward compatibility.

### `@timeseries-dba`
Отвечает за Postgres/Timescale.

Фокус:
- hypertables;
- indexes;
- retention / compression;
- EXPLAIN-based validation;
- migration safety;
- исключение лишней synchronous DB pressure из hot path.

### `@sre-rollout`
Отвечает за observability и safe release.

Фокус:
- metrics и structured logs;
- SLO / SLI;
- shadow / canary / ramp;
- rollback triggers;
- degraded safe modes;
- operational runbooks.

### `@ml-replay-engineer`
Отвечает за replay / dataset / ML validation.

Фокус:
- deterministic replay;
- baseline diffing;
- dataset schemas;
- calibration / drift;
- shadow vs enforce;
- недопущение unverifiable model changes в production.

## 4.5 Collaboration protocol

В pack зафиксирована последовательность координации:
1. `@trade-lead` формулирует задачу и выбирает специалистов.
2. Специалисты дают предметные рекомендации.
3. `@trade-lead` собирает единый implementation plan.
4. `@sre-rollout` проверяет production safety.
5. Если задействован replay или ML — финальная валидация через `@ml-replay-engineer`.

## 4.6 Output quality bar

`agents.md` требует:
- repository-ready content;
- exact filenames;
- ENV keys;
- migrations;
- contracts;
- tests;
- metrics;
- rollback steps;
- явное разделение fact / assumption / risk.

---

## 5. Документация по skills

## 5.1 Общая логика skills

Все skills проектно-специфичны и лежат в `.agents/skills/`.

Каждый skill:
- задаёт отдельный кусок экспертизы;
- активируется по смыслу запроса;
- усиливает агента не общими советами, а конкретным operational style.

Ниже — документация по каждому реализованному skill.

---

## 5.2 `trade-project-core`

### Назначение
Главный базовый skill для любых TRADE-задач.

### Когда используется
- общий архитектурный вопрос;
- cross-service интеграция;
- review production changes;
- ambiguous TRADE request;
- latency-sensitive refactor;
- подготовка implementation plan.

### Что навязывает
- ответ в production-формате;
- обязательные sections;
- `Facts / Assumptions / Risks`;
- explicit time semantics;
- backward compatibility по Redis/WS;
- versioned payload contracts;
- observability for every major change.

### Практический смысл
Если запрос широкий или затрагивает несколько подсистем, именно этот skill должен быть базовым.

---

## 5.3 `trade-data-quality-time`

### Назначение
Skill для всех проблем, связанных с временем и качеством рыночных данных.

### Когда использовать
- inconsistent timestamp units;
- epoch sec/ms confusion;
- out-of-order data;
- monotonicity violations;
- stale ticks/bars;
- source-consistency issues;
- bad bars / malformed payloads.

### Ключевые правила
- timestamp format должен быть назван явно;
- timezone должна быть названа явно;
- must handle monotonicity;
- bad path: `detect -> sanitize -> quarantine -> metrics`;
- robust handling of outliers;
- distinguish sanitize vs quarantine policy.

### Ожидаемые deliverables
- schema/contract fixes;
- quarantine rules;
- metrics and counters by reason;
- tests на bad-time и ordering edge cases.

### Практический смысл
Это skill для всего, что может silently разрушить determinism или replayability.

---

## 5.4 `trade-go-redis-ingest`

### Назначение
Skill для Go ingestion и обменного edge layer.

### Когда использовать
- Binance/exchange WebSocket;
- reconnect handling;
- ping/pong;
- parser stability;
- Redis publishing;
- stream contracts;
- hot-path performance.

### Основные правила
- explicit normalized time fields;
- deterministic sequencing where possible;
- graceful shutdown;
- allocation-aware hot path;
- simple and explicit contracts;
- observability на ingestion edge.

### Ожидаемые deliverables
- file-by-file Go changes;
- Redis channel/stream contract;
- metrics;
- reconnect/backpressure logic;
- integration/load validation plan.

---

## 5.5 `trade-python-signal-engine`

### Назначение
Skill для Python signal workers и detector logic.

### Когда использовать
- new detector;
- refactor of signal pipeline;
- feature extraction;
- rolling thresholds;
- volatility/volume/orderflow analysis;
- Redis consumer/producer logic;
- replay-safe Python changes.

### Основные правила
- разделять ingestion, feature extraction, decision logic, publishing;
- prefer pure functions for feature math;
- avoid hidden mutable globals;
- publish machine-readable reason codes;
- robust estimators over fragile mean/std for noisy streams;
- explicitly document warmup requirements;
- distinguish sample insufficiency from negative signal.

### Required signal contract
Каждый сигнал должен иметь:
- trigger conditions;
- cooldown/dedup policy;
- historical lookback;
- confidence/severity;
- reason codes/evidence;
- false-positive controls.

### Required tests
- unit;
- edge cases;
- Redis integration;
- replay tests;
- latency/load budget checks.

---

## 5.6 `trade-api-ui-contracts`

### Назначение
Skill для NestJS, Next.js, DTO и transport layer contracts.

### Когда использовать
- Redis -> NestJS adapters;
- DTO/versioning;
- WebSocket contracts;
- REST contracts;
- frontend payload changes;
- UI rendering latency-sensitive data.

### Главные правила
- no implicit payload shape drift;
- explicit DTO/schema versioning;
- preserve backward compatibility;
- validate boundaries;
- align API and UI semantics.

### Deliverables
- DTOs/interfaces/contracts;
- API changes;
- frontend consumption changes;
- test plan для transport contracts.

---

## 5.7 `trade-timescale-postgres`

### Назначение
Skill для хранения истории и метрик в Postgres/Timescale.

### Когда использовать
- schema design;
- migrations;
- hypertables;
- retention/compression;
- indexes;
- continuous aggregates;
- performance tuning;
- write-path offloading.

### Главные правила
- explicit reversible migrations where practical;
- exact timestamp semantics;
- separate hot ingest from analytical aggregates when needed;
- indexes must reflect real query predicates;
- avoid synchronous DB writes on hot path.

### Required analysis
- workload shape;
- table design;
- index plan;
- retention/compression plan;
- rollback plan;
- EXPLAIN validation;
- DB observability.

### Deliverables
- DDL/migrations;
- backfill strategy;
- query examples;
- tuning notes;
- lock/bloat/retention failure modes.

---

## 5.8 `trade-observability-rollout`

### Назначение
Skill для production observability и release safety.

### Когда использовать
- production readiness review;
- metrics/logging/alert design;
- SLO/SLI;
- shadow/canary/enforce planning;
- rollback strategy;
- degradation planning.

### Обязательные deliverables
- RED metrics или equivalent;
- domain metrics;
- structured log fields;
- alert rules with thresholds;
- rollout stages;
- rollback triggers.

### Preferred rollout ladder
1. Local verification
2. Replay/backtest/fixture validation
3. Shadow mode
4. Canary
5. Gradual ramp
6. Full enablement

### Практический смысл
Любая production-affecting change без этого skill будет неполной.

---

## 5.9 `trade-ml-replay-gating`

### Назначение
Skill для deterministic replay, regression control и ML-sensitive changes.

### Когда использовать
- baseline diffing;
- replay dataset export;
- ML confirm logic;
- calibration/drift;
- offline validation;
- promotion logic;
- shadow vs enforce для ML.

### Основные правила
- canonical dataset contract;
- explicit timestamp normalization;
- measurable pass/fail criteria;
- no unverifiable ML changes in production path;
- clear retention assumptions.

### Deliverables
- dataset schema;
- replay steps;
- comparison metrics;
- failure thresholds;
- rollout safety gates.

---

## 6. Документация по workflows

## 6.1 Общая логика workflows

Workflows — это готовые slash-команды, которые стандартизируют типовой рабочий процесс.

Они нужны, когда важно не просто получить ответ, а пройти определённую orchestration sequence.

Ниже — документация по каждому workflow.

---

## 6.2 `/trade-audit`

### Файл
`.agents/workflows/trade-audit.md`

### Назначение
Полный production-readiness аудит подсистемы или всего pipeline.

### Когда использовать
- архитектурный аудит;
- audit before release;
- technical debt review;
- contract review;
- latency / risk / observability audit.

### Что делает
- поднимает `@trade-lead`;
- всегда грузит `trade-project-core` и `trade-observability-rollout`;
- по scope подключает остальные skills;
- собирает единый merged report.

### Структура результата
- Goal
- Facts
- Assumptions
- Risks
- Findings by subsystem
- Required file changes
- Tests
- Metrics / alerts
- Rollout / rollback
- Prod checklist

### Приоритизация
- P0 — unsafe / data-corrupting / capital-risk / broken prod path
- P1 — high operational risk / fragile contracts / weak observability
- P2 — correctness / maintainability debt
- P3 — optimization / polish

### Пример
```text
/trade-audit full pipeline
/trade-audit python orderflow worker
/trade-audit Redis -> NestJS -> WS contracts
```

---

## 6.3 `/trade-new-signal`

### Файл
`.agents/workflows/trade-new-signal.md`

### Назначение
Создание или redesign нового detector/gate/signal.

### Когда использовать
- новый volatility spike detector;
- новый gate;
- redesign confirmation path;
- refactor signal logic.

### Что делает
- превращает идею в concrete problem statement;
- всегда включает `trade-python-signal-engine`, `trade-data-quality-time`, `trade-observability-rollout`;
- подключает остальные skills по необходимости;
- сначала заставляет `@microstructure-analyst` определить рыночную логику и edge;
- затем переводит это в конкретный implementation plan;
- завершает метриками и rollout ladder.

### Выход
- Signal definition
- Architecture changes
- File-by-file implementation plan
- Tests
- Metrics / alerts
- Rollout / rollback

### Пример
```text
/trade-new-signal volatility spike with robust baseline
/trade-new-signal add spoofing-resilience gate for meme symbols
```

---

## 6.4 `/trade-replay`

### Файл
`.agents/workflows/trade-replay.md`

### Назначение
Построение deterministic replay и regression-validation path.

### Когда использовать
- replay of ticks/books/signals;
- baseline diffing;
- dataset export;
- offline regression checks;
- ML validation.

### Что делает
- определяет replay scope;
- обязательно включает `trade-ml-replay-gating`, `trade-data-quality-time`, `trade-observability-rollout`;
- при необходимости подключает Python/Go/DB skills;
- задаёт canonical payload schema, ordering rules, baseline artifacts, comparison metrics и failure thresholds.

### Выход
- Replay scope and data contracts
- Required files/scripts/ENV/storage
- Validation metrics and pass/fail thresholds
- Tests
- Rollout / rollback for replay path

### Пример
```text
/trade-replay of_confirm BTCUSDT last 24h
/trade-replay ml_confirm regression baseline
```

---

## 6.5 `/trade-rollout`

### Файл
`.agents/workflows/trade-rollout.md`

### Назначение
Безопасный план выпуска production-affecting change.

### Когда использовать
- deploy detector;
- enable new gate;
- promote ML model;
- switch contracts;
- change DB/storage path.

### Что делает
- переводит change в release plan;
- всегда использует `trade-project-core` и `trade-observability-rollout`;
- требует stage-by-stage rollout;
- подключает subsystem owner review;
- задаёт measurable thresholds и rollback triggers.

### Выход
- Preconditions
- Stage-by-stage rollout
- Metrics and alert thresholds
- Automatic rollback triggers
- Manual rollback procedure
- Post-deploy validation checklist

### Пример
```text
/trade-rollout ml_confirm challenger promotion
/trade-rollout new orderflow gate on BTC and ETH only
```

---

## 6.6 `/trade-postmortem`

### Файл
`.agents/workflows/trade-postmortem.md`

### Назначение
Root-cause analysis по incident/regression.

### Когда использовать
- bad signals;
- stale ticks;
- data corruption;
- regression after rollout;
- outage or unexpected trading behavior.

### Что делает
- собирает incident summary и impact;
- подключает нужные subsystem skills;
- реконструирует timeline;
- разделяет observed facts и inference;
- требует corrective/preventive actions.

### Выход
- Incident summary
- Facts
- Assumptions
- Risks
- Timeline
- Likely root causes
- Contributing factors
- Immediate mitigation
- Permanent corrective actions
- Required tests / monitors / alerts
- Rollout / rollback changes to prevent recurrence

### Пример
```text
/trade-postmortem stale ticks caused false entries
/trade-postmortem replay mismatch after new detector rollout
```

---

## 7. Как пользоваться конфигурацией

## 7.1 Установка

1. Скопировать `.agents/` в корень репозитория `trade`.
2. Переоткрыть workspace в Antigravity.
3. Работать либо через обычные запросы с `TRADE:`, либо через slash-команды.

## 7.2 Когда использовать обычный запрос

Используйте обычный чат, если задача точечная.

Примеры:

```text
TRADE: проверь time semantics в Redis -> Python -> NestJS пайплайне
TRADE: добавь detector volatility spike с robust baseline median/MAD
TRADE: предложи Timescale schema для history и metrics
TRADE: подготовь Prometheus metrics и alerts для signal worker
```

## 7.3 Когда использовать workflow

Используйте workflow, если задача сложная и повторяемая.

- `/trade-audit` — нужен большой аудит
- `/trade-new-signal` — создаётся новый сигнал или gate
- `/trade-replay` — нужна replay/regression логика
- `/trade-rollout` — нужен safe production rollout
- `/trade-postmortem` — нужен RCA по incident/regression

## 7.4 Как выбирать между skills и workflows

Практически:
- skill выбирает сам Antigravity по смыслу;
- workflow выбираете вы slash-командой;
- `agents.md` задаёт общие правила всегда внутри workspace;
- skill уточняет узкую экспертизу;
- workflow принудительно задаёт последовательность исполнения.

---

## 8. Рекомендуемый стиль запросов

Для лучшего срабатывания routing используйте:
- префикс `TRADE:`;
- явное имя подсистемы;
- ожидаемый тип результата.

Хорошие примеры:

```text
TRADE: аудит Go ingest для Binance futures. Нужны contracts, reconnect, metrics и rollback plan.
TRADE: улучши Python detector. Нужны diff по файлам, unit/integration/replay tests и latency budget.
TRADE: спроектируй Timescale hypertables для signal history. Нужны SQL, retention, compression и observability.
```

Плохие примеры:

```text
сделай лучше
оптимизируй проект
что думаешь о коде
```

Причина: routing skill'ов лучше работает, когда запрос содержит конкретный subsystem + task type.

---

## 9. Порядок расширения

## 9.1 Когда добавлять новый skill

Добавляйте новый skill, если:
- появляется новая устойчивая предметная область;
- инструкции по ней слишком объёмны для `agents.md`;
- эта экспертиза должна подгружаться только по релевантным задачам.

Примеры будущих skill'ов для `trade`:
- `trade-execution-risk`
- `trade-news-integration`
- `trade-telegram-ops`
- `trade-feature-store`
- `trade-golden-replay`

## 9.2 Когда добавлять новый workflow

Добавляйте workflow, если:
- сценарий повторяется;
- у него есть стабильная choreography;
- важен fixed execution order.

Примеры будущих workflow'ов:
- `/trade-contract-check`
- `/trade-ml-promotion`
- `/trade-slo-review`
- `/trade-dq-investigation`
- `/trade-schema-change`

## 9.3 Правила для нового skill

Новый skill должен:
- иметь отдельную папку;
- содержать `SKILL.md`;
- иметь точный `description`;
- описывать когда использовать skill;
- задавать явные deliverables;
- не дублировать без причины `agents.md`.

---

## 10. Ограничения и ожидания

Эта конфигурация:
- не заменяет code review;
- не гарантирует, что Antigravity всегда выберет идеальный skill;
- не отменяет необходимости проверять реальные контракты в коде;
- не подменяет ваши production approvals.

Но она сильно улучшает:
- consistency;
- routing по доменам;
- полноту deliverables;
- качество rollout/rollback thinking;
- дисциплину по времени, data-quality и observability.

---

## 11. Краткая памятка

### Если задача про всю систему
Использовать `TRADE:` или `/trade-audit`.

### Если задача про новый detector/gate
Использовать `TRADE:` или `/trade-new-signal`.

### Если задача про replay/ML validation
Использовать `TRADE:` или `/trade-replay`.

### Если задача про production release
Использовать `TRADE:` или `/trade-rollout`.

### Если задача про incident/regression
Использовать `TRADE:` или `/trade-postmortem`.

---

## 12. Рекомендуемое следующее улучшение

Следующий практичный шаг для pack:
- добавить `scripts/` в отдельные skills, если вы хотите, чтобы Antigravity не только писал план, но и выполнял часть рутинных проверок;
- добавить `references/` для контрактов Redis, DTO-схем, naming rules, metric catalog;
- вынести golden examples для signals / replay / rollout в отдельные ресурсы;
- при необходимости сократить `agents.md` и часть общих правил перенести в `trade-project-core`, чтобы уменьшить шум в always-on контексте.
