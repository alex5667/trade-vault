---
title: "План обучения: инженерная организация production-проекта trade"
created: "2026-05-31"
language: "ru"
project_context: "Go → Redis → Python → NestJS → Next.js → PostgreSQL/Timescale"
format: "Markdown / Obsidian-ready"
tags:
  - engineering-management
  - software-architecture
  - data-engineering
  - distributed-systems
  - sre
  - observability
  - security-engineering
  - mlops
  - ai-assisted-development
  - trade-systems
---

# План обучения: инженерная организация production-проекта trade

## 1. Цель

Цель — вырасти из пользователя AI-инструментов в **technical owner / инженерного архитектора**, который умеет управлять созданием сложной production-системы:

```text
Go market-data collectors
        ↓
Redis Streams / queues
        ↓
Python analysis / signals / ML
        ↓
NestJS aggregation / API / WebSocket
        ↓
Next.js UI
        ↓
PostgreSQL / Timescale history, metrics, labels, signals
```

Фокус обучения не на синтаксисе Go, Python или TypeScript, а на инженерной организации:

- архитектура;
- контракты данных;
- надёжность;
- replayability;
- управление изменениями;
- observability;
- тестовая стратегия;
- безопасность;
- ML/AI engineering;
- контроль AI-generated кода.

Главная формула:

> Не “как написать код”, а “как построить систему, которую можно развивать, проверять, наблюдать, откатывать и доверять ей”.

---

## 2. Что есть

### Факты

- Есть практический проект с несколькими сервисами и потоками данных.
- Есть базовые знания программирования.
- Активно используются AI-инструменты: Codex, Claude Code, Gemini.
- Нужен слой выше кодинга: организация, архитектура, управление качеством, production-мышление.
- Для trade-проекта критичны: время, свежесть данных, дубли, gaps, stale events, latency, replay, риск и объяснимость сигналов.

### Предположения

- Оптимальный путь — прикладная программа на 9–12 месяцев, а не классический CS с нуля.
- Обучение должно идти через документы и артефакты: ADR, C4, RFC, contracts, runbooks, SLO, dashboards, failure matrix.
- AI должен использоваться как controlled execution tool, а не как “случайный генератор diffs”.

### Риски

- Хаотичное чтение без практики.
- Ранний уход в Kubernetes/Kafka/Raft/ML papers до формирования инженерного каркаса.
- Слепое доверие AI-generated коду.
- Отсутствие контрактов данных между Go, Redis, Python, NestJS и UI.
- Отсутствие replayability: невозможно доказать, почему сигнал появился.
- Отсутствие метрик data quality: система “работает”, но данные могут быть плохими.

---

## 3. Карта тегов

Используйте эти теги в Obsidian/Notion/GitHub Docs для заметок, ADR, RFC и задач.

### Core engineering

- `#engineering-mindset`
- `#software-engineering`
- `#technical-ownership`
- `#maintainability`
- `#technical-debt`
- `#code-review`
- `#ai-code-review`

### Architecture

- `#architecture`
- `#c4`
- `#adr`
- `#rfc`
- `#service-boundaries`
- `#contracts`
- `#dto`
- `#api-design`
- `#backward-compatibility`

### Data / streaming

- `#data-engineering`
- `#event-contracts`
- `#schema-versioning`
- `#idempotency`
- `#deduplication`
- `#ordering`
- `#backpressure`
- `#redis-streams`
- `#consumer-groups`
- `#replayability`

### Time / market data

- `#market-data`
- `#klines`
- `#ticks`
- `#orderbook`
- `#epoch-ms`
- `#clock-skew`
- `#stale-data`
- `#gaps`
- `#duplicates`
- `#data-quality`

### Storage

- `#postgresql`
- `#timescale`
- `#hypertables`
- `#continuous-aggregates`
- `#retention`
- `#compression`
- `#indexes`
- `#transactions`
- `#wal`
- `#query-plans`

### Distributed systems / reliability

- `#distributed-systems`
- `#partial-failure`
- `#timeouts`
- `#retries`
- `#at-least-once`
- `#exactly-once-myth`
- `#consistency`
- `#fault-tolerance`
- `#failure-matrix`

### SRE / Observability

- `#sre`
- `#slo`
- `#sli`
- `#error-budget`
- `#observability`
- `#metrics`
- `#logs`
- `#traces`
- `#opentelemetry`
- `#runbook`
- `#incident-response`
- `#postmortem`

### DevOps / delivery

- `#devops`
- `#ci-cd`
- `#iac`
- `#feature-flags`
- `#canary`
- `#blue-green`
- `#rollout`
- `#rollback`
- `#dora`

### Security

- `#security`
- `#owasp`
- `#asvs`
- `#nist-ssdf`
- `#slsa`
- `#secrets`
- `#authn`
- `#authz`
- `#supply-chain-security`
- `#audit-logs`

### ML / AI Engineering

- `#mlops`
- `#production-ml`
- `#feature-engineering`
- `#labels`
- `#leakage`
- `#calibration`
- `#drift`
- `#train-serve-skew`
- `#champion-challenger`
- `#model-registry`

### Trade/risk

- `#trade-systems`
- `#risk-management`
- `#position-sizing`
- `#signal-quality`
- `#reason-codes`
- `#backtest-vs-live`
- `#latency-budget`
- `#risk-controls`

---

## 4. Каталог источников

### 4.1 Обязательные бесплатные источники

| ID | Источник | Тип | Для чего | Теги | Ссылка |
|---|---|---:|---|---|---|
| CORE-01 | Software Engineering at Google | free HTML book | Инженерная культура, code review, testing, large-scale changes | `#software-engineering` `#maintainability` `#code-review` | https://abseil.io/resources/swe-book/html/toc.html |
| CORE-02 | The Twelve-Factor App | free guide | ENV, config, backing services, logs, dev/prod parity | `#devops` `#config` `#logs` | https://12factor.net/ |
| ARCH-01 | C4 Model | free guide | Визуализация архитектуры: context/container/component/code | `#architecture` `#c4` | https://c4model.com/ |
| ARCH-02 | Architectural Decision Records | free guide | Фиксация решений, альтернатив и последствий | `#adr` `#decision-log` | https://adr.github.io/ |
| SRE-01 | Google SRE Books | free books | SLO, monitoring, incident response, capacity, postmortems | `#sre` `#slo` `#incident-response` | https://sre.google/books/ |
| OBS-01 | OpenTelemetry Docs | free docs | Metrics, logs, traces, context propagation | `#observability` `#opentelemetry` | https://opentelemetry.io/docs/ |
| DORA-01 | DORA Metrics | free guide | Delivery metrics: speed + safety of changes | `#dora` `#devops` | https://dora.dev/guides/dora-metrics/ |
| CLOUD-01 | AWS Well-Architected Framework | free docs | Architecture review через 6 pillars | `#architecture` `#cloud` `#reliability` | https://docs.aws.amazon.com/wellarchitected/latest/framework/the-pillars-of-the-framework.html |
| CLOUD-02 | Google Cloud Architecture Center | free docs | Reference architectures, reliability/security patterns | `#cloud` `#architecture` | https://docs.cloud.google.com/architecture |
| SEC-01 | OWASP Top Ten | free guide | Базовая карта web-security рисков | `#security` `#owasp` | https://owasp.org/www-project-top-ten/ |
| SEC-02 | OWASP ASVS | free standard | Проверка security controls и требований | `#security` `#asvs` | https://owasp.org/www-project-application-security-verification-standard/ |
| SEC-03 | NIST SSDF SP 800-218 | free standard/PDF | Secure SDLC, vulnerability response, secure release | `#security` `#nist-ssdf` | https://csrc.nist.gov/pubs/sp/800/218/final |
| SEC-04 | SLSA | free framework | Supply-chain security, build integrity, artifacts | `#security` `#slsa` `#supply-chain-security` | https://slsa.dev/ |
| ML-01 | Google Rules of Machine Learning | free guide | 43 правила production ML | `#mlops` `#production-ml` | https://developers.google.com/machine-learning/guides/rules-of-ml |
| ML-02 | Google ML Crash Course: Production ML Systems | free course | Production ML, data verification, feature extraction | `#mlops` `#production-ml` | https://developers.google.com/machine-learning/crash-course/production-ml-systems |
| ML-03 | Google Cloud MLOps: CD/automation pipelines | free architecture guide | CI/CD/CT для ML-систем | `#mlops` `#ci-cd` | https://docs.cloud.google.com/architecture/mlops-continuous-delivery-and-automation-pipelines-in-machine-learning |

### 4.2 Обязательные источники по данным и distributed systems

| ID | Источник | Тип | Для чего | Теги | Ссылка |
|---|---|---:|---|---|---|
| DATA-01 | Designing Data-Intensive Applications, 2nd ed. | paid book / official page | Data systems, storage, replication, transactions, streams | `#data-engineering` `#distributed-systems` | https://www.oreilly.com/library/view/designing-data-intensive-applications/9781098119058/ |
| DIST-01 | MIT 6.5840 Distributed Systems | free course | Fault tolerance, replication, consistency, case studies | `#distributed-systems` `#fault-tolerance` | https://pdos.csail.mit.edu/6.824/ |
| DB-01 | CMU 15-445/645 Database Systems | free course | Storage, indexes, transactions, recovery, query execution | `#databases` `#postgresql` | https://15445.courses.cs.cmu.edu/ |
| DB-02 | PostgreSQL Documentation | free docs | Indexes, transactions, WAL, monitoring | `#postgresql` `#wal` `#indexes` | https://www.postgresql.org/docs/current/ |
| TS-01 | Timescale/TigerData Docs | free docs | Hypertables, continuous aggregates, retention, compression | `#timescale` `#hypertables` | https://www.tigerdata.com/docs/ |
| STREAM-01 | Redis Streams Docs | free docs | Streams, consumer groups, ack, pending entries, replay | `#redis-streams` `#consumer-groups` | https://redis.io/docs/latest/develop/data-types/streams/ |

### 4.3 Источники по текущему стеку trade-проекта

| ID | Источник | Тип | Для чего | Теги | Ссылка |
|---|---|---:|---|---|---|
| TRADE-01 | Binance Spot WebSocket Streams | free docs | Klines, trades, timestamps, ping/pong, reconnect policy | `#market-data` `#websocket` `#epoch-ms` | https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams |
| GO-01 | Go Documentation | free docs | Goroutines, channels, concurrency, tooling | `#go` `#concurrency` | https://go.dev/doc/ |
| PY-01 | Python asyncio Queue | free docs | Async queues, producer/consumer, backpressure | `#python` `#asyncio` `#backpressure` | https://docs.python.org/3/library/asyncio-queue.html |
| BACKEND-01 | NestJS WebSocket Gateways | free docs | WS gateway, guards, pipes, adapters | `#nestjs` `#websocket` | https://docs.nestjs.com/websockets/gateways |
| FRONT-01 | Next.js Data Fetching | free docs | Server/client data flow, streaming UI | `#nextjs` `#frontend` | https://nextjs.org/docs/app/getting-started/fetching-data |

### 4.4 Платные книги / long-form, которые стоит купить или читать через легальный доступ

| ID | Источник | Приоритет | Для чего | Теги | Ссылка |
|---|---|---:|---|---|---|
| BOOK-01 | Release It! — Michael Nygard | High | Production stability, failure modes, integration points | `#reliability` `#production` | https://pragprog.com/titles/mnee2/release-it-second-edition/ |
| BOOK-02 | Building Microservices, 2nd ed. — Sam Newman | Medium | Service boundaries, deployment, testing, monitoring microservices | `#microservices` `#architecture` | https://samnewman.io/books/building_microservices_2nd_edition/ |
| BOOK-03 | Fundamentals of Software Architecture — Mark Richards, Neal Ford | Medium | Architecture thinking, trade-offs, architecture characteristics | `#architecture` `#trade-offs` | https://www.oreilly.com/library/view/fundamentals-of-software/9781098175504/ |
| BOOK-04 | Accelerate — Forsgren, Humble, Kim | Medium | DORA, delivery performance, engineering metrics | `#dora` `#engineering-management` | https://dl.acm.org/doi/10.5555/3235404 |

---

## 5. Что “подтянуть” как файлы и заметки

Создайте в репозитории документации проекта папку:

```text
docs/engineering-learning/
  00_index.md
  01_sources.md
  02_tags.md
  03_monthly_plan.md
  04_templates/
    adr-template.md
    rfc-template.md
    data-contract-template.md
    failure-matrix-template.md
    slo-template.md
    runbook-template.md
    ai-task-template.md
  05_trade-project/
    c4-context.md
    c4-container.md
    market-data-contracts.md
    redis-streams-contracts.md
    signal-contracts.md
    timescale-storage-plan.md
    observability-map.md
    risk-register.md
```

### Легально доступные online/download материалы

- `CORE-01` — читать online HTML. Не нужно скачивать, лучше делать заметки по главам.
- `SEC-03` — NIST SSDF имеет официальную PDF/Excel выгрузку на странице публикации.
- `ML-03` — Google Cloud MLOps whitepaper/guide доступен через Google Cloud resources.
- `SRE-01` — SRE Books читать online; для практики лучше конспектировать главы в свои notes.
- `OWASP` / `ASVS` / `SLSA` — использовать как чеклисты, не как книги для последовательного чтения.

Не добавляйте в проект пиратские PDF платных книг. Для `DATA-01`, `BOOK-01`, `BOOK-02`, `BOOK-03`, `BOOK-04` используйте страницы издателей, O’Reilly/ACM/PragProg или купленную копию.

---

## 6. Roadmap на 12 месяцев

## Месяц 1 — Инженерное мышление и контроль AI-кода

### Цель

Перестать мыслить “задача → код”. Начать мыслить:

```text
проблема → контекст → ограничения → варианты → решение → последствия → проверка → rollout → rollback
```

### Читать

- `CORE-01` Software Engineering at Google: главы про Software Engineering vs Programming, Code Review, Testing, Large-Scale Changes.
- `ARCH-02` ADR.
- `CORE-02` Twelve-Factor: Config, Logs, Backing Services.

### Практика на trade-проекте

Создать первые документы:

- `docs/architecture/adr/0001-use-redis-streams-for-market-data.md`
- `docs/architecture/adr/0002-time-policy-epoch-ms.md`
- `docs/architecture/adr/0003-signal-reason-codes.md`

### Результат месяца

Вы умеете принимать задачу от AI не как готовый diff, а как инженерное изменение с reason, tests и rollback.

### Теги

`#engineering-mindset` `#adr` `#ai-code-review` `#twelve-factor`

---

## Месяц 2 — Архитектура и границы сервисов

### Цель

Увидеть систему слоями: кто производит данные, кто потребляет, где состояние, где контракты.

### Читать

- `ARCH-01` C4 Model.
- `CLOUD-01` AWS Well-Architected: Operational Excellence, Reliability.
- `BOOK-03` выборочно: architecture characteristics, trade-offs.

### Практика на trade-проекте

Сделать C4-документы:

- System Context: пользователь, Binance, Go collectors, Python, NestJS, Next.js, Postgres/Timescale.
- Container diagram: Go, Redis, Python, NestJS, Next.js, DB, observability stack.
- Component diagram для Python signal service.
- Dynamic diagram для сценария: `kline → Redis → Python signal → NestJS WS → UI`.

### Результат месяца

Вы можете объяснить проект без кода: сервисы, ответственность, данные, риски.

### Теги

`#architecture` `#c4` `#service-boundaries` `#contracts`

---

## Месяц 3 — Data contracts и streaming foundation

### Цель

Понять, как данные рождаются, передаются, ломаются, версионируются и восстанавливаются.

### Читать

- `DATA-01` DDIA: intro, data models, encoding/evolution.
- `STREAM-01` Redis Streams.
- `TRADE-01` Binance WebSocket Streams.

### Практика на trade-проекте

Описать контракты:

```text
MarketKlineEvent v1
MarketTickEvent v1
OrderBookSnapshotEvent v1
VolatilitySignalEvent v1
VolumeSpikeSignalEvent v1
ModelPredictionEvent v1
```

Для каждого события указать:

- `event_name`
- `schema_version`
- `producer`
- `consumer`
- `idempotency_key`
- `event_time_ms`
- `ingest_time_ms`
- `source`
- `symbol`
- `timeframe`
- `required fields`
- `optional fields`
- `bad data policy`
- `retry behavior`
- `dead-letter/quarantine behavior`

### Результат месяца

Вы отличаете event от command, Pub/Sub от Stream, raw market data от derived signal.

### Теги

`#data-engineering` `#redis-streams` `#event-contracts` `#schema-versioning` `#idempotency`

---

## Месяц 4 — PostgreSQL / Timescale / storage thinking

### Цель

Понимать БД как storage engine + query engine + transaction system, а не просто таблицы.

### Читать

- `DB-01` CMU 15-445: storage, indexes, transactions.
- `DB-02` PostgreSQL docs: indexes, WAL, monitoring.
- `TS-01` Timescale docs: hypertables, continuous aggregates, retention.

### Практика на trade-проекте

Создать storage plan:

```text
raw_klines hypertable
raw_ticks hypertable
orderbook_snapshots hypertable
signals hypertable
model_predictions hypertable
signal_outcomes hypertable
service_metrics hypertable
```

Для каждой таблицы указать:

- partition time column;
- chunk interval;
- indexes;
- retention;
- compression;
- continuous aggregates;
- hot/warm/cold strategy;
- expected write rate;
- expected query patterns.

### Результат месяца

Вы умеете оценить, что будет при росте данных x10/x100.

### Теги

`#postgresql` `#timescale` `#hypertables` `#continuous-aggregates` `#retention` `#query-plans`

---

## Месяц 5 — Distributed systems и failure matrix

### Цель

Понять, что несколько сервисов — это не “больше кода”, а новая категория отказов.

### Читать

- `DIST-01` MIT 6.5840: introduction, RPC, fault tolerance, replication, Raft overview.
- `DATA-01` DDIA: replication, partitioning, transactions, distributed systems trouble.
- `BOOK-01` Release It!: stability patterns и failure modes.

### Практика на trade-проекте

Создать `failure-matrix.md`:

| Failure | Detection | Mitigation | Metric | Alert | Recovery |
|---|---|---|---|---|---|
| Binance WS disconnect | reconnect counter, stale data | reconnect, resubscribe | `binance_ws_reconnect_total` | warning/page по impact | replay gap if possible |
| Go producer duplicate | idempotency key | dedupe | `market_data_duplicate_total` | no page | quarantine if excessive |
| Redis unavailable | write errors | retry with backoff | `redis_write_errors_total` | page if sustained | degrade / stop ingestion |
| Python consumer dies after processing before ack | pending entries | reclaim pending | `redis_stream_pending_count` | warning/page | XAUTOCLAIM / replay |
| NestJS WS drops clients | connection count/drop rate | reconnect clients | `ws_client_disconnect_total` | warning | client retry |
| Timescale lag | freshness metric | batch insert tuning | `db_write_lag_ms` | page if stale signals | pause model promotion |

### Результат месяца

Вы понимаете at-least-once, duplicate processing, idempotency, retries, timeouts, partial failure.

### Теги

`#distributed-systems` `#partial-failure` `#failure-matrix` `#retries` `#timeouts` `#at-least-once`

---

## Месяц 6 — Testing strategy

### Цель

Проверять систему не только unit-тестами.

### Читать

- `CORE-01` Testing chapters.
- `SRE-01` главы про monitoring/release/postmortems.
- `BOOK-01` Release It! по integration failure.

### Практика на trade-проекте

Для каждого pipeline-компонента определить тесты:

| Уровень | Что проверять |
|---|---|
| Unit | parsing Binance events, signal formulas, DTO validation |
| Integration | Go → Redis Streams, Python consumer group, NestJS Redis consumer |
| Contract | JSON schemas для events/signals |
| Replay | воспроизведение окна market data и сравнение signal outputs |
| Golden | фиксированные input candles → фиксированные expected signals |
| Load | burst klines/ticks по 5–50 symbols |
| Regression | найденный bug превращается в тест |

### Результат месяца

Вы умеете принять/отклонить AI-generated diff по тестовой стратегии.

### Теги

`#testing` `#contract-tests` `#golden-tests` `#replayability` `#load-tests`

---

## Месяц 7 — Observability и SRE

### Цель

Видеть production через метрики, логи, трассы и SLO.

### Читать

- `SRE-01` Google SRE Books: SLO, monitoring, incident response.
- `OBS-01` OpenTelemetry Docs.
- `DORA-01` DORA Metrics.

### Практика на trade-проекте

Создать dashboard map:

#### Market data freshness

- `market_data_event_lag_ms`
- `market_data_gap_total`
- `market_data_duplicate_total`
- `market_data_stale_total`
- `binance_ws_reconnect_total`

#### Redis Streams

- `redis_stream_len`
- `redis_stream_pending_count`
- `redis_stream_consumer_idle_ms`
- `redis_stream_ack_latency_ms`
- `redis_stream_claimed_total`

#### Python signals

- `signal_generated_total{type,reason_code}`
- `signal_processing_latency_ms`
- `signal_error_total`
- `signal_quarantine_total`
- `feature_missing_total`

#### NestJS / WebSocket

- `ws_clients_connected`
- `ws_emit_latency_ms`
- `api_request_duration_ms`
- `api_error_total`

#### Timescale

- `db_insert_latency_ms`
- `db_write_lag_ms`
- `continuous_aggregate_refresh_lag_ms`
- `hypertable_chunk_count`
- `retention_job_fail_total`

### Результат месяца

Для каждого алерта вы можете сказать: impact, owner, first checks, mitigation, rollback.

### Теги

`#sre` `#observability` `#opentelemetry` `#slo` `#metrics` `#runbook`

---

## Месяц 8 — DevOps и безопасная доставка изменений

### Цель

Научиться менять production безопасно.

### Читать

- `DORA-01` DORA Metrics.
- `CLOUD-01` AWS Well-Architected: Operational Excellence.
- `CLOUD-02` Google Cloud Architecture Center.

### Практика на trade-проекте

Для каждой крупной фичи использовать rollout:

```text
OFF
shadow mode
internal only
canary 5%
canary 25%
full rollout
monitoring window
rollback trigger
```

Пример для новой модели сигналов:

```text
v15_model = challenger
v14_model = champion
v15 writes predictions but does not affect UI/risk
compare PR-AUC, calibration, pass-rate, live hit-rate
promote only after gates
rollback to v14 if ECE/pass-rate/freshness degrades
```

### Результат месяца

Вы мыслите не “залить код”, а “управляемо изменить production”.

### Теги

`#devops` `#ci-cd` `#canary` `#rollback` `#dora` `#feature-flags`

---

## Месяц 9 — Security engineering

### Цель

Встроить security в архитектуру, а не проверять “потом”.

### Читать

- `SEC-01` OWASP Top Ten.
- `SEC-02` OWASP ASVS.
- `SEC-03` NIST SSDF.
- `SEC-04` SLSA.

### Практика на trade-проекте

Security review checklist:

- Где secrets: Binance API, DB, Redis, JWT, OAuth?
- Кто имеет доступ к admin endpoints?
- Где auth boundary: NestJS API, WS namespace, dashboard?
- Есть ли rate limit?
- Логируются ли sensitive values?
- Есть ли audit logs для ручных действий?
- Проверяются ли dependencies?
- Есть ли pinned versions?
- Можно ли подменить Docker image / artifact?
- Есть ли separation между dev/staging/prod?

### Результат месяца

Вы можете провести security review задачи до merge.

### Теги

`#security` `#owasp` `#asvs` `#nist-ssdf` `#slsa` `#secrets` `#authz`

---

## Месяц 10 — ML / AI Engineering

### Цель

Понимать production ML как систему вокруг модели, а не только модель.

### Читать

- `ML-01` Google Rules of ML.
- `ML-02` Production ML Systems.
- `ML-03` MLOps pipelines.

### Практика на trade-проекте

Для каждой ML-фичи описывать:

```text
feature_name
feature_version
data_source
training_availability
serving_availability
missing_policy
leakage_risk
train_serve_skew_check
drift_metric
coverage_metric
offline_metric
online_metric
calibration_metric
promotion_gate
rollback_model
```

Champion/challenger checklist:

- baseline model exists;
- challenger writes shadow predictions;
- prediction distribution monitored;
- calibration monitored;
- pass-rate monitored;
- label delay documented;
- leakage checks done;
- rollback is one config change;
- model version is attached to every signal.

### Результат месяца

Вы отличаете “красивая offline-метрика” от “модель безопасна для production”.

### Теги

`#mlops` `#production-ml` `#leakage` `#calibration` `#drift` `#champion-challenger`

---

## Месяц 11 — Управление AI-разработкой

### Цель

Использовать Codex/Claude/Gemini как контролируемую инженерную команду.

### Практика

Каждую AI-задачу давать только через spec-first шаблон:

```md
# AI Task

## Role
You are a senior production engineer.

## Goal
...

## Current context
...

## Files allowed to change
...

## Files forbidden to change
...

## Constraints
- strict typing only
- no hidden dependencies
- preserve public contracts
- deterministic timestamps
- idempotency required

## Expected contract
...

## Required tests
- unit
- integration
- contract
- replay/golden if data pipeline

## Required observability
- metrics
- logs
- reason codes
- alerts if action-signal

## Rollout
...

## Rollback
...

## Output format
1. Plan
2. Risks
3. Diff
4. Tests
5. Metrics
6. Rollout/RB
```

### Результат месяца

AI перестаёт быть “магическим программистом” и становится controlled execution tool.

### Теги

`#ai-assisted-development` `#spec-first` `#ai-code-review` `#contracts` `#rollout`

---

## Месяц 12 — Инженерное управление проектом

### Цель

Собрать знания в систему управления проектом.

### Создать 5 постоянных документов

1. `Architecture Map`
2. `ADR Log`
3. `Risk Register`
4. `Runbooks`
5. `Engineering Metrics Dashboard`

### Результат месяца

Вы управляете проектом как инженерной системой, а не как набором задач.

### Теги

`#technical-ownership` `#roadmap` `#risk-register` `#architecture-review` `#engineering-management`

---

## 7. Недельный режим обучения

Оптимальный режим:

```text
5 дней в неделю по 60–90 минут
1 день практика 2–3 часа
1 день отдых / повторение
```

### Шаблон недели

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

## 8. Практические шаблоны

## 8.1 ADR template

```md
# ADR-000X: Title

## Status
Proposed | Accepted | Deprecated | Superseded

## Context
What problem are we solving?
What constraints exist?
What is the production impact?

## Decision
What exactly are we choosing?

## Alternatives considered
1. Option A
2. Option B
3. Option C

## Consequences
Positive:
- ...

Negative:
- ...

## Risks
- ...

## Observability
Metrics:
- ...

Logs:
- ...

Alerts:
- ...

## Rollout
- ...

## Rollback
- ...
```

## 8.2 Data contract template

```md
# Event Contract: MarketKlineEvent v1

## Producer
Go Binance collector

## Consumers
Python analysis service
NestJS aggregation service
Timescale writer

## Schema
```json
{
  "event_name": "market.kline",
  "schema_version": 1,
  "source": "binance_spot",
  "symbol": "BTCUSDT",
  "timeframe": "1m",
  "event_time_ms": 1710000000000,
  "ingest_time_ms": 1710000000123,
  "open": "0.0",
  "high": "0.0",
  "low": "0.0",
  "close": "0.0",
  "volume": "0.0",
  "is_closed": true,
  "idempotency_key": "binance_spot:BTCUSDT:1m:1710000000000"
}
```

## Time policy
- Exchange timestamps: epoch ms.
- Internal timestamps: epoch ms.
- UI formatting: user timezone only at presentation layer.
- Reject/quarantine if event_time_ms is in the future beyond allowed skew.

## Bad data policy
Detect → sanitize/quarantine → metric → reason code.

## Idempotency
Key: `source:symbol:timeframe:event_time_ms`.

## Replay
Raw event must be replayable from Redis/Timescale without changing derived signal output.
```

## 8.3 SLO template

```md
# SLO: Market Data Freshness

## User impact
UI and signal engine must not operate on stale market data.

## SLI
p95(now_ms - event_time_ms) for closed klines.

## SLO
99% of closed kline events are processed within 2 seconds.

## Error budget
1% stale/late events per rolling 24h.

## Alert
Page only if stale data can affect live action-signal.

## Dashboard
- freshness p50/p95/p99
- gaps
- duplicates
- stale count
- reconnect count
```

## 8.4 Runbook template

```md
# Runbook: Redis Stream Pending Entries Growing

## Symptom
`redis_stream_pending_count` grows for > 5 minutes.

## Impact
Signals may be delayed or duplicated after reclaim.

## First checks
1. Is Python consumer alive?
2. Is Redis reachable?
3. Are errors increasing?
4. Are pending entries old or fresh?
5. Are ack latencies increasing?

## Mitigation
- restart consumer if dead;
- use XAUTOCLAIM for stale pending;
- reduce batch size if processing latency high;
- temporarily pause non-critical consumers.

## Rollback
Switch signal pipeline to last stable consumer version.

## Follow-up
Create regression test and incident note.
```

---

## 9. Метрики развития

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

---

## 10. Правильная последовательность чтения

1. `CORE-01` Software Engineering at Google.
2. `ARCH-01` C4 + `ARCH-02` ADR.
3. `CORE-02` Twelve-Factor App.
4. `DATA-01` Designing Data-Intensive Applications.
5. `STREAM-01` Redis Streams + `TS-01` Timescale docs.
6. `SRE-01` Google SRE.
7. `OBS-01` OpenTelemetry + `DORA-01` DORA.
8. `SEC-01` / `SEC-02` / `SEC-03` / `SEC-04`.
9. `ML-01` / `ML-02` / `ML-03`.
10. `DIST-01` MIT + `DB-01` CMU выборочно.

---

## 11. Что не делать в начале

- Не начинать с Kubernetes.
- Не начинать с Kafka internals.
- Не начинать с Raft implementation.
- Не начинать с ML papers.
- Не начинать с cloud certifications.
- Не читать 10 книг одновременно.
- Не просить AI сразу писать код без контракта, тестов и rollback.
- Не добавлять новый сервис, пока не описаны границы ответственности.
- Не строить live trading/risk automation без reason-codes, SLO и kill-switch.

---

## 12. Prod-checklist для technical owner

Для каждого изменения:

- [ ] Я понимаю проблему, а не только задачу.
- [ ] Есть схема системы.
- [ ] Есть границы ответственности.
- [ ] Есть контракт данных.
- [ ] Есть schema version.
- [ ] Есть idempotency key.
- [ ] Есть timestamp policy: epoch ms/sec, TZ, skew, monotonicity.
- [ ] Есть bad data policy: detect → sanitize/quarantine → metrics.
- [ ] Есть reason codes / error codes.
- [ ] Есть тестовая стратегия.
- [ ] Есть replay/golden test для data/signals.
- [ ] Есть observability: metrics/logs/traces.
- [ ] Есть security considerations.
- [ ] Есть rollout.
- [ ] Есть rollback.
- [ ] Есть критерий успеха.
- [ ] Есть критерий остановки.
- [ ] Есть документированное решение.

---

## 13. Минимальный набор документов для вашего trade-проекта

Создайте и поддерживайте:

```text
docs/
  architecture/
    c4-context.md
    c4-container.md
    c4-components-python-signals.md
    adr/
      0001-use-redis-streams.md
      0002-time-policy-epoch-ms.md
      0003-signal-reason-codes.md
      0004-timescale-storage-strategy.md
  contracts/
    market-kline-event-v1.md
    market-tick-event-v1.md
    orderbook-event-v1.md
    volatility-signal-event-v1.md
    ml-prediction-event-v1.md
  sre/
    slo-market-data-freshness.md
    slo-signal-latency.md
    dashboard-map.md
    alerts.md
    runbooks/
      redis-pending-growing.md
      binance-ws-disconnect.md
      timescale-write-lag.md
  security/
    security-review-checklist.md
    secrets-map.md
    dependency-policy.md
  ml/
    feature-registry.md
    model-promotion-policy.md
    champion-challenger.md
    leakage-checklist.md
  risk/
    risk-register.md
    kill-switch-policy.md
```

---

## 14. Первая практическая неделя

### День 1

Прочитать:

- `CORE-01` Software Engineering vs Programming.
- `ARCH-02` ADR intro.

Сделать заметку:

```text
Почему “код работает” недостаточно для production-системы?
```

### День 2

Создать ADR:

```text
ADR-0001: Use Redis Streams instead of Redis Pub/Sub for reliable signal pipeline
```

### День 3

Описать C4 Context для trade-проекта.

### День 4

Создать первый data contract:

```text
MarketKlineEvent v1
```

### День 5

Дать AI задачу:

```text
Проверь мой MarketKlineEvent v1 contract как senior data engineer.
Найди риски: time skew, duplicates, gaps, stale data, idempotency, replayability.
Не пиши код. Дай только review и улучшенный контракт.
```

### День 6

Создать dashboard-map v0:

- freshness;
- duplicates;
- gaps;
- Redis pending;
- signal latency;
- DB write lag.

### День 7

Повторить и привести заметки к единому формату.

---

## 15. Итоговая установка

Ваша сила не в том, чтобы писать код быстрее AI.

Ваша сила — понимать лучше AI:

- что строить;
- зачем строить;
- где границы;
- какие данные считаются валидными;
- где система может обмануть вас;
- как проверить;
- как наблюдать;
- как откатить;
- как не потерять контроль над production.

Это и есть переход от “человека, который просит AI написать код” к инженеру, который управляет созданием сложной системы.
