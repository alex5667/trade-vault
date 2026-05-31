# 01. Каталог источников

## 1. Обязательные бесплатные источники

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

## 2. Обязательные источники по данным и distributed systems

| ID | Источник | Тип | Для чего | Теги | Ссылка |
|---|---|---:|---|---|---|
| DATA-01 | Designing Data-Intensive Applications, 2nd ed. | paid book / official page | Data systems, storage, replication, transactions, streams | `#data-engineering` `#distributed-systems` | https://www.oreilly.com/library/view/designing-data-intensive-applications/9781098119058/ |
| DIST-01 | MIT 6.5840 Distributed Systems | free course | Fault tolerance, replication, consistency, case studies | `#distributed-systems` `#fault-tolerance` | https://pdos.csail.mit.edu/6.824/ |
| DB-01 | CMU 15-445/645 Database Systems | free course | Storage, indexes, transactions, recovery, query execution | `#databases` `#postgresql` | https://15445.courses.cs.cmu.edu/ |
| DB-02 | PostgreSQL Documentation | free docs | Indexes, transactions, WAL, monitoring | `#postgresql` `#wal` `#indexes` | https://www.postgresql.org/docs/current/ |
| TS-01 | Timescale/TigerData Docs | free docs | Hypertables, continuous aggregates, retention, compression | `#timescale` `#hypertables` | https://www.tigerdata.com/docs/ |
| STREAM-01 | Redis Streams Docs | free docs | Streams, consumer groups, ack, pending entries, replay | `#redis-streams` `#consumer-groups` | https://redis.io/docs/latest/develop/data-types/streams/ |

## 3. Источники по текущему стеку trade-проекта

| ID | Источник | Тип | Для чего | Теги | Ссылка |
|---|---|---:|---|---|---|
| TRADE-01 | Binance Spot WebSocket Streams | free docs | Klines, trades, timestamps, ping/pong, reconnect policy | `#market-data` `#websocket` `#epoch-ms` | https://developers.binance.com/docs/binance-spot-api-docs/web-socket-streams |
| GO-01 | Go Documentation | free docs | Goroutines, channels, concurrency, tooling | `#go` `#concurrency` | https://go.dev/doc/ |
| PY-01 | Python asyncio Queue | free docs | Async queues, producer/consumer, backpressure | `#python` `#asyncio` `#backpressure` | https://docs.python.org/3/library/asyncio-queue.html |
| BACKEND-01 | NestJS WebSocket Gateways | free docs | WS gateway, guards, pipes, adapters | `#nestjs` `#websocket` | https://docs.nestjs.com/websockets/gateways |
| FRONT-01 | Next.js Data Fetching | free docs | Server/client data flow, streaming UI | `#nextjs` `#frontend` | https://nextjs.org/docs/app/getting-started/fetching-data |

## 4. Платные книги / long-form

| ID | Источник | Приоритет | Для чего | Теги | Ссылка |
|---|---|---:|---|---|---|
| BOOK-01 | Release It! — Michael Nygard | High | Production stability, failure modes, integration points | `#reliability` `#production` | https://pragprog.com/titles/mnee2/release-it-second-edition/ |
| BOOK-02 | Building Microservices, 2nd ed. — Sam Newman | Medium | Service boundaries, deployment, testing, monitoring microservices | `#microservices` `#architecture` | https://samnewman.io/books/building_microservices_2nd_edition/ |
| BOOK-03 | Fundamentals of Software Architecture — Mark Richards, Neal Ford | Medium | Architecture thinking, trade-offs, architecture characteristics | `#architecture` `#trade-offs` | https://www.oreilly.com/library/view/fundamentals-of-software/9781098175504/ |
| BOOK-04 | Accelerate — Forsgren, Humble, Kim | Medium | DORA, delivery performance, engineering metrics | `#dora` `#engineering-management` | https://dl.acm.org/doi/10.5555/3235404 |

## Правильная последовательность чтения

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

*Примечание: Не добавлять пиратские PDF платных книг в репозиторий проекта.*
