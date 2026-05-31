# README_FOR_TEAM

Короткая документация для ежедневной работы с Antigravity-конфигурацией проекта `trade`.

---

## 1. Что это

В репозитории `trade` используется workspace-конфигурация Antigravity через папку `.agents/`.

Она нужна, чтобы агент:
- понимал контекст проекта без длинных объяснений в каждом чате;
- отвечал в production-формате;
- автоматически подхватывал нужные skills;
- запускал повторяемые сценарии через workflows.

Базовая идея:

`Go -> Redis -> Python -> NestJS -> Next.js -> Postgres/Timescale`

Цели проекта:
- reliability
- deterministic time
- data quality control
- low latency
- observability
- controlled risk

---

## 2. Где что лежит

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

---

## 3. За что отвечает каждый блок

### `.agents/agents.md`
Главный файл правил workspace.

Задаёт:
- роли команды;
- общий стиль работы;
- обязательный формат ответа;
- инженерные стандарты;
- правила по времени, качеству данных, rollout/rollback, метрикам и тестам.

### `.agents/skills/*`
Набор доменных инструкций.

Skill — это не команда и не код для запуска. Это контекстный модуль, который Antigravity подмешивает автоматически, когда задача совпадает по смыслу.

### `.agents/workflows/*`
Повторяемые сценарии.

Обычно это slash-команды для типовых процессов: аудит, новый сигнал, replay, rollout, postmortem.

---

## 4. Как этим пользоваться каждый день

### Вариант 1. Обычный запрос

Пишите задачу в чат, начиная с `TRADE:`.

Примеры:

```text
TRADE: проверь контракты Python -> Redis -> NestJS -> WS -> Next.js
```

```text
TRADE: добавь новый detector для volatility spike с pytest, Prometheus metrics и rollout plan
```

```text
TRADE: спроектируй Timescale schema для signal history и outcome metrics
```

### Вариант 2. Workflow-команды

Используйте готовые workflows, когда задача типовая.

Примеры:

```text
/trade-audit full pipeline
```

```text
/trade-new-signal volatility spike with robust baseline
```

```text
/trade-replay of_confirm BTCUSDT last 24h
```

```text
/trade-rollout ml_confirm challenger promotion
```

```text
/trade-postmortem stale ticks caused false entries
```

---

## 5. Какие skills у нас есть

### `trade-project-core`
Используется для общих задач по архитектуре и production-подаче.

Когда нужен:
- общий аудит подсистемы;
- архитектурные решения;
- сквозные изменения по пайплайну;
- задачи, где нужен формат: цель → факты → риски → план → реализация.

### `trade-data-quality-time`
Используется для времени, timestamp, ordering и качества данных.

Когда нужен:
- epoch ms / sec;
- timezone;
- monotonicity;
- bad time detection;
- sanitize/quarantine;
- robust statistics, median/MAD.

### `trade-go-redis-ingest`
Используется для Go ingestion и доставки в Redis.

Когда нужен:
- Binance WS;
- reconnect;
- ping/pong;
- low-latency publish;
- stream/pubsub contracts;
- graceful shutdown;
- ingest metrics.

### `trade-python-signal-engine`
Используется для Python-логики анализа и генерации сигналов.

Когда нужен:
- detectors;
- signal gating;
- robust baselines;
- feature extraction;
- fail-open/fail-closed design;
- pytest/integration/replay testing.

### `trade-api-ui-contracts`
Используется для NestJS / Next.js / DTO / WebSocket контрактов.

Когда нужен:
- payload versioning;
- DTO validation;
- backward compatibility;
- WebSocket delivery;
- API boundary checks;
- e2e flow.

### `trade-timescale-postgres`
Используется для схем, SQL и производительности БД.

Когда нужен:
- hypertables;
- индексы;
- retention/compression;
- metrics tables;
- explain/analyze;
- write/read path trade-offs.

### `trade-observability-rollout`
Используется для observability и безопасного rollout.

Когда нужен:
- Prometheus metrics;
- alert rules;
- dashboards;
- shadow/canary/enforce;
- rollback criteria;
- SLO/SLI.

### `trade-ml-replay-gating`
Используется для ML gating и deterministic replay.

Когда нужен:
- golden replay;
- baseline diff;
- regression checks;
- ML confirm gate;
- drift/calibration;
- rollout champion/challenger.

---

## 6. Как Antigravity выбирает нужный skill

Skills не запускаются вручную.

Antigravity подбирает их автоматически по смыслу запроса.

Чтобы нужный skill подтянулся с высокой вероятностью:
- начинайте запрос с `TRADE:`;
- явно указывайте подсистему;
- пишите ожидаемый результат.

Хорошо:

```text
TRADE: добавь Redis contract versioning между Python signals и NestJS aggregator, нужны DTO, тесты, метрики и rollback
```

Плохо:

```text
улучши проект
```

---

## 7. Какой формат ответа считается нормой

Для нетривиальных задач ожидается структура:

1. Цель  
2. Факты  
3. Предположения  
4. Риски  
5. План  
6. Реализация  
7. Тесты  
8. Метрики/алерты  
9. Rollout/Rollback  
10. Prod-checklist

Для change-request желательно получать:
- какие файлы менять;
- какие функции/классы менять;
- ENV;
- SQL / migrations;
- тесты;
- метрики / логи / алерты.

---

## 8. Базовые правила проекта

### Время и единицы
Всегда фиксировать:
- epoch_ms или epoch_s;
- timezone;
- требования к ordering и monotonicity.

Если время плохое:

`detect -> sanitize -> quarantine -> metrics`

### Data quality
- не делать тихих преобразований двусмысленных timestamps;
- проверять контракты на границах;
- выбросы обрабатывать устойчиво;
- ошибки данных выносить в метрики и логи.

### Low latency
- hot path должен быть коротким и предсказуемым;
- медленный I/O выносить из критического пути;
- оптимизации делать по схеме `measure -> change -> re-measure`.

### Production safety
- не включать risky behavior без feature flag;
- использовать shadow / canary / rollback;
- явно писать fail-open или fail-closed.

---

## 9. Когда использовать какой workflow

### `/trade-audit`
Когда нужен аудит существующей подсистемы.

Примеры:
- ingest pipeline;
- signal engine;
- DB schema;
- contracts;
- observability readiness.

### `/trade-new-signal`
Когда добавляется новый detector / signal / gate / filter.

Примеры:
- volatility spike;
- spoofing filter;
- volume shock;
- news-aware gate.

### `/trade-replay`
Когда нужен replay/regression/deterministic validation.

Примеры:
- baseline diff;
- проверка после refactor;
- сравнение champion vs challenger.

### `/trade-rollout`
Когда изменение готовится к production.

Примеры:
- новый detector;
- новый ML gate;
- новые threshold/policy;
- новый storage/contract.

### `/trade-postmortem`
Когда уже был инцидент и нужно RCA.

Примеры:
- stale ticks;
- broken timestamps;
- false signals;
- stream lag;
- schema regression.

---

## 10. Как добавлять новые skills

Добавляйте новый skill, если появляется отдельный устойчивый домен задач.

Хорошие кандидаты:
- отдельный news pipeline;
- orderbook microstructure;
- execution/risk engine;
- feature store;
- Telegram ops workflow.

Шаги:
1. Создать новую папку в `.agents/skills/<skill-name>/`
2. Добавить `SKILL.md`
3. В YAML frontmatter написать точный `description`
4. В markdown описать правила, deliverables и ограничения
5. Проверить на реальной задаче, что skill подхватывается

Не делайте skill слишком общим. Один skill = один устойчивый домен.

---

## 11. Как добавлять новые workflows

Добавляйте workflow, если сценарий повторяется и его удобно вызывать slash-командой.

Хорошие кандидаты:
- `/trade-contract-check`
- `/trade-db-review`
- `/trade-alert-pack`
- `/trade-ml-guard`

Правило:
- workflow должен запускать понятный сценарий;
- не дублировать skill;
- не быть слишком широким.

Skill = знания.  
Workflow = маршрут работы.

---

## 12. Типовые хорошие запросы

```text
TRADE: проверь Redis stream contracts для signals:of:inputs и trades:closed, нужны риски, тесты и observability
```

```text
TRADE: предложи rollout для нового OF gate с shadow, canary, alert thresholds и rollback conditions
```

```text
TRADE: оптимизируй hot path Python detector, сначала baseline latency, потом изменения, потом re-measure
```

```text
TRADE: спроектируй Timescale retention/compression policy для signal history и replay datasets
```

---

## 13. Типовые плохие запросы

Плохо:
- «проверь всё»
- «улучши систему»
- «сделай красиво»
- «оптимизируй код»

Почему плохо:
- неясна подсистема;
- неясен expected output;
- выше риск, что подтянется не тот контекст.

Лучше уточнять:
- компонент;
- ожидаемый результат;
- нужен ли diff / SQL / tests / alerts / rollout.

---

## 14. Признаки, что конфигурация работает правильно

Вы видите, что агент:
- отвечает в trade-формате;
- учитывает время, latency, contracts, observability;
- пишет про тесты и rollback без отдельного напоминания;
- предлагает production-safe изменения, а не абстрактные советы.

---

## 15. Если что-то не подхватывается

Проверьте:
1. Workspace открыт на корень `trade`
2. Папка называется именно `.agents`
3. Внутри есть `agents.md`
4. Skills лежат в `.agents/skills/.../SKILL.md`
5. Workflows лежат в `.agents/workflows/`
6. Запрос конкретный и начинается с `TRADE:`
7. После изменений workspace переоткрыт

---

## 16. Рекомендуемый порядок работы команды

Для новых задач:
1. Сформулировать задачу через `TRADE:`
2. Если задача типовая — использовать workflow
3. Получить план и diff
4. Прогнать тесты / replay / latency checks
5. Подготовить rollout
6. Только потом применять в production

---

## 17. Что желательно добавить дальше

Практичные следующие шаги:
- отдельный skill под `news pipeline`;
- отдельный skill под `execution/risk engine`;
- workflow `/trade-contract-check`;
- workflow `/trade-alert-pack`;
- workflow `/trade-db-review`;
- ссылки на реальные stream keys, ENV и metric names проекта.

## 18. Model Routing Policy

| Task type | Default model lane | Mode | Escalate when |
|---|---|---|---|
| Small fix | Flash | Fast | touches >2 subsystems |
| Contract check | Flash | Fast | breaking change suspected |
| New signal | Flash first | Fast -> Planning | redesign or unclear metrics |
| Incident RCA | Pro/Opus | Planning | always for ambiguous prod issues |
| Architecture | Pro/Opus | Planning | always |

---

## 19. Короткая памятка

Использовать так:
- для обычной задачи: `TRADE: ...`
- для типовой процедуры: `/trade-...`

Желательно явно указывать:
- компонент;
- expected output;
- нужен ли diff / tests / SQL / metrics / alerts / rollout.

Минимально хороший шаблон запроса:

```text
TRADE: [что меняем] в [каком компоненте]. Нужны [diff/tests/sql/metrics/alerts/rollout].
```

