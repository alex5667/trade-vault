# README_FOR_TEAM

Короткая рабочая документация для ежедневной работы с Antigravity-конфигурацией проекта `trade`.

---

## 1. Что это

В репозитории `trade` используется workspace-конфигурация Antigravity через папку `.agents/`.

Она нужна, чтобы агент:
- понимал контекст проекта без длинных объяснений в каждом чате;
- отвечал в production-формате;
- автоматически подхватывал нужные skills;
- запускал повторяемые сценарии через workflows;
- экономно использовал дорогие модели.

Базовый pipeline проекта:

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
    MODEL_ROUTING.md
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
      trade-quality-gates/
        SKILL.md
      trade-contract-regression/
        SKILL.md
      trade-latency-benchmarking/
        SKILL.md
      trade-resilience-failure-drills/
        SKILL.md
    workflows/
      trade-audit.md
      trade-new-signal.md
      trade-replay.md
      trade-rollout.md
      trade-postmortem.md
      trade-quality-gate.md
      trade-contract-check.md
      trade-latency-audit.md
      trade-failure-drill.md
      trade-regression-pack.md
      trade-fast-fix.md
      trade-fast-contract-check.md
      trade-fast-test-gen.md
      trade-fast-log-triage.md
      trade-fast-doc-update.md
      trade-pro-architecture.md
      trade-pro-incident.md
      trade-pro-rollout-review.md
      trade-pro-ml-gate-review.md
      trade-pro-schema-change.md
  QUALITY_PLAYBOOK.md
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
- правила по времени, качеству данных, rollout/rollback, метрикам и тестам;
- политику использования моделей.

### `.agents/MODEL_ROUTING.md`
Короткая policy по выбору lane:
- когда использовать Flash;
- когда обязательна эскалация на premium model;
- как экономить токены без потери качества.

### `.agents/skills/*`
Набор доменных инструкций.

Skill — это не команда и не код для запуска. Это контекстный модуль, который Antigravity подмешивает автоматически, когда задача совпадает по смыслу.

### `.agents/workflows/*`
Повторяемые сценарии.

Обычно это slash-команды для типовых процессов: аудит, новый сигнал, replay, rollout, postmortem, quality gate, contract check, fast lane, premium review.

### `QUALITY_PLAYBOOK.md`
Операционный playbook.

Определяет:
- когда какой workflow запускать;
- что обязательно перед `merge`, `canary`, `prod`;
- какие артефакты должны появиться;
- какие есть stop conditions.

---

## 4. Ежедневный режим работы

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

Базовые:

```text
/trade-audit full pipeline
/trade-new-signal volatility spike with robust baseline
/trade-replay of_confirm BTCUSDT last 24h
/trade-rollout ml_confirm challenger promotion
/trade-postmortem stale ticks caused false entries
```

Quality-layer:

```text
/trade-quality-gate new orderflow gate
/trade-contract-check Redis payload for signals
/trade-latency-audit python detector hot path
/trade-failure-drill Redis unavailable for 30s
/trade-regression-pack ml_confirm rollout changes
```

Cost-aware fast lane:

```text
/trade-fast-fix small ws reconnect fix
/trade-fast-contract-check additive DTO change
/trade-fast-test-gen tests for kline parser
/trade-fast-log-triage summarize Redis timeout logs
/trade-fast-doc-update update rollout docs
```

Premium lane:

```text
/trade-pro-architecture signal pipeline redesign
/trade-pro-incident ambiguous prod RCA
/trade-pro-rollout-review risky execution gate rollout
/trade-pro-ml-gate-review new ML gating policy
/trade-pro-schema-change breaking Timescale migration
```

---

## 5. Политика использования моделей

### Default lane
Используйте дешёвую модель по умолчанию:
- **Gemini Flash**
- режим **Fast**

Подходит для:
- локальных фиксов;
- additive changes;
- contract validation без redesign;
- test generation;
- log triage;
- doc updates;
- bounded refactors.

### Premium lane
Переключайтесь на дорогую модель только при явной необходимости:
- **Gemini Pro / Claude Opus / другая premium reasoning model**
- режим **Planning**

Обязательная эскалация, если:
- затронуто больше 2 подсистем;
- есть архитектурный redesign;
- есть риск breaking change;
- неясен root cause прод-инцидента;
- меняется execution/risk/regime/ML logic;
- нужна cross-service reasoning цепочка.

### Правило процесса
Всегда предпочитайте:

`Flash triage -> narrow scope -> premium escalation only if triggers fire`

---

## 6. Матрица выбора lane

| Тип задачи | Default lane | Mode | Когда эскалировать |
|---|---|---|---|
| Маленький багфикс | Flash | Fast | затронуто >2 подсистем |
| Проверка DTO / payload | Flash | Fast | suspected breaking change |
| Генерация тестов | Flash | Fast | нужен redesign test strategy |
| Суммаризация логов | Flash | Fast | причина неочевидна, нужен RCA |
| Новый detector / signal | Flash first | Fast -> Planning | меняется architecture / risk policy |
| Schema migration | Premium | Planning | всегда, если migration нетривиальная |
| Incident RCA | Premium | Planning | всегда для ambiguous prod issues |
| Architecture review | Premium | Planning | всегда |
| Rollout policy review | Premium | Planning | при high-risk change |
| ML gating / replay redesign | Premium | Planning | всегда для non-trivial changes |

---

## 7. Какие skills у нас есть

### Core stack skills
- `trade-project-core` — общая архитектура, production framing, cross-system tasks.
- `trade-data-quality-time` — timestamps, units, monotonicity, bad-time handling.
- `trade-go-redis-ingest` — Go ingestion, WS, publish, reconnect, metrics.
- `trade-python-signal-engine` — detectors, gates, signal logic, replayable analysis.
- `trade-api-ui-contracts` — NestJS/Next.js/DTO/WS contracts.
- `trade-timescale-postgres` — hypertables, indexes, retention, compression, performance.
- `trade-observability-rollout` — metrics, alerts, dashboards, shadow/canary/enforce.
- `trade-ml-replay-gating` — ML gating, replay, baseline diff, regression.

### Quality skills
- `trade-quality-gates` — pass/fail quality gate, required artifacts, go/no-go.
- `trade-contract-regression` — compatibility, schema drift, versioning.
- `trade-latency-benchmarking` — baseline/change/re-measure, latency budget.
- `trade-resilience-failure-drills` — drills, fail-open/fail-closed, kill switches.

### Cost-aware expectation
У каждого ключевого skill должны быть разделы:
- `Default lane`
- `Scope rules`
- `Escalate to premium if`
- `Token discipline`

---

## 8. Какие workflows использовать чаще всего

### Для повседневной дешёвой работы
- `/trade-fast-fix`
- `/trade-fast-contract-check`
- `/trade-fast-test-gen`
- `/trade-fast-log-triage`
- `/trade-fast-doc-update`

### Для контроля качества
- `/trade-quality-gate`
- `/trade-contract-check`
- `/trade-latency-audit`
- `/trade-failure-drill`
- `/trade-regression-pack`

### Для тяжёлых решений
- `/trade-pro-architecture`
- `/trade-pro-incident`
- `/trade-pro-rollout-review`
- `/trade-pro-ml-gate-review`
- `/trade-pro-schema-change`

---

## 9. Минимальные правила для команды

1. Не тратьте premium model на docs, test scaffolding и локальные additive fixes.
2. Не держите Flash на ambiguous RCA, schema redesign и architecture.
3. Для hot path всегда делайте:
   `baseline -> change -> re-measure`
4. Для контрактов всегда проверяйте backward compatibility.
5. Перед `merge / canary / prod` обязательно сверяйтесь с `QUALITY_PLAYBOOK.md`.
6. Если задача может быть решена локально, ограничьте scope и не запускайте repo-wide анализ.

---

## 10. Быстрый старт для нового change-set

### Если change маленький
1. Запустите `/trade-fast-fix` или `/trade-fast-contract-check`.
2. Получите минимальный diff.
3. Пройдите `/trade-quality-gate`, если изменение не тривиально.

### Если change затрагивает несколько подсистем
1. Начните с Flash triage.
2. Если появился escalation trigger — переключитесь на premium lane.
3. Пройдите quality workflows.
4. Сверьтесь с `QUALITY_PLAYBOOK.md`.

### Если это risky rollout
1. `/trade-pro-rollout-review`
2. `/trade-failure-drill`
3. `/trade-regression-pack`
4. Сверка с `QUALITY_PLAYBOOK.md`

---

## 11. Что расширять дальше

Следующие полезные additions:
- `.agents/rules/` для жёстких обязательных правил;
- шаблоны skill/workflow для быстрого добавления новых модулей;
- GitHub Actions quality gates;
- contract test suite;
- replay artifact registry;
- dashboard catalog.

---

## 12. Главное правило

Сначала ограничьте задачу и попытайтесь закрыть её на дешёвом lane.
Эскалируйте на дорогую модель только тогда, когда без этого реально растёт риск ошибки или теряется качество.

См. также:
- `QUALITY_PLAYBOOK.md`
- `.agents/agents.md`
- `.agents/MODEL_ROUTING.md`
