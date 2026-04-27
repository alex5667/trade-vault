# QUALITY_PLAYBOOK.md

## Назначение
Этот playbook задаёт единый процесс контроля качества для проекта `trade`.
Он нужен, чтобы любые изменения в `Go -> Redis -> Python -> NestJS -> Next.js -> Postgres/Timescale` проходили одинаковую проверку перед `merge`, `canary` и `prod`.

Цели:
- не пропускать schema drift и несовместимые контракты;
- не ломать hot path по latency;
- не выкатывать изменения без наблюдаемости и rollback;
- делать качество проверяемым, а не субъективным.

---

## Основные принципы

### 1. Любое изменение должно быть проверяемым
На каждый change-set должны быть:
- явные границы изменения;
- тесты;
- метрики;
- критерии успеха/провала;
- план rollback.

### 2. Для hot path обязателен цикл
`baseline -> change -> re-measure`

### 3. Для контрактов обязателен контроль совместимости
Нельзя молча менять:
- Redis payload shape;
- DTO/WS schema;
- DB schema;
- timestamp units;
- enum values / field names / semantic meaning полей.

### 4. Для времени и качества данных действуют жёсткие правила
Обязательно указывать:
- формат времени: `epoch_ms`, `epoch_s` или ISO-8601;
- timezone;
- правила monotonicity;
- поведение при bad time:
  `detect -> sanitize -> quarantine -> metrics`

### 5. Перед rollout должны быть stop conditions
Выкатка запрещена, если нет:
- rollback-плана;
- alerting;
- pass/fail criteria;
- проверок деградации по latency / correctness / contracts.

---

## Workflow matrix

| Тип изменения | Обязательные workflow |
|---|---|
| Новый detector / signal / gate | `/trade-new-signal`, `/trade-quality-gate`, `/trade-regression-pack` |
| Изменение payload / schema / DTO / stream contract | `/trade-contract-check`, `/trade-quality-gate` |
| Изменение hot path / latency-sensitive участка | `/trade-latency-audit`, `/trade-quality-gate` |
| Изменение rollout logic / feature flags / guardrails | `/trade-rollout`, `/trade-quality-gate`, `/trade-failure-drill` |
| Изменение ML / replay / gating | `/trade-replay`, `/trade-regression-pack`, `/trade-quality-gate` |
| Инцидент / сбой / деградация | `/trade-postmortem`, при необходимости `/trade-failure-drill` |
| Любое существенное infra / reliability изменение | `/trade-quality-gate`, `/trade-failure-drill`, `/trade-rollout` |

---

## Когда какой workflow запускать

### `/trade-quality-gate <change>`
Запускать:
- перед merge любого заметного change-set;
- при изменении логики принятия решений;
- при изменении ENV/flags/thresholds;
- при изменении обработки данных на границах сервисов.

Должен проверить:
- полноту change-set;
- тестовое покрытие на уровне риска изменения;
- observability;
- rollout/rollback;
- наличие явных pass/fail criteria.

Ожидаемые артефакты:
- quality summary;
- список блокирующих рисков;
- список обязательных тестов;
- список обязательных метрик/алертов;
- решение: `PASS / PASS_WITH_CONDITIONS / FAIL`.

---

### `/trade-contract-check <scope>`
Запускать:
- при любых изменениях в stream payload;
- при изменении DTO / JSON / Redis message format;
- при изменении DB schema;
- при изменении units / enum / semantic meaning полей.

Должен проверить:
- backward compatibility;
- versioning strategy;
- nullable/optional fields;
- безопасное поведение старых consumers;
- schema drift risk.

Ожидаемые артефакты:
- contract diff;
- migration notes;
- compatibility matrix producer/consumer;
- список breaking changes;
- решение: `COMPATIBLE / COMPATIBLE_WITH_MIGRATION / BREAKING`.

---

### `/trade-latency-audit <scope>`
Запускать:
- при изменении hot path;
- при добавлении тяжёлых вычислений;
- при изменении сериализации/десериализации;
- при добавлении DB/Redis/network I/O в критический путь.

Должен проверить:
- baseline latency;
- предполагаемую стоимость изменения;
- worst-case path;
- p50/p95/p99;
- CPU / RAM impact;
- необходимость выноса I/O из hot path.

Ожидаемые артефакты:
- benchmark plan;
- baseline metrics;
- post-change metrics;
- вывод по деградации;
- рекомендации по оптимизации.

---

### `/trade-failure-drill <scenario>`
Запускать:
- перед canary для критичных изменений;
- перед prod для infra / reliability / streaming / execution changes;
- после инцидента для валидации corrective actions.

Типовые сценарии:
- Redis unavailable;
- Redis lag / backlog;
- duplicate events;
- out-of-order timestamps;
- stale ticks;
- partially malformed payload;
- DB slow / DB unavailable;
- metrics exporter unavailable;
- feature flag misconfiguration.

Ожидаемые артефакты:
- drill scenario;
- expected vs actual behavior;
- fail-open / fail-closed decision;
- kill switch instructions;
- rollback readiness notes.

---

### `/trade-regression-pack <change>`
Запускать:
- перед canary для signal/ML/gate changes;
- перед prod для logic changes;
- после существенного refactor.

Должен проверить:
- replay/regression coverage;
- deterministic outputs;
- baseline diff;
- распределение reason codes / decisions;
- влияние на outcomes, если доступны.

Ожидаемые артефакты:
- replay set;
- baseline snapshot;
- diff report;
- список новых/пропавших срабатываний;
- вывод: acceptable / suspicious / blocking.

---

### `/trade-rollout <change>`
Запускать:
- перед canary;
- перед prod;
- при изменениях feature flags, enforce logic, execution gates, ML promotion.

Должен определить:
- этапы `shadow -> canary -> enforce`;
- критерии продвижения;
- freeze conditions;
- rollback triggers;
- owners и порядок реакции.

Ожидаемые артефакты:
- rollout plan;
- monitoring plan;
- rollback plan;
- kill switches / feature flags;
- go/no-go checklist.

---

### `/trade-postmortem <incident>`
Запускать:
- после прод-инцидента;
- после unexpected regression;
- после серьёзного canary rollback.

Ожидаемые артефакты:
- timeline;
- root cause;
- contributing factors;
- detection gaps;
- corrective actions;
- prevention tasks.

---

## Stage-based process

## 1. Перед merge

### Обязательно
- пройти `/trade-quality-gate`;
- если есть контрактные изменения — пройти `/trade-contract-check`;
- если затронут hot path — пройти `/trade-latency-audit`;
- если затронут signal/gate/ML — пройти `/trade-regression-pack`.

### Минимальный набор артефактов перед merge
- diff / список файлов;
- unit tests;
- integration tests или contract tests;
- список ENV изменений;
- список новых/изменённых метрик;
- rollback note;
- краткое risk summary.

### Merge stop conditions
Не мержить, если:
- нет тестов на основной риск изменения;
- есть unbounded ambiguity по time units / schema / semantics;
- нет observability для новой логики;
- не описан rollback;
- есть unresolved breaking changes;
- есть latency regression без объяснения и budget.

---

## 2. Перед canary

### Обязательно
- всё из merge-stage должно быть уже готово;
- пройти `/trade-rollout`;
- пройти `/trade-failure-drill` для критичных сценариев;
- пройти `/trade-regression-pack` на representative dataset;
- если была оптимизация hot path — иметь baseline и re-measure.

### Минимальный набор артефактов перед canary
- rollout plan;
- canary scope;
- canary metrics & dashboards;
- alert thresholds;
- rollback triggers;
- kill-switch instructions;
- replay diff / regression report;
- drill results.

### Canary stop conditions
Не начинать canary, если:
- нет метрик, по которым можно быстро понять состояние;
- нет alarm thresholds;
- нет kill switch / feature flag;
- нет rollback steps;
- нет ясных критериев exit из canary;
- drill показал unsafe behavior без исправления.

---

## 3. Перед prod / full enforce

### Обязательно
- canary завершён успешно;
- отсутствуют unresolved critical findings;
- есть решение по fail-open / fail-closed;
- есть финальный `/trade-rollout`;
- проверены post-canary metrics;
- regression diff признан приемлемым.

### Минимальный набор артефактов перед prod
- canary summary;
- final go/no-go decision;
- final metric thresholds;
- rollback owner и шаги rollback;
- список feature flags;
- список известных residual risks;
- post-deploy monitoring window.

### Prod stop conditions
Не выкатывать в full prod, если:
- canary дал unexplained signal drift;
- появились schema/contract anomalies;
- выросла p95/p99 latency сверх бюджета;
- нет уверенности в data quality handling;
- rollback не проверен;
- алерты или дашборды ещё не готовы.

---

## Обязательные артефакты по категориям

### Код и контракты
- unified diff или список изменённых файлов;
- DTO/schema contract;
- migration/compatibility notes;
- stream key / topic / channel notes;
- versioning note при breaking-adjacent change.

### Тесты
- unit tests;
- integration tests;
- contract tests;
- replay/regression tests для signal logic;
- latency/load checks для hot path.

### Метрики и алерты
Минимально для новой логики:
- success/error counters;
- reason codes / reject reasons;
- latency histogram;
- stale/bad data counters;
- quarantine counters;
- version/feature-flag visibility.

Минимально для alerting:
- elevated error rate;
- latency breach;
- stale data;
- no-data / low-data;
- contract parse failures;
- abnormal drop/spike in decisions.

### Операционное управление
- rollout steps;
- rollback steps;
- feature flag list;
- ownership;
- incident response note.

---

## Рекомендованный порядок запуска workflow по типовым change-set

### Новый detector / signal
1. `/trade-new-signal <idea>`
2. `/trade-quality-gate <change>`
3. `/trade-regression-pack <change>`
4. `/trade-rollout <change>`
5. `/trade-failure-drill <scenario>`

### Изменение payload / DTO / schema
1. `/trade-contract-check <scope>`
2. `/trade-quality-gate <change>`
3. `/trade-regression-pack <change>` если затрагивает поведение
4. `/trade-rollout <change>` если нужен staged deployment

### Оптимизация hot path
1. `/trade-latency-audit <scope>`
2. `/trade-quality-gate <change>`
3. `/trade-failure-drill <scenario>` если затронута устойчивость
4. `/trade-rollout <change>`

### Инцидент
1. `/trade-postmortem <incident>`
2. `/trade-failure-drill <recreated scenario>`
3. `/trade-quality-gate <fix>`
4. `/trade-rollout <fix>`

---

## Definition of Done для quality
Изменение считается готовым к следующей стадии только если:
- риск понятен и задокументирован;
- контракты проверены;
- тесты есть и проходят;
- метрики и алерты определены;
- latency измерена, если это важно;
- replay/regression проведён, если меняется логика сигналов;
- rollout/rollback описаны;
- есть явное решение `go / no-go`.

---

## Короткий checklist

### Перед merge
- [ ] Изменение описано
- [ ] Риски перечислены
- [ ] Тесты добавлены
- [ ] Метрики добавлены
- [ ] Контракты проверены
- [ ] Rollback описан

### Перед canary
- [ ] Rollout plan готов
- [ ] Drill пройден
- [ ] Alerts готовы
- [ ] Kill switch готов
- [ ] Replay/regression пройден
- [ ] Exit criteria заданы

### Перед prod
- [ ] Canary успешен
- [ ] Regression приемлем
- [ ] Latency в бюджете
- [ ] Нет критичных contract/data-quality проблем
- [ ] Rollback owner назначен
- [ ] Go/No-Go принято

---

## Примеры команд
```text
/trade-quality-gate new volatility gate for meme symbols
/trade-contract-check Redis payload for signals:v2
/trade-latency-audit of_confirm hot path
/trade-failure-drill Redis lag 30s with stale ticks
/trade-regression-pack ml_confirm challenger rollout
/trade-rollout promote canary to enforce
/trade-postmortem false entries during stale market data
```

---

## Итог
Этот playbook обязателен для всех нетривиальных изменений в `trade`.
Его задача — сделать качество воспроизводимым процессом:
- одинаковые проверки,
- одинаковые артефакты,
- одинаковые stop conditions,
- одинаковая дисциплина rollout.
