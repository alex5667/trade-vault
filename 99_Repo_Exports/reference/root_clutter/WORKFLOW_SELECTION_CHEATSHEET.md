# WORKFLOW_SELECTION_CHEATSHEET.md

## Когда какой workflow запускать

### 1. Нужно быстро и дёшево
Используйте **fast-lane**:

- `/trade-fast-fix` — локальный фикс без redesign
- `/trade-fast-contract-check` — быстрая проверка контракта
- `/trade-fast-test-gen` — быстро нагенерировать тестовый scaffold
- `/trade-fast-log-triage` — быстро разобрать симптомы по логам
- `/trade-fast-doc-update` — обновить README / playbook / docs

**Когда подходит:**
- 1–2 файла
- локальная проблема
- additive change
- нет архитектурного пересмотра
- нет prod-risk redesign

---

### 2. Проблема непонятна
Используйте:

- `/trade-parallel-investigation`

**Когда подходит:**
- неясный root cause
- подозрение на несколько подсистем
- нужно параллельно проверить ingestion / signals / contracts / DB / latency

**Типичный результат:**
- гипотезы
- подтверждённые факты
- кто виноват / где узкое место
- что проверять дальше

---

### 3. Изменение понятно, но нужен нормальный review по цепочке
Используйте:

- `/trade-sequential-review`

**Когда подходит:**
- новый detector / signal / gate
- изменение payload / DTO / schema
- refactor через несколько слоёв
- нужен ordered review: logic → contracts → storage → latency → rollout

**Типичный результат:**
- последовательный review
- список файлов / тестов / метрик
- риски и условия rollout

---

### 4. Нужен go / no-go перед merge, canary или prod
Используйте:

- `/trade-release-gate`

**Когда подходит:**
- перед merge
- перед canary
- перед prod
- когда нужен формальный PASS / FAIL

**Типичный результат:**
- PASS
- PASS WITH CONDITIONS
- FAIL
- обязательные stop conditions
- rollback / mitigation steps

---

### 5. Нужен полный аудит подсистемы
Используйте:

- `/trade-audit`

**Когда подходит:**
- хотите понять общее состояние компонента
- нужен production-readiness review
- нужно собрать риски по всей подсистеме

---

### 6. Добавляете новый сигнал
Используйте:

- `/trade-new-signal`

**Когда подходит:**
- новый detector
- новый gate
- новый signal pipeline
- новый execution / confirmation path

---

### 7. Нужен replay / regression
Используйте:

- `/trade-replay`
- `/trade-regression-pack`

**Когда подходит:**
- сравнение old vs new
- replay исторических данных
- regression перед rollout
- проверка ML / confirm / detector logic

---

### 8. Нужен rollout
Используйте:

- `/trade-rollout`
- `/trade-pro-rollout-review` — если rollout рискованный

**Когда подходит:**
- shadow → canary → enforce
- нужна стратегия rollout / rollback
- change затрагивает prod behavior

---

### 9. Нужен postmortem
Используйте:

- `/trade-postmortem`
- `/trade-pro-incident` — если инцидент сложный

**Когда подходит:**
- прод-инцидент
- ложные входы / выпадение сигналов / stale data / Redis outage
- нужно RCA и corrective actions

---

### 10. Нужна архитектура или тяжёлое решение
Используйте **premium lane**:

- `/trade-pro-architecture`
- `/trade-pro-ml-gate-review`
- `/trade-pro-schema-change`
- `/trade-pro-incident`
- `/trade-pro-rollout-review`

**Когда обязательно эскалировать:**
- затронуто >2 подсистем
- неясный root cause
- non-backward-compatible change
- ML / regime / execution-risk redesign
- schema / migration / retention redesign
- высокий риск для prod

---

## Самая короткая схема выбора

### Fast lane
Если задача:
- локальная
- ограниченная
- без redesign
- без тяжёлого reasoning

→ используйте `trade-fast-*`

### Investigation lane
Если проблема неясна

→ используйте `/trade-parallel-investigation`

### Review lane
Если change уже понятен, но нужен нормальный review по цепочке

→ используйте `/trade-sequential-review`

### Gate lane
Если нужно принять решение о выпуске

→ используйте `/trade-release-gate`

### Premium lane
Если задача:
- архитектурная
- многослойная
- high-risk
- требует глубокого reasoning

→ используйте `trade-pro-*`

---

## Мини-шпаргалка

| Ситуация | Что запускать |
|---|---|
| Мелкий фикс | `/trade-fast-fix` |
| Быстро проверить контракт | `/trade-fast-contract-check` |
| Быстро понять логи | `/trade-fast-log-triage` |
| Новый сигнал | `/trade-new-signal` |
| Непонятная проблема | `/trade-parallel-investigation` |
| Пошаговый review change | `/trade-sequential-review` |
| Проверка перед merge/canary/prod | `/trade-release-gate` |
| Replay / regression | `/trade-replay` / `/trade-regression-pack` |
| Rollout | `/trade-rollout` |
| Инцидент / RCA | `/trade-postmortem` / `/trade-pro-incident` |
| Архитектурное решение | `/trade-pro-architecture` |

---

## Правило по умолчанию

1. Сначала **fast-lane**, если задача ограниченная.
2. Если root cause неясен — **parallel investigation**.
3. Если change понятен, но нужен системный review — **sequential review**.
4. Перед выпуском — всегда **release-gate**.
5. На premium-модель переходить только при явных escalation triggers.
