# Почему нет сделок с ok=1 в dataset

## Проблема

В dataset для калибровки все сделки имеют `ok=0`, что делает невозможной калибровку (требуется `pass_rate >= 15%`).

## Анализ причин

### 1. Логика установки `ok` в OFConfirmEngine

В `python-worker/core/of_confirm_engine.py`:

```python
# Строка 650: ok устанавливается из решения gate
ok = 1 if bool(dec.ok) else 0

# Строка 656-661: Score threshold veto
score_min = _f(cfg.get("of_score_min", 0.65), 0.65)
if ok == 1 and score < score_min:
    ok = 0  # Veto по score
```

**Проблема:** Даже если gate решение `dec.ok = True`, сделка блокируется если `score < score_min` (по умолчанию 0.65).

### 2. Возможные причины `ok=0` для всех сделок

#### A. Score слишком низкий
- Все сделки имеют `score < 0.65` (дефолтный threshold)
- Или в конфигурации установлен более высокий `of_score_min` (например, 0.70)

#### B. Gate решение `dec.ok = False`
- Недостаточно legs (have < need)
- Сценарий не прошел проверку (например, vol_shock, saw_chop)
- Book health veto
- Data health veto

#### C. Дополнительные veto после gate решения
- Cancellation spike gate (строка 729)
- ML ENFORCE veto (строка 1208)
- Meta ENFORCE veto (строка 1334)
- Vol shock fail closed (строка 669)
- Saw chop fail closed (строка 679)

### 3. Проверка в replay mode

В replay mode используется конфигурация из inputs (`cfg` из inputs). Если:
- В inputs нет правильной конфигурации → используется дефолт
- В конфигурации установлен слишком высокий `of_score_min` → все сделки блокируются
- В inputs нет нужных индикаторов → legs не проходят → `have < need` → `ok=0`

## Диагностика

### Шаг 1: Проверить replay output

```bash
docker exec scanner-of-nightly-calibrate-timer sh -c '
LATEST=$(find /var/lib/trade/of_reports/out -name "nightly_*" -type d ! -name "*meta*" | sort | tail -1)
python3 << EOF
import json
with open("$LATEST/of_replay_engine.ndjson") as f:
    rows = [json.loads(l) for l in f if l.strip()]

ok1 = sum(1 for r in rows if r.get("ok") == 1)
scores = [r.get("score", 0) for r in rows]
print(f"ok=1: {ok1}/{len(rows)}")
print(f"Score range: {min(scores):.3f} - {max(scores):.3f}")
print(f"Scores < 0.65: {sum(1 for s in scores if s < 0.65)}/{len(scores)}")
EOF
'
```

### Шаг 2: Проверить причины блокировки

```bash
# Топ причин для ok=0
docker exec scanner-of-nightly-calibrate-timer sh -c '
LATEST=$(find /var/lib/trade/of_reports/out -name "nightly_*" -type d ! -name "*meta*" | sort | tail -1)
python3 << EOF
import json
from collections import Counter

with open("$LATEST/of_replay_engine.ndjson") as f:
    rows = [json.loads(l) for l in f if l.strip()]

ok0_rows = [r for r in rows if r.get("ok") == 0]
reasons = [r.get("reason", "unknown")[:60] for r in ok0_rows[:200]]
for reason, count in Counter(reasons).most_common(10):
    print(f"{count:4d}x: {reason}")
EOF
'
```

### Шаг 3: Проверить have/need

```bash
docker exec scanner-of-nightly-calibrate-timer sh -c '
LATEST=$(find /var/lib/trade/of_reports/out -name "nightly_*" -type d ! -name "*meta*" | sort | tail -1)
python3 << EOF
import json

with open("$LATEST/of_replay_engine.ndjson") as f:
    rows = [json.loads(l) for l in f if l.strip()]

have_need = [(r.get("have", 0), r.get("need", 0)) for r in rows]
have_need_ok = sum(1 for h, n in have_need if h >= n and n > 0)
print(f"Have >= Need: {have_need_ok}/{len(have_need)} ({have_need_ok/len(have_need)*100:.1f}%)")
EOF
'
```

## Решения

### Решение 1: Снизить `of_score_min` в конфигурации

Если все сделки блокируются из-за score threshold:

```python
# В конфигурации для canary symbols
cfg["of_score_min"] = 0.60  # вместо 0.65
```

### Решение 2: Проверить конфигурацию в inputs

Убедиться, что в inputs есть правильная конфигурация:

```bash
docker exec scanner-of-nightly-calibrate-timer sh -c '
LATEST=$(find /var/lib/trade/of_reports/out -name "nightly_*" -type d ! -name "*meta*" | sort | tail -1)
head -1 "$LATEST/of_inputs_canary.ndjson" | python3 -m json.tool | grep -A 5 "cfg"
'
```

### Решение 3: Проверить индикаторы в inputs

Убедиться, что в inputs есть все необходимые индикаторы для legs:

```bash
# Проверить наличие индикаторов
docker exec scanner-of-nightly-calibrate-timer sh -c '
LATEST=$(find /var/lib/trade/of_reports/out -name "nightly_*" -type d ! -name "*meta*" | sort | tail -1)
python3 << EOF
import json

with open("$LATEST/of_inputs_canary.ndjson") as f:
    sample = json.loads(f.readline())

# Проверить наличие ключевых индикаторов
indicators = ["obi_stable", "iceberg_strict", "ofi_stable", "sweep_recent", "reclaim_recent"]
for ind in indicators:
    val = sample.get(ind, "MISSING")
    print(f"{ind}: {val}")
EOF
'
```

### Решение 4: Временно отключить score veto для калибровки

Для калибровки можно временно использовать более мягкий threshold:

```python
# В nightly_gate_calibrate_bundle.py, перед вызовом calibrate_gate_params
# Можно добавить фильтр по score_min=0.60 вместо 0.65
```

## Следующие шаги

1. ✅ Запустить диагностику выше для понимания конкретной причины
2. 🔄 Исправить конфигурацию или индикаторы в inputs
3. 🔄 Перезапустить калибровку
4. 🔄 Проверить, что появились сделки с `ok=1` в dataset

