# OBI Sustained Improvements - Документация

## Обзор изменений

В `BaseOrderFlowHandler` улучшена логика определения **sustained OBI** для более точного соответствия теории order flow.

## 🎯 Проблема

**Старая логика:**
```python
sustained = abs(avg) >= threshold
```

Проблема: OBI считается sustained если **среднее** по окну превышает порог, но это не учитывает:
- Сколько наблюдений в окне
- Насколько устойчив сигнал (доля сильных наблюдений)
- Может быть 1 сильное наблюдение и 9 слабых → avg высокий, но не sustained

## ✅ Решение

**Новая логика (опциональная):**

```python
# 1. Базовая проверка по среднему (как раньше)
sustained = abs(avg) >= threshold

# 2. Дополнительная проверка persistence (устойчивости)
if use_fraction and len(window) >= min_samples:
    # Считаем долю "сильных" наблюдений в правильном направлении
    strong_count = count(v where v * sign(avg) >= threshold)
    fraction = strong_count / len(window)
    
    # sustained только если достаточная доля окна сильная
    sustained = sustained AND (fraction >= min_fraction)
```

## 📊 Параметры

### 1. `OBI_SUSTAINED_USE_FRACTION` (default: `"true"`)

Включает/выключает новую логику проверки persistence.

```bash
# Включить (по умолчанию)
export OBI_SUSTAINED_USE_FRACTION=true

# Отключить (вернуться к старой логике)
export OBI_SUSTAINED_USE_FRACTION=false
```

**Когда отключить:**
- Для тестирования старого поведения
- Если новая логика дает слишком мало sustained сигналов

### 2. `OBI_SUSTAINED_MIN_SAMPLES` (default: `3`)

Минимальное количество наблюдений в окне для проверки persistence.

```bash
# Строже (нужно больше данных)
export OBI_SUSTAINED_MIN_SAMPLES=5

# Мягче (меньше данных достаточно)
export OBI_SUSTAINED_MIN_SAMPLES=2
```

**Рекомендации:**
- **Высокочастотные инструменты** (crypto): 3-5
- **Низкочастотные** (commodities): 2-3
- Зависит от `obi_min_duration` (сколько секунд окно)

### 3. `OBI_SUSTAINED_MIN_FRACTION` (default: `0.6`)

Минимальная доля "сильных" наблюдений в окне (60%).

```bash
# Строже (нужно больше устойчивости)
export OBI_SUSTAINED_MIN_FRACTION=0.75  # 75%

# Мягче (меньше устойчивости достаточно)
export OBI_SUSTAINED_MIN_FRACTION=0.5   # 50%
```

**Рекомендации:**
- **Консервативно**: 0.7-0.8 (высокое качество сигналов)
- **Стандартно**: 0.6 (баланс)
- **Агрессивно**: 0.5 (больше сигналов)

## 🔍 Примеры

### Пример 1: Устойчивый OBI (sustained = True)

```python
# Параметры
obi_threshold = 0.5
min_samples = 3
min_fraction = 0.6

# Окно OBI (10 наблюдений)
obi_state = [
    (ts1, 0.6),   # strong bid
    (ts2, 0.7),   # strong bid
    (ts3, 0.5),   # strong bid
    (ts4, 0.6),   # strong bid
    (ts5, 0.4),   # weak bid
    (ts6, 0.6),   # strong bid
    (ts7, 0.7),   # strong bid
    (ts8, 0.3),   # weak bid
    (ts9, 0.6),   # strong bid
    (ts10, 0.5),  # strong bid
]

# Расчет
avg = 0.57  # среднее
abs(avg) >= 0.5  # ✅ True (базовая проверка)

# Persistence check
sign = +1.0 (avg > 0)
strong_count = 8  # наблюдений >= 0.5 с положительным знаком
fraction = 8 / 10 = 0.8
fraction >= 0.6  # ✅ True

# Итог: sustained = True ✅
# Интерпретация: Устойчивый bid pressure, 80% окна сильные
```

### Пример 2: Неустойчивый OBI (sustained = False)

```python
# Параметры те же

# Окно OBI (10 наблюдений)
obi_state = [
    (ts1, 0.9),   # ОЧЕНЬ strong bid
    (ts2, 0.2),   # weak bid
    (ts3, 0.1),   # weak bid
    (ts4, 0.3),   # weak bid
    (ts5, 0.2),   # weak bid
    (ts6, 0.1),   # weak bid
    (ts7, 0.2),   # weak bid
    (ts8, 0.3),   # weak bid
    (ts9, 0.2),   # weak bid
    (ts10, 0.1),  # weak bid
]

# Расчет
avg = 0.26  # среднее (низкое из-за многих слабых)
abs(avg) >= 0.5  # ❌ False

# Но даже если бы avg был >= 0.5:
# strong_count = 1  # только одно наблюдение >= 0.5
# fraction = 1 / 10 = 0.1
# fraction >= 0.6  # ❌ False

# Итог: sustained = False ❌
# Интерпретация: Один всплеск, но не устойчивый pressure
```

### Пример 3: Высокое среднее, но низкая persistence

```python
# Окно OBI (5 наблюдений)
obi_state = [
    (ts1, 0.9),   # strong bid
    (ts2, 0.8),   # strong bid
    (ts3, 0.3),   # weak bid
    (ts4, 0.2),   # weak bid
    (ts5, 0.1),   # weak bid
]

# Расчет
avg = 0.46  # среднее (чуть ниже порога)
abs(avg) >= 0.5  # ❌ False (не проходит базовую проверку)

# Но если порог был 0.4:
abs(avg) >= 0.4  # ✅ True
strong_count = 2  # наблюдений >= 0.5
fraction = 2 / 5 = 0.4
fraction >= 0.6  # ❌ False

# Итог: sustained = False ❌
# Интерпретация: Среднее высокое, но только 40% окна сильные
```

## 🎯 Влияние на сигналы

### Absorption сигналы

```python
# Условие absorption
if (
    ctx.weak_progress
    and is_near_level_atr(...)
    and (not obi_confirms)  # ← OBI НЕ sustained
):
    # Генерируем absorption
```

**Эффект:**
- **Строже sustained** → больше `not obi_confirms` → **больше absorption сигналов**
- **Мягче sustained** → меньше `not obi_confirms` → **меньше absorption сигналов**

### Breakout сигналы

```python
# Условие breakout
if (
    breakout_level
    and (obi_confirms or not ctx.obi_sustained)  # ← OBI sustained подтверждает
):
    # Генерируем breakout
```

**Эффект:**
- **Строже sustained** → меньше `obi_confirms` → **меньше breakout сигналов** (нужен более устойчивый OBI)
- **Мягче sustained** → больше `obi_confirms` → **больше breakout сигналов**

## 🔧 Настройка под инструмент

### Высокочастотные (Crypto)

```bash
# Строже: нужна высокая устойчивость
export OBI_SUSTAINED_USE_FRACTION=true
export OBI_SUSTAINED_MIN_SAMPLES=5
export OBI_SUSTAINED_MIN_FRACTION=0.7

# Результат: меньше ложных sustained, выше качество
```

### Среднечастотные (Forex)

```bash
# Стандартно
export OBI_SUSTAINED_USE_FRACTION=true
export OBI_SUSTAINED_MIN_SAMPLES=3
export OBI_SUSTAINED_MIN_FRACTION=0.6

# Результат: баланс качества и количества
```

### Низкочастотные (Commodities)

```bash
# Мягче: данных меньше
export OBI_SUSTAINED_USE_FRACTION=true
export OBI_SUSTAINED_MIN_SAMPLES=2
export OBI_SUSTAINED_MIN_FRACTION=0.5

# Результат: больше сигналов при меньших данных
```

## 📈 Мониторинг

### Логи при старте

```
Init BaseOrderFlowHandler for BTCUSDT | ... | 
OBI_sustained: use_frac=True min_samples=3 min_frac=0.60 | ...
```

### Метрики для отслеживания

1. **Доля sustained OBI** — сколько % времени OBI sustained
2. **Качество сигналов** — win rate при sustained vs non-sustained
3. **Количество absorption** — изменение после включения новой логики

### Дебаг

Добавить логирование в `_get_obi()`:

```python
if self.obi_sustained_use_fraction and self._obi_state:
    # ... расчет ...
    if self.processed_ticks % 100 == 0:
        self.logger.debug(
            "OBI sustained check: n=%d strong=%d frac=%.2f sustained=%s",
            n, strong, frac, sustained
        )
```

## 🧪 Тестирование

### A/B тест

```bash
# Группа A (старая логика)
export OBI_SUSTAINED_USE_FRACTION=false

# Группа B (новая логика)
export OBI_SUSTAINED_USE_FRACTION=true
export OBI_SUSTAINED_MIN_FRACTION=0.6

# Сравнить:
# - Количество сигналов
# - Win rate
# - Sharpe ratio
```

### Оптимизация параметров

```python
# Тест разных min_fraction
for frac in [0.5, 0.6, 0.7, 0.8]:
    export OBI_SUSTAINED_MIN_FRACTION=$frac
    # Запустить бэктест
    # Измерить метрики
```

## ⚠️ Важные замечания

1. **Обратная совместимость:**
   - По умолчанию новая логика **включена**
   - Можно отключить через `OBI_SUSTAINED_USE_FRACTION=false`

2. **Влияние на частоту сигналов:**
   - Строже sustained → меньше breakout, больше absorption
   - Мягче sustained → больше breakout, меньше absorption

3. **Зависимость от obi_min_duration:**
   - Больше duration → больше наблюдений в окне → более надежная проверка
   - Меньше duration → меньше наблюдений → может не хватить для min_samples

4. **Не влияет на OBI avg:**
   - Среднее OBI (`obi_avg`) вычисляется как раньше
   - Изменяется только флаг `sustained`

## 📚 Теория

### Почему это важно?

**Order Flow Theory:**
- **Sustained imbalance** = устойчивое преобладание одной стороны
- Один сильный всплеск ≠ sustained
- Нужна **persistence** (устойчивость во времени)

**Аналогия:**
- Старая логика: "Средняя температура по больнице"
- Новая логика: "Сколько пациентов действительно больны?"

### Математическое обоснование

```
Старая: sustained = E[OBI] >= θ
Новая: sustained = E[OBI] >= θ AND P(|OBI| >= θ) >= ρ

где:
E[OBI] = среднее OBI
θ = порог (threshold)
P(|OBI| >= θ) = доля наблюдений выше порога
ρ = минимальная доля (min_fraction)
```

## 🚀 Следующие шаги

1. **Бэктест** на исторических данных
2. **Оптимизация** параметров под каждый инструмент
3. **Мониторинг** метрик в production
4. **A/B тест** старой vs новой логики

---

## Absorption Require Weak Progress

### Обзор

Добавлен флаг `ABSORPTION_REQUIRE_WEAK_PROGRESS` для опциональной обязательности `weak_progress` в absorption сигналах.

### Параметр

```bash
# По умолчанию (требуется weak_progress)
export ABSORPTION_REQUIRE_WEAK_PROGRESS=true

# Отключить обязательность
export ABSORPTION_REQUIRE_WEAK_PROGRESS=false
```

### Логика

```python
# Старое условие
if ctx.weak_progress and near_level and not obi_confirms:
    # absorption

# Новое условие
if ((not require_weak) or ctx.weak_progress) and near_level and not obi_confirms:
    # absorption
```

### Когда использовать

**`ABSORPTION_REQUIRE_WEAK_PROGRESS=false`:**
- Для инструментов с высокой волатильностью
- Когда weak_progress слишком строгий критерий
- Для тестирования влияния weak_progress

**`ABSORPTION_REQUIRE_WEAK_PROGRESS=true` (default):**
- Стандартное поведение
- Соответствует теории (absorption = слабое движение)
- Меньше ложных absorption сигналов

### Эффект

- `false` → **больше absorption сигналов** (слабее фильтр)
- `true` → **меньше absorption сигналов** (строже фильтр)

---

**Версия:** 1.0  
**Дата:** 2025-11-29  
**Автор:** Trading Systems Team

