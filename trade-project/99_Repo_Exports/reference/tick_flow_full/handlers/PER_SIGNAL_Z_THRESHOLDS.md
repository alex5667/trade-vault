# Per-Signal Z-Thresholds - Документация

## Обзор

В `BaseOrderFlowHandler` добавлены **раздельные Z-пороги** для разных типов сигналов, что позволяет тонко настраивать чувствительность каждого типа сигнала независимо.

## 🎯 Проблема

**Старая логика:**
```python
# Единый порог для всех сигналов
if z_abs >= config.delta_z_threshold:
    # Генерируем любой сигнал (breakout/absorption/extreme)
```

**Проблемы:**
- Все сигналы используют один порог
- Нельзя сделать breakout строже, а absorption мягче
- Нет гибкости для оптимизации под инструмент

## ✅ Решение

**Новая логика:**
```python
# Раздельные пороги для каждого типа
if z_abs >= breakout_z_threshold:
    # Breakout сигнал

if z_abs >= absorption_z_threshold:
    # Absorption сигнал

if z_abs >= extreme_z_threshold:
    # Extreme сигнал
```

## 📊 Параметры

### 1. `MAIN_Z_THRESHOLD` (auto-calculated)

**Главный порог** для входа в логику генерации сигналов.

```bash
# По умолчанию: минимум из всех порогов
MAIN_Z_THRESHOLD = min(delta_z_threshold, breakout_z_threshold, absorption_z_threshold)

# Можно переопределить вручную
export MAIN_Z_THRESHOLD=2.5
```

**Назначение:**
- Фильтрует custom hooks
- Если `z < main_z_threshold` → только custom сигналы
- Если `z >= main_z_threshold` → проверяем breakout/absorption/extreme

**Когда изменять:**
- Обычно не нужно (auto-calculated)
- Изменять только если хотите особую логику

### 2. `BREAKOUT_Z_THRESHOLD` (default: `delta_z_threshold`)

Порог Z-score для **breakout** сигналов.

```bash
# По умолчанию = базовый порог
export BREAKOUT_Z_THRESHOLD=3.0

# Строже (меньше breakout сигналов, выше качество)
export BREAKOUT_Z_THRESHOLD=3.5

# Мягче (больше breakout сигналов)
export BREAKOUT_Z_THRESHOLD=2.5
```

**Эффект:**
- **Выше** → меньше breakout, но выше качество
- **Ниже** → больше breakout, но ниже качество

**Рекомендации:**
- **Crypto (высокая волатильность):** 2.5-3.0
- **Forex (средняя волатильность):** 3.0-3.5
- **Commodities (низкая волатильность):** 3.5-4.0

### 3. `ABSORPTION_Z_THRESHOLD` (default: `delta_z_threshold`)

Порог Z-score для **absorption** сигналов.

```bash
# По умолчанию = базовый порог
export ABSORPTION_Z_THRESHOLD=3.0

# Строже (меньше absorption, только сильные)
export ABSORPTION_Z_THRESHOLD=3.5

# Мягче (больше absorption, ловим слабые)
export ABSORPTION_Z_THRESHOLD=2.5
```

**Эффект:**
- **Выше** → меньше absorption, только явные поглощения
- **Ниже** → больше absorption, ловим ранние признаки

**Рекомендации:**
- **Для fade стратегий:** 2.5-3.0 (ловим ранние absorption)
- **Для консервативных:** 3.5-4.0 (только явные)

### 4. `EXTREME_Z_THRESHOLD` (default: `delta_z_threshold * 1.6`)

Порог Z-score для **extreme** сигналов (экстремальная активность).

```bash
# По умолчанию = базовый * 1.6
export EXTREME_Z_THRESHOLD=4.8  # если базовый 3.0

# Строже (только очень экстремальные)
export EXTREME_Z_THRESHOLD=5.5

# Мягче (больше extreme сигналов)
export EXTREME_Z_THRESHOLD=4.0
```

**Эффект:**
- **Выше** → меньше extreme, только аномальные
- **Ниже** → больше extreme, ловим сильные движения

**Рекомендации:**
- **Стандартно:** `base * 1.6` (default)
- **Агрессивно:** `base * 1.4`
- **Консервативно:** `base * 2.0`

### 5. `EXTREME_Z_MULT` (default: `1.6`)

Множитель для расчета extreme порога.

```bash
# По умолчанию
export EXTREME_Z_MULT=1.6

# Строже
export EXTREME_Z_MULT=2.0

# Мягче
export EXTREME_Z_MULT=1.4
```

**Формула:**
```python
extreme_z_threshold = delta_z_threshold * extreme_z_mult
```

## 🎯 Примеры конфигураций

### Пример 1: Стандартная (все одинаковые)

```bash
# Все сигналы с одним порогом (как раньше)
export BTCUSDT_DELTA_Z_THRESHOLD=3.0
export BREAKOUT_Z_THRESHOLD=3.0
export ABSORPTION_Z_THRESHOLD=3.0
export EXTREME_Z_MULT=1.6  # extreme = 4.8

# Результат:
# main_z = 3.0
# breakout_z = 3.0
# absorption_z = 3.0
# extreme_z = 4.8
```

### Пример 2: Строгий breakout, мягкий absorption

```bash
# Breakout только на сильных спайках
export BREAKOUT_Z_THRESHOLD=3.5

# Absorption ловим раньше
export ABSORPTION_Z_THRESHOLD=2.5

# Extreme стандартно
export EXTREME_Z_MULT=1.6

# Результат:
# main_z = 2.5 (минимум)
# breakout_z = 3.5 (строго)
# absorption_z = 2.5 (мягко)
# extreme_z = 4.8 (если base=3.0)
```

### Пример 3: Crypto высокочастотный

```bash
# Базовый порог низкий (много шума)
export BTCUSDT_DELTA_Z_THRESHOLD=2.7

# Breakout строже (фильтруем ложные пробои)
export BREAKOUT_Z_THRESHOLD=3.2

# Absorption мягче (ловим ранние признаки)
export ABSORPTION_Z_THRESHOLD=2.5

# Extreme очень строго (только аномалии)
export EXTREME_Z_MULT=2.0  # extreme = 5.4

# Результат:
# main_z = 2.5
# breakout_z = 3.2
# absorption_z = 2.5
# extreme_z = 5.4
```

### Пример 4: Commodities низкочастотный

```bash
# Базовый порог высокий (мало данных)
export XAUUSD_DELTA_Z_THRESHOLD=3.0

# Breakout стандартно
export BREAKOUT_Z_THRESHOLD=3.0

# Absorption чуть мягче
export ABSORPTION_Z_THRESHOLD=2.8

# Extreme стандартно
export EXTREME_Z_MULT=1.6  # extreme = 4.8

# Результат:
# main_z = 2.8
# breakout_z = 3.0
# absorption_z = 2.8
# extreme_z = 4.8
```

## 🔍 Логика работы

### Блок-схема

```python
def _generate_signals(ctx):
    z_abs = abs(ctx.z_delta)
    
    # 1. Проверка main_z_threshold
    if z_abs < main_z_threshold:
        # Только custom hooks
        return check_custom_conditions()
    
    # 2. Проверка absorption
    if (z_abs >= absorption_z_threshold
        and weak_progress
        and near_level
        and not obi_confirms):
        return generate_absorption()
    
    # 3. Проверка breakout
    if (z_abs >= breakout_z_threshold
        and breakout_level
        and obi_confirms):
        return generate_breakout()
    
    # 4. Проверка extreme
    if z_abs >= extreme_z_threshold:
        return generate_extreme()
    
    # 5. Custom hooks
    return check_custom_conditions()
```

### Приоритет проверок

1. **main_z_threshold** — gate для входа в основную логику
2. **absorption** — проверяется первым (если условия выполнены)
3. **breakout** — проверяется вторым
4. **extreme** — проверяется третьим
5. **custom** — проверяется в конце

## 📈 Влияние на сигналы

### Breakout сигналы

```python
# Старое
if breakout_level and obi_confirms:
    generate_breakout()

# Новое
if breakout_level and (z_abs >= breakout_z_threshold) and obi_confirms:
    generate_breakout()
```

**Эффект:**
- Добавлен явный порог Z для breakout
- Можно сделать breakout строже независимо от absorption

### Absorption сигналы

```python
# Старое
if weak_progress and near_level and not obi_confirms:
    generate_absorption()

# Новое
if (z_abs >= absorption_z_threshold) 
   and weak_progress 
   and near_level 
   and not obi_confirms:
    generate_absorption()
```

**Эффект:**
- Добавлен явный порог Z для absorption
- Можно сделать absorption мягче для ранней детекции

### Extreme сигналы

```python
# Старое
if z_abs >= config.delta_z_threshold * 1.6:
    generate_extreme()

# Новое
if z_abs >= extreme_z_threshold:
    generate_extreme()
```

**Эффект:**
- Явный параметр вместо hardcoded множителя
- Можно настроить через env

## 🧪 Оптимизация параметров

### A/B тестирование

```bash
# Группа A (стандартные пороги)
export BREAKOUT_Z_THRESHOLD=3.0
export ABSORPTION_Z_THRESHOLD=3.0

# Группа B (оптимизированные)
export BREAKOUT_Z_THRESHOLD=3.5
export ABSORPTION_Z_THRESHOLD=2.5

# Сравнить метрики:
# - Количество сигналов каждого типа
# - Win rate по типам
# - Sharpe ratio
# - Max drawdown
```

### Grid Search

```python
# Перебор комбинаций
for breakout_z in [2.5, 3.0, 3.5, 4.0]:
    for absorption_z in [2.0, 2.5, 3.0, 3.5]:
        # Запустить бэктест
        # Измерить метрики
        # Найти оптимум
```

### Walk-Forward оптимизация

```python
# Оптимизация на обучающем периоде
train_period = "2024-01-01 to 2024-06-30"
optimal_params = optimize(train_period)

# Тест на валидационном периоде
test_period = "2024-07-01 to 2024-09-30"
results = backtest(test_period, optimal_params)

# Если результаты хорошие → применяем в production
```

## 📊 Мониторинг

### Логи при старте

```
Init BaseOrderFlowHandler for BTCUSDT | ... |
Z: main=2.50 breakout=3.20 absorption=2.50 extreme=5.40 | ...
```

### Метрики для отслеживания

1. **Распределение сигналов по типам:**
   - Breakout: 40%
   - Absorption: 30%
   - Extreme: 20%
   - Custom: 10%

2. **Win rate по типам:**
   - Breakout: 55%
   - Absorption: 60%
   - Extreme: 65%

3. **Средний Z-score по типам:**
   - Breakout: 3.5
   - Absorption: 3.2
   - Extreme: 5.8

### Grafana dashboard

```sql
-- Количество сигналов по типам
SELECT 
    signal_kind,
    COUNT(*) as count,
    AVG(z_delta) as avg_z
FROM signals
WHERE timestamp > NOW() - INTERVAL '24 hours'
GROUP BY signal_kind

-- Win rate по типам
SELECT 
    signal_kind,
    SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as win_rate
FROM signals
JOIN trades ON signals.sid = trades.sid
WHERE signals.timestamp > NOW() - INTERVAL '7 days'
GROUP BY signal_kind
```

## ⚠️ Важные замечания

### 1. Обратная совместимость

По умолчанию все пороги равны `delta_z_threshold`:
```python
breakout_z = delta_z_threshold
absorption_z = delta_z_threshold
extreme_z = delta_z_threshold * 1.6
```

Старое поведение сохраняется если не задавать env variables.

### 2. Main Z threshold

`main_z_threshold` вычисляется автоматически как минимум:
```python
main_z = min(delta_z, breakout_z, absorption_z)
```

Это гарантирует что сигналы не пропустятся если их порог ниже базового.

### 3. Взаимодействие с OBI

Пороги Z **независимы** от OBI проверок:
- Breakout: `z >= breakout_z AND obi_confirms`
- Absorption: `z >= absorption_z AND NOT obi_confirms`

### 4. Custom hooks

Custom hooks срабатывают при `z < main_z_threshold`:
```python
if z_abs < main_z_threshold:
    return check_custom_conditions()
```

Если хотите custom hooks при любом Z → установите `main_z_threshold` очень высоко.

## 🎓 Теория

### Почему раздельные пороги?

**Order Flow Theory:**
- **Breakout** требует сильного импульса → высокий порог
- **Absorption** можно ловить раньше → низкий порог
- **Extreme** только аномалии → очень высокий порог

**Статистическое обоснование:**
```
P(breakout success | z >= 3.5) > P(breakout success | z >= 2.5)
P(absorption success | z >= 2.5) ≈ P(absorption success | z >= 3.5)

→ Оптимально: breakout_z = 3.5, absorption_z = 2.5
```

### ROC анализ

```python
# Для каждого типа сигнала строим ROC кривую
# Находим оптимальный порог по критерию Youden's J

breakout_optimal_z = find_optimal_threshold(
    signals=breakout_signals,
    outcomes=breakout_outcomes,
    metric='youden_j'
)
```

## 🚀 Следующие шаги

1. **Бэктест** с разными комбинациями порогов
2. **Оптимизация** под каждый инструмент
3. **A/B тест** в production
4. **Мониторинг** метрик по типам сигналов
5. **Итеративная оптимизация** на основе результатов

---

**Версия:** 1.0  
**Дата:** 2025-11-29  
**Автор:** Trading Systems Team

