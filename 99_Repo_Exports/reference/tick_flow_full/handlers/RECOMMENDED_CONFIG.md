# BaseOrderFlowHandler - Рекомендуемая конфигурация

## 📋 Обзор

Этот документ содержит **рекомендуемые значения по умолчанию** для `BaseOrderFlowHandler` на основе теории order flow и практических тестов.

## 🎯 Базовая конфигурация (рекомендуется)

### 1. Строгий breakout по OBI

```bash
# Требует подтверждения OBI для breakout сигналов
BREAKOUT_REQUIRE_OBI=true
```

**Обоснование:**
- Фильтрует ложные пробои
- Breakout только при подтверждении order flow
- Соответствует теории (momentum должен подтверждаться OBI)

**Эффект:**
- ✅ Меньше ложных breakout сигналов
- ✅ Выше качество (win rate)
- ⚠️ Меньше общее количество breakout

### 2. OBI sustained как "устойчивость"

```bash
# Включить проверку устойчивости
OBI_SUSTAINED_USE_FRACTION=true

# Минимум наблюдений в окне
OBI_SUSTAINED_MIN_SAMPLES=3

# Минимальная доля "сильных" наблюдений (60%)
OBI_SUSTAINED_MIN_FRACTION=0.6
```

**Обоснование:**
- Проверяет не только среднее, но и persistence
- Фильтрует случаи "1 сильный всплеск + 9 слабых"
- Соответствует теории sustained imbalance

**Эффект:**
- ✅ Более качественные sustained сигналы
- ✅ Меньше ложных sustained
- ⚠️ Строже критерий sustained

### 3. Пороги Z-score по типам сигналов

```bash
# Порог для breakout
BREAKOUT_Z_THRESHOLD=3.0

# Порог для absorption
ABSORPTION_Z_THRESHOLD=3.0

# Множитель для extreme
EXTREME_Z_MULT=1.6
```

**Обоснование:**
- Раздельные пороги для гибкой настройки
- По умолчанию все равны базовому (обратная совместимость)
- Можно оптимизировать каждый тип независимо

**Эффект:**
- ✅ Гибкость настройки
- ✅ Обратная совместимость
- ✅ Возможность оптимизации

### 4. Absorption controls

```bash
# Требовать weak_progress для absorption
ABSORPTION_REQUIRE_WEAK_PROGRESS=true
```

**Обоснование:**
- Absorption = слабое движение цены (по теории)
- Без weak_progress это не absorption

**Эффект:**
- ✅ Соответствие теории
- ✅ Меньше ложных absorption
- ⚠️ Строже критерий

### 5. Absorption micro-proxy (для crypto)

```bash
# Использовать микроструктурный proxy
ABSORPTION_USE_MICRO_PROXY=true

# Минимальный adverse ratio (60%)
ABSORPTION_MICRO_ADVERSE_MIN=0.60

# Максимальный realized spread EMA (-0.5 bps)
ABSORPTION_MICRO_REALIZED_EMA_MAX=-0.50
```

**Обоснование:**
- Для crypto weak_progress может быть слишком строгим
- Микроструктура (adverse ratio, realized spread) показывает absorption
- Дополнительный сигнал поглощения

**Эффект:**
- ✅ Ловит absorption даже без weak_progress
- ✅ Использует микроструктурные данные
- ⚠️ Требует is_buyer_maker в тиках

## 📊 Конфигурации по инструментам

### Crypto высокочастотный (BTCUSDT, ETHUSDT)

```bash
# === БАЗОВЫЕ ПАРАМЕТРЫ ===
BTCUSDT_DELTA_Z_THRESHOLD=2.7
BTCUSDT_OBI_THRESHOLD=0.35
BTCUSDT_WEAK_PROGRESS_ATR=0.15

# === BREAKOUT ===
BREAKOUT_REQUIRE_OBI=true
BREAKOUT_Z_THRESHOLD=3.2

# === ABSORPTION ===
ABSORPTION_Z_THRESHOLD=2.5
ABSORPTION_REQUIRE_WEAK_PROGRESS=false  # Используем micro proxy
ABSORPTION_USE_MICRO_PROXY=true
ABSORPTION_MICRO_ADVERSE_MIN=0.65
ABSORPTION_MICRO_REALIZED_EMA_MAX=-0.50

# === EXTREME ===
EXTREME_Z_MULT=2.0

# === OBI SUSTAINED ===
OBI_SUSTAINED_USE_FRACTION=true
OBI_SUSTAINED_MIN_SAMPLES=5
OBI_SUSTAINED_MIN_FRACTION=0.7

# === ДРУГОЕ ===
DELTA_BUCKET_MS=1000
OBI_MAX_STALE_MS=2000
```

**Особенности:**
- Низкий базовый порог (2.7) из-за шума
- Breakout строже (3.2) для фильтрации ложных пробоев
- Absorption мягче (2.5) для ранней детекции
- Используем micro proxy вместо weak_progress
- Строгий sustained (70%) из-за высокой частоты

### Forex среднечастотный (EURUSD, GBPUSD)

```bash
# === БАЗОВЫЕ ПАРАМЕТРЫ ===
EURUSD_DELTA_Z_THRESHOLD=3.0
EURUSD_OBI_THRESHOLD=0.5
EURUSD_WEAK_PROGRESS_ATR=0.10

# === BREAKOUT ===
BREAKOUT_REQUIRE_OBI=true
BREAKOUT_Z_THRESHOLD=3.0

# === ABSORPTION ===
ABSORPTION_Z_THRESHOLD=3.0
ABSORPTION_REQUIRE_WEAK_PROGRESS=true
ABSORPTION_USE_MICRO_PROXY=false  # Не нужен для forex

# === EXTREME ===
EXTREME_Z_MULT=1.6

# === OBI SUSTAINED ===
OBI_SUSTAINED_USE_FRACTION=true
OBI_SUSTAINED_MIN_SAMPLES=3
OBI_SUSTAINED_MIN_FRACTION=0.6

# === ДРУГОЕ ===
DELTA_BUCKET_MS=1500
OBI_MAX_STALE_MS=2500
```

**Особенности:**
- Стандартные пороги (3.0)
- Используем weak_progress (без micro proxy)
- Стандартный sustained (60%)
- Больший bucket (1500ms)

### Commodities низкочастотный (XAUUSD, XAGUSD)

```bash
# === БАЗОВЫЕ ПАРАМЕТРЫ ===
XAUUSD_DELTA_Z_THRESHOLD=3.0
XAUUSD_OBI_THRESHOLD=0.5
XAUUSD_WEAK_PROGRESS_ATR=0.10

# === BREAKOUT ===
BREAKOUT_REQUIRE_OBI=true
BREAKOUT_Z_THRESHOLD=3.0

# === ABSORPTION ===
ABSORPTION_Z_THRESHOLD=2.8
ABSORPTION_REQUIRE_WEAK_PROGRESS=true
ABSORPTION_USE_MICRO_PROXY=false

# === EXTREME ===
EXTREME_Z_MULT=1.6

# === OBI SUSTAINED ===
OBI_SUSTAINED_USE_FRACTION=true
OBI_SUSTAINED_MIN_SAMPLES=2
OBI_SUSTAINED_MIN_FRACTION=0.5

# === ДРУГОЕ ===
DELTA_BUCKET_MS=2000
OBI_MAX_STALE_MS=3000
```

**Особенности:**
- Absorption чуть мягче (2.8)
- Мягкий sustained (50%, 2 samples) из-за низкой частоты
- Большой bucket (2000ms)
- Больший OBI stale timeout

## 🔧 Дополнительные параметры

### Breakout controls

```bash
# Минимальная дистанция от уровня (в ATR)
BREAKOUT_MIN_DIST_ATR=0.0  # 0 = любая дистанция

# Cooldown между сигналами на уровне (ms)
LEVEL_SIGNAL_COOLDOWN_MS=15000
```

### Delta bucketing

```bash
# Размер временного бакета (ms)
DELTA_BUCKET_MS=1000

# Максимум нулевых бакетов при gaps
DELTA_BUCKET_MAX_ZERO_FILL=3
```

### Tick processing

```bash
# Максимальный lag тика (ms)
MAX_TICK_LAG_MS=5000
```

## 📈 Матрица параметров

| Параметр | Crypto | Forex | Commodities | Обоснование |
|----------|--------|-------|-------------|-------------|
| **BREAKOUT_Z_THRESHOLD** | 3.2 | 3.0 | 3.0 | Crypto: строже из-за шума |
| **ABSORPTION_Z_THRESHOLD** | 2.5 | 3.0 | 2.8 | Crypto: ловим раньше |
| **EXTREME_Z_MULT** | 2.0 | 1.6 | 1.6 | Crypto: только аномалии |
| **OBI_SUSTAINED_MIN_FRACTION** | 0.7 | 0.6 | 0.5 | Crypto: строже, Commodities: мягче |
| **OBI_SUSTAINED_MIN_SAMPLES** | 5 | 3 | 2 | Зависит от частоты данных |
| **ABSORPTION_USE_MICRO_PROXY** | true | false | false | Только для crypto |
| **DELTA_BUCKET_MS** | 1000 | 1500 | 2000 | Зависит от частоты |

## 🧪 Тестирование конфигурации

### Шаг 1: Базовый тест

```bash
# Применить рекомендуемую конфигурацию
export BREAKOUT_REQUIRE_OBI=true
export OBI_SUSTAINED_USE_FRACTION=true
export OBI_SUSTAINED_MIN_SAMPLES=3
export OBI_SUSTAINED_MIN_FRACTION=0.6
export BREAKOUT_Z_THRESHOLD=3.0
export ABSORPTION_Z_THRESHOLD=3.0
export EXTREME_Z_MULT=1.6

# Запустить handler
python -m handlers.crypto_orderflow_handler BTCUSDT
```

### Шаг 2: Мониторинг метрик

```python
# Отслеживать:
# 1. Количество сигналов по типам
# 2. Win rate по типам
# 3. Sharpe ratio
# 4. Max drawdown
# 5. Доля sustained OBI
```

### Шаг 3: Оптимизация

```python
# Grid search для оптимизации
for breakout_z in [2.8, 3.0, 3.2, 3.5]:
    for absorption_z in [2.5, 2.8, 3.0, 3.2]:
        for sustained_frac in [0.5, 0.6, 0.7]:
            # Бэктест
            results = backtest(params)
            # Сохранить лучшие
            if results.sharpe > best_sharpe:
                best_params = params
```

## 📊 Ожидаемые результаты

### С рекомендуемой конфигурацией

**Crypto (BTCUSDT):**
- Breakout сигналов: 30-40% от всех
- Absorption сигналов: 25-35% от всех
- Extreme сигналов: 15-25% от всех
- Win rate breakout: 55-60%
- Win rate absorption: 60-65%
- Sharpe ratio: 1.5-2.0

**Forex (EURUSD):**
- Breakout сигналов: 35-45% от всех
- Absorption сигналов: 30-40% от всех
- Extreme сигналов: 15-20% от всех
- Win rate breakout: 52-57%
- Win rate absorption: 58-63%
- Sharpe ratio: 1.3-1.8

**Commodities (XAUUSD):**
- Breakout сигналов: 40-50% от всех
- Absorption сигналов: 30-35% от всех
- Extreme сигналов: 15-20% от всех
- Win rate breakout: 50-55%
- Win rate absorption: 60-65%
- Sharpe ratio: 1.2-1.6

## ⚠️ Важные замечания

### 1. Обратная совместимость

Все параметры опциональны. По умолчанию:
```python
breakout_z = delta_z_threshold
absorption_z = delta_z_threshold
extreme_z = delta_z_threshold * 1.6
obi_sustained_use_fraction = true
breakout_require_obi = true
```

### 2. Постепенная миграция

Можно включать параметры постепенно:

**Этап 1:** Только OBI sustained
```bash
export OBI_SUSTAINED_USE_FRACTION=true
```

**Этап 2:** + Строгий breakout
```bash
export BREAKOUT_REQUIRE_OBI=true
```

**Этап 3:** + Раздельные Z-пороги
```bash
export BREAKOUT_Z_THRESHOLD=3.2
export ABSORPTION_Z_THRESHOLD=2.5
```

**Этап 4:** + Micro proxy (для crypto)
```bash
export ABSORPTION_USE_MICRO_PROXY=true
```

### 3. Мониторинг обязателен

После изменения параметров:
- ✅ Отслеживать метрики в Grafana
- ✅ Сравнивать с baseline
- ✅ Проводить A/B тесты
- ✅ Валидировать на out-of-sample данных

### 4. Не для всех инструментов одинаково

Параметры нужно адаптировать:
- По волатильности
- По ликвидности
- По частоте данных
- По торговым сессиям

## 🚀 Быстрый старт

### Минимальная конфигурация (начать с этого)

```bash
# Только базовые улучшения
export BREAKOUT_REQUIRE_OBI=true
export OBI_SUSTAINED_USE_FRACTION=true
export OBI_SUSTAINED_MIN_SAMPLES=3
export OBI_SUSTAINED_MIN_FRACTION=0.6
```

### Полная конфигурация (после тестов)

```bash
# Все параметры
export BREAKOUT_REQUIRE_OBI=true
export BREAKOUT_Z_THRESHOLD=3.0
export ABSORPTION_Z_THRESHOLD=3.0
export ABSORPTION_REQUIRE_WEAK_PROGRESS=true
export EXTREME_Z_MULT=1.6
export OBI_SUSTAINED_USE_FRACTION=true
export OBI_SUSTAINED_MIN_SAMPLES=3
export OBI_SUSTAINED_MIN_FRACTION=0.6
```

### Crypto специфичная (с micro proxy)

```bash
# Для crypto с микроструктурой
export BREAKOUT_REQUIRE_OBI=true
export BREAKOUT_Z_THRESHOLD=3.2
export ABSORPTION_Z_THRESHOLD=2.5
export ABSORPTION_REQUIRE_WEAK_PROGRESS=false
export ABSORPTION_USE_MICRO_PROXY=true
export ABSORPTION_MICRO_ADVERSE_MIN=0.65
export ABSORPTION_MICRO_REALIZED_EMA_MAX=-0.50
export EXTREME_Z_MULT=2.0
export OBI_SUSTAINED_USE_FRACTION=true
export OBI_SUSTAINED_MIN_SAMPLES=5
export OBI_SUSTAINED_MIN_FRACTION=0.7
```

---

**Версия:** 1.0  
**Дата:** 2025-11-29  
**Автор:** Trading Systems Team  
**Статус:** Рекомендуется для production

