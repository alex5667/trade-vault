# Полная Интеграция Системы Скоринга Сигналов

## Обзор

Система скоринга сигналов полностью интегрирована и готова к использованию. Она предоставляет комплексную оценку качества сигналов на основе локальных квантильных Z по нескольким метрикам с поддержкой per-symbol и per-pattern конфигурации.

## Архитектура

### Компоненты Системы

1. **`signal_scoring.config`** - Конфигурация скоринга из ENV переменных
2. **`signal_scoring.ctx`** - Контекст сигнала с метриками
3. **`signal_scoring.engine`** - Движок скоринга с локальными квантилями
4. **Обновленный калибратор** - Поддержка нескольких метрик
5. **SQL миграция** - Добавление колонок метрик в БД
6. **Интеграция в BaseOrderFlowHandler** - Полная интеграция скоринга

## ENV Конфигурация

### Глобальные Параметры
```bash
# Глобальный минимальный confidence (0-100)
CRYPTO_SIGNAL_MIN_CONF=80

# Уровень для golden pattern (>= GOLDEN_PATTERN_MIN_CONFIDENCE)
GOLDEN_PATTERN_MIN_CONFIDENCE=90
```

### Per-Symbol Параметры
```bash
# Отдельный минимум для золота
CRYPTO_SIGNAL_MIN_CONF__XAUUSD=20

# Можно добавить для других символов:
# CRYPTO_SIGNAL_MIN_CONF__BTCUSDT=75
# CRYPTO_SIGNAL_MIN_CONF__ETHUSDT=70
```

### Вес Метрик
```bash
# Вес каждой метрики в комбинированном score (0.0-1.0)
SIGNAL_METRIC_WEIGHT__DELTA_SPIKE_Z=1.0
SIGNAL_METRIC_WEIGHT__OBI=0.7
SIGNAL_METRIC_WEIGHT__WEAK_PROGRESS=0.5
SIGNAL_METRIC_WEIGHT__ATR_QUANTILE=0.3
```

### Per-Pattern Параметры
```bash
# Вес паттерна (мультипликатор к combined_q)
SIGNAL_PATTERN_WEIGHT__BREAKOUT_R1=1.2
SIGNAL_PATTERN_WEIGHT__FADE_PDH=1.1
SIGNAL_PATTERN_WEIGHT__FADE_HTF_OB=1.3

# Минимальный confidence для паттерна (override глобального)
SIGNAL_PATTERN_MIN_CONF__BREAKOUT_R1=85
SIGNAL_PATTERN_MIN_CONF__FADE_PDH=80
SIGNAL_PATTERN_MIN_CONF__FADE_HTF_OB=88
```

## Алгоритм Скоринга

### 1. Локальные Квантили
Для каждой метрики вычисляется локальный квантиль (0-1) на основе исторических данных:

```python
# delta_spike_z: high = good (invert=False)
q_delta = eval_local_quantile(cdf, value)

# weak_progress: low = good (invert=True)
q_weak = 1.0 - eval_local_quantile(cdf, value)
```

### 2. Комбинированный Confidence
```python
# Взвешенная средняя по метрикам
combined_q = sum(q_i * weight_i) / sum(weights)

# Применение pattern weight
combined_q = combined_q * pattern_weight

# Конвертация в 0-100
confidence = round(combined_q * 100)
```

### 3. Golden Pattern
```python
is_golden = confidence >= GOLDEN_PATTERN_MIN_CONFIDENCE
golden_label = f"{pattern}_golden" if is_golden else None
```

### 4. Фильтр should_emit
```python
# Confidence >= min_confidence (symbol + pattern specific)
should_emit = confidence >= min_conf
```

## Интеграция в Код

### Инициализация в BaseOrderFlowHandler
```python
from local_calibration.store import LocalCalibrationStore
from signal_scoring import ScoringConfig

# В __init__
calib_store = LocalCalibrationStore()
calib_store.load_from_db(PG_DSN)
scoring_cfg = ScoringConfig.from_env()
self._scoring_engine = SignalScoringEngine(calib_store, scoring_cfg)
```

### Использование в _compute_confidence
```python
def _compute_confidence(self, ctx, signal_type):
    # Создание SignalContext для скоринга
    scoring_ctx = SignalContext(
        ts=ctx.ts_utc,
        symbol=ctx.symbol,
        side=ctx.side,
        session=ctx.session,
        regime=ctx.regime,
        pattern_name=signal_type,
        delta_spike_z=ctx.deltaSpikeZ,
        obi=ctx.obi,
        weak_progress=ctx.weakProgress,
        atr_quantile=ctx.atr_quantile,
    )

    # Вычисление confidence
    confidence = self._scoring_engine.compute_confidence(scoring_ctx)

    # Копирование результатов обратно
    ctx.confidence = scoring_ctx.confidence
    ctx.is_golden_pattern = scoring_ctx.is_golden_pattern
    # ... остальные поля

    return confidence, breakdown
```

### Использование в _should_emit
```python
def _should_emit(self, ctx):
    # Создание scoring context
    scoring_ctx = SignalContext(...)
    return self._scoring_engine.should_emit(scoring_ctx)
```

## Payload Сигнала

```json
{
  "symbol": "XAUUSD",
  "confidence": 92,
  "minConfidenceUsed": 20,
  "isGoldenPattern": true,
  "goldenPatternLabel": "breakout_r1_golden",
  "metrics": {
    "deltaSpikeZ": 2.5,
    "deltaSpikeZLocalQ": 0.95,
    "obi": 1.8,
    "obiLocalQ": 0.88,
    "weakProgress": 0.2,
    "weakProgressLocalQ": 0.92,
    "atrQuantile": 0.85,
    "atrLocalQ": 0.78
  }
}
```

## Настройка и Запуск

### 1. Применение Миграции
```bash
psql -d your_database -f python-worker/migrations/002_add_signal_metrics.sql
```

### 2. Запуск Калибровки
```bash
PG_DSN="postgresql://..." python -m local_calibration.calibrate_local_thresholds
```

### 3. Перезапуск Системы
```bash
make up
```

### 4. Проверка Работы
```bash
python python-worker/test_signal_scoring.py
```

## Калибровка

### Обновленный Калибратор
- Поддержка 4 метрик: `delta_spike_z`, `obi`, `weak_progress`, `atr_quantile`
- Обновленная модель `SignalRow` со всеми метриками
- Обновленный SQL запрос для загрузки данных

### Запуск
```bash
# Активация всех метрик
METRICS = ("delta_spike_z", "obi", "weak_progress", "atr_quantile")

# Запуск калибровки
python -m local_calibration.calibrate_local_thresholds
```

## Мониторинг

### Проверка Калибровки
```bash
python python-worker/scripts/check_calibration_results.py
```

### Логи Скоринга
```bash
grep "SignalScoringEngine" logs/*.log
```

### Тестирование
```bash
python python-worker/test_signal_scoring.py
```

## Примеры Конфигурации

### Консервативная Настройка
```bash
CRYPTO_SIGNAL_MIN_CONF=85
GOLDEN_PATTERN_MIN_CONFIDENCE=95
SIGNAL_METRIC_WEIGHT__DELTA_SPIKE_Z=1.2
CRYPTO_SIGNAL_MIN_CONF__XAUUSD=25
```

### Агрессивная Настройка
```bash
CRYPTO_SIGNAL_MIN_CONF=70
GOLDEN_PATTERN_MIN_CONFIDENCE=85
SIGNAL_METRIC_WEIGHT__DELTA_SPIKE_Z=0.8
CRYPTO_SIGNAL_MIN_CONF__XAUUSD=15
```

## Диагностика

### Возможные Проблемы

1. **Отсутствие калибровки**
   ```
   Решение: Запустить calibrate_local_thresholds
   ```

2. **Низкий confidence**
   ```
   Решение: Проверить веса метрик и пороги
   ```

3. **Не срабатывает golden**
   ```
   Решение: Проверить GOLDEN_PATTERN_MIN_CONFIDENCE
   ```

4. **XAUUSD фильтруется**
   ```
   Решение: Проверить CRYPTO_SIGNAL_MIN_CONF__XAUUSD
   ```

## Результат

✅ **Локальные квантили** по 4 метрикам для каждого кластера (symbol, session, regime)
✅ **Комбинированный confidence** 0-100 с весами метрик
✅ **Per-symbol фильтры** (XAUUSD: 20 vs глобальные 80)
✅ **Per-pattern конфигурация** (веса и минимумы)
✅ **Golden patterns** с confidence >= 90
✅ **Полная интеграция** в BaseOrderFlowHandler
✅ **Тестирование** и мониторинг

Система полностью готова к продакшену! 🚀📊
