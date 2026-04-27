# Система Скоринга Сигналов

## Обзор

Новая система скоринга сигналов предоставляет комплексный подход к оценке качества сигналов на основе локальных квантильных Z по нескольким метрикам, с поддержкой per-symbol и per-pattern конфигурации.

## Архитектура

### Компоненты

1. **`signal_scoring.config`** - Конфигурация скоринга из ENV переменных
2. **`signal_scoring.ctx`** - Контекст сигнала с метриками
3. **`signal_scoring.engine`** - Движок скоринга с локальными квантилями
4. **Локальная калибровка** - Хранение CDF для каждого (symbol, session, regime, metric)

### Метрики

- `delta_spike_z` - Z-score силы сигнала delta
- `obi` - Order Book Imbalance
- `weak_progress` - Weak progress indicator (|range|/ATR)
- `atr_quantile` - ATR quantile (волатильность)

## Конфигурация через ENV

### Глобальные параметры

```bash
# Глобальный минимальный confidence (0-100)
CRYPTO_SIGNAL_MIN_CONF=80

# Уровень для golden pattern (>= GOLDEN_PATTERN_MIN_CONFIDENCE)
GOLDEN_PATTERN_MIN_CONFIDENCE=90
```

### Per-symbol параметры

```bash
# Отдельный минимум для золота
CRYPTO_SIGNAL_MIN_CONF__XAUUSD=20

# Можно добавить для других символов:
# CRYPTO_SIGNAL_MIN_CONF__BTCUSDT=75
# CRYPTO_SIGNAL_MIN_CONF__ETHUSDT=70
```

### Вес метрик

```bash
# Вес каждой метрики в комбинированном score (0.0-1.0)
SIGNAL_METRIC_WEIGHT__DELTA_SPIKE_Z=1.0
SIGNAL_METRIC_WEIGHT__OBI=0.7
SIGNAL_METRIC_WEIGHT__WEAK_PROGRESS=0.5
SIGNAL_METRIC_WEIGHT__ATR_QUANTILE=0.3
```

### Per-pattern параметры

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

## Алгоритм скоринга

### 1. Локальные квантили

Для каждой метрики вычисляется локальный квантиль (0-1) на основе исторических данных для конкретного кластера (symbol, session, regime):

```python
# delta_spike_z: high = good (нет инверсии)
q_delta = eval_local_quantile(cdf, value)

# weak_progress: low = good (инверсия)
q_weak = 1.0 - eval_local_quantile(cdf, value)
```

### 2. Взвешенное среднее

```python
combined_q = sum(q_i * weight_i) / sum(weights)
```

### 3. Pattern weight

```python
combined_q = combined_q * pattern_weight
```

### 4. Финальный confidence

```python
confidence = round(combined_q * 100)  # 0-100
```

### 5. Golden pattern

```python
is_golden = confidence >= GOLDEN_PATTERN_MIN_CONFIDENCE
golden_label = f"{pattern}_golden" if is_golden else None
```

### 6. Фильтр should_emit

```python
# 1. Confidence >= min_confidence (symbol + pattern specific)
# 2. Возврат True/False
```

## Интеграция

### В BaseOrderFlowHandler

```python
# Инициализация в __init__
from local_calibration.store import LocalCalibrationStore
from signal_scoring import SignalScoringEngine, ScoringConfig

calib_store = LocalCalibrationStore()
calib_store.load_from_db(PG_DSN)
scoring_cfg = ScoringConfig.from_env()
self._scoring_engine = SignalScoringEngine(calib_store, scoring_cfg)
```

### В _compute_confidence

```python
# Создаем SignalContext для скоринга
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

# Вычисляем confidence
confidence = self._scoring_engine.compute_confidence(scoring_ctx)

# Копируем результаты обратно
ctx.confidence = scoring_ctx.confidence
ctx.is_golden_pattern = scoring_ctx.is_golden_pattern
# ... остальные поля
```

### В _should_emit

```python
# Используем engine для принятия решения
return self._scoring_engine.should_emit(scoring_ctx)
```

## Payload сигнала

```json
{
  "symbol": "XAUUSD",
  "confidence": 87,
  "minConfidenceUsed": 20,
  "isGoldenPattern": true,
  "goldenPatternLabel": "breakout_R1_golden",
  "metrics": {
    "deltaSpikeZ": 2.34,
    "deltaSpikeZLocalQ": 0.97,
    "obi": 1.85,
    "obiLocalQ": 0.89,
    "weakProgress": 0.15,
    "weakProgressLocalQ": 0.92,
    "atrQuantile": 0.75,
    "atrLocalQ": 0.68
  }
}
```

## Калибровка

### SQL миграция

```sql
-- Добавляем колонки метрик
ALTER TABLE signals
    ADD COLUMN IF NOT EXISTS delta_spike_z   DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS obi             DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS weak_progress   DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS atr_quantile    DOUBLE PRECISION;
```

### Запуск калибровки

```bash
# Активируем все метрики в calibrate_local_thresholds.py
METRICS = ("delta_spike_z", "obi", "weak_progress", "atr_quantile")

# Запускаем калибровку
python -m local_calibration.calibrate_local_thresholds
```

## Мониторинг

### Проверка результатов

```bash
# Статистика калибровки
python python-worker/scripts/check_calibration_results.py
```

### Метрики качества

- **Количество записей**: > 100 на кластер
- **Средний размер выборки**: > 300 сигналов
- **NULL значения**: < 5%
- **Распределение confidence**: 0-100 с пиками в целевых диапазонах

## Примеры конфигурации

### Консервативная настройка

```bash
CRYPTO_SIGNAL_MIN_CONF=85
GOLDEN_PATTERN_MIN_CONFIDENCE=95
SIGNAL_METRIC_WEIGHT__DELTA_SPIKE_Z=1.2
SIGNAL_METRIC_WEIGHT__OBI=0.8
CRYPTO_SIGNAL_MIN_CONF__XAUUSD=25
```

### Агрессивная настройка

```bash
CRYPTO_SIGNAL_MIN_CONF=70
GOLDEN_PATTERN_MIN_CONFIDENCE=85
SIGNAL_METRIC_WEIGHT__DELTA_SPIKE_Z=0.8
SIGNAL_METRIC_WEIGHT__OBI=0.6
CRYPTO_SIGNAL_MIN_CONF__XAUUSD=15
```

## Диагностика

### Возможные проблемы

1. **Нет калибровки**: Проверить PG_DSN и запустить калибровку
2. **Низкий confidence**: Проверить веса метрик и пороги
3. **Не срабатывает golden**: Проверить GOLDEN_PATTERN_MIN_CONFIDENCE
4. **XAUUSD фильтруется**: Проверить CRYPTO_SIGNAL_MIN_CONF__XAUUSD

### Логи

```bash
# Логи скоринга
grep "SignalScoringEngine" logs/*.log

# Логи калибровки
python -m local_calibration.calibrate_local_thresholds 2>&1 | tee calib.log
```

## Миграция

### Существующие сигналы

Для обратной совместимости старая система confidence продолжает работать параллельно с новой.

### Переход

1. Запустить калибровку для всех метрик
2. Добавить параметры в docker-compose.yml
3. Перезапустить сервисы
4. Мониторить качество сигналов
5. Отрегулировать веса при необходимости

## Расширение

### Добавление новой метрики

1. Добавить в SignalContext
2. Добавить в SignalRow (калибратор)
3. Добавить в METRICS
4. Добавить вес в ScoringConfig
5. Обновить SQL миграцию
6. Добавить логику в engine.py

### Новый паттерн

1. Добавить SIGNAL_PATTERN_WEIGHT__PATTERN_NAME
2. Добавить SIGNAL_PATTERN_MIN_CONF__PATTERN_NAME (опционально)
3. Перезапустить сервисы
