# Confidence Calculation Reference

Эта папка содержит все файлы, участвующие в расчёте confidence для сигналов crypto orderflow.

## Структура расчёта confidence

```
Tick → DeltaSpikeDetector → DN-GATE → ConfidenceScorer → Signal
```

## Основные компоненты

### 1. Delta Detection & Z-Score Calculation

**Файл**: `crypto_orderflow_detectors.py`
- **Класс**: `DeltaSpikeDetector`
- **Функция**: Вычисляет Z-score для delta (агрессивные покупки/продажи)
- **Окно**: 120 тиков (по умолчанию)
- **Формула**: `z = (delta - mean) / std_dev`
- **Порог**: `delta_z_threshold` (3.0 по умолчанию)

### 2. DN-GATE (Delta Notional Gate)

**Файл**: `tick_processor.py`
- **Функция**: Фильтрует сигналы по размеру delta в USD
- **Калибратор**: `delta_notional_calibrator.py` (автоподстройка порогов)
- **Пороги**: 
  - BTCUSDT: tier0=$50k, tier1=$80k, tier2=$150k
  - ETHUSDT: tier0=$25k, tier1=$40k, tier2=$75k

### 3. Confidence Scoring

**Файл**: `signal_confidence.py`
- **Класс**: `ConfidenceScorer`
- **Входы**: Z-score, OBI, spread, micro-level data
- **Выход**: Confidence score [0.05, 0.98]
- **Порог**: `main_z_thr` = 2.5
- **Минимум**: 0.05 (confidence floor)

### 4. Strategy & Pipeline

**Файл**: `orderflow_strategy.py`
- **Класс**: `OrderFlowStrategy`
- **Функция**: Координирует детекторы и публикацию сигналов
- **Инициализация**: `ConfidenceScorer(main_z_thr=2.5)`

## Конфигурация

### Docker Compose

**Файл**: `docker-compose-crypto-orderflow.yml`

Ключевые ENV переменные:
```yaml
# Z-score thresholds
- DELTA_Z_THRESHOLD=3.0
- BTC_DELTA_Z_THRESHOLD=2.7
- ETH_DELTA_Z_THRESHOLD=2.5

# DN-GATE thresholds
- BTC_DN_TIER0_USD=50000
- BTC_DN_TIER1_USD=80000
- BTC_DN_TIER2_USD=150000
- ETH_DN_TIER0_USD=25000
- ETH_DN_TIER1_USD=40000
- ETH_DN_TIER2_USD=75000

# Calibrator settings
- DN_CALIB_ENABLE=1
- DN_CALIB_MIN_SAMPLES=300

# Debug
- CRYPTO_OF_DEBUG_DELTAS=1
```

### Instrument Config

**Файл**: `instrument_config.py`
- **Функция**: `get_default_delta_tiers(symbol)` - дефолтные пороги DN-GATE
- **Функция**: `OrderFlowConfig.from_env()` - загрузка конфига из ENV
- **Пресеты**: BTCUSDT, ETHUSDT, SOLUSDT, etc.

### Configuration Loader

**Файл**: `configuration.py`
- **Класс**: `OrderFlowConfigLoader`
- **Функция**: Загружает конфиг из Redis + ENV overrides
- **Метод**: `build_symbol_config(symbol)` - финальный конфиг для символа

## Вспомогательные компоненты

### Quantile Estimator

**Файл**: `quantile_p2.py`
- **Класс**: `P2Quantile`
- **Функция**: Онлайн-оценка квантилей для калибратора
- **Использование**: DN-GATE calibrator (p50/p80/p95)

### Runtime State

**Файл**: `orderflow_runtime.py`
- **Класс**: `SymbolRuntime`
- **Функция**: Хранит состояние для каждого символа
- **Содержит**: delta_detector, dn_calib, dynamic_cfg, etc.

### PnL Math

**Файл**: `pnl_math.py`
- **Функция**: `get_symbol_info(symbol)` - tick_size, lot_step, etc.
- **Использование**: Конвертация в USD, расчёт размеров позиций

## Тесты

**Файл**: `test_zscore_calculation.py`
- **Тесты**: 4 unit tests для валидации Z-score расчёта
- **Результаты**: Все тесты проходят, обнаружен bias 10-20%

## Поток данных

```
1. Tick приходит в OrderFlowStrategy
   ↓
2. DeltaSpikeDetector.push(tick)
   → Вычисляет delta и Z-score
   → Возвращает delta_event = {"delta": X, "z": Z, ...}
   ↓
3. TickProcessor.process_tick(runtime, tick)
   → Получает delta_event
   → Проверяет DN-GATE (delta_usd vs tiers)
   → Если pass: продолжает
   ↓
4. ConfidenceScorer.score(kind, side, ctx)
   → Получает Z-score из ctx
   → Вычисляет confidence [0.05, 0.98]
   → Возвращает (confidence, parts)
   ↓
5. SignalPipeline.publish(signal)
   → Если confidence >= 0.70: публикует
   → Иначе: NOTIFY-SUPPRESS
```

## Ключевые метрики

```bash
# Z-score distribution
curl -s http://localhost:8000/metrics | grep "delta_z"

# DN-GATE decisions
curl -s http://localhost:8000/metrics | grep "dn_gate"

# Confidence distribution
curl -s http://localhost:8000/metrics | grep "signal_confidence"

# Calibrator status
curl -s http://localhost:8000/metrics | grep "dn_calib_n"
```

## Известные проблемы

### 1. Z-Score Bias (10-20%)

**Проблема**: Текущее значение delta включено в расчёт mean/std  
**Эффект**: Z-scores занижены на 10-20%  
**Решение**: Исключить текущее значение из статистики  
**Статус**: Задокументировано в `zscore_analysis.md`

### 2. Low Confidence (0.05)

**Проблема**: Большинство сигналов имеют confidence=0.05  
**Причины**:
1. ✅ DN-GATE fixed (пороги понижены)
2. ⚠️ Слабые Z-scores в текущем рынке
3. ⚠️ 10-20% bias в Z-score расчёте

**Решение**: Ждать волатильности или исправить bias

## Рекомендации

1. **Исправить Z-score bias** (Option 1 в zscore_analysis.md)
2. **Мониторить DN-GATE calibrator** (когда n >= 300)
3. **Настроить пороги** под текущую волатильность
4. **Включить DEBUG_DELTAS=1** для анализа

## Дополнительная документация

- `zscore_analysis.md` - Полный анализ Z-score расчёта
- `walkthrough.md` - DN-GATE threshold adjustment walkthrough
- `implementation_plan.md` - План понижения DN-GATE порогов
