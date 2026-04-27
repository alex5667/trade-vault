# Текущее использование GPU

## Статус GPU

### Использование на хосте
```
GPU Utilization: 1%
Memory Utilization: 1%
Memory Used: 660 MB / 12288 MB
```

### Статус в контейнере
```
GPU enabled: True
GPU memory used: 0.0 MB (в контейнере scanner-python-worker)
```

## Проблема: GPU не используется активно

### Причины низкого использования:

1. **Батч-обработка не срабатывает**:
   - Батч накапливается до 50 свечей, но свечи приходят по одной
   - Батч обрабатывается только когда заполняется или каждые 60 секунд
   - Если свечей мало, батч не заполняется

2. **Методы из metrics/features.py вызываются редко**:
   - `zscore()`, `atr_from_bars()`, `cvd_from_delta()` вызываются только при запросах
   - Нет постоянного потока данных для обработки

3. **Одиночная обработка свечей**:
   - В `_process_closed_candle()` свечи обрабатываются по одной через CPU
   - Батч-обработка происходит параллельно, но не заменяет одиночную

## Где ДОЛЖНЫ выполняться вычисления на GPU:

### 1. Order Flow обработка (`candle_of_worker.py`)
- ✅ `_process_candle_batch()` - батч-обработка через GPU (50 свечей)
- ❌ `_process_closed_candle()` - одиночная обработка через CPU (fallback)

### 2. Метрики (`metrics/features.py`)
- ✅ `zscore()` - GPU для всех массивов
- ✅ `atr_from_bars()` - GPU для всех массивов  
- ✅ `cvd_from_delta()` - GPU для всех массивов

### 3. GPU Compute Service методы
- ✅ `compute_delta_batch()` - Delta вычисления
- ✅ `compute_cvd()` - Cumulative Volume Delta
- ✅ `compute_z_scores()` - z-score с rolling window
- ✅ `compute_atr_batch()` - ATR для батча
- ✅ `compute_body_atr_ratio()` - bodyATR
- ✅ `compute_delta_ratio()` - deltaRatio

## Рекомендации для увеличения использования GPU:

1. **Уменьшить размер батча** для более частой обработки
2. **Обрабатывать одиночные свечи через GPU** (даже если батч не заполнен)
3. **Добавить логирование** вызовов GPU методов для мониторинга
4. **Проверить частоту вызовов** методов из `metrics/features.py`

