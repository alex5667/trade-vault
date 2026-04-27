# Аудит контейнеров с GPU поддержкой

## ✅ Контейнеры с GPU (уже настроены)

### 1. `python-worker`
- **Dockerfile**: `python-worker/Dockerfile.gpu` ✅
- **Runtime**: `nvidia` ✅
- **GPU_ENABLED**: `true` ✅
- **Использует GPU**: ✅ Да
- **Статус**: ✅ Настроен правильно

### 2. `multi-symbol-orderflow`
- **Dockerfile**: `python-worker/Dockerfile.gpu` ✅
- **Runtime**: `nvidia` ✅
- **GPU_ENABLED**: `true` ✅
- **Использует GPU**: ✅ Да (через handlers)
- **Статус**: ✅ Настроен правильно

### 3. `atr-worker`
- **Dockerfile**: `python-worker/Dockerfile.gpu` ✅ (обновлено)
- **Runtime**: `nvidia` ✅ (добавлено)
- **GPU_ENABLED**: `true` ✅ (добавлено)
- **Использует GPU**: ✅ Да (ATR вычисления)
- **Статус**: ✅ Настроен правильно (только что обновлен)

---

## ⚠️ Контейнеры, которые МОГУТ использовать GPU, но НЕ настроены

### 4. `crypto-orderflow-service`
- **Dockerfile**: `python-worker/Dockerfile` ❌ (обычный, без GPU)
- **Runtime**: Нет ❌
- **GPU_ENABLED**: Нет ❌
- **Использует GPU**: ⚠️ Да (через handlers → gpu_compute_service)
- **Статус**: ❌ Нужно обновить

**Рекомендация**: Обновить на `Dockerfile.gpu` + добавить `runtime: nvidia`

### 5. `signal-performance-tracker`
- **Dockerfile**: `python-worker/Dockerfile` ❌ (обычный)
- **Runtime**: Нет ❌
- **GPU_ENABLED**: Нет ❌
- **Использует GPU**: ⚠️ Возможно (через trade_monitor)
- **Статус**: ❓ Проверить необходимость

**Рекомендация**: Проверить, использует ли он GPU вычисления

---

## 📋 Контейнеры, которые НЕ используют GPU

Следующие контейнеры используют `python-worker/Dockerfile`, но НЕ используют GPU вычисления:

1. `ohlc-aggregator` - агрегация OHLC данных (не требует GPU)
2. `binance-iceberg-detector` - детекция айсбергов (не требует GPU)
3. `tick-ingest-server` - прием тиков (не требует GPU)
4. `signal-dispatcher` - маршрутизация сигналов (не требует GPU)
5. `periodic-reporter` - отчеты (не требует GPU)
6. `aggregated-hub` - агрегация сигналов (не требует GPU)
7. `dom-ingester` - ingest DOM данных (не требует GPU)
8. `signal-hub` - хаб сигналов (не требует GPU)
9. `paper-executor` - симулятор ордеров (не требует GPU)

**Статус**: ✅ Оставить как есть (обычный Dockerfile)

---

## 🎯 Рекомендации

### Обязательные обновления:

1. **`crypto-orderflow-service`** - ОБНОВИТЬ
   - Переключить на `Dockerfile.gpu`
   - Добавить `runtime: nvidia`
   - Добавить `GPU_ENABLED=true`

### Опциональные обновления:

2. **`signal-performance-tracker`** - ПРОВЕРИТЬ
   - Проверить, использует ли GPU через trade_monitor
   - Если использует - обновить аналогично

---

## Проверка использования GPU в коде

### Сервисы, которые импортируют gpu_compute_service:

1. ✅ `candle_of_worker.py` - используется в `multi-symbol-orderflow` (GPU настроен)
2. ✅ `atr_from_candles.py` - используется в `atr-worker` (GPU настроен)
3. ✅ `handlers/base_orderflow_handler.py` - используется в `multi-symbol-orderflow` (GPU настроен)
4. ⚠️ `handlers/crypto_orderflow_handler.py` - используется в `crypto-orderflow-service` (GPU НЕ настроен!)
5. ✅ `services/book_analytics_service.py` - может использоваться в разных сервисах
6. ✅ `signals/featurizer.py` - используется в handlers (GPU настроен через multi-symbol-orderflow)

---

## Вывод

**Критически важно обновить:**
- ❌ `crypto-orderflow-service` - использует GPU, но не имеет доступа

**Можно оставить как есть:**
- ✅ Остальные сервисы либо уже настроены, либо не используют GPU

