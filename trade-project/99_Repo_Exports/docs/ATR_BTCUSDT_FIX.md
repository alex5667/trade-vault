# 🔧 Исправление ATR=1.0 для BTCUSDT

## 🐛 Проблема

Для BTCUSDT всегда показывается `ATR=1.00`, что приводит к:
- ❌ Неправильным уровням SL/TP (слишком узкие)
- ❌ Неправильному расчету lot size
- ❌ Неправильной оценке риска

### Пример из Telegram:

```
🚨 🟢 BTCUSDT LONG @ 90661.30
🛑 SL 90660.70 | TP1 90662.08 (0.78 ATR)
📊 ATR=1.00 | Conf=85%
```

**Реальный ATR для BTCUSDT на 1m**: ~20-40 (не 1.0!)

---

## 🔍 Причина

### 1. Hardcoded Fallback в `crypto_orderflow_service.py`

**Файл**: `python-worker/services/crypto_orderflow_service.py`

```python
# Строка 74
DEFAULT_CONFIG: Dict[str, Any] = {
    ...
    "fallback_atr": 1.0,  # ← ПРОБЛЕМА: hardcoded 1.0!
    ...
}

# Строка 724
if atr <= 0:
    atr = _safe_float(cfg.get("fallback_atr"), 1.0)  # ← Fallback на 1.0
    atr_source = "fallback"
```

### 2. Отсутствие ATR в Redis

```bash
$ docker exec scanner-redis redis-cli KEYS "ATR:BTCUSDT:*"
(empty array)  # ← Нет данных!
```

**Причина**: ATR-трекер не запущен или не пишет данные для BTCUSDT.

### 3. Логика в `crypto_orderflow_service._calculate_levels()`

```python
atr_raw = _safe_float(indicators.get("atr"))  # ← 0.0 (нет в indicators)
atr = atr_raw

if atr <= 0:
    cache_atr = self.atr_cache.get(runtime.symbol, atr_tf)  # ← None (нет в Redis)
    atr = cache_atr if cache_atr is not None else 0.0

if atr <= 0:
    atr = _safe_float(cfg.get("fallback_atr"), 1.0)  # ← Fallback 1.0!
```

**Результат**: Всегда используется `fallback_atr=1.0`.

---

## ✅ Решения

### **Решение 1: Запустить ATR-трекер для крипты** (Рекомендуется)

#### 1.1. Проверить, есть ли ATR-трекер в `docker-compose.yml`:

```bash
grep -i "atr" docker-compose.yml
```

Если нет сервиса `atr-from-candles` или `atr-tracker`, нужно добавить.

#### 1.2. Добавить ATR-трекер в `docker-compose.yml`:

```yaml
  atr-tracker-crypto:
    build:
      context: .
      dockerfile: python-worker/Dockerfile
    container_name: scanner-atr-tracker-crypto
    environment:
      - REDIS_URL=redis://scanner-redis-worker-1:6379/0
      - ATR_SYMBOLS=BTCUSDT,ETHUSDT
      - ATR_TIMEFRAMES=1m,5m,15m
      - ATR_PERIOD=14
      - PYTHONPATH=/app
    depends_on:
      - scanner-redis-worker-1
    command: python -m services.atr_from_candles
    restart: unless-stopped
```

#### 1.3. Перезапустить:

```bash
docker-compose up -d atr-tracker-crypto
```

#### 1.4. Проверить, что ATR появился в Redis:

```bash
docker exec scanner-redis redis-cli HGETALL "ATR:BTCUSDT:M1"
# Ожидаем:
# 1) "atr"
# 2) "27.5"  (или другое реальное значение)
# 3) "lastCloseTime"
# 4) "1732896123456"
```

---

### **Решение 2: Исправить fallback_atr для каждого символа** (Временное)

#### 2.1. Обновить `DEFAULT_CONFIG` в `crypto_orderflow_service.py`:

```python
DEFAULT_CONFIG: Dict[str, Any] = {
    ...
    "fallback_atr": 30.0,  # ← Изменить на реалистичное значение для BTCUSDT
    ...
}
```

**Проблема**: Это глобальное значение для всех символов. Для ETHUSDT нужно другое (~3-5).

#### 2.2. Добавить per-symbol fallback:

```python
# В _calculate_levels()
if atr <= 0:
    # Per-symbol fallback
    symbol_fallbacks = {
        "BTCUSDT": 30.0,  # Типичный ATR для BTCUSDT 1m
        "ETHUSDT": 4.0,   # Типичный ATR для ETHUSDT 1m
    }
    atr = symbol_fallbacks.get(runtime.symbol, 1.0)
    atr_source = "fallback-symbol"
```

---

### **Решение 3: Использовать _estimate_atr() из BaseOrderFlowHandler** (Лучше чем hardcode)

#### 3.1. В `crypto_orderflow_service.py`, заменить:

```python
if atr <= 0:
    atr = _safe_float(cfg.get("fallback_atr"), 1.0)
    atr_source = "fallback"
```

На:

```python
if atr <= 0:
    # Используем 0.03% от цены (как в BaseOrderFlowHandler._estimate_atr)
    atr = entry * 0.0003
    atr_source = "estimated"
```

**Результат для BTCUSDT ~90000**: `90000 * 0.0003 = 27` ✅

---

## 🎯 Рекомендация

**Комбинированный подход**:

1. **Краткосрочно** (сейчас):
   - Исправить `fallback_atr` на per-symbol значения
   - Или использовать `entry * 0.0003`

2. **Долгосрочно** (в течение дня):
   - Запустить ATR-трекер для BTCUSDT/ETHUSDT
   - Убедиться, что данные пишутся в Redis

---

## 📝 Патч (Краткосрочное решение)

### Файл: `python-worker/services/crypto_orderflow_service.py`

**Найти строку 723-725**:

```python
if atr <= 0:
    atr = _safe_float(cfg.get("fallback_atr"), 1.0)
    atr_source = "fallback"
```

**Заменить на**:

```python
if atr <= 0:
    # Per-symbol fallback (реалистичные значения для 1m)
    symbol_fallbacks = {
        "BTCUSDT": 30.0,  # Типичный ATR для BTCUSDT 1m: 20-40
        "ETHUSDT": 4.0,   # Типичный ATR для ETHUSDT 1m: 3-6
        "BNBUSDT": 0.5,   # Типичный ATR для BNBUSDT 1m: 0.3-0.8
        "SOLUSDT": 0.3,   # Типичный ATR для SOLUSDT 1m: 0.2-0.5
    }
    atr = symbol_fallbacks.get(runtime.symbol, entry * 0.0003)
    atr_source = "fallback-symbol"
```

---

## ✅ Проверка после исправления

### 1. Перезапустить сервис:

```bash
docker-compose up -d --build crypto-orderflow-service
```

### 2. Проверить логи:

```bash
docker logs -f scanner-crypto-orderflow | grep "atr="
```

**Ожидаем**:

```
🎯 rocket_v1 detected in _calculate_levels: trail_profile=rocket_v1, entry=90661.30, atr=30.00
```

### 3. Проверить сигнал в Telegram:

```
🚨 🟢 BTCUSDT LONG @ 90661.30
🛑 SL 90643.30 | TP1 90684.70 (0.78 ATR)
📊 ATR=30.00 | Conf=85%
```

**SL**: `90661.30 - (30 * 0.6) = 90643.30` ✅  
**TP1**: `90661.30 + (30 * 0.78) = 90684.70` ✅

---

## 📊 Сравнение

| Параметр | До исправления | После исправления |
|----------|---------------|-------------------|
| ATR | 1.0 | 30.0 |
| SL distance | 0.6 | 18.0 |
| TP1 distance | 0.78 | 23.4 |
| Risk/Reward | Нереалистичный | Реалистичный |

---

## 🔗 Связанные файлы

- `python-worker/services/crypto_orderflow_service.py` - Основной сервис
- `python-worker/handlers/base_orderflow_handler.py` - `_estimate_atr()` метод
- `docker-compose.yml` - Конфигурация ATR-трекера
- `python-worker/services/atr_from_candles.py` - ATR-трекер

---

**Дата**: 2025-11-29  
**Приоритет**: 🔥 CRITICAL  
**Статус**: ⏳ Требует исправления

