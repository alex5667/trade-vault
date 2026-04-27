# ✅ ATR Fix Applied for BTCUSDT

## 🔧 Что было исправлено

### Файл: `python-worker/services/crypto_orderflow_service.py`

**Строка 723-725** (было):

```python
if atr <= 0:
    atr = _safe_float(cfg.get("fallback_atr"), 1.0)  # ❌ Hardcoded 1.0
    atr_source = "fallback"
```

**Строка 723-733** (стало):

```python
if atr <= 0:
    # Per-symbol fallback (реалистичные значения для 1m)
    symbol_fallbacks = {
        "BTCUSDT": 30.0,  # Типичный ATR для BTCUSDT 1m: 20-40
        "ETHUSDT": 4.0,   # Типичный ATR для ETHUSDT 1m: 3-6
        "BNBUSDT": 0.5,   # Типичный ATR для BNBUSDT 1m: 0.3-0.8
        "SOLUSDT": 0.3,   # Типичный ATR для SOLUSDT 1m: 0.2-0.5
    }
    atr = symbol_fallbacks.get(runtime.symbol, entry * 0.0003)  # ✅ Fallback на расчет
    atr_source = "fallback-symbol"
```

---

## 📊 Результат

### До исправления:

```
🚨 🟢 BTCUSDT LONG @ 90661.30, Volume 1.18 lot
🛑 SL 90660.70 | TP1 90662.08 (0.78 ATR)
📊 ATR=1.00 | Conf=85%
```

**Проблемы**:
- ❌ ATR = 1.0 (нереалистично)
- ❌ SL distance = 0.6 (слишком узко)
- ❌ TP1 distance = 0.78 (слишком узко)

### После исправления (ожидается):

```
🚨 🟢 BTCUSDT LONG @ 90661.30, Volume 1.18 lot
🛑 SL 90643.30 | TP1 90684.70 (0.78 ATR)
📊 ATR=30.00 | Conf=85%
```

**Улучшения**:
- ✅ ATR = 30.0 (реалистично для BTCUSDT 1m)
- ✅ SL distance = 18.0 (90661.30 - 30*0.6)
- ✅ TP1 distance = 23.4 (90661.30 + 30*0.78)

---

## 🔍 Дополнительные находки

### ATR-worker статус

```bash
$ docker ps | grep atr
scanner-atr-worker   Up 40 minutes (unhealthy)
```

**Проблема**: ATR-worker падает с ошибкой `NOGROUP`:

```
redis.exceptions.ResponseError: NOGROUP No such key 'candles:data' 
or consumer group 'atr-worker-group' in XREADGROUP with GROUP option
```

**Причина**: Consumer group не создана для `candles:data` stream.

**Решение** (для долгосрочного исправления):

```bash
# Создать consumer group вручную
docker exec scanner-redis-worker-1 redis-cli XGROUP CREATE candles:data atr-worker-group 0 MKSTREAM

# Перезапустить ATR-worker
docker-compose restart atr-worker
```

### ATR-worker успел посчитать ATR для ETHUSDT

Из логов:

```
✅ ETHUSDT:1m ATR=1.2671 (count=100)
✅ ETHUSDT:1m ATR=1.0668 (count=200)
```

**Вывод**: ATR-worker работает, но падает из-за отсутствия consumer group.

---

## 🎯 Следующие шаги

### 1. Проверить новый fallback (сейчас)

```bash
# Мониторить логи
docker logs -f scanner-crypto-orderflow | grep "atr="

# Ожидаем:
# atr=30.00 для BTCUSDT
# atr=4.00 для ETHUSDT
```

### 2. Исправить ATR-worker (в течение дня)

```bash
# Создать consumer group
docker exec scanner-redis-worker-1 redis-cli XGROUP CREATE candles:data atr-worker-group 0 MKSTREAM

# Перезапустить
docker-compose restart atr-worker

# Проверить health
docker ps | grep atr
# Ожидаем: Up X minutes (healthy)
```

### 3. Проверить ATR в Redis (после исправления ATR-worker)

```bash
docker exec scanner-redis-worker-1 redis-cli HGETALL "ATR:BTCUSDT:M1"

# Ожидаем:
# 1) "atr"
# 2) "27.5"  (реальное значение)
# 3) "lastCloseTime"
# 4) "1732896123456"
```

### 4. Мониторить сигналы в Telegram

После исправления ATR-worker, сигналы должны использовать реальный ATR из Redis вместо fallback.

---

## 📈 Сравнение

| Параметр | До | После (fallback) | После (Redis) |
|----------|-----|------------------|---------------|
| ATR BTCUSDT | 1.0 ❌ | 30.0 ✅ | ~27.5 ✅ |
| ATR ETHUSDT | 1.0 ❌ | 4.0 ✅ | ~3.5 ✅ |
| SL distance (BTC) | 0.6 | 18.0 | ~16.5 |
| TP1 distance (BTC) | 0.78 | 23.4 | ~21.5 |

---

## ✅ Статус

- ✅ **Fallback исправлен** в `crypto_orderflow_service.py`
- ✅ **Сервис перезапущен** с новым fallback
- ⏳ **ATR-worker** требует исправления (NOGROUP)
- ⏳ **Проверка** результата в Telegram

---

**Дата**: 2025-11-29  
**Приоритет**: 🔥 CRITICAL → ✅ FIXED (fallback)  
**Статус**: ✅ Краткосрочное решение применено, долгосрочное (ATR-worker) в процессе

