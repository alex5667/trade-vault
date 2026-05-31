# 📊 Резюме: Проблема ATR=1.0 для BTCUSDT

## 🐛 Проблема

**Симптом**: Для BTCUSDT всегда показывается `ATR=1.00` в сигналах Telegram.

```
🚨 🟢 BTCUSDT LONG @ 90661.30
🛑 SL 90660.70 | TP1 90662.08 (0.78 ATR)
📊 ATR=1.00 | Conf=85%
```

**Последствия**:
- ❌ Слишком узкие SL/TP уровни (0.6/0.78 пунктов вместо 18/23)
- ❌ Неправильный расчет lot size
- ❌ Высокий риск ложных срабатываний SL

---

## 🔍 Причина

### 1. Hardcoded Fallback

**Файл**: `python-worker/services/crypto_orderflow_service.py` (строка 74, 724)

```python
DEFAULT_CONFIG = {
    "fallback_atr": 1.0,  # ← Hardcoded!
}

# В _calculate_levels():
if atr <= 0:
    atr = _safe_float(cfg.get("fallback_atr"), 1.0)  # ← Fallback 1.0
```

### 2. Отсутствие ATR в Redis

```bash
$ docker exec scanner-redis redis-cli KEYS "ATR:BTCUSDT:*"
(empty array)  # ← Нет данных!
```

### 3. ATR-worker Unhealthy

```bash
$ docker ps | grep atr
scanner-atr-worker   Up 40 minutes (unhealthy)
```

**Ошибка**: `NOGROUP No such key 'candles:data' or consumer group 'atr-worker-group'`

---

## ✅ Решение (Выполнено)

### 1. Исправлен Fallback ✅

**Файл**: `python-worker/services/crypto_orderflow_service.py`

**Было**:
```python
if atr <= 0:
    atr = _safe_float(cfg.get("fallback_atr"), 1.0)
    atr_source = "fallback"
```

**Стало**:
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

## ⏳ Требуется Дополнительно

### 1. Исправить дубликаты в docker-compose.yml ⚠️

**Проблема**: Docker Compose не может пересобрать образ из-за дубликатов environment variables.

```bash
services.crypto-orderflow-service.environment contains non-unique items:
- BREAKOUT_REQUIRE_OBI=true (дубликат на строках 1254 и 1411)
```

**Решение**: Удалить дубликаты вручную.

**Команда для поиска**:
```bash
grep -n "BREAKOUT_REQUIRE_OBI" docker-compose.yml
# Найдено на строках: 1254, 1295, 1296, 1411, 1412
# Нужно оставить только один раз (например, строка 1411)
```

### 2. Пересобрать и перезапустить crypto-orderflow-service

После исправления дубликатов:

```bash
cd /home/alex/front/trade/scanner_infra
docker-compose up -d --build crypto-orderflow-service
```

### 3. Исправить ATR-worker (долгосрочно)

**Создать consumer group**:
```bash
docker exec scanner-redis-worker-1 redis-cli XGROUP CREATE candles:data atr-worker-group 0 MKSTREAM
```

**Перезапустить**:
```bash
docker-compose restart atr-worker
```

**Проверить health**:
```bash
docker ps | grep atr
# Ожидаем: Up X minutes (healthy)
```

---

## 📈 Ожидаемый Результат

### До исправления:
```
ATR=1.00
SL distance = 0.6
TP1 distance = 0.78
```

### После исправления (fallback):
```
ATR=30.00
SL distance = 18.0  (30 * 0.6)
TP1 distance = 23.4 (30 * 0.78)
```

### После исправления ATR-worker (долгосрочно):
```
ATR=27.5  (реальное значение из Redis)
SL distance = 16.5
TP1 distance = 21.5
```

---

## 🎯 Следующие Шаги

1. **Сейчас** (вручную):
   - Исправить дубликаты в `docker-compose.yml`
   - Пересобрать `crypto-orderflow-service`

2. **В течение дня**:
   - Исправить ATR-worker (создать consumer group)
   - Проверить, что ATR пишется в Redis

3. **Мониторинг**:
   - Проверить следующий сигнал в Telegram
   - Убедиться, что ATR=30.0 (fallback) или ATR~27.5 (Redis)

---

## 📝 Файлы

- ✅ `python-worker/services/crypto_orderflow_service.py` - Исправлен fallback
- ⏳ `docker-compose.yml` - Требуется исправить дубликаты
- 📄 `ATR_BTCUSDT_FIX.md` - Детальное описание проблемы
- 📄 `ATR_FIX_APPLIED.md` - Статус исправления

---

**Дата**: 2025-11-29  
**Статус**: ✅ Fallback исправлен, ⏳ Требуется пересборка контейнера  
**Приоритет**: 🔥 CRITICAL

