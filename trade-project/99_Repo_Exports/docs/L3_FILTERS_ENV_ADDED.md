# ✅ L3-Lite Filters Added to docker-compose.yml

## 📝 Что добавлено

Добавлены environment variables для L3-lite фильтров в `docker-compose.yml` для сервиса `multi-symbol-orderflow`:

### 1. **Breakout L3-lite фильтры**

```yaml
# ═══ BREAKOUT L3-LITE FILTERS ═══
- BREAKOUT_USE_L3_FILTERS=true
- BREAKOUT_L3_MAX_CANCEL_TO_TRADE=3.0
- BREAKOUT_L3_MIN_TAKER_RATE=0.0
- BREAKOUT_L3_MAX_ETA_SEC=0.0
```

**Назначение**:
- `BREAKOUT_USE_L3_FILTERS=true` - включить L3-lite фильтры для breakout
- `BREAKOUT_L3_MAX_CANCEL_TO_TRADE=3.0` - отклонить если cancel-to-trade >= 3.0
- `BREAKOUT_L3_MIN_TAKER_RATE=0.0` - минимальный taker rate (0 = отключено)
- `BREAKOUT_L3_MAX_ETA_SEC=0.0` - максимальный ETA fill (0 = отключено)

### 2. **Absorption L3-lite фильтры**

```yaml
# ═══ ABSORPTION L3-LITE FILTERS ═══
- ABSORPTION_USE_L3_FILTERS=true
- ABSORPTION_L3_MIN_TAKER_RATE=0.0
```

**Назначение**:
- `ABSORPTION_USE_L3_FILTERS=true` - включить L3-lite фильтры для absorption
- `ABSORPTION_L3_MIN_TAKER_RATE=0.0` - минимальный taker rate (0 = отключено)

### 3. **Extreme L3-lite фильтры**

```yaml
# ═══ EXTREME L3-LITE FILTERS ═══
- EXTREME_USE_L3_FILTERS=true
- EXTREME_L3_MAX_CANCEL_TO_TRADE=6.0
- EXTREME_L3_MIN_TAKER_RATE=0.0
```

**Назначение**:
- `EXTREME_USE_L3_FILTERS=true` - включить L3-lite фильтры для extreme
- `EXTREME_L3_MAX_CANCEL_TO_TRADE=6.0` - отклонить если cancel-to-trade >= 6.0
- `EXTREME_L3_MIN_TAKER_RATE=0.0` - минимальный taker rate (0 = отключено)

---

## 🔧 Исправлены дубликаты

**Проблема**: В `multi-symbol-orderflow` был дубликат `BREAKOUT_REQUIRE_OBI=true`

**Решение**: Удален дубликат на строке 1295, оставлен только на строке 1254

---

## 🎯 Как использовать

### Вариант 1: Использовать текущие значения (рекомендуется для начала)

```bash
# Перезапустить сервис с новыми env
docker-compose up -d multi-symbol-orderflow
```

**Результат**:
- ✅ Breakout фильтры включены (`cancel_to_trade < 3.0`)
- ✅ Absorption фильтры включены (но `rate_min=0` → не фильтрует)
- ✅ Extreme фильтры включены (`cancel_to_trade < 6.0`)

### Вариант 2: Настроить под свои нужды

Отредактируйте `docker-compose.yml`:

```yaml
# Для строгой фильтрации breakout
- BREAKOUT_L3_MAX_CANCEL_TO_TRADE=2.0  # более строго
- BREAKOUT_L3_MIN_TAKER_RATE=5.0       # требовать >= 5 qty/sec

# Для фильтрации absorption по потоку
- ABSORPTION_L3_MIN_TAKER_RATE=3.0     # требовать >= 3 qty/sec

# Для строгой фильтрации extreme
- EXTREME_L3_MAX_CANCEL_TO_TRADE=4.0   # более строго
- EXTREME_L3_MIN_TAKER_RATE=10.0       # требовать >= 10 qty/sec
```

Затем:

```bash
docker-compose up -d multi-symbol-orderflow
```

### Вариант 3: Отключить фильтры

```yaml
- BREAKOUT_USE_L3_FILTERS=false
- ABSORPTION_USE_L3_FILTERS=false
- EXTREME_USE_L3_FILTERS=false
```

---

## 📊 Мониторинг

### Проверить что фильтры работают:

```bash
# Смотреть логи
docker logs -f scanner-multi-symbol-orderflow | grep "L3"

# Проверить метрики в сигналах
docker exec scanner-redis redis-cli XREAD COUNT 1 STREAMS signals:orderflow:BTCUSDT 0-0
```

### Проверить метрики в signal.indicators:

```json
{
  "taker_buy_rate_ema": 15.234567,
  "taker_sell_rate_ema": 12.345678,
  "cancel_to_trade_ask": 0.280123,
  "cancel_to_trade_bid": 0.154321,
  "eta_fill_ask_sec": 8.123,
  "eta_fill_bid_sec": 10.456
}
```

---

## ⚠️ Важно

### 1. **Значение `0.0` отключает проверку**
- `BREAKOUT_L3_MIN_TAKER_RATE=0.0` → проверка taker rate отключена
- `BREAKOUT_L3_MAX_ETA_SEC=0.0` → проверка ETA отключена

### 2. **Комбинированные условия (AND)**
- Для breakout: `ctr >= ctr_max AND rate < rate_min` → отклонить
- Оба условия должны выполниться

### 3. **Постепенное включение**
1. Начните с текущих значений
2. Мониторьте логи и audit
3. Настройте пороги под свои данные

---

## ✅ Статус

- ✅ **Breakout L3-lite фильтры**: добавлены в docker-compose.yml
- ✅ **Absorption L3-lite фильтры**: добавлены в docker-compose.yml
- ✅ **Extreme L3-lite фильтры**: добавлены в docker-compose.yml
- ✅ **Дубликаты исправлены**: `BREAKOUT_REQUIRE_OBI` удален
- ⏳ **Требуется перезапуск**: `docker-compose up -d multi-symbol-orderflow`

---

## 📚 Связанные документы

- `L3_LITE_FILTERS_INTEGRATION.md` - Полная документация фильтров
- `L3_LITE_INTEGRATION_COMPLETE.md` - Полная документация L3-lite
- `docker-compose.yml` - Конфигурация

---

**Дата**: 2025-11-29  
**Сервис**: `multi-symbol-orderflow`  
**Статус**: ✅ ENV добавлены, требуется перезапуск  
**Команда**: `docker-compose up -d multi-symbol-orderflow`

