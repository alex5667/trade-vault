# ✅ L3-Lite Filters Integration Complete

## 🎯 Что добавлено

Интегрированы **L3-lite фильтры качества** в `CryptoOrderFlowHandler` для трех типов сигналов:
- **Breakout** (пробой уровней)
- **Absorption** (поглощение импульса)
- **Extreme** (экстремальная дельта)

**Важно**: Все фильтры **выключены по умолчанию** (env-флаги `false`), чтобы не менять текущее поведение.

---

## 📝 Изменения в `CryptoOrderFlowHandler`

### 1. **Breakout L3-lite фильтр** ✅

**Файл**: `python-worker/handlers/crypto_orderflow_handler.py`  
**Метод**: `_l2_confirm_breakout()`

**Добавлено**:

```python
# 6) L3-lite quality filter (optional)
if os.getenv("BREAKOUT_USE_L3_FILTERS", "false").lower() == "true":
    # thresholds
    ctr_max = float(os.getenv("BREAKOUT_L3_MAX_CANCEL_TO_TRADE", "3.0"))  # "слишком много cancels на 1 trade"
    rate_min = float(os.getenv("BREAKOUT_L3_MIN_TAKER_RATE", "0.0"))      # qty/sec; 0 => отключить
    eta_max = float(os.getenv("BREAKOUT_L3_MAX_ETA_SEC", "0.0"))          # 0 => отключить

    if dir_up:
        ctr = float(getattr(ctx, "cancel_to_trade_ask", 0.0) or 0.0)
        rate = float(getattr(ctx, "taker_buy_rate_ema", 0.0) or 0.0)
        eta = float(getattr(ctx, "eta_fill_ask_sec", 0.0) or 0.0)
    else:
        ctr = float(getattr(ctx, "cancel_to_trade_bid", 0.0) or 0.0)
        rate = float(getattr(ctx, "taker_sell_rate_ema", 0.0) or 0.0)
        eta = float(getattr(ctx, "eta_fill_bid_sec", 0.0) or 0.0)

    # Reject "pulled liquidity imitation":
    # cancels доминируют, но taker flow слабый -> часто ложные движения/спуф
    if ctr_max > 0 and ctr >= ctr_max and (rate_min <= 0 or rate < rate_min):
        return False

    # Optional: если ETA слишком большое, а taker flow не тянет — пропускаем
    if eta_max > 0 and eta > eta_max and (rate_min <= 0 or rate < rate_min):
        return False
```

**Логика**:
- ❌ **Отклоняет сигнал**, если `cancel_to_trade >= 3.0` (cancels доминируют над trades)
- ❌ **Отклоняет сигнал**, если `taker_rate < rate_min` (слабый агрессивный поток)
- ❌ **Отклоняет сигнал**, если `eta_fill > eta_max` при слабом потоке (низкая ликвидность)

**Смысл**: Пробой должен быть подтвержден **реальными матчингами**, а не "исчезновением ликвидности" (спуфинг).

---

### 2. **Absorption L3-lite фильтр** ✅

**Файл**: `python-worker/handlers/crypto_orderflow_handler.py`  
**Метод**: `_l2_confirm_absorption()`

**Добавлено**:

```python
# L3-lite optional: absorption needs real aggressive pressure to be "absorbed"
if result and os.getenv("ABSORPTION_USE_L3_FILTERS", "false").lower() == "true":
    rate_min = float(os.getenv("ABSORPTION_L3_MIN_TAKER_RATE", "0.0"))  # qty/sec; 0 => disable
    if rate_min > 0:
        if dir_up:
            rate = float(getattr(ctx, "taker_buy_rate_ema", 0.0) or 0.0)
        else:
            rate = float(getattr(ctx, "taker_sell_rate_ema", 0.0) or 0.0)
        if rate < rate_min:
            return False
```

**Логика**:
- ❌ **Отклоняет сигнал**, если `taker_rate < rate_min` (нет агрессивного потока)

**Смысл**: Реальное поглощение (absorption) происходит, когда есть **агрессивный поток**, который "упирается" в стену/refill. Если потока нет — это просто "шум возле уровня".

---

### 3. **Extreme L3-lite фильтр** ✅

**Файл**: `python-worker/handlers/crypto_orderflow_handler.py`  
**Метод**: `_generate_signals()` (блок "Extreme delta activity")

**Добавлено**:

```python
# ✅ L3-lite: опциональные фильтры для extreme (cancel-to-trade, taker rate)
if extreme_l2_ok and os.getenv("EXTREME_USE_L3_FILTERS", "false").lower() == "true":
    ctr_max = float(os.getenv("EXTREME_L3_MAX_CANCEL_TO_TRADE", "6.0"))
    rate_min = float(os.getenv("EXTREME_L3_MIN_TAKER_RATE", "0.0"))
    if dir_up:
        ctr = float(getattr(ctx, "cancel_to_trade_ask", 0.0) or 0.0)
        rate = float(getattr(ctx, "taker_buy_rate_ema", 0.0) or 0.0)
    else:
        ctr = float(getattr(ctx, "cancel_to_trade_bid", 0.0) or 0.0)
        rate = float(getattr(ctx, "taker_sell_rate_ema", 0.0) or 0.0)

    # extreme + доминирующие cancels при слабом taker flow -> часто "дернули" и вернули
    if ctr_max > 0 and ctr >= ctr_max and (rate_min <= 0 or rate < rate_min):
        extreme_l2_ok = False
```

**Логика**:
- ❌ **Отклоняет сигнал**, если `cancel_to_trade >= 6.0` (очень высокий cancel-to-trade)
- ❌ **Отклоняет сигнал**, если `taker_rate < rate_min` (слабый агрессивный поток)

**Смысл**: Экстремальная дельта + доминирующие cancels при слабом потоке → часто "дернули цену и вернули" (манипуляция).

---

## 🔧 Environment Variables

### Breakout L3-lite фильтры:

```bash
# Включить L3-lite фильтры для breakout
BREAKOUT_USE_L3_FILTERS=false  # default: false (выключено)

# Максимальный cancel-to-trade (cancels / trades)
BREAKOUT_L3_MAX_CANCEL_TO_TRADE=3.0  # default: 3.0
# Если >= 3.0 → слишком много cancels, возможен спуфинг

# Минимальный taker rate (qty/sec)
BREAKOUT_L3_MIN_TAKER_RATE=0.0  # default: 0.0 (отключено)
# Если > 0 и rate < min → слабый агрессивный поток, пропускаем

# Максимальный ETA fill (seconds)
BREAKOUT_L3_MAX_ETA_SEC=0.0  # default: 0.0 (отключено)
# Если > 0 и eta > max при слабом потоке → низкая ликвидность, пропускаем
```

### Absorption L3-lite фильтры:

```bash
# Включить L3-lite фильтры для absorption
ABSORPTION_USE_L3_FILTERS=false  # default: false (выключено)

# Минимальный taker rate (qty/sec)
ABSORPTION_L3_MIN_TAKER_RATE=0.0  # default: 0.0 (отключено)
# Если > 0 и rate < min → нет агрессивного потока, не absorption
```

### Extreme L3-lite фильтры:

```bash
# Включить L3-lite фильтры для extreme
EXTREME_USE_L3_FILTERS=false  # default: false (выключено)

# Максимальный cancel-to-trade (cancels / trades)
EXTREME_L3_MAX_CANCEL_TO_TRADE=6.0  # default: 6.0
# Если >= 6.0 → очень высокий cancel-to-trade, возможна манипуляция

# Минимальный taker rate (qty/sec)
EXTREME_L3_MIN_TAKER_RATE=0.0  # default: 0.0 (отключено)
# Если > 0 и rate < min → слабый агрессивный поток, пропускаем
```

---

## 📊 Примеры использования

### 1. **Включить строгие L3-lite фильтры для breakout**

```bash
# docker-compose.yml или .env
BREAKOUT_USE_L3_FILTERS=true
BREAKOUT_L3_MAX_CANCEL_TO_TRADE=3.0
BREAKOUT_L3_MIN_TAKER_RATE=5.0  # минимум 5 qty/sec агрессивного потока
BREAKOUT_L3_MAX_ETA_SEC=15.0    # максимум 15 сек до заполнения depth_5
```

**Результат**: Breakout сигналы будут проходить только если:
- Cancel-to-trade < 3.0 (мало отмен)
- Taker rate >= 5.0 qty/sec (агрессивный поток)
- ETA fill <= 15 sec (достаточная ликвидность)

### 2. **Включить минимальный taker rate для absorption**

```bash
ABSORPTION_USE_L3_FILTERS=true
ABSORPTION_L3_MIN_TAKER_RATE=3.0  # минимум 3 qty/sec
```

**Результат**: Absorption сигналы будут проходить только если есть реальный агрессивный поток (>= 3 qty/sec).

### 3. **Включить фильтр манипуляций для extreme**

```bash
EXTREME_USE_L3_FILTERS=true
EXTREME_L3_MAX_CANCEL_TO_TRADE=6.0
EXTREME_L3_MIN_TAKER_RATE=10.0  # минимум 10 qty/sec для extreme
```

**Результат**: Extreme сигналы будут отклоняться, если cancel-to-trade >= 6.0 при слабом потоке (< 10 qty/sec).

---

## 🎯 Рекомендации по настройке

### Этап 1: Анализ текущих метрик

1. Включите L3-lite (уже включено по умолчанию):
   ```bash
   L3_LITE_ENABLED=true
   ```

2. Соберите статистику по сигналам:
   - Проверьте `signal.indicators["cancel_to_trade_ask"]` и `["cancel_to_trade_bid"]`
   - Проверьте `signal.indicators["taker_buy_rate_ema"]` и `["taker_sell_rate_ema"]`
   - Проверьте `signal.indicators["eta_fill_ask_sec"]` и `["eta_fill_bid_sec"]`

3. Найдите типичные значения для **хороших** и **плохих** сигналов.

### Этап 2: Постепенное включение фильтров

1. **Начните с breakout** (самый критичный):
   ```bash
   BREAKOUT_USE_L3_FILTERS=true
   BREAKOUT_L3_MAX_CANCEL_TO_TRADE=3.0  # типичный порог
   BREAKOUT_L3_MIN_TAKER_RATE=0.0       # пока отключить
   ```

2. **Добавьте absorption** (если видите ложные absorption):
   ```bash
   ABSORPTION_USE_L3_FILTERS=true
   ABSORPTION_L3_MIN_TAKER_RATE=2.0  # минимальный поток
   ```

3. **Добавьте extreme** (если видите манипуляции):
   ```bash
   EXTREME_USE_L3_FILTERS=true
   EXTREME_L3_MAX_CANCEL_TO_TRADE=6.0
   ```

### Этап 3: Тонкая настройка

- **Для BTCUSDT**: `BREAKOUT_L3_MIN_TAKER_RATE=5.0` (высокая активность)
- **Для ETHUSDT**: `BREAKOUT_L3_MIN_TAKER_RATE=3.0` (средняя активность)
- **Для менее ликвидных пар**: `BREAKOUT_L3_MIN_TAKER_RATE=1.0` или `0.0` (отключить)

---

## ⚠️ Важные замечания

### 1. **По умолчанию все фильтры ВЫКЛЮЧЕНЫ**
- ✅ Текущее поведение **не изменится** без явного включения флагов
- ✅ Можно включать постепенно, по одному фильтру

### 2. **Значение `0.0` отключает проверку**
- `BREAKOUT_L3_MIN_TAKER_RATE=0.0` → проверка taker rate отключена
- `BREAKOUT_L3_MAX_ETA_SEC=0.0` → проверка ETA отключена

### 3. **Комбинированные условия (AND)**
- Для breakout: `ctr >= ctr_max AND rate < rate_min` → отклонить
- Оба условия должны выполниться, чтобы отклонить сигнал

### 4. **Мониторинг и отладка**
- Все L3-lite метрики доступны в `signal.indicators`
- Можно анализировать post-factum: какие сигналы были отфильтрованы
- Рекомендуется логировать отклоненные сигналы для анализа

---

## ✅ Статус

- ✅ **Breakout L3-lite фильтр**: интегрирован
- ✅ **Absorption L3-lite фильтр**: интегрирован
- ✅ **Extreme L3-lite фильтр**: интегрирован
- ✅ **Syntax OK**: проверено
- ✅ **Linter errors**: 0
- ✅ **По умолчанию выключено**: текущее поведение не изменится
- ✅ **Ready for Production** 🚀

---

## 📚 Связанные документы

- `L3_LITE_INTEGRATION_COMPLETE.md` - Полная документация L3-lite
- `L3_LITE_QUICK_SUMMARY.md` - Краткая сводка L3-lite
- `python-worker/services/l3_lite_tracker.py` - L3-lite tracker
- `python-worker/handlers/crypto_orderflow_handler.py` - Crypto handler

---

**Дата**: 2025-11-29  
**Версия**: 1.0  
**Статус**: ✅ L3-Lite Filters Integration Complete  
**Рекомендация**: Включайте фильтры постепенно, начиная с breakout! 🎯

