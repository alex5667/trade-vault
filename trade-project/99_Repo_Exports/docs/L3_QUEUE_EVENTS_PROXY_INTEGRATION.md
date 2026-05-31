# ✅ L3-lite Queue-Events Proxy Integration Complete

## 📝 Что добавлено

Интегрирован **L3-lite Queue-Events Proxy** — упрощенная версия L3-lite для дополнительных метрик `taker_buy_qty_bucket`, `pull_*_qty_proxy` и расчета `cancel_to_trade` и `eta_fill`.

---

## 🆕 Новые файлы

### 1. **`services/l3_queue_events_proxy.py`**

Новый модуль для L3-lite queue-events proxy:

```python
@dataclass
class L3BucketStats:
    buy_qty: float = 0.0
    sell_qty: float = 0.0
    buy_rate_ema: float = 0.0
    sell_rate_ema: float = 0.0

class L3QueueEventsProxy:
    """
    L3-lite:
      - считаем taker buy/sell qty внутри delta-bucket
      - на закрытии bucket -> считаем qty/sec и обновляем EMA
    """
```

**Методы**:
- `on_trade(side: int, qty: float)` - учет сделки (side: +1 buy, -1 sell)
- `on_bucket_close() -> L3BucketStats` - расчет EMA rates и сброс аккумуляторов

---

## 🔧 Изменения в `base_orderflow_handler.py`

### 1. **SignalContext: добавлены 4 новых поля**

```python
# L3-lite (queue-events proxy) - дополнительные метрики
taker_buy_qty_bucket: float = 0.0
taker_sell_qty_bucket: float = 0.0
pull_ask_qty_proxy: float = 0.0
pull_bid_qty_proxy: float = 0.0
```

### 2. **Импорт**

```python
from services.l3_queue_events_proxy import L3QueueEventsProxy
```

### 3. **`__init__`: инициализация трекера**

```python
# ----- L3-lite queue-events proxy (дополнительные метрики)
l3_alpha = float(os.getenv("L3_TAKER_RATE_EMA_ALPHA", "0.12"))
self.l3_queue = L3QueueEventsProxy(bucket_ms=self.delta_bucket_ms, alpha=l3_alpha)
```

### 4. **`_process_tick`: кормим трейды в L3-queue proxy**

```python
# L3-lite queue-events proxy: taker flow per tick (cheap)
if tick.last and tick.volume and tick.volume > 0:
    side = self._taker_side(tick)
    if side in (-1, 1):
        self.l3_queue.on_trade(side, float(tick.volume))
```

### 5. **`_process_tick`: заполнение метрик на границе бакета**

Сразу после `L3LiteTracker.attach_to_context(ctx)`:

```python
# --- L3-lite queue-events proxy metrics at bucket boundary ---
st = self.l3_queue.on_bucket_close()
ctx.taker_buy_qty_bucket = float(st.buy_qty)
ctx.taker_sell_qty_bucket = float(st.sell_qty)
ctx.taker_buy_rate_ema = float(st.buy_rate_ema)
ctx.taker_sell_rate_ema = float(st.sell_rate_ema)

# cancel/pull proxy from L2 ch-ratios (works only if L2 filled)
eps = 1e-9
ask_pull_ratio = max(0.0, -float(getattr(ctx, "ask_top5_ratio", 0.0) or 0.0))
bid_pull_ratio = max(0.0, -float(getattr(ctx, "bid_top5_ratio", 0.0) or 0.0))

ctx.pull_ask_qty_proxy = ask_pull_ratio * float(getattr(ctx, "depth_ask_5", 0.0) or 0.0)
ctx.pull_bid_qty_proxy = bid_pull_ratio * float(getattr(ctx, "depth_bid_5", 0.0) or 0.0)

# cancel-to-trade ratios (dimensionless)
ctx.cancel_to_trade_ask = float(ctx.pull_ask_qty_proxy) / (float(ctx.taker_buy_qty_bucket) + eps)
ctx.cancel_to_trade_bid = float(ctx.pull_bid_qty_proxy) / (float(ctx.taker_sell_qty_bucket) + eps)

# ETA to consume near top5 depth (seconds)
ctx.eta_fill_ask_sec = float(getattr(ctx, "depth_ask_5", 0.0) or 0.0) / (float(ctx.taker_buy_rate_ema) + eps)
ctx.eta_fill_bid_sec = float(getattr(ctx, "depth_bid_5", 0.0) or 0.0) / (float(ctx.taker_sell_rate_ema) + eps)
```

### 6. **`_ctx_l2_debug`: добавлены queue-proxy метрики**

```python
# --- L3-lite (queue-events proxy) ---
"taker_buy_qty_bucket": float(getattr(ctx, "taker_buy_qty_bucket", 0.0) or 0.0),
"taker_sell_qty_bucket": float(getattr(ctx, "taker_sell_qty_bucket", 0.0) or 0.0),
"pull_ask_qty_proxy": float(getattr(ctx, "pull_ask_qty_proxy", 0.0) or 0.0),
"pull_bid_qty_proxy": float(getattr(ctx, "pull_bid_qty_proxy", 0.0) or 0.0),
```

---

## 📊 Метрики в `SignalContext`

### **Bucket-уровень**:
- `taker_buy_qty_bucket` - суммарный объем taker-buy за bucket
- `taker_sell_qty_bucket` - суммарный объем taker-sell за bucket

### **Pull/Cancel proxy**:
- `pull_ask_qty_proxy` - прокси "cancels/pulls" на ask (из L2 ch-ratios)
- `pull_bid_qty_proxy` - прокси "cancels/pulls" на bid

### **Rates (EMA)**:
- `taker_buy_rate_ema` - qty/sec (taker-buy)
- `taker_sell_rate_ema` - qty/sec (taker-sell)

### **Ratios**:
- `cancel_to_trade_ask` = `pull_ask_qty_proxy / taker_buy_qty_bucket`
- `cancel_to_trade_bid` = `pull_bid_qty_proxy / taker_sell_qty_bucket`

### **ETA**:
- `eta_fill_ask_sec` = `depth_ask_5 / taker_buy_rate_ema`
- `eta_fill_bid_sec` = `depth_bid_5 / taker_sell_rate_ema`

---

## 🎯 Как это работает

### 1. **На каждом тике**:
```python
# Определяем taker side (+1 buy, -1 sell)
side = self._taker_side(tick)
# Кормим в L3-queue proxy
self.l3_queue.on_trade(side, tick.volume)
```

### 2. **На границе бакета**:
```python
# Закрываем bucket, получаем stats
st = self.l3_queue.on_bucket_close()
# Заполняем ctx
ctx.taker_buy_qty_bucket = st.buy_qty
ctx.taker_buy_rate_ema = st.buy_rate_ema
# ... и т.д.
```

### 3. **Расчет cancel-to-trade**:
```python
# pull proxy из L2 ch-ratios
ask_pull_ratio = max(0.0, -ctx.ask_top5_ratio)
ctx.pull_ask_qty_proxy = ask_pull_ratio * ctx.depth_ask_5

# cancel-to-trade
ctx.cancel_to_trade_ask = ctx.pull_ask_qty_proxy / (ctx.taker_buy_qty_bucket + eps)
```

---

## 🔧 Environment Variables

### Новые переменные:

```bash
# L3-lite queue-events proxy
L3_TAKER_RATE_EMA_ALPHA=0.12  # alpha для EMA rates
```

### Используются существующие:

```bash
DELTA_BUCKET_MS=1000  # размер bucket (используется для расчета rates)
```

---

## 📈 Использование в фильтрах

Эти метрики автоматически доступны в `CryptoOrderFlowHandler` для L3-lite фильтров:

### **Breakout фильтр**:
```python
if dir_up:
    ctr = ctx.cancel_to_trade_ask
    rate = ctx.taker_buy_rate_ema
    eta = ctx.eta_fill_ask_sec
else:
    ctr = ctx.cancel_to_trade_bid
    rate = ctx.taker_sell_rate_ema
    eta = ctx.eta_fill_bid_sec

# Reject "pulled liquidity imitation"
if ctr >= ctr_max and rate < rate_min:
    return False
```

### **Absorption фильтр**:
```python
if dir_up:
    rate = ctx.taker_buy_rate_ema
else:
    rate = ctx.taker_sell_rate_ema

if rate < rate_min:
    return False
```

---

## ✅ Проверка

### 1. **Синтаксис**:
```bash
cd python-worker
python -c "import ast; ast.parse(open('services/l3_queue_events_proxy.py').read()); print('✅ OK')"
python -c "import ast; ast.parse(open('handlers/base_orderflow_handler.py').read()); print('✅ OK')"
```

### 2. **Метрики в сигналах**:
```bash
docker logs -f scanner-crypto-orderflow | grep "taker_buy_qty_bucket\|pull_ask_qty_proxy"
```

### 3. **Проверить в indicators**:
```bash
docker exec scanner-redis redis-cli XREAD COUNT 1 STREAMS signals:orderflow:BTCUSDT 0-0
```

Должны быть поля:
```json
{
  "taker_buy_qty_bucket": 123.45,
  "taker_sell_qty_bucket": 98.76,
  "pull_ask_qty_proxy": 12.34,
  "pull_bid_qty_proxy": 8.90,
  "cancel_to_trade_ask": 0.1,
  "cancel_to_trade_bid": 0.09,
  "eta_fill_ask_sec": 15.3,
  "eta_fill_bid_sec": 18.7
}
```

---

## 🔄 Отличия от `L3LiteTracker`

| Метрика | `L3LiteTracker` | `L3QueueEventsProxy` |
|---------|-----------------|----------------------|
| **taker rates** | ✅ (из trades + book) | ✅ (из trades) |
| **cancel rates** | ✅ (из book depth delta) | ❌ |
| **cancel-to-trade** | ✅ (cancel_rate / taker_rate) | ✅ (pull_proxy / bucket_qty) |
| **ETA fill** | ✅ (depth / taker_rate) | ✅ (depth / taker_rate_ema) |
| **bucket qty** | ❌ | ✅ |
| **pull proxy** | ❌ | ✅ |

**Вывод**: `L3QueueEventsProxy` дополняет `L3LiteTracker`, предоставляя bucket-уровневые метрики и pull-proxy из L2 ch-ratios.

---

## 📚 Связанные документы

- `L3_LITE_INTEGRATION_COMPLETE.md` - Полная документация L3LiteTracker
- `L3_LITE_FILTERS_INTEGRATION.md` - Документация фильтров
- `L3_FILTERS_ENV_ADDED.md` - ENV переменные в docker-compose.yml

---

## ✅ Статус

- ✅ **`l3_queue_events_proxy.py`**: создан
- ✅ **SignalContext**: добавлены 4 поля
- ✅ **BaseOrderFlowHandler**: интегрирован L3QueueEventsProxy
- ✅ **_process_tick**: кормим trades + заполняем ctx
- ✅ **_ctx_l2_debug**: добавлены queue-proxy метрики
- ✅ **Синтаксис**: проверен
- ⏳ **Требуется перезапуск**: `docker-compose up -d --build crypto-orderflow-service multi-symbol-orderflow`

---

**Дата**: 2025-11-29  
**Версия**: 1.0  
**Статус**: ✅ Integration Complete

