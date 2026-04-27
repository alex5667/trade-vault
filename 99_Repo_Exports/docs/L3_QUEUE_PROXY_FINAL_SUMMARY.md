# ✅ L3-lite Queue-Events Proxy - Final Integration Summary

## 🎯 Что реализовано

Интегрирован **L3-lite Queue-Events Proxy** — упрощенная версия L3-lite для расчета дополнительных метрик на уровне bucket:

### **Новые метрики**:
1. `taker_buy_qty_bucket` - суммарный объем taker-buy за bucket
2. `taker_sell_qty_bucket` - суммарный объем taker-sell за bucket
3. `pull_ask_qty_proxy` - прокси "cancels/pulls" на ask (из L2 ch-ratios)
4. `pull_bid_qty_proxy` - прокси "cancels/pulls" на bid
5. `cancel_to_trade_ask` - ratio pull/trade на ask
6. `cancel_to_trade_bid` - ratio pull/trade на bid
7. `eta_fill_ask_sec` - ETA съедания depth_ask_5
8. `eta_fill_bid_sec` - ETA съедания depth_bid_5

---

## 📦 Новые файлы

### 1. **`python-worker/services/l3_queue_events_proxy.py`** (1.8 KB)

```python
class L3QueueEventsProxy:
    """
    L3-lite:
      - считаем taker buy/sell qty внутри delta-bucket
      - на закрытии bucket -> считаем qty/sec и обновляем EMA
    """
    def on_trade(self, side: int, qty: float) -> None
    def on_bucket_close(self) -> L3BucketStats
```

---

## 🔧 Изменения в существующих файлах

### 1. **`python-worker/handlers/base_orderflow_handler.py`**

#### A) SignalContext: +4 поля
```python
# L3-lite (queue-events proxy) - дополнительные метрики
taker_buy_qty_bucket: float = 0.0
taker_sell_qty_bucket: float = 0.0
pull_ask_qty_proxy: float = 0.0
pull_bid_qty_proxy: float = 0.0
```

#### B) Импорт
```python
from services.l3_queue_events_proxy import L3QueueEventsProxy
```

#### C) `__init__`: инициализация
```python
l3_alpha = float(os.getenv("L3_TAKER_RATE_EMA_ALPHA", "0.12"))
self.l3_queue = L3QueueEventsProxy(bucket_ms=self.delta_bucket_ms, alpha=l3_alpha)
```

#### D) `_process_tick`: кормим trades
```python
# L3-lite queue-events proxy: taker flow per tick (cheap)
if tick.last and tick.volume and tick.volume > 0:
    side = self._taker_side(tick)
    if side in (-1, 1):
        self.l3_queue.on_trade(side, float(tick.volume))
```

#### E) `_process_tick`: заполнение ctx на границе bucket
```python
# --- L3-lite queue-events proxy metrics at bucket boundary ---
st = self.l3_queue.on_bucket_close()
ctx.taker_buy_qty_bucket = float(st.buy_qty)
ctx.taker_sell_qty_bucket = float(st.sell_qty)
ctx.taker_buy_rate_ema = float(st.buy_rate_ema)
ctx.taker_sell_rate_ema = float(st.sell_rate_ema)

# cancel/pull proxy from L2 ch-ratios
eps = 1e-9
ask_pull_ratio = max(0.0, -float(getattr(ctx, "ask_top5_ratio", 0.0) or 0.0))
bid_pull_ratio = max(0.0, -float(getattr(ctx, "bid_top5_ratio", 0.0) or 0.0))

ctx.pull_ask_qty_proxy = ask_pull_ratio * float(getattr(ctx, "depth_ask_5", 0.0) or 0.0)
ctx.pull_bid_qty_proxy = bid_pull_ratio * float(getattr(ctx, "depth_bid_5", 0.0) or 0.0)

# cancel-to-trade ratios
ctx.cancel_to_trade_ask = float(ctx.pull_ask_qty_proxy) / (float(ctx.taker_buy_qty_bucket) + eps)
ctx.cancel_to_trade_bid = float(ctx.pull_bid_qty_proxy) / (float(ctx.taker_sell_qty_bucket) + eps)

# ETA to consume near top5 depth
ctx.eta_fill_ask_sec = float(getattr(ctx, "depth_ask_5", 0.0) or 0.0) / (float(ctx.taker_buy_rate_ema) + eps)
ctx.eta_fill_bid_sec = float(getattr(ctx, "depth_bid_5", 0.0) or 0.0) / (float(ctx.taker_sell_rate_ema) + eps)
```

#### F) `_ctx_l2_debug`: добавлены queue-proxy метрики
```python
# --- L3-lite (queue-events proxy) ---
"taker_buy_qty_bucket": float(getattr(ctx, "taker_buy_qty_bucket", 0.0) or 0.0),
"taker_sell_qty_bucket": float(getattr(ctx, "taker_sell_qty_bucket", 0.0) or 0.0),
"pull_ask_qty_proxy": float(getattr(ctx, "pull_ask_qty_proxy", 0.0) or 0.0),
"pull_bid_qty_proxy": float(getattr(ctx, "pull_bid_qty_proxy", 0.0) or 0.0),
```

### 2. **`docker-compose.yml`**

Добавлена env переменная в оба сервиса (`multi-symbol-orderflow` и `crypto-orderflow-service`):

```yaml
# ═══ L3-LITE QUEUE-EVENTS PROXY ═══
- L3_TAKER_RATE_EMA_ALPHA=0.12
```

---

## 🔄 Как это работает

### **Поток данных**:

```
Tick (trade) 
  ↓
_taker_side(tick) → +1 (buy) / -1 (sell)
  ↓
l3_queue.on_trade(side, qty)
  ↓ (накопление в bucket)
Bucket boundary
  ↓
l3_queue.on_bucket_close() → L3BucketStats
  ↓
Заполнение SignalContext:
  - taker_buy_qty_bucket, taker_sell_qty_bucket
  - taker_buy_rate_ema, taker_sell_rate_ema (qty/sec)
  ↓
Расчет pull_proxy из L2 ch-ratios:
  - ask_pull_ratio = max(0, -ask_top5_ratio)
  - pull_ask_qty_proxy = ask_pull_ratio * depth_ask_5
  ↓
Расчет cancel-to-trade:
  - cancel_to_trade_ask = pull_ask_qty_proxy / taker_buy_qty_bucket
  ↓
Расчет ETA:
  - eta_fill_ask_sec = depth_ask_5 / taker_buy_rate_ema
```

---

## 📊 Метрики в сигналах

### **signal.indicators**:
```json
{
  "taker_buy_qty_bucket": 123.45,
  "taker_sell_qty_bucket": 98.76,
  "pull_ask_qty_proxy": 12.34,
  "pull_bid_qty_proxy": 8.90,
  "taker_buy_rate_ema": 15.234567,
  "taker_sell_rate_ema": 12.345678,
  "cancel_to_trade_ask": 0.1,
  "cancel_to_trade_bid": 0.09,
  "eta_fill_ask_sec": 15.3,
  "eta_fill_bid_sec": 18.7
}
```

### **manual_payload.audit_context**:
Те же поля автоматически включены через `_ctx_l2_debug()`.

---

## 🎯 Использование в L3-lite фильтрах

Эти метрики автоматически доступны в `CryptoOrderFlowHandler` для фильтров:

### **Breakout фильтр** (уже интегрирован):
```python
if os.getenv("BREAKOUT_USE_L3_FILTERS", "false").lower() == "true":
    ctr_max = float(os.getenv("BREAKOUT_L3_MAX_CANCEL_TO_TRADE", "3.0"))
    rate_min = float(os.getenv("BREAKOUT_L3_MIN_TAKER_RATE", "0.0"))
    
    if dir_up:
        ctr = ctx.cancel_to_trade_ask
        rate = ctx.taker_buy_rate_ema
    else:
        ctr = ctx.cancel_to_trade_bid
        rate = ctx.taker_sell_rate_ema
    
    # Reject "pulled liquidity imitation"
    if ctr >= ctr_max and (rate_min <= 0 or rate < rate_min):
        return False
```

### **Absorption фильтр** (уже интегрирован):
```python
if os.getenv("ABSORPTION_USE_L3_FILTERS", "false").lower() == "true":
    rate_min = float(os.getenv("ABSORPTION_L3_MIN_TAKER_RATE", "0.0"))
    if rate_min > 0:
        rate = ctx.taker_buy_rate_ema if dir_up else ctx.taker_sell_rate_ema
        if rate < rate_min:
            return False
```

---

## ⚙️ Environment Variables

### **Новая переменная**:
```bash
L3_TAKER_RATE_EMA_ALPHA=0.12  # alpha для EMA rates (0.01-0.5)
```

### **Используются существующие**:
```bash
DELTA_BUCKET_MS=1000  # размер bucket для расчета rates

# L3-lite фильтры (уже добавлены в docker-compose.yml)
BREAKOUT_USE_L3_FILTERS=true
BREAKOUT_L3_MAX_CANCEL_TO_TRADE=3.0
BREAKOUT_L3_MIN_TAKER_RATE=0.0
BREAKOUT_L3_MAX_ETA_SEC=0.0

ABSORPTION_USE_L3_FILTERS=true
ABSORPTION_L3_MIN_TAKER_RATE=0.0

EXTREME_USE_L3_FILTERS=true
EXTREME_L3_MAX_CANCEL_TO_TRADE=6.0
EXTREME_L3_MIN_TAKER_RATE=0.0
```

---

## ✅ Проверка

### 1. **Синтаксис**:
```bash
cd python-worker
python -m py_compile services/l3_queue_events_proxy.py
python -m py_compile handlers/base_orderflow_handler.py
# ✅ All files compile successfully
```

### 2. **docker-compose.yml**:
```bash
docker-compose config | grep L3_TAKER_RATE_EMA_ALPHA
# Должно быть 2 строки (multi-symbol-orderflow + crypto-orderflow-service)
```

### 3. **Применить изменения**:
```bash
docker-compose up -d --build crypto-orderflow-service multi-symbol-orderflow
```

### 4. **Проверить логи**:
```bash
docker logs -f scanner-crypto-orderflow | grep "taker_buy_qty_bucket\|pull_ask_qty_proxy"
```

### 5. **Проверить метрики в сигналах**:
```bash
docker exec scanner-redis redis-cli XREAD COUNT 1 STREAMS signals:orderflow:BTCUSDT 0-0
```

---

## 🔄 Отличия от `L3LiteTracker`

| Метрика | `L3LiteTracker` | `L3QueueEventsProxy` |
|---------|-----------------|----------------------|
| **Источник данных** | Trades + Book depth delta | Trades + L2 ch-ratios |
| **taker rates** | ✅ (из trades + book) | ✅ (из trades) |
| **cancel rates** | ✅ (из book depth delta) | ❌ (используется pull_proxy) |
| **cancel-to-trade** | ✅ (cancel_rate / taker_rate) | ✅ (pull_proxy / bucket_qty) |
| **ETA fill** | ✅ (depth / taker_rate) | ✅ (depth / taker_rate_ema) |
| **bucket qty** | ❌ | ✅ |
| **pull proxy** | ❌ | ✅ |

**Вывод**: `L3QueueEventsProxy` **дополняет** `L3LiteTracker`, предоставляя:
- Bucket-уровневые метрики (`taker_*_qty_bucket`)
- Pull-proxy из L2 ch-ratios (`pull_*_qty_proxy`)
- Альтернативный расчет `cancel_to_trade` (через pull_proxy вместо cancel_rate)

---

## 📚 Связанные документы

- `L3_LITE_INTEGRATION_COMPLETE.md` - Полная документация L3LiteTracker
- `L3_LITE_FILTERS_INTEGRATION.md` - Документация фильтров
- `L3_FILTERS_ENV_ADDED.md` - ENV переменные в docker-compose.yml
- `L3_QUEUE_EVENTS_PROXY_INTEGRATION.md` - Детальная документация

---

## ✅ Статус

- ✅ **`l3_queue_events_proxy.py`**: создан (1.8 KB)
- ✅ **SignalContext**: добавлены 4 поля
- ✅ **BaseOrderFlowHandler**: интегрирован L3QueueEventsProxy
- ✅ **_process_tick**: кормим trades + заполняем ctx
- ✅ **_ctx_l2_debug**: добавлены queue-proxy метрики
- ✅ **docker-compose.yml**: добавлена `L3_TAKER_RATE_EMA_ALPHA=0.12`
- ✅ **Синтаксис**: проверен
- ✅ **Валидация**: `docker-compose config` OK
- ⏳ **Требуется перезапуск**: `docker-compose up -d --build crypto-orderflow-service multi-symbol-orderflow`

---

## 🚀 Команда для применения

```bash
cd /home/alex/front/trade/scanner_infra
docker-compose up -d --build crypto-orderflow-service multi-symbol-orderflow
```

---

**Дата**: 2025-11-29  
**Версия**: 1.0  
**Статус**: ✅ **Integration Complete**  
**Автор**: Senior Go/Python Developer + Senior Trading Systems Analyst

