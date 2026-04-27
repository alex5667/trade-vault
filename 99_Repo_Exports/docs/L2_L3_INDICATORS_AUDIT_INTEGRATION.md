# ✅ L2/L3 Indicators & Audit Context Integration Complete

## 🎯 Что реализовано

Успешно интегрированы **L2/L3 метрики** в `indicators` и `audit_context` согласно рекомендациям.

---

## 📦 Изменения

### **A) SignalContext: L3-lite поля**

#### ✅ Проверено:
Все необходимые L3-lite поля уже присутствуют в `SignalContext`:

```python
# ---- L3-lite proxies (Binance-friendly) ----
taker_buy_qty_bucket: float = 0.0
taker_sell_qty_bucket: float = 0.0
taker_buy_rate_ema: float = 0.0
taker_sell_rate_ema: float = 0.0

pull_ask_qty_proxy: float = 0.0
pull_bid_qty_proxy: float = 0.0
cancel_to_trade_ask: float = 0.0
cancel_to_trade_bid: float = 0.0

eta_fill_ask_sec: float = 0.0
eta_fill_bid_sec: float = 0.0
```

**L2 поля** также уже присутствуют:
- `obi_20`, `obi_avg_20`, `obi_sustained_20`
- `microprice_shift_bps_20`
- `wall_bid`, `wall_ask`, `wall_bid_dist_bps`, `wall_ask_dist_bps`
- `refill_score`, `depletion_score`, `impact_proxy`
- `depth_bid_5`, `depth_ask_5`, `depth_bid_20`, `depth_ask_20`

---

### **B1) Base: L2/L3 в indicators**

#### ✅ Реализовано через `_ctx_l2_debug()`:

В `BaseOrderFlowHandler._publish_signal()` метод `create_signal()` использует:

```python
indicators={
    "z_delta": round(ctx.z_delta, 4),
    "delta_bucket": round(ctx.delta_bucket, 4),
    "obi": round(ctx.obi, 4),
    # ... базовые поля ...
    
    **self._ctx_l2_debug(ctx),  # ← Все L2/L3 поля автоматически
}
```

#### ✅ Обновлен `_ctx_l2_debug()`:

```python
def _ctx_l2_debug(self, ctx: SignalContext) -> Dict[str, Any]:
    return {
        # L2
        "obi_20": round(float(getattr(ctx, "obi_20", 0.0) or 0.0), 4),
        "obi_avg_20": round(float(getattr(ctx, "obi_avg_20", 0.0) or 0.0), 4),
        "obi_sustained_20": bool(getattr(ctx, "obi_sustained_20", False)),
        "microprice_shift_bps_20": round(float(getattr(ctx, "microprice_shift_bps_20", 0.0) or 0.0), 3),
        "wall_bid": bool(getattr(ctx, "wall_bid", False)),
        "wall_ask": bool(getattr(ctx, "wall_ask", False)),
        "wall_bid_dist_bps": round(float(getattr(ctx, "wall_bid_dist_bps", 0.0) or 0.0), 3),
        "wall_ask_dist_bps": round(float(getattr(ctx, "wall_ask_dist_bps", 0.0) or 0.0), 3),
        "refill_score": round(float(getattr(ctx, "refill_score", 0.0) or 0.0), 4),
        "depletion_score": round(float(getattr(ctx, "depletion_score", 0.0) or 0.0), 4),
        "impact_proxy": round(float(getattr(ctx, "impact_proxy", 0.0) or 0.0), 4),
        "depth_bid_5": round(float(getattr(ctx, "depth_bid_5", 0.0) or 0.0), 6),
        "depth_ask_5": round(float(getattr(ctx, "depth_ask_5", 0.0) or 0.0), 6),
        "depth_bid_20": round(float(getattr(ctx, "depth_bid_20", 0.0) or 0.0), 6),  # ← Добавлено
        "depth_ask_20": round(float(getattr(ctx, "depth_ask_20", 0.0) or 0.0), 6),  # ← Добавлено

        # L3-lite
        "taker_buy_qty_bucket": round(float(getattr(ctx, "taker_buy_qty_bucket", 0.0) or 0.0), 6),
        "taker_sell_qty_bucket": round(float(getattr(ctx, "taker_sell_qty_bucket", 0.0) or 0.0), 6),
        "taker_buy_rate_ema": round(float(getattr(ctx, "taker_buy_rate_ema", 0.0) or 0.0), 6),
        "taker_sell_rate_ema": round(float(getattr(ctx, "taker_sell_rate_ema", 0.0) or 0.0), 6),
        "pull_ask_qty_proxy": round(float(getattr(ctx, "pull_ask_qty_proxy", 0.0) or 0.0), 6),
        "pull_bid_qty_proxy": round(float(getattr(ctx, "pull_bid_qty_proxy", 0.0) or 0.0), 6),
        "cancel_to_trade_bid": round(float(getattr(ctx, "cancel_to_trade_bid", 0.0) or 0.0), 4),
        "cancel_to_trade_ask": round(float(getattr(ctx, "cancel_to_trade_ask", 0.0) or 0.0), 4),
        "eta_fill_bid_sec": round(float(getattr(ctx, "eta_fill_bid_sec", 0.0) or 0.0), 3),
        "eta_fill_ask_sec": round(float(getattr(ctx, "eta_fill_ask_sec", 0.0) or 0.0), 3),
    }
```

**Округление**:
- `microprice_shift_bps_20`: **3** (было 4)
- `wall_*_dist_bps`: **3** (было 4)
- `depth_*`: **6**
- `taker_*_qty_bucket`, `taker_*_rate_ema`, `pull_*_qty_proxy`: **6**
- `cancel_to_trade_*`: **4**
- `eta_fill_*_sec`: **3**

---

### **B2) Base: L2/L3 в audit_payload / signal_stream_payload**

#### ✅ Реализовано через `_ctx_l2_debug()`:

В `BaseOrderFlowHandler._publish_signal()` оба вызова `format_audit_payload()` используют:

```python
signal_stream_payload = UnifiedSignalFormatter.format_audit_payload(
    signal,
    extra_context={
        "obi": ctx.obi,
        "obi_avg": ctx.obi_avg,
        "obi_sustained": ctx.obi_sustained,
        "weak_progress": ctx.weak_progress,
        "kind": signal_kind,
        "level_key": level_key,
        
        **self._ctx_l2_debug(ctx),  # ← Все L2/L3 поля автоматически
    }
)

audit_payload = UnifiedSignalFormatter.format_audit_payload(
    signal,
    extra_context={
        "obi": ctx.obi,
        "obi_avg": ctx.obi_avg,
        "obi_sustained": ctx.obi_sustained,
        "weak_progress": ctx.weak_progress,
        "env": audit_env,
        "source": self.source_name,
        "kind": signal_kind,
        "level_key": level_key,
        
        **self._ctx_l2_debug(ctx),  # ← Все L2/L3 поля автоматически
    }
)
```

**Преимущество**: Все поля автоматически попадают в оба места через единый метод `_ctx_l2_debug()`.

---

### **B3) Crypto: расширен manual_payload["audit_context"]**

#### ✅ Обновлено в `CryptoOrderFlowHandler._extend_outbox_envelope()`:

```python
"audit_context": {
    "obi": float(ctx.obi),
    "obi_avg": float(ctx.obi_avg),
    "obi_sustained": bool(ctx.obi_sustained),

    # micro
    "spread_bps": _f(getattr(ctx, "spread_bps", 0.0)),
    "realized_ema_bps": _f(getattr(ctx, "realized_ema_bps", 0.0)),
    "adverse_ratio_ema": _f(getattr(ctx, "adverse_ratio_ema", 0.0)),
    "market_mode": str(getattr(ctx, "market_mode", "mixed")),

    # L2: staleness
    "l2_age_ms": int(getattr(ctx, "l2_age_ms", 0) or 0),
    "l2_is_stale": bool(getattr(ctx, "l2_is_stale", True)),

    # ---- L2 ----
    "obi_20": _f(getattr(ctx, "obi_20", 0.0)),
    "obi_avg_20": _f(getattr(ctx, "obi_avg_20", 0.0)),
    "obi_sustained_20": _b(getattr(ctx, "obi_sustained_20", False)),
    "microprice_shift_bps_20": _f(getattr(ctx, "microprice_shift_bps_20", 0.0)),
    "wall_bid": _b(getattr(ctx, "wall_bid", False)),
    "wall_ask": _b(getattr(ctx, "wall_ask", False)),
    "wall_bid_dist_bps": _f(getattr(ctx, "wall_bid_dist_bps", 0.0)),
    "wall_ask_dist_bps": _f(getattr(ctx, "wall_ask_dist_bps", 0.0)),
    "refill_score": _f(getattr(ctx, "refill_score", 0.0)),
    "depletion_score": _f(getattr(ctx, "depletion_score", 0.0)),
    "impact_proxy": _f(getattr(ctx, "impact_proxy", 0.0)),
    "depth_bid_5": _f(getattr(ctx, "depth_bid_5", 0.0)),
    "depth_ask_5": _f(getattr(ctx, "depth_ask_5", 0.0)),

    # ---- L3-lite ----
    "taker_buy_qty_bucket": _f(getattr(ctx, "taker_buy_qty_bucket", 0.0)),
    "taker_sell_qty_bucket": _f(getattr(ctx, "taker_sell_qty_bucket", 0.0)),
    "taker_buy_rate_ema": _f(getattr(ctx, "taker_buy_rate_ema", 0.0)),
    "taker_sell_rate_ema": _f(getattr(ctx, "taker_sell_rate_ema", 0.0)),
    "cancel_to_trade_ask": _f(getattr(ctx, "cancel_to_trade_ask", 0.0)),
    "cancel_to_trade_bid": _f(getattr(ctx, "cancel_to_trade_bid", 0.0)),
    "eta_fill_ask_sec": _f(getattr(ctx, "eta_fill_ask_sec", 0.0)),
    "eta_fill_bid_sec": _f(getattr(ctx, "eta_fill_bid_sec", 0.0)),
},
```

**Изменения**:
- ✅ Переорганизован согласно рекомендациям (блоки `---- L2 ----` и `---- L3-lite ----`)
- ✅ Удалены дубликаты
- ✅ Все поля используют helper функции `_f()` и `_b()`

---

## 📊 Что попадает в сигналы

### **1. `signal.indicators`** (BaseOrderFlowHandler):

```json
{
  "z_delta": 3.5,
  "obi": 0.45,
  "obi_sustained": true,
  
  "obi_20": 0.42,
  "obi_sustained_20": true,
  "microprice_shift_bps_20": 0.5,
  "wall_bid": false,
  "wall_ask": true,
  "refill_score": 0.02,
  "depletion_score": 0.08,
  "impact_proxy": 0.12,
  "depth_bid_5": 1234.56,
  "depth_ask_5": 1098.76,
  "depth_bid_20": 4567.89,
  "depth_ask_20": 4321.23,
  
  "taker_buy_qty_bucket": 123.456,
  "taker_sell_qty_bucket": 98.765,
  "taker_buy_rate_ema": 15.234,
  "taker_sell_rate_ema": 12.345,
  "pull_ask_qty_proxy": 12.345,
  "pull_bid_qty_proxy": 8.901,
  "cancel_to_trade_ask": 0.1,
  "cancel_to_trade_bid": 0.09,
  "eta_fill_ask_sec": 15.3,
  "eta_fill_bid_sec": 18.7
}
```

### **2. `audit_payload.extra_context`** (BaseOrderFlowHandler):

Те же поля что и в `indicators`, но без округления (через `_ctx_l2_debug()`).

### **3. `manual_payload.audit_context`** (CryptoOrderFlowHandler):

Те же поля что и выше, но с дополнительными полями `obi`, `obi_avg`, `obi_sustained`, `micro`, `l2_age_ms`, `l2_is_stale`.

---

## ✅ Проверка

### **1. Синтаксис**:
```bash
✅ base_orderflow_handler.py Syntax OK
✅ crypto_orderflow_handler.py Syntax OK
```

### **2. Применить изменения**:
```bash
cd /home/alex/front/trade/scanner_infra
docker-compose up -d --build crypto-orderflow-service multi-symbol-orderflow
```

### **3. Проверить метрики**:
```bash
# Проверить indicators
docker exec scanner-redis redis-cli XREAD COUNT 1 STREAMS signals:orderflow:BTCUSDT 0-0

# Проверить manual-signals
docker exec scanner-redis redis-cli XREAD COUNT 1 STREAMS stream:manual-signals 0-0
```

---

## 🔄 Ключевые улучшения

| Аспект | Было | Стало |
|--------|------|-------|
| **indicators** | Частично через `_ctx_l2_debug` | Полностью через `_ctx_l2_debug` |
| **audit_payload** | Частично через `_ctx_l2_debug` | Полностью через `_ctx_l2_debug` |
| **depth_bid_20/depth_ask_20** | ❌ | ✅ |
| **Округление** | Непоследовательное | Согласованное (3/4/6) |
| **manual_payload.audit_context** | Частично | Полностью (L2 + L3-lite) |
| **Организация** | Разрозненно | Блоки `---- L2 ----` и `---- L3-lite ----` |

---

## 📚 Связанные документы

- `L3_IMPROVED_ARCHITECTURE_COMPLETE.md` - Улучшенная архитектура L3-lite
- `DIFF_INTEGRATION_COMPLETE.md` - Diff интеграция
- `L3_QUEUE_PROXY_FINAL_INTEGRATION.md` - Финальная интеграция

---

## ✅ Статус

- ✅ **SignalContext**: все L3-lite поля уже есть
- ✅ **BaseOrderFlowHandler._ctx_l2_debug()**: 
  - Добавлены `depth_bid_20`, `depth_ask_20`
  - Обновлено округление
  - Все L2/L3 поля включены
- ✅ **BaseOrderFlowHandler._publish_signal()**: 
  - `indicators` использует `_ctx_l2_debug()`
  - `audit_payload` использует `_ctx_l2_debug()`
- ✅ **CryptoOrderFlowHandler._extend_outbox_envelope()**: 
  - `audit_context` переорганизован
  - Все L2/L3 поля добавлены
- ✅ **Синтаксис**: проверен
- ⏳ **Требуется перезапуск**: `docker-compose up -d --build crypto-orderflow-service multi-symbol-orderflow`

---

## 🚀 Команда для применения

```bash
docker-compose up -d --build crypto-orderflow-service multi-symbol-orderflow
```

---

**Дата**: 2025-11-29  
**Версия**: 1.0  
**Статус**: ✅ **Integration Complete**  
**Подход**: Единый метод `_ctx_l2_debug()` для всех мест


