# ✅ Base OrderFlow Handler - L2 Staleness Tracking & Auto-Propagation COMPLETE

## 🎯 Что было добавлено

### 1. **L2 Staleness Tracking в SignalContext** ✅

Добавлены 3 служебных поля для отслеживания "свежести" L2-данных:

```python
@dataclass
class SignalContext:
    # ... existing fields ...
    
    # L2 staleness/debug
    l2_ts: int = 0              # Timestamp последнего L2-снимка
    l2_age_ms: int = 0          # Возраст L2-данных (ms)
    l2_is_stale: bool = True    # True если L2 протух
```

**Назначение**:
- ✅ `l2_ts` - когда был получен последний валидный L2-снимок
- ✅ `l2_age_ms` - сколько миллисекунд прошло с последнего снимка
- ✅ `l2_is_stale` - флаг "протухания" (если `age > L2_MAX_STALE_MS`)

---

### 2. **Tracking L2 Timestamp в BaseOrderFlowHandler** ✅

#### 2.1. В `__init__`:

```python
self._l2_last: Optional[L2Snapshot] = None
self._l2_last_ts: int = 0  # ✅ НОВОЕ: timestamp последнего L2-снимка
```

#### 2.2. В `_process_book()`:

```python
self._l2_last = snap

# additional OBI(20)
self._last_obi_20 = float(snap.m.obi_20)
self._last_obi_20_ts = ts

self._track_obi(ts, self._last_obi, self._last_obi_20)

# ✅ НОВОЕ: Update L2 snapshot timestamp
self._l2_last_ts = ts
```

**Результат**: Каждый раз при получении нового L2-снимка сохраняется его timestamp.

---

### 3. **Staleness Check в `_process_tick()`** ✅

#### 3.1. Вычисление staleness (сразу после создания `ctx`):

```python
ctx = SignalContext(
    ts=tick.ts,
    price=mid,
    # ... other fields ...
)

# --- L2 staleness bookkeeping ---
l2_max_stale_ms = int(os.getenv("L2_MAX_STALE_MS", "1500"))

if self._l2_last_ts > 0:
    ctx.l2_ts = int(self._l2_last_ts)
    ctx.l2_age_ms = int(max(0, tick.ts - self._l2_last_ts))
    ctx.l2_is_stale = bool(ctx.l2_age_ms > l2_max_stale_ms)
else:
    ctx.l2_ts = 0
    ctx.l2_age_ms = 0
    ctx.l2_is_stale = True
```

**Логика**:
- ✅ Если `_l2_last_ts == 0` → L2 никогда не получали → `stale=True`
- ✅ Если `age > L2_MAX_STALE_MS` (дефолт 1500ms) → `stale=True`
- ✅ Иначе → `stale=False`

#### 3.2. Условное прикрепление L2 к ctx (только если fresh):

```python
# attach L2 metrics snapshot (only if fresh)
if (
    self._l2_last
    and self._l2_last.m
    and self._l2_last.m.mid > 0
    and (not ctx.l2_is_stale)  # ✅ НОВОЕ: не приклеиваем протухший L2
):
    m = self._l2_last.m
    ch = self._l2_last.ch
    
    ctx.depth_bid_5 = m.depth_bid_5
    ctx.depth_ask_5 = m.depth_ask_5
    # ... все остальные L2-поля ...
```

**Результат**: Если L2 протух (например, book-stream отстал на >1.5s), то L2-поля в `ctx` останутся дефолтными (0/False), а фильтры корректно отработают.

---

### 4. **Автоматический Проброс L2 в indicators и audit_payload** ✅

#### 4.1. Новый helper-метод `_ctx_l2_debug()`:

```python
def _ctx_l2_debug(self, ctx: SignalContext) -> Dict[str, Any]:
    """
    Минимальный набор L2-полей для indicators и audit_payload.
    """
    return {
        "l2_ts": int(getattr(ctx, "l2_ts", 0) or 0),
        "l2_age_ms": int(getattr(ctx, "l2_age_ms", 0) or 0),
        "l2_is_stale": bool(getattr(ctx, "l2_is_stale", True)),

        "obi_20": float(getattr(ctx, "obi_20", 0.0) or 0.0),
        "obi_avg_20": float(getattr(ctx, "obi_avg_20", 0.0) or 0.0),
        "obi_sustained_20": bool(getattr(ctx, "obi_sustained_20", False)),

        "microprice_shift_bps_20": float(getattr(ctx, "microprice_shift_bps_20", 0.0) or 0.0),

        "wall_bid": bool(getattr(ctx, "wall_bid", False)),
        "wall_ask": bool(getattr(ctx, "wall_ask", False)),
        "wall_bid_dist_bps": float(getattr(ctx, "wall_bid_dist_bps", 0.0) or 0.0),
        "wall_ask_dist_bps": float(getattr(ctx, "wall_ask_dist_bps", 0.0) or 0.0),

        "refill_score": float(getattr(ctx, "refill_score", 0.0) or 0.0),
        "depletion_score": float(getattr(ctx, "depletion_score", 0.0) or 0.0),

        "impact_proxy": float(getattr(ctx, "impact_proxy", 0.0) or 0.0),

        "depth_bid_5": float(getattr(ctx, "depth_bid_5", 0.0) or 0.0),
        "depth_ask_5": float(getattr(ctx, "depth_ask_5", 0.0) or 0.0),
    }
```

**Назначение**: Один метод для сбора всех критичных L2-полей из `ctx` с безопасными fallback'ами.

#### 4.2. В `_publish_signal()` - добавление в `indicators`:

```python
indicators={
    "z_delta": round(ctx.z_delta, 4),
    "delta_bucket": round(ctx.delta_bucket, 4),
    # ... existing fields ...
    "emoji": emoji,

    **self._ctx_l2_debug(ctx),  # ✅ НОВОЕ: автопроброс L2
}
```

#### 4.3. В `_publish_signal()` - добавление в `signal_stream_payload`:

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

        **self._ctx_l2_debug(ctx),  # ✅ НОВОЕ: автопроброс L2
    }
)
```

#### 4.4. В `_publish_signal()` - добавление в `audit_payload`:

```python
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

        **self._ctx_l2_debug(ctx),  # ✅ НОВОЕ: автопроброс L2
    }
)
```

**Результат**: Все L2-поля автоматически попадают в:
- ✅ `signal.indicators` (для анализа и отчетов)
- ✅ `signals:{strategy}:{symbol}` stream (signal_stream_payload)
- ✅ `signals:audit:{symbol}` stream (audit_payload)

---

### 5. **L2 в manual_payload.audit_context (CryptoOrderFlowHandler)** ✅

В `crypto_orderflow_handler.py`, метод `_extend_outbox_envelope()`:

```python
"audit_context": {
    "obi": float(ctx.obi),
    "obi_avg": float(ctx.obi_avg),
    "obi_sustained": bool(ctx.obi_sustained),

    # micro
    "spread_bps": _f(getattr(ctx, "spread_bps", 0.0)),
    # ... other micro fields ...

    # ✅ НОВОЕ: L2 staleness
    "l2_age_ms": int(getattr(ctx, "l2_age_ms", 0) or 0),
    "l2_is_stale": bool(getattr(ctx, "l2_is_stale", True)),

    # ✅ НОВОЕ: L2 metrics (все остальные поля уже были добавлены ранее)
    "obi_20": _f(getattr(ctx, "obi_20", 0.0)),
    "obi_avg_20": _f(getattr(ctx, "obi_avg_20", 0.0)),
    "obi_sustained_20": _b(getattr(ctx, "obi_sustained_20", False)),
    
    "microprice_shift_bps_20": _f(getattr(ctx, "microprice_shift_bps_20", 0.0)),
    
    "wall_bid": _b(getattr(ctx, "wall_bid", False)),
    "wall_bid_dist_bps": _f(getattr(ctx, "wall_bid_dist_bps", 0.0)),
    "wall_ask": _b(getattr(ctx, "wall_ask", False)),
    "wall_ask_dist_bps": _f(getattr(ctx, "wall_ask_dist_bps", 0.0)),
    
    "refill_score": _f(getattr(ctx, "refill_score", 0.0)),
    "depletion_score": _f(getattr(ctx, "depletion_score", 0.0)),
    "impact_proxy": _f(getattr(ctx, "impact_proxy", 0.0)),
    
    "depth_bid_5": _f(getattr(ctx, "depth_bid_5", 0.0)),
    "depth_ask_5": _f(getattr(ctx, "depth_ask_5", 0.0)),
},
```

**Результат**: Manual-signals теперь содержат полный L2-контекст + staleness info.

---

## 📊 Структура данных

### Signal.indicators (для всех сигналов):

```json
{
  "kind": "breakout",
  "z_delta": 3.5,
  "obi": 0.5,
  
  // ✅ L2 staleness
  "l2_ts": 1732896123456,
  "l2_age_ms": 234,
  "l2_is_stale": false,
  
  // ✅ L2 metrics
  "obi_20": 0.4567,
  "obi_avg_20": 0.4321,
  "obi_sustained_20": true,
  "microprice_shift_bps_20": 1.234,
  "wall_bid": false,
  "wall_ask": true,
  "wall_ask_dist_bps": 8.5,
  "refill_score": 0.02,
  "depletion_score": 0.15,
  "impact_proxy": 0.25,
  "depth_bid_5": 123.456789,
  "depth_ask_5": 98.765432
}
```

### Manual Payload audit_context (crypto только):

```json
{
  "sid": "sig_123",
  "symbol": "BTCUSDT",
  "side": "LONG",
  
  "audit_context": {
    "obi": 0.5,
    "spread_bps": 1.234,
    
    // ✅ L2 staleness
    "l2_age_ms": 234,
    "l2_is_stale": false,
    
    // ✅ L2 metrics (все те же поля)
    "obi_20": 0.4567,
    "wall_ask": true,
    "depletion_score": 0.15,
    "depth_bid_5": 123.456789
  }
}
```

---

## 🔍 Использование

### 1. Отладка "почему сигнал отфильтрован":

```python
# Проверка в логах/Redis
signal = redis.xrevrange("signals:cryptoorderflow:BTCUSDT", "+", "-", count=1)[0]
inds = json.loads(signal[1]["indicators"])

if inds["l2_is_stale"]:
    print(f"⚠️ L2 был протухшим! Age: {inds['l2_age_ms']}ms")
    print("Фильтры не сработали из-за отсутствия свежих L2-данных")

if not inds["obi_sustained_20"]:
    print("❌ OBI_20 не sustained → breakout отфильтрован")

if inds["wall_ask"] and inds["wall_ask_dist_bps"] < 10.0:
    print("❌ Ask wall близко → breakout отфильтрован")
```

### 2. Анализ качества L2-данных:

```python
# Статистика по staleness
signals = redis.xrange("signals:cryptoorderflow:BTCUSDT", "-", "+", count=100)
stale_count = 0
total_age = 0

for _, fields in signals:
    inds = json.loads(fields["indicators"])
    if inds.get("l2_is_stale", True):
        stale_count += 1
    total_age += inds.get("l2_age_ms", 0)

print(f"Stale rate: {stale_count/len(signals):.1%}")
print(f"Avg L2 age: {total_age/len(signals):.0f}ms")
```

### 3. Фильтрация сигналов по L2-качеству:

```python
# Только сигналы с fresh L2
fresh_signals = [
    s for s in signals 
    if not json.loads(s[1]["indicators"]).get("l2_is_stale", True)
]

# Только сигналы с high-quality L2 (age < 500ms)
hq_signals = [
    s for s in signals
    if json.loads(s[1]["indicators"]).get("l2_age_ms", 9999) < 500
]
```

---

## 🎯 Преимущества

### 1. **Защита от протухших L2-данных**
- ✅ Если book-stream отстал на >1.5s, L2-поля не приклеиваются к `ctx`
- ✅ Фильтры корректно работают с дефолтными значениями (не ломаются)
- ✅ В indicators видно `l2_is_stale=true` → понятно, почему фильтр не сработал

### 2. **Полная прозрачность L2-контекста**
- ✅ Каждый сигнал содержит `l2_age_ms` → можно оценить качество данных
- ✅ Можно строить метрики по staleness rate
- ✅ Легко дебажить "почему сигнал прошел/не прошел фильтр"

### 3. **Автоматический проброс (DRY)**
- ✅ Один метод `_ctx_l2_debug()` → все L2-поля в indicators/audit
- ✅ Не нужно дублировать код в каждом handler'е
- ✅ Легко добавить новые L2-поля (один раз в `_ctx_l2_debug`)

### 4. **Консистентность данных**
- ✅ Base гарантирует, что L2-поля либо fresh, либо отсутствуют
- ✅ Crypto handler не может "случайно" использовать протухший L2
- ✅ Manual-signals содержат полный контекст для воспроизведения условий

---

## 🧪 Тестирование

### Проверка staleness tracking:

```python
# Тест: L2 staleness корректно вычисляется
handler = BaseOrderFlowHandler("BTCUSDT")
handler._l2_last_ts = 1000000

tick = Tick(ts=1001000, bid=100, ask=100.1, last=100.05, volume=1, flags=0)
handler._process_tick(tick)

# ctx.l2_age_ms должен быть 1000
# ctx.l2_is_stale должен быть False (< 1500ms)
```

### Проверка автопроброса:

```python
# Тест: _ctx_l2_debug() возвращает все поля
ctx = SignalContext(...)
ctx.obi_20 = 0.5
ctx.l2_age_ms = 123
ctx.l2_is_stale = False

l2_debug = handler._ctx_l2_debug(ctx)
assert l2_debug["obi_20"] == 0.5
assert l2_debug["l2_age_ms"] == 123
assert l2_debug["l2_is_stale"] == False
```

### Проверка защиты от stale L2:

```python
# Тест: Протухший L2 не приклеивается к ctx
handler._l2_last_ts = 1000000
handler._l2_last = L2Snapshot(...)

tick = Tick(ts=1003000, ...)  # age=3000ms > 1500ms
handler._process_tick(tick)

# ctx.depth_bid_5 должен быть 0.0 (дефолт)
# ctx.l2_is_stale должен быть True
```

---

## ✅ Статус

- ✅ **SignalContext**: добавлены `l2_ts`, `l2_age_ms`, `l2_is_stale`
- ✅ **BaseOrderFlowHandler**: tracking `_l2_last_ts` в `_process_book()`
- ✅ **_process_tick()**: staleness check + условное прикрепление L2
- ✅ **_ctx_l2_debug()**: helper для автопроброса L2-полей
- ✅ **_publish_signal()**: автопроброс в indicators + audit_payload
- ✅ **CryptoOrderFlowHandler**: L2 staleness в manual_payload.audit_context
- ✅ **Syntax OK**: Python compile успешен для обоих handlers
- ✅ **Linter errors**: 0
- ✅ **Ready for Production** 🚀

---

## 📚 Связанные документы

- `CRYPTO_L2_FIELDS_COMPLETE.md` - L2-поля в indicators и audit_context
- `CRYPTO_L2_INTEGRATION_COMPLETE.md` - L2-фильтры для сигналов
- `L2_METRICS_INTEGRATION.md` - Полная документация L2-метрик
- `python-worker/handlers/base_orderflow_handler.py` - Обновленный Base handler
- `python-worker/handlers/crypto_orderflow_handler.py` - Обновленный Crypto handler

---

## 🔧 Конфигурация

### Environment Variables:

```bash
# Максимальный возраст L2-данных (ms)
L2_MAX_STALE_MS=1500  # default: 1500ms (1.5s)

# Если book-stream отстает больше чем на 1.5s,
# L2-поля не будут приклеены к ctx
```

### Рекомендации:

- ✅ `L2_MAX_STALE_MS=1500` - для крипты (быстрый рынок)
- ✅ `L2_MAX_STALE_MS=2500` - для золота/форекса (медленнее)
- ✅ Мониторить `l2_age_ms` в indicators → если часто >1000ms, book-stream отстает

---

**Дата**: 2025-11-29  
**Версия**: 1.0  
**Статус**: ✅ L2 Staleness Tracking & Auto-Propagation Complete  
**Рекомендация**: Использовать `l2_age_ms` и `l2_is_stale` для анализа качества L2-данных! 📊

