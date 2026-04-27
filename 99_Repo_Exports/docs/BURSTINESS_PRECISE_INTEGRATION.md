# ✅ Точная интеграция BurstinessTracker

## Выполнено

### 1. Пороги добавлены в `__init__`

```python
# ---- Burst/quality gates (bucket=DELTA_BUCKET_MS) ----
self.min_trades_breakout = int(os.getenv("MIN_TRADES_BREAKOUT", "20"))
self.burst_ratio_min = float(os.getenv("BURST_RATIO_MIN", "1.6"))
self.fano_min = float(os.getenv("FANO_MIN", "1.5"))
self.flip_ratio_max = float(os.getenv("FLIP_RATIO_MAX", "0.70"))
self.imbalance_min = float(os.getenv("IMBALANCE_MIN", "0.20"))  # OBI proxy
```

### 2. ✅ КРИТИЧНО: Исправлен порядок bucket-advance vs on_trade

**Проблема была:** `on_trade()` вызывался до понимания, что бакет сменился, поэтому первый трейд нового бакета засчитывался в старый.

**Решение:**

```python
delta = self._classify_delta(tick)

# текущий bucket id
b = tick.ts // max(self.delta_bucket_ms, 1)

# bucket boundary (первый тик нового бакета)
bucket_changed = (self._bucket_id is not None and b != self._bucket_id)

# 1) закрываем предыдущий бакет ДО учета текущего тика/сделки в новом бакете
if bucket_changed:
    if self.l3 is not None:
        self._l3_last_stats = self.l3.on_bucket_advance(bucket_id=int(b))
    self._burst_last_stats = self.burst.on_bucket_advance(bucket_id=int(b))

# 2) теперь учитываем текущую сделку в НОВОМ бакете
is_trade = bool(tick.flags & 1) or bool(tick.volume and tick.volume > 0)
if is_trade:
    side = self._taker_side(tick)
    if self.l3 is not None:
        self.l3.on_trade(side=side, qty=float(tick.volume or 0.0))
    self.burst.on_trade(ts=int(tick.ts), side=side)

# 3) delta bucketization (и триггер сигналов)
bucket_closed = self._feed_delta_bucket(delta, tick.ts)
```

**Результат:** Теперь трейды корректно попадают в правильный бакет.

### 3. Прокинуты burst-метрики в SignalContext (без getattr)

```python
# ---- attach Burstiness stats (за предыдущий бакет) ----
if self._burst_last_stats is not None:
    bs = self._burst_last_stats
    ctx.burst_trade_count_bucket = int(bs.trade_count_bucket)
    ctx.burst_rate_short = float(bs.rate_short)
    ctx.burst_rate_long = float(bs.rate_long)
    ctx.burst_ratio = float(bs.burst_ratio)
    ctx.burst_cv_dt = float(bs.cv_dt)
    ctx.burst_fano_counts = float(bs.fano_counts)
    ctx.burst_flip_ratio = float(bs.flip_ratio)
```

**Изменение:** Убраны все `getattr()`, используются прямые поля из `BurstStats` dataclass.

### 4. Добавлен хелпер `_burst_gate_ok()`

```python
def _burst_gate_ok(self, ctx: SignalContext) -> bool:
    """Проверка quality gate для burstiness метрик."""
    # imbalance proxy: OBI avg (лучше 20, иначе 5)
    obi_avg_used = float(ctx.obi_avg_20 or 0.0)
    if obi_avg_used == 0.0:
        obi_avg_used = float(ctx.obi_avg or 0.0)
    imbalance = abs(obi_avg_used)

    return (
        int(ctx.burst_trade_count_bucket or 0) >= self.min_trades_breakout
        and float(ctx.burst_ratio or 0.0) >= self.burst_ratio_min
        and float(ctx.burst_fano_counts or 0.0) >= self.fano_min
        and float(ctx.burst_flip_ratio or 0.0) <= self.flip_ratio_max
        and float(imbalance) >= self.imbalance_min
    )
```

### 5. Применено к breakout/extreme

**Breakout:**
```python
if lvl and (z_abs >= self.breakout_z_threshold) and burst_ok:
    breakout_ok = obi_confirms if self.breakout_require_obi else ...
    if breakout_ok and self._cooldown_ok("breakout", lvl, ctx.ts):
        # generate signal
```

**Extreme:**
```python
if z_abs >= self.extreme_z_threshold:
    if (obi_confirms or (not ctx.obi_sustained)) and burst_ok and self._cooldown_ok("extreme", "na", ctx.ts):
        # generate signal
```

## Переменные окружения

```yaml
MIN_TRADES_BREAKOUT=20
BURST_RATIO_MIN=1.6
FANO_MIN=1.5
FLIP_RATIO_MAX=0.70
IMBALANCE_MIN=0.20
```

## Результат

✅ **Все рекомендации выполнены:**
- Пороги объявлены и используются
- Порядок вызовов исправлен (трейды не попадают в прошлый бакет)
- Burst метрики прокидются без getattr
- Quality gate применяется к breakout/extreme
- Хелпер `_burst_gate_ok()` вынесен в отдельный метод

## Статус

✅ **ГОТОВО К ИСПОЛЬЗОВАНИЮ**

