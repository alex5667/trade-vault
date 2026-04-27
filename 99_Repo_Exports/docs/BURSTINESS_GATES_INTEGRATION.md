# ✅ Интеграция Burstiness Quality Gates

## Выполнено

### 1. Подключены пороги в `__init__`

Добавлены пороги после burst-настроек:

```python
# ---- Burst/quality gates (bucket-level) ----
self.min_trades_breakout = int(os.getenv("MIN_TRADES_BREAKOUT", "20"))
self.burst_ratio_min = float(os.getenv("BURST_RATIO_MIN", "1.6"))
self.fano_min = float(os.getenv("FANO_MIN", "1.5"))
self.flip_ratio_max = float(os.getenv("FLIP_RATIO_MAX", "0.70"))
self.imbalance_min = float(os.getenv("IMBALANCE_MIN", "0.20"))  # OBI proxy
```

### 2. Перемещен вызов `on_bucket_advance`

**Было:**
```python
# Burstiness tracker: when bucket advances, close previous bucket stats
self._burst_last_stats = self.burst.on_bucket_advance(bucket_id=int(b))

# Сигналогенерация ... только на границе бакета
if not bucket_closed:
    return
```

**Стало:**
```python
# Сигналогенерация ... только на границе бакета
if not bucket_closed:
    return

# Burstiness tracker: закрыли бакет -> получили stats за предыдущий бакет
self._burst_last_stats = self.burst.on_bucket_advance(bucket_id=int(b))
```

Теперь `on_bucket_advance` вызывается только при закрытии бакета, что дешевле и надежнее.

### 3. Прокинуты burst-метрики в ctx

Добавлено перед `_generate_signals(ctx)`:

```python
# ---- attach Burstiness stats (bucket-level) ----
if self._burst_last_stats is not None:
    bs = self._burst_last_stats
    ctx.burst_trade_count_bucket = int(getattr(bs, "trade_count_bucket", None) or ...)
    ctx.burst_rate_short = float(getattr(bs, "rate_short", 0.0) or 0.0)
    ctx.burst_rate_long = float(getattr(bs, "rate_long", 0.0) or 0.0)
    ctx.burst_ratio = float(getattr(bs, "burst_ratio", None) or ...)
    ctx.burst_cv_dt = float(getattr(bs, "cv_dt", None) or ...)
    ctx.burst_fano_counts = float(getattr(bs, "fano_counts", None) or ...)
    ctx.burst_flip_ratio = float(getattr(bs, "flip_ratio", None) or ...)
```

### 4. Использованы пороги в `_generate_signals()`

#### A) Вычислен imbalance по OBI

```python
# imbalance proxy: используем OBI avg (предпочтительно 20 уровней, если есть)
obi_avg_used = float(getattr(ctx, "obi_avg_20", 0.0) or 0.0)
if obi_avg_used == 0.0:
    obi_avg_used = float(ctx.obi_avg or 0.0)
imbalance = abs(obi_avg_used)
```

#### B) Quality gate

```python
# burst quality gate
burst_ok = (
    (int(getattr(ctx, "burst_trade_count_bucket", 0) or 0) >= self.min_trades_breakout) and
    (float(getattr(ctx, "burst_ratio", 0.0) or 0.0) >= self.burst_ratio_min) and
    (float(getattr(ctx, "burst_fano_counts", 0.0) or 0.0) >= self.fano_min) and
    (float(getattr(ctx, "burst_flip_ratio", 1.0) or 1.0) <= self.flip_ratio_max) and
    (imbalance >= self.imbalance_min)
)
```

#### C) Применено к breakout

```python
if lvl and (z_abs >= self.breakout_z_threshold):
    breakout_ok = obi_confirms if self.breakout_require_obi else ...
    
    # Quality gate: burstiness metrics
    if not burst_ok:
        self.logger.info(
            "Breakout blocked by burst gate: trades=%d ratio=%.3f fano=%.3f flip=%.3f imb=%.3f",
            ...
        )
    
    if breakout_ok and burst_ok and self._cooldown_ok("breakout", lvl, ctx.ts):
        # generate breakout signal
```

#### D) Применено к extreme

```python
if z_abs >= self.extreme_z_threshold:
    if (obi_confirms or (not ctx.obi_sustained)) and burst_ok and self._cooldown_ok("extreme", "na", ctx.ts):
        # generate extreme signal
```

## Переменные окружения для docker-compose

```yaml
MIN_TRADES_BREAKOUT=20
BURST_RATIO_MIN=1.6
FANO_MIN=1.5
FLIP_RATIO_MAX=0.70
IMBALANCE_MIN=0.20
```

## Статус

✅ **ВСЁ ИНТЕГРИРОВАНО И ГОТОВО К ИСПОЛЬЗОВАНИЮ**

- ✅ Пороги объявлены в `__init__`
- ✅ Burst метрики прокидваются в ctx
- ✅ Quality gate применяется к breakout и extreme
- ✅ Логирование при блокировке сигнала

