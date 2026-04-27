# ✅ Интеграция BurstinessTracker

## Выполнено

### 1. Создан сервис `services/burstiness_tracker.py`

Реализован класс `BurstinessTracker` с методами:
- `on_trade(ts, side)` - обработка каждой сделки
- `on_bucket_advance(bucket_id)` - обработка перехода на новый бакет, возвращает `BurstStats`

**Метрики:**
- `trade_count_bucket` - количество сделок в бакете
- `rate_short` - EWMA интенсивность короткого окна
- `rate_long` - EWMA интенсивность длинного окна
- `burst_ratio` - отношение короткой к длинной интенсивности
- `cv_dt` - коэффициент вариации интервалов между сделками
- `fano_counts` - Fano factor (дисперсия/среднее) для количества сделок
- `flip_ratio` - доля переключений направления

### 2. Интеграция в `BaseOrderFlowHandler`

#### A) Импорт
```python
from services.burstiness_tracker import BurstinessTracker, BurstStats
```

#### B) Инициализация в `__init__`
```python
# ----- Burstiness tracker
burst_half_life_short_ms = int(os.getenv("BURST_HALF_LIFE_SHORT_MS", "250"))
burst_half_life_long_ms = int(os.getenv("BURST_HALF_LIFE_LONG_MS", "2000"))
burst_fano_window_buckets = int(os.getenv("BURST_FANO_WINDOW_BUCKETS", "60"))
burst_dt_alpha = float(os.getenv("BURST_DT_ALPHA", "0.05"))
self.burst = BurstinessTracker(
    bucket_ms=self.delta_bucket_ms,
    half_life_short_ms=burst_half_life_short_ms,
    half_life_long_ms=burst_half_life_long_ms,
    fano_window_buckets=burst_fano_window_buckets,
    dt_alpha=burst_dt_alpha,
)
self._burst_last_stats: Optional[BurstStats] = None
```

#### C) Использование в `_process_tick()`

**На каждом trade tick:**
```python
# Burstiness tracker: on each trade tick
if is_trade:
    side = self._taker_side(tick)
    self.burst.on_trade(ts=int(tick.ts), side=side)
```

**При переходе на новый бакет:**
```python
# Burstiness tracker: when bucket advances, close previous bucket stats
self._burst_last_stats = self.burst.on_bucket_advance(bucket_id=int(b))
```

#### D) Добавлены поля в `SignalContext`
```python
# Burstiness metrics
burst_trade_count_bucket: int = 0
burst_rate_short: float = 0.0
burst_rate_long: float = 0.0
burst_ratio: float = 0.0
burst_cv_dt: float = 0.0
burst_fano_counts: float = 0.0
burst_flip_ratio: float = 0.0
```

#### E) Заполнение полей в контексте
```python
# attach Burstiness stats
if self._burst_last_stats is not None:
    ctx.burst_trade_count_bucket = int(self._burst_last_stats.trade_count_bucket)
    ctx.burst_rate_short = float(self._burst_last_stats.rate_short)
    ctx.burst_rate_long = float(self._burst_last_stats.rate_long)
    ctx.burst_ratio = float(self._burst_last_stats.burst_ratio)
    ctx.burst_cv_dt = float(self._burst_last_stats.cv_dt)
    ctx.burst_fano_counts = float(self._burst_last_stats.fano_counts)
    ctx.burst_flip_ratio = float(self._burst_last_stats.flip_ratio)
```

#### F) Добавлены поля в `_ctx_l2_debug()` для audit payload
```python
# Burstiness metrics
"burst_trade_count_bucket": int(getattr(ctx, "burst_trade_count_bucket", 0) or 0),
"burst_rate_short": round(float(getattr(ctx, "burst_rate_short", 0.0) or 0.0), 6),
"burst_rate_long": round(float(getattr(ctx, "burst_rate_long", 0.0) or 0.0), 6),
"burst_ratio": round(float(getattr(ctx, "burst_ratio", 0.0) or 0.0), 4),
"burst_cv_dt": round(float(getattr(ctx, "burst_cv_dt", 0.0) or 0.0), 4),
"burst_fano_counts": round(float(getattr(ctx, "burst_fano_counts", 0.0) or 0.0), 4),
"burst_flip_ratio": round(float(getattr(ctx, "burst_flip_ratio", 0.0) or 0.0), 4),
```

## Переменные окружения

Можно настроить через переменные окружения:
- `BURST_HALF_LIFE_SHORT_MS` - полупериод короткого окна (по умолчанию 250 мс)
- `BURST_HALF_LIFE_LONG_MS` - полупериод длинного окна (по умолчанию 2000 мс)
- `BURST_FANO_WINDOW_BUCKETS` - размер окна для Fano factor (по умолчанию 60 бакетов)
- `BURST_DT_ALPHA` - альфа для EWMA интервалов (по умолчанию 0.05)

## Использование

BurstinessTracker автоматически работает для всех handlers, наследующихся от `BaseOrderFlowHandler`:
- `CryptoOrderFlowHandler`
- `XAUUSDOrderFlowHandler`
- И другие handlers

Метрики доступны в:
- `SignalContext` - для логики генерации сигналов
- `_ctx_l2_debug()` - в audit payload и indicators

## Производительность

- O(1) на каждый тик
- Без аллокаций на горячем пути
- Минимальные вычисления (EWMA, rolling window с deque)

## Статус

✅ **ВСЁ ИНТЕГРИРОВАНО И ГОТОВО К ИСПОЛЬЗОВАНИЮ**

