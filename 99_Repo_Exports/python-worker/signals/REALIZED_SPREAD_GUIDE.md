# Realized Spread Tracker - Руководство

## Обзор

`RealizedSpreadTracker` — легковесный трекер для микроструктурного анализа криптовалютных рынков. Отслеживает "post-factum" follow-through относительно mid price через заданную задержку (lag).

## Концепция

### Realized Spread

**Realized spread** измеряет, насколько цена продолжила движение в направлении агрессора после сделки:

```
realized_bps = q × (mid_now - trade_price) / trade_price × 10,000
```

Где:
- `q = +1` для taker buy (агрессивная покупка)
- `q = -1` для taker sell (агрессивная продажа)
- `mid_now` — mid price через `lag_ms` после сделки
- `trade_price` — цена сделки

### Интерпретация

| Значение | Интерпретация | Торговое значение |
|----------|---------------|-------------------|
| **> +2 bps** | **Strong Momentum** | Агрессор прав, цена продолжает движение. Высокая вероятность продолжения тренда. |
| **0 to +2 bps** | **Momentum** | Умеренное продолжение движения. |
| **-1 to 0 bps** | **Mixed** | Неопределенное состояние рынка. |
| **< -1 bps** | **Absorption** | Агрессор поглощен, цена развернулась. Возможен разворот или консолидация. |

### Adverse Ratio

**Adverse ratio** — доля сделок, где цена пошла против агрессора:

| Значение | Интерпретация |
|----------|---------------|
| **< 0.3** | Сильный momentum, агрессоры правы |
| **0.3 - 0.5** | Смешанный режим |
| **> 0.5** | Absorption, агрессоры ошибаются |

## Использование

### Базовый пример

```python
from signals.realized_spread import RealizedSpreadTracker

# Создаем трекер с lag 2 секунды
tracker = RealizedSpreadTracker(lag_ms=2000)

# На каждом тике обновляем
metrics = tracker.update(
    ts=1234567890000,  # timestamp в миллисекундах
    bid=50000.0,
    ask=50001.0,
    last=50001.0,
    is_buyer_maker=False,  # buyer is taker => buy aggression
)

# Анализируем метрики
print(f"Realized spread: {metrics.realized_ema_bps:.2f} bps")
print(f"Current spread: {metrics.spread_bps:.2f} bps")
print(f"Adverse ratio: {metrics.adverse_ratio_ema:.2%}")
print(f"Trades processed: {metrics.realized_count}")
```

### Интеграция с BaseOrderFlowHandler

```python
from handlers.base_orderflow_handler import BaseOrderFlowHandler, Tick, SignalContext
from signals.realized_spread import RealizedSpreadTracker, interpret_metrics

class CryptoOrderFlowHandler(BaseOrderFlowHandler):
    def __init__(self, symbol: str, config=None):
        super().__init__(symbol, config)
        
        # Создаем трекер
        self.spread_tracker = RealizedSpreadTracker(
            lag_ms=2000,
            ema_alpha=0.12,
        )
    
    def _augment_context_microstructure(self, ctx: SignalContext, tick: Tick) -> None:
        """Обогащаем контекст микроструктурными метриками."""
        # Обновляем трекер
        metrics = self.spread_tracker.update(
            ts=tick.ts,
            bid=tick.bid,
            ask=tick.ask,
            last=tick.last,
            is_buyer_maker=tick.is_buyer_maker,
        )
        
        # Заполняем контекст
        ctx.spread_bps = metrics.spread_bps
        ctx.realized_bps = metrics.realized_bps
        ctx.realized_ema_bps = metrics.realized_ema_bps
        ctx.adverse_ratio_ema = metrics.adverse_ratio_ema
        ctx.market_mode = interpret_metrics(metrics)
        
        # Логируем периодически
        if metrics.realized_count % 100 == 0:
            self.logger.info(
                "Microstructure: realized=%.2f bps, adverse=%.2%%, mode=%s",
                metrics.realized_ema_bps,
                metrics.adverse_ratio_ema,
                ctx.market_mode
            )
```

### Использование метрик в сигналах

```python
def _postprocess_signal(self, signal: Signal, ctx: SignalContext) -> None:
    """Корректируем трейлинг на основе микроструктуры."""
    
    # Агрессивный трейлинг при strong momentum
    if ctx.market_mode == "strong_momentum" and ctx.adverse_ratio_ema < 0.25:
        signal.trail_profile = "aggressive_taker"
        signal.trail_after_tp1 = True
    
    # Консервативный при absorption
    elif ctx.market_mode == "absorption" or ctx.adverse_ratio_ema > 0.5:
        signal.trail_profile = "conservative_maker"
        signal.trail_after_tp1 = False
    
    # Добавляем метрики в индикаторы
    signal.indicators.update({
        "spread_bps": round(ctx.spread_bps, 2),
        "realized_ema_bps": round(ctx.realized_ema_bps, 2),
        "adverse_ratio_ema": round(ctx.adverse_ratio_ema, 4),
        "market_mode": ctx.market_mode,
    })
```

## Параметры

### Конструктор RealizedSpreadTracker

```python
RealizedSpreadTracker(
    lag_ms=2000,              # Задержка для расчета realized spread
    max_pending=4096,         # Максимум pending trades в очереди
    ema_alpha=0.12,           # Alpha для EMA realized spread
    spread_ema_alpha=0.08,    # Alpha для EMA текущего спреда
    adverse_ema_alpha=0.08,   # Alpha для EMA adverse ratio
    max_gap_ms=30000,         # Максимальный gap для очистки pending
)
```

### Рекомендуемые значения

| Параметр | Crypto (высокая частота) | Crypto (средняя частота) | Forex/Commodities |
|----------|--------------------------|--------------------------|-------------------|
| `lag_ms` | 1000-2000 | 3000-5000 | 5000-10000 |
| `ema_alpha` | 0.15-0.20 | 0.10-0.15 | 0.05-0.10 |
| `max_pending` | 4096 | 2048 | 1024 |

## Метрики

### RealizedSpreadMetrics

```python
@dataclass(frozen=True)
class RealizedSpreadMetrics:
    realized_bps: float          # Последний realized spread
    realized_ema_bps: float      # EMA realized spread
    spread_bps: float            # Текущий L1 спред
    spread_ema_bps: float        # EMA спреда
    adverse_ratio_ema: float     # EMA доли adverse trades
    realized_count: int          # Количество обработанных trades
```

## Источники данных

### Binance Futures aggTrade

Поле `is_buyer_maker` берется из Binance Futures aggTrade stream:

```json
{
  "e": "aggTrade",
  "E": 1234567890000,
  "s": "BTCUSDT",
  "a": 12345,
  "p": "50000.00",
  "q": "0.001",
  "f": 100,
  "l": 105,
  "T": 1234567890000,
  "m": true  // ← is_buyer_maker
}
```

- `m = true` → buyer was maker → **sell aggression** → `q = -1`
- `m = false` → buyer was taker → **buy aggression** → `q = +1`

### go-worker интеграция

go-worker должен публиковать `is_buyer_maker` в `stream:tick_{symbol}`:

```go
// В go-worker/binance/multiplexed_ws_client.go
fields := map[string]interface{}{
    "ts":              timestamp,
    "bid":             bid,
    "ask":             ask,
    "last":            price,
    "volume":          quantity,
    "flags":           flags,
    "is_buyer_maker":  isBuyerMaker,  // ← добавить
}
```

## Производительность

### Сложность операций

- `update()`: **O(1)** amortized (deque операции)
- `get_metrics()`: **O(1)**
- Память: **O(max_pending)** — обычно ~32KB для 4096 trades

### Оптимизации

1. **Без Redis/JSON** — только арифметика и deque
2. **Lazy maturation** — trades матурятся только при новых тиках
3. **Bounded pending** — автоматическая очистка старых trades
4. **EMA вместо скользящего окна** — O(1) память

## Примеры интерпретации

### Сценарий 1: Strong Buy Momentum

```
realized_ema_bps: +5.2
adverse_ratio_ema: 0.18
market_mode: "strong_momentum"

Интерпретация:
- Агрессивные покупки продолжают толкать цену вверх
- Только 18% сделок оказались неудачными
- Высокая вероятность продолжения роста
- Рекомендация: агрессивный трейлинг, tight stops
```

### Сценарий 2: Absorption

```
realized_ema_bps: -3.1
adverse_ratio_ema: 0.68
market_mode: "absorption"

Интерпретация:
- Агрессивные покупки поглощаются рынком
- 68% сделок оказались неудачными
- Возможен разворот или сильная консолидация
- Рекомендация: консервативный трейлинг, широкие stops
```

### Сценарий 3: Mixed Market

```
realized_ema_bps: +0.3
adverse_ratio_ema: 0.45
market_mode: "mixed"

Интерпретация:
- Рынок в неопределенном состоянии
- Примерно 50/50 успешных/неудачных агрессий
- Возможна консолидация или низкая ликвидность
- Рекомендация: стандартный трейлинг, избегать агрессивных входов
```

## Troubleshooting

### Проблема: realized_count всегда 0

**Причина:** Trades не детектируются

**Решение:**
1. Проверьте что `is_buyer_maker` поступает из go-worker
2. Убедитесь что `last` price обновляется
3. Проверьте что `lag_ms` не слишком большой

### Проблема: Метрики не обновляются

**Причина:** Gap в данных

**Решение:**
1. Проверьте что тики поступают регулярно
2. Увеличьте `max_gap_ms` если есть легитимные gaps
3. Проверьте логи на warnings о gap detection

### Проблема: adverse_ratio всегда высокий

**Причина:** Неправильное определение направления агрессии

**Решение:**
1. Проверьте корректность `is_buyer_maker` из go-worker
2. Убедитесь что `q` правильно определяется
3. Проверьте что mid price корректно вычисляется

## См. также

- `base_orderflow_handler.py` — базовый handler с хуками
- `crypto_orderflow_handler.py` — полная реализация для crypto
- `MICROSTRUCTURE_INTEGRATION.md` — документация по интеграции
- Binance API: https://binance-docs.github.io/apidocs/futures/en/#aggregate-trade-streams

