# Microstructure Integration в BaseOrderFlowHandler

## Обзор изменений

В `base_orderflow_handler.py` добавлены точки расширения для интеграции микроструктурного анализа (особенно для криптовалют).

## Изменения в базовом классе

### 1. Расширение `Tick` dataclass

Добавлено поле для хранения информации о направлении сделки (taker side):

```python
@dataclass
class Tick:
    ts: int
    bid: float
    ask: float
    last: float
    volume: float
    flags: int
    is_buyer_maker: Optional[bool] = None  # ← NEW
```

**Назначение:** Хранит информацию о том, был ли покупатель maker'ом (т.е. продавец был taker).
- `True` = buyer is maker → sell aggression (bearish)
- `False` = buyer is taker → buy aggression (bullish)
- `None` = информация недоступна

### 2. Расширение `SignalContext` dataclass

Добавлены поля для микроструктурных метрик:

```python
@dataclass
class SignalContext:
    # ... existing fields ...
    
    # Microstructure fields for crypto
    spread_bps: float = 0.0              # Текущий спред в базисных пунктах
    realized_bps: float = 0.0            # Реализованный спред последней сделки
    realized_ema_bps: float = 0.0        # EMA реализованного спреда
    adverse_ratio_ema: float = 0.0       # EMA доли неблагоприятных сделок
    market_mode: str = "mixed"           # Режим рынка: "maker", "taker", "mixed"
```

### 3. Парсинг `is_buyer_maker` в `_parse_tick()`

Метод `_parse_tick()` теперь извлекает `is_buyer_maker` из stream fields:

```python
# Fast path: flat field в stream
ibm = fields.get("is_buyer_maker")
if ibm is not None:
    is_buyer_maker = (str(ibm).lower() == "true")

# Fallback: из JSON data
elif tick_json and isinstance(tick_json, dict) and "is_buyer_maker" in tick_json:
    is_buyer_maker = bool(tick_json["is_buyer_maker"])
```

### 4. Hook для обогащения контекста: `_augment_context_microstructure()`

После создания `SignalContext` вызывается хук (если определен в наследнике):

```python
ctx = SignalContext(...)

# Hook: allow subclasses to augment context with microstructure data
if hasattr(self, "_augment_context_microstructure"):
    self._augment_context_microstructure(ctx, tick)

self._generate_signals(ctx)
```

**Использование в наследнике:**

```python
def _augment_context_microstructure(self, ctx: SignalContext, tick: Tick) -> None:
    """Обогащаем контекст микроструктурными метриками."""
    # Обновляем микроструктуру
    self._update_microstructure(tick)
    
    # Заполняем поля контекста
    ctx.spread_bps = self._current_spread_bps
    ctx.realized_bps = self._realized_bps
    ctx.realized_ema_bps = self._realized_ema.value
    ctx.adverse_ratio_ema = self._adverse_ratio_ema.value
    ctx.market_mode = self._detect_market_mode()
```

### 5. Hook для постобработки сигнала: `_postprocess_signal()`

После создания `Signal` и перед формированием envelope вызывается хук:

```python
signal = create_signal(...)
signal.trail_after_tp1 = trail_after_tp1
signal.trail_profile = trail_profile

# Hook: allow subclasses to postprocess signal
if hasattr(self, "_postprocess_signal"):
    self._postprocess_signal(signal, ctx)

# ---- envelope ----
redis_payload = UnifiedSignalFormatter.format_redis_payload(signal)
```

**Использование в наследнике:**

```python
def _postprocess_signal(self, signal: Signal, ctx: SignalContext) -> None:
    """Корректируем трейлинг и добавляем микроструктурные индикаторы."""
    # Adjust trailing based on market mode
    if ctx.market_mode == "maker":
        signal.trail_profile = "conservative_maker"
    elif ctx.market_mode == "taker" and ctx.adverse_ratio_ema < 0.3:
        signal.trail_profile = "aggressive_taker"
    
    # Add microstructure indicators
    signal.indicators["spread_bps"] = round(ctx.spread_bps, 2)
    signal.indicators["realized_ema_bps"] = round(ctx.realized_ema_bps, 2)
    signal.indicators["adverse_ratio_ema"] = round(ctx.adverse_ratio_ema, 4)
    signal.indicators["market_mode"] = ctx.market_mode
```

## Обратная совместимость

Все изменения **полностью обратно совместимы**:

1. ✅ Новые поля в `Tick` и `SignalContext` имеют значения по умолчанию
2. ✅ Хуки вызываются только если определены (`hasattr()` проверка)
3. ✅ Существующие наследники работают без изменений
4. ✅ `is_buyer_maker` опционален — если отсутствует, просто `None`

## Пример использования в CryptoOrderFlowHandler

```python
class CryptoOrderFlowHandler(BaseOrderFlowHandler):
    def __init__(self, symbol: str, config: Optional[OrderFlowConfig] = None):
        super().__init__(symbol, config)
        
        # Microstructure tracking
        self._current_spread_bps = 0.0
        self._realized_bps = 0.0
        self._realized_ema = EMA(alpha=0.1)
        self._adverse_ratio_ema = EMA(alpha=0.05)
        # ... etc
    
    def _augment_context_microstructure(self, ctx: SignalContext, tick: Tick) -> None:
        """Обогащаем контекст микроструктурными метриками."""
        self._update_microstructure(tick)
        
        ctx.spread_bps = self._current_spread_bps
        ctx.realized_bps = self._realized_bps
        ctx.realized_ema_bps = self._realized_ema.value
        ctx.adverse_ratio_ema = self._adverse_ratio_ema.value
        ctx.market_mode = self._detect_market_mode()
    
    def _postprocess_signal(self, signal: Signal, ctx: SignalContext) -> None:
        """Корректируем трейлинг и добавляем индикаторы."""
        # Adjust trailing based on microstructure
        if ctx.market_mode == "maker" or ctx.adverse_ratio_ema > 0.4:
            signal.trail_profile = "conservative_maker"
        elif ctx.market_mode == "taker" and ctx.adverse_ratio_ema < 0.25:
            signal.trail_profile = "aggressive_taker"
        
        # Add microstructure to indicators
        signal.indicators.update({
            "spread_bps": round(ctx.spread_bps, 2),
            "realized_ema_bps": round(ctx.realized_ema_bps, 2),
            "adverse_ratio_ema": round(ctx.adverse_ratio_ema, 4),
            "market_mode": ctx.market_mode,
        })
```

## Преимущества архитектуры

1. **Чистота базового класса** — никакой crypto-специфичной логики
2. **Расширяемость** — наследники могут добавлять свою логику через хуки
3. **Производительность** — хуки вызываются только если определены
4. **Тестируемость** — легко мокировать и тестировать отдельно
5. **Обратная совместимость** — существующий код работает без изменений

## Источник данных

Поле `is_buyer_maker` должно поступать из go-worker через Redis Stream:

- **Binance Futures aggTrade** содержит поле `m` (is buyer maker)
- go-worker должен публиковать это поле в `stream:tick_{symbol}`
- Формат: `{"ts": ..., "bid": ..., "ask": ..., "is_buyer_maker": true/false}`

## См. также

- `crypto_orderflow_handler.py` — полная реализация для криптовалют
- `MICROSTRUCTURE_ANALYSIS.md` — документация по микроструктурному анализу
- Binance API docs: https://binance-docs.github.io/apidocs/futures/en/#aggregate-trade-streams

