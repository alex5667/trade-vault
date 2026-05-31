# Структура L2Snapshot

## Ответ

**L2Snapshot НЕ содержит исходных массивов bids/asks.**

Он содержит только **вычисленные агрегированные метрики**.

## Структура L2Snapshot

```python
@dataclass
class L2Snapshot:
    """
    Снимок L2-метрик с изменениями.
    
    Attributes:
        m: Полные L2-метрики (L2Metrics)
        ch: Изменения глубины относительно предыдущего снимка (L2Change)
    """
    m: L2Metrics
    ch: L2Change
```

## Структура L2Metrics

`L2Metrics` содержит только агрегированные метрики, **не исходные массивы**:

```python
@dataclass
class L2Metrics:
    # Базовые цены
    ts: int                    # Timestamp (ms)
    best_bid: float           # Лучшая цена покупки
    best_ask: float           # Лучшая цена продажи
    mid: float                # Mid price (best_bid + best_ask) / 2
    spread_bps: float         # Spread в базисных пунктах
    
    # Глубина (суммарный объем)
    depth_bid_5: float        # Bid depth на 5 уровнях
    depth_ask_5: float        # Ask depth на 5 уровнях
    depth_bid_20: float       # Bid depth на 20 уровнях
    depth_ask_20: float       # Ask depth на 20 уровнях
    
    # Order Book Imbalance
    obi_5: float              # OBI на 5 уровнях
    obi_20: float             # OBI на 20 уровнях
    
    # Эластичность (slope)
    slope_bid_20: float       # Bid slope на 20 уровнях
    slope_ask_20: float       # Ask slope на 20 уровнях
    
    # Microprice
    microprice_20: float      # Взвешенная microprice на 20 уровнях
    microprice_shift_bps_20: float  # Отклонение от mid (bps)
    
    # Wall detection
    wall_bid: bool            # True если bid wall обнаружен
    wall_ask: bool            # True если ask wall обнаружен
    wall_bid_dist_bps: float  # Расстояние до bid wall (bps)
    wall_ask_dist_bps: float  # Расстояние до ask wall (bps)
    
    # Top depth (для tracking)
    bid_top3: float           # Bid depth на 3 уровнях
    ask_top3: float           # Ask depth на 3 уровнях
    bid_top5: float           # Bid depth на 5 уровнях (дубликат depth_bid_5)
    ask_top5: float           # Ask depth на 5 уровнях (дубликат depth_ask_5)
```

## Структура L2Change

```python
@dataclass
class L2Change:
    """
    Относительные изменения топ-глубины книги заявок.
    """
    bid_top3_ratio: float = 0.0  # Изменение bid depth на 3 уровнях (ratio)
    ask_top3_ratio: float = 0.0  # Изменение ask depth на 3 уровнях (ratio)
    bid_top5_ratio: float = 0.0  # Изменение bid depth на 5 уровнях (ratio)
    ask_top5_ratio: float = 0.0  # Изменение ask depth на 5 уровнях (ratio)
```

## Где исходные bids/asks?

Исходные массивы `bids` и `asks` (формат `[[price, volume], ...]`) используются только **внутри функции `compute_l2_metrics()`** для вычисления метрик, но **не сохраняются** в `L2Snapshot`.

## Пример использования

```python
# Входные данные (book dict)
book = {
    "ts": 1732881234567,
    "bids": [[96500.50, 1.234], [96500.00, 2.456], ...],
    "asks": [[96501.00, 0.987], [96501.50, 1.543], ...]
}

# Обработка
tracker = L2BookTracker()
snap = tracker.feed(book)

# В snap НЕТ исходных bids/asks
# snap.m.bids  # ❌ НЕ СУЩЕСТВУЕТ
# snap.m.asks  # ❌ НЕ СУЩЕСТВУЕТ

# Но есть агрегированные метрики
print(snap.m.best_bid)        # ✅ 96500.50
print(snap.m.depth_bid_5)     # ✅ суммарный объем на 5 уровнях
print(snap.m.obi_5)           # ✅ imbalance
```

## Если нужны исходные уровни

Если вам нужны исходные массивы bids/asks, вы должны:
1. Сохранить их отдельно перед вызовом `feed()`
2. Или модифицировать `L2Snapshot`/`L2Metrics` для включения исходных данных
3. Или использовать исходный `book` dict напрямую

## Оптимизация

Отсутствие исходных массивов в `L2Snapshot` - это **оптимизация**:
- Меньше памяти
- Быстрее сериализация/десериализация
- Достаточно агрегированных метрик для большинства задач

