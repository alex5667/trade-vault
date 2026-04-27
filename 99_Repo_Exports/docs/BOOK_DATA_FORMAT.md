# 📖 Формат book_data (Order Book / DOM)

## 🎯 Общее описание

`book_data` — это структура данных, содержащая **снимок книги заявок (Order Book / Depth of Market)** для конкретного символа в определенный момент времени.

Данные поступают из:

- **Binance Futures WebSocket** (`depth20@100ms`) для криптовалют
- **MT5 BookBridge** для традиционных инструментов (Gold, Forex)

---

## 📋 Структура book_data

### Python (Dict[str, Any]):

```python
book_data = {
    "ts": 1732881234567,           # Timestamp в миллисекундах (int)
    "symbol": "BTCUSDT",            # Символ инструмента (str)
    "bids": [                       # Список заявок на покупку (list of lists)
        [96500.50, 1.234],          # [price: float, volume: float]
        [96500.00, 2.456],
        [96499.50, 0.789],
        [96499.00, 3.210],
        [96498.50, 1.567],
        # ... до 20 уровней (depth20)
    ],
    "asks": [                       # Список заявок на продажу (list of lists)
        [96501.00, 0.987],          # [price: float, volume: float]
        [96501.50, 1.543],
        [96502.00, 2.109],
        [96502.50, 0.654],
        [96503.00, 1.876],
        # ... до 20 уровней (depth20)
    ]
}
```

### Go (struct):

```go
type depthUpdate struct {
    EventTime int64      `json:"E"`      // Timestamp (ms)
    UpdateID  int64      `json:"u"`      // Update ID
    Bids      [][]string `json:"bids"`   // [["price", "volume"], ...]
    Asks      [][]string `json:"asks"`   // [["price", "volume"], ...]
    Symbol    string     `json:"s"`      // Symbol (e.g., "BTCUSDT")
}
```

---

## 🔍 Детальное описание полей

### 1. `ts` (timestamp)

- **Тип**: `int` (миллисекунды)
- **Описание**: Unix timestamp в миллисекундах, когда был создан снимок книги
- **Пример**: `1732881234567` → 2025-11-29 12:00:34.567 UTC
- **Использование**: Для определения актуальности данных (staleness check)

```python
# Проверка актуальности book
now_ms = int(time.time() * 1000)
book_age_ms = now_ms - book_data["ts"]
is_stale = book_age_ms > OBI_MAX_STALE_MS  # default 2500ms
```

### 2. `symbol` (опционально)

- **Тип**: `str`
- **Описание**: Символ инструмента (BTCUSDT, ETHUSDT, XAUUSD, etc.)
- **Пример**: `"BTCUSDT"`, `"XAUUSD"`
- **Примечание**: Может отсутствовать в некоторых форматах

### 3. `bids` (заявки на покупку)

- **Тип**: `List[List[float, float]]` или `List[List[str, str]]` (Go)
- **Описание**: Список уровней цен с объемами заявок на покупку
- **Формат**: `[[price, volume], [price, volume], ...]`
- **Сортировка**: По убыванию цены (best bid первый)
- **Количество уровней**: До 20 (depth20)

**Пример**:

```python
bids = [
    [96500.50, 1.234],  # Best bid: цена 96500.50, объем 1.234 BTC
    [96500.00, 2.456],  # Второй уровень
    [96499.50, 0.789],  # Третий уровень
    # ...
]
```

**Интерпретация**:

- `bids[0][0]` — **Best Bid Price** (лучшая цена покупки)
- `bids[0][1]` — **Best Bid Volume** (объем на лучшей цене покупки)
- Чем выше в списке, тем ближе к текущей рыночной цене

### 4. `asks` (заявки на продажу)

- **Тип**: `List[List[float, float]]` или `List[List[str, str]]` (Go)
- **Описание**: Список уровней цен с объемами заявок на продажу
- **Формат**: `[[price, volume], [price, volume], ...]`
- **Сортировка**: По возрастанию цены (best ask первый)
- **Количество уровней**: До 20 (depth20)

**Пример**:

```python
asks = [
    [96501.00, 0.987],  # Best ask: цена 96501.00, объем 0.987 BTC
    [96501.50, 1.543],  # Второй уровень
    [96502.00, 2.109],  # Третий уровень
    # ...
]
```

**Интерпретация**:

- `asks[0][0]` — **Best Ask Price** (лучшая цена продажи)
- `asks[0][1]` — **Best Ask Volume** (объем на лучшей цене продажи)
- Чем выше в списке, тем ближе к текущей рыночной цене

---

## 📊 Примеры book_data

### Пример 1: BTCUSDT (Binance Futures)

```python
book_data = {
    "ts": 1732881234567,
    "symbol": "BTCUSDT",
    "bids": [
        [96500.50, 1.234],   # Best bid: 96500.50 @ 1.234 BTC
        [96500.00, 2.456],
        [96499.50, 0.789],
        [96499.00, 3.210],
        [96498.50, 1.567],
        [96498.00, 0.432],
        [96497.50, 2.109],
        [96497.00, 1.876],
        [96496.50, 0.654],
        [96496.00, 1.321],
        [96495.50, 2.543],
        [96495.00, 0.987],
        [96494.50, 1.765],
        [96494.00, 0.543],
        [96493.50, 2.198],
        [96493.00, 1.432],
        [96492.50, 0.876],
        [96492.00, 1.654],
        [96491.50, 2.321],
        [96491.00, 0.765]
    ],
    "asks": [
        [96501.00, 0.987],   # Best ask: 96501.00 @ 0.987 BTC
        [96501.50, 1.543],
        [96502.00, 2.109],
        [96502.50, 0.654],
        [96503.00, 1.876],
        [96503.50, 0.432],
        [96504.00, 2.198],
        [96504.50, 1.321],
        [96505.00, 0.765],
        [96505.50, 1.987],
        [96506.00, 2.543],
        [96506.50, 0.876],
        [96507.00, 1.654],
        [96507.50, 2.321],
        [96508.00, 0.543],
        [96508.50, 1.765],
        [96509.00, 2.109],
        [96509.50, 0.987],
        [96510.00, 1.432],
        [96510.50, 2.876]
    ]
}
```

**Spread**: `96501.00 - 96500.50 = 0.50 USD` (1 tick)

### Пример 2: XAUUSD (Gold)

```python
book_data = {
    "ts": 1732881234567,
    "symbol": "XAUUSD",
    "bids": [
        [2650.45, 12.5],    # Best bid: 2650.45 @ 12.5 lots
        [2650.40, 8.3],
        [2650.35, 15.7],
        [2650.30, 6.2],
        [2650.25, 10.4],
        # ... до 20 уровней
    ],
    "asks": [
        [2650.50, 9.8],     # Best ask: 2650.50 @ 9.8 lots
        [2650.55, 14.2],
        [2650.60, 7.5],
        [2650.65, 11.3],
        [2650.70, 5.9],
        # ... до 20 уровней
    ]
}
```

**Spread**: `2650.50 - 2650.45 = 0.05 USD` (5 points)

### Пример 3: Минимальный формат (без symbol)

```python
book_data = {
    "ts": 1732881234567,
    "bids": [[96500.50, 1.234], [96500.00, 2.456]],
    "asks": [[96501.00, 0.987], [96501.50, 1.543]]
}
```

---

## 🔧 Использование в коде

### 1. Парсинг book_data из Redis Stream

```python
def _parse_book(self, fields: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Парсит book_data из Redis Stream message.

    Args:
        fields: Поля сообщения из Redis Stream

    Returns:
        book_data dict или None если парсинг не удался
    """
    if not fields:
        return None

    # Извлечение raw данных
    raw = fields.get("data") or fields.get("payload")
    if raw is None:
        return None

    # Преобразование в строку
    raw_s = raw.decode("utf-8") if isinstance(raw, (bytes, bytearray)) else str(raw)

    # Парсинг JSON
    try:
        return json.loads(raw_s) if isinstance(raw_s, str) else None
    except Exception:
        return None
```

### 2. Обработка book_data

```python
def _process_book(self, book_data: Dict[str, Any]) -> None:
    """
    Обрабатывает снимок Order Book.

    Args:
        book_data: DOM данные с bids/asks
    """
    self.processed_books += 1

    # Извлечение timestamp
    ts = int(book_data.get("ts", 0)) or int(time.time() * 1000)

    # Вычисление OBI (Order Book Imbalance)
    real_obi = obi_from_book(book_data, depth=5)
    if real_obi is None:
        return

    # Обновление состояния OBI
    self._last_obi = float(real_obi)
    self._last_obi_ts = ts
    self._track_obi(ts, self._last_obi)
```

### 3. Вычисление OBI (Order Book Imbalance)

```python
def obi_from_book(book: Optional[Dict[str, Any]], depth: int = 5) -> Optional[float]:
    """
    Вычисляет Order Book Imbalance (OBI) из DOM snapshot.

    OBI = (bid_volume - ask_volume) / (bid_volume + ask_volume)

    Args:
        book: Словарь с ключами "bids" и "asks"
              bids: [[price, volume], ...]
              asks: [[price, volume], ...]
        depth: Количество уровней для учета (default 5)

    Returns:
        OBI в диапазоне [-1, 1] или None если book пустой
        +1 = сильное преобладание bid (давление покупателей)
        -1 = сильное преобладание ask (давление продавцов)
         0 = баланс
    """
    if not book:
        return None

    bids = book.get("bids", [])
    asks = book.get("asks", [])

    if not bids or not asks:
        return None

    # Сортировка и выбор top N уровней
    bids_sorted = sorted(bids, key=lambda x: x[0], reverse=True)[:depth]
    asks_sorted = sorted(asks, key=lambda x: x[0])[:depth]

    # Суммирование объемов
    bid_vol = sum(float(level[1]) for level in bids_sorted)
    ask_vol = sum(float(level[1]) for level in asks_sorted)

    total = bid_vol + ask_vol
    if total < 1e-9:
        return None

    # OBI = (bid - ask) / (bid + ask)
    obi = (bid_vol - ask_vol) / total
    return obi
```

**Пример расчета**:

```python
book_data = {
    "bids": [
        [96500.50, 1.234],  # 1.234 BTC
        [96500.00, 2.456],  # 2.456 BTC
        [96499.50, 0.789],  # 0.789 BTC
        [96499.00, 3.210],  # 3.210 BTC
        [96498.50, 1.567],  # 1.567 BTC
    ],
    "asks": [
        [96501.00, 0.987],  # 0.987 BTC
        [96501.50, 1.543],  # 1.543 BTC
        [96502.00, 2.109],  # 2.109 BTC
        [96502.50, 0.654],  # 0.654 BTC
        [96503.00, 1.876],  # 1.876 BTC
    ]
}

bid_vol = 1.234 + 2.456 + 0.789 + 3.210 + 1.567 = 9.256 BTC
ask_vol = 0.987 + 1.543 + 2.109 + 0.654 + 1.876 = 7.169 BTC
total = 9.256 + 7.169 = 16.425 BTC

OBI = (9.256 - 7.169) / 16.425 = 2.087 / 16.425 = +0.127

# Интерпретация: +0.127 = слабое преобладание покупателей (12.7%)
```

### 4. Детекция Iceberg Orders

```python
from signals.orderbook_metrics import BestLevelTracker

tracker = BestLevelTracker(
    min_duration_ms=1500,      # Минимальная длительность "залипания"
    refresh_min_abs=1.0,       # Минимальное увеличение объема для refresh
    refresh_count_target=2     # Целевое количество refresh-ей
)

# При каждом book update
tracker.feed_book(book_data, timestamp_ms)

# Проверка iceberg
if tracker.is_iceberg("bid", timestamp_ms):
    print("🧊 Iceberg order detected at bid level!")
    # Крупный скрытый ордер на покупку
```

### 5. Извлечение Best Bid/Ask

```python
def get_best_bid_ask(book_data: Dict[str, Any]) -> Tuple[Optional[float], Optional[float]]:
    """
    Извлекает лучшие цены bid/ask из book_data.

    Returns:
        (best_bid_price, best_ask_price) или (None, None)
    """
    bids = book_data.get("bids", [])
    asks = book_data.get("asks", [])

    best_bid = float(bids[0][0]) if bids and len(bids[0]) >= 1 else None
    best_ask = float(asks[0][0]) if asks and len(asks[0]) >= 1 else None

    return best_bid, best_ask

# Использование
best_bid, best_ask = get_best_bid_ask(book_data)
if best_bid and best_ask:
    spread = best_ask - best_bid
    mid_price = (best_bid + best_ask) / 2.0
    print(f"Spread: {spread:.2f}, Mid: {mid_price:.2f}")
```

---

## 🌊 Поток данных (Data Flow)

### 1. Binance Futures → Go Worker → Redis Stream

```
Binance WebSocket (depth20@100ms)
    ↓
go-worker/internal/binance/futures_depth_stream.go
    ↓ (парсинг JSON)
depthUpdate struct {
    EventTime: 1732881234567,
    Symbol: "BTCUSDT",
    Bids: [["96500.50", "1.234"], ...],
    Asks: [["96501.00", "0.987"], ...]
}
    ↓ (публикация в Redis)
XADD stream:book_BTCUSDT * data '{"ts":1732881234567,"bids":[[96500.50,1.234],...],"asks":[[96501.00,0.987],...]}'
```

### 2. Redis Stream → Python Worker → Handler

```
Redis Stream: stream:book_BTCUSDT
    ↓ (XREADGROUP)
python-worker/handlers/base_orderflow_handler.py
    ↓ (_parse_book)
book_data = {
    "ts": 1732881234567,
    "bids": [[96500.50, 1.234], ...],
    "asks": [[96501.00, 0.987], ...]
}
    ↓ (_process_book)
OBI calculation → Signal generation
```

---

## 📊 Метрики и использование

### 1. Order Book Imbalance (OBI)

- **Формула**: `OBI = (bid_vol - ask_vol) / (bid_vol + ask_vol)`
- **Диапазон**: `[-1, +1]`
- **Интерпретация**:
  - `OBI > +0.5`: Сильное давление покупателей (bullish)
  - `OBI < -0.5`: Сильное давление продавцов (bearish)
  - `OBI ≈ 0`: Баланс (neutral)

### 2. Spread (спред)

- **Формула**: `spread = best_ask - best_bid`
- **Использование**: Оценка ликвидности рынка
- **Пример**: `96501.00 - 96500.50 = 0.50 USD`

### 3. Mid Price (средняя цена)

- **Формула**: `mid = (best_bid + best_ask) / 2`
- **Использование**: Справедливая рыночная цена
- **Пример**: `(96500.50 + 96501.00) / 2 = 96500.75 USD`

### 4. Depth (глубина рынка)

- **Формула**: `total_bid_vol + total_ask_vol`
- **Использование**: Оценка ликвидности на N уровнях
- **Пример**: `9.256 + 7.169 = 16.425 BTC` (depth=5)

---

## ⚙️ Конфигурация

### Environment Variables:

```bash
# Book stream settings
BTCUSDT_BOOK_STREAM=stream:book_BTCUSDT
ETHUSDT_BOOK_STREAM=stream:book_ETHUSDT
XAUUSD_BOOK_STREAM=stream:book_XAUUSD

# OBI settings
OBI_MAX_STALE_MS=2500              # Максимальный возраст book (мс)
OBI_THRESHOLD=0.5                  # Порог OBI для sustained
OBI_MIN_DURATION=2.0               # Минимальная длительность OBI (сек)

# OBI sustained quality (persistence in window)
OBI_SUSTAINED_USE_FRACTION=true   # Использовать проверку по фракции
OBI_SUSTAINED_MIN_SAMPLES=3       # Минимум сэмплов в окне
OBI_SUSTAINED_MIN_FRACTION=0.6    # Минимум 60% сэмплов должны подтверждать направление

# Iceberg detection
ICEBERG_MIN_DURATION=1500          # Минимальная длительность (мс)
ICEBERG_REFRESH_COUNT=2            # Минимум refresh-ей
ICEBERG_REFRESH_MIN_ABS=1.0        # Минимальное увеличение объема
```

---

## 🔍 Отладка

### Проверка book_data в Redis:

```bash
# Читать последние book updates
redis-cli XREVRANGE stream:book_BTCUSDT + - COUNT 1

# Пример вывода:
1) "1732881234567-0"
2) 1) "data"
   2) "{\"ts\":1732881234567,\"bids\":[[96500.50,1.234],...],\"asks\":[[96501.00,0.987],...]}"
```

### Логирование в Python:

```python
def _process_book(self, book_data: Dict[str, Any]) -> None:
    ts = int(book_data.get("ts", 0))
    bids = book_data.get("bids", [])
    asks = book_data.get("asks", [])

    best_bid = float(bids[0][0]) if bids else None
    best_ask = float(asks[0][0]) if asks else None

    obi = obi_from_book(book_data, depth=5)

    self.logger.debug(
        f"Book update: ts={ts}, best_bid={best_bid}, best_ask={best_ask}, "
        f"obi={obi:.3f}, bids_levels={len(bids)}, asks_levels={len(asks)}"
    )
```

---

## 📚 Связанные файлы

### Go Worker (источник данных):

- `go-worker/internal/binance/futures_depth_stream.go` - WebSocket подключение к Binance
- `go-worker/binance/multiplexed_ws_client.go` - Мультиплексированный клиент

### Python Worker (обработка):

- `python-worker/handlers/base_orderflow_handler.py` - Базовый handler с `_process_book`
- `python-worker/signals/orderbook_metrics.py` - Метрики и детекция iceberg
- `python-worker/signals/detectors.py` - Функция `obi_from_book`
- `python-worker/signals/featurizer.py` - Дополнительные фичи из book

### Документация:

- `documentation/full_guide/data_flow.md` - Полный поток данных
- `documentation/redis_trade_storage_format.md` - Форматы Redis данных

---

## ✅ Резюме

### Ключевые поля book_data:

```python
{
    "ts": int,                    # Timestamp (ms)
    "symbol": str,                # Символ (опционально)
    "bids": [[price, vol], ...],  # Заявки на покупку (до 20 уровней)
    "asks": [[price, vol], ...]   # Заявки на продажу (до 20 уровней)
}
```

### Основные метрики:

- **OBI** (Order Book Imbalance): `(bid_vol - ask_vol) / total_vol`
- **Spread**: `best_ask - best_bid`
- **Mid Price**: `(best_bid + best_ask) / 2`
- **Depth**: Суммарный объем на N уровнях

### Использование:

- ✅ Подтверждение breakout сигналов (OBI confirms)
- ✅ Детекция absorption (OBI противоречит delta)
- ✅ Обнаружение iceberg orders (refresh detection)
- ✅ Оценка ликвидности рынка (depth analysis)

---

**Дата**: 2025-11-29  
**Версия**: 1.0  
**Статус**: ✅ Production
