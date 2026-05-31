# 🔄 Полный цикл сигнала: от формирования до отчета (2025-11-26)

> Детальное описание полного жизненного цикла торгового сигнала: от момента генерации до финального отчета в Telegram.

---

## 📋 Содержание

1. [Обзор цикла](#обзор-цикла)
2. [Этап 1: Генерация сигнала](#этап-1-генерация-сигнала)
3. [Этап 2: Публикация и распространение](#этап-2-публикация-и-распространение)
4. [Этап 3: Создание виртуальной позиции](#этап-3-создание-виртуальной-позиции)
5. [Этап 4: Отслеживание позиции](#этап-4-отслеживание-позиции)
6. [Этап 5: Обработка событий](#этап-5-обработка-событий)
7. [Этап 6: Закрытие позиции](#этап-6-закрытие-позиции)
8. [Этап 7: Агрегация статистики](#этап-7-агрегация-статистики)
9. [Этап 8: Формирование отчета](#этап-8-формирование-отчета)
10. [Этап 9: Отправка в Telegram](#этап-9-отправка-в-telegram)
11. [Диаграмма последовательности](#диаграмма-последовательности)
12. [FAQ](#faq)

---

## 🎯 Обзор цикла

Полный цикл сигнала состоит из 9 основных этапов:

```
1. Генерация сигнала
   ↓
2. Публикация в Redis Streams
   ↓
3. Создание виртуальной позиции
   ↓
4. Отслеживание тиков и обновление P&L
   ↓
5. Обработка событий (TP1_HIT, SL_HIT, TRAILING_MOVE)
   ↓
6. Закрытие позиции
   ↓
7. Агрегация статистики
   ↓
8. Формирование отчета
   ↓
9. Отправка в Telegram
```

---

## 📡 Этап 1: Генерация сигнала

### Источники сигналов

Сигналы генерируются различными компонентами системы:

| Источник               | Компонент                | Тип сигнала  |
| ---------------------- | ------------------------ | ------------ |
| **OrderFlow Analysis** | `CryptoOrderFlowHandler` | `orderflow`  |
| **Technical Analysis** | `SignalGenerator`        | `ta`         |
| **Aggregated Hub V2**  | `AggregatedSignalHubV2`  | `aggregated` |
| **Manual Signals**     | Ручной ввод через API    | `manual`     |

### Структура сигнала

```json
{
 "sid": "signal-XAUUSD-1731012450",
 "symbol": "XAUUSD",
 "direction": "LONG",
 "entry": 2765.5,
 "tp": [2770.0, 2774.5, 2781.0],
 "sl": 2758.0,
 "confidence": 0.87,
 "trail_after_tp1": true,
 "trail_profile": "rocket_v1",
 "atr": 2.6,
 "regime": "Momentum",
 "strategy": "cryptoorderflow",
 "source": "OrderFlow",
 "tf": "M1",
 "lot": 0.01,
 "ts": 1731012450000
}
```

### Ключевые поля

- **`sid`** — уникальный идентификатор сигнала
- **`symbol`** — торговый инструмент
- **`direction`** — направление (`LONG`/`SHORT`)
- **`entry`** — цена входа
- **`tp`** — массив целей фиксации прибыли (TP1, TP2, TP3)
- **`sl`** — стоп-лосс
- **`trail_after_tp1`** — флаг включения трейлинг стопа после TP1
- **`trail_profile`** — профиль трейлинг стопа
- **`atr`** — значение ATR для расчета трейлинг стопа

### Расчет trail_after_tp1 и trail_profile

Флаг `trail_after_tp1` и профиль `trail_profile` рассчитываются в следующих сервисах:

| Сервис                        | Файл                                                  | Метод/Логика                                                | Описание                                                                 |
| ----------------------------- | ----------------------------------------------------- | ----------------------------------------------------------- | ------------------------------------------------------------------------ |
| **Aggregated Hub V2**         | `python-worker/aggregated_signal_hub_v2.py`           | `step()` — логика выбора на основе `confidence` и `z_delta` | Включает трейлинг для сигналов с `conf >= 0.60`, выбор профиля по силе   |
| **Base OrderFlow Handler**    | `python-worker/handlers/base_orderflow_handler.py`    | `_publish_signal()` — логика на основе `z_delta`            | Включает трейлинг для сигналов с `z_delta >= 4.5`, выбор профиля по силе |
| **TP1 Trailing Orchestrator** | `python-worker/services/tp1_trailing_orchestrator.py` | `_process_tp1_event()` — чтение из сигнала                  | Читает `trail_after_tp1` и `trail_profile` из исходного сигнала          |

#### Логика выбора trail_after_tp1 и trail_profile

**Aggregated Hub V2:**

```python
# Включаем трейлинг для качественных сигналов (conf >= 0.60)
if conf >= 0.60:
    trail_after_tp1 = True

    # Выбор профиля на основе силы сигнала
    if conf >= 0.85 and z_delta >= 6.0:
        trail_profile = "rocket_v1"      # ATR × 0.6 (агрессивный)
    elif conf >= 0.75 and z_delta >= 4.5:
        trail_profile = "rocket_v1"      # ATR × 0.6
    elif conf >= 0.65:
        trail_profile = "lock_and_trail" # ATR × 0.8 (средний)
    else:
        trail_profile = "wide_swing"     # ATR × 1.2 (консервативный)
```

**Base OrderFlow Handler:**

```python
# Включаем трейлинг для сигналов с сильным z_delta
if z_delta >= 4.5:
    trail_after_tp1 = True

    if z_delta >= 6.0:
        trail_profile = "rocket_v1"      # ATR × 0.6
    elif z_delta >= 5.0:
        trail_profile = "lock_and_trail" # ATR × 0.8
    else:
        trail_profile = "lock_and_trail" # ATR × 0.8
```

**Значения по умолчанию:**

- `trail_after_tp1 = False` (трейлинг отключен)
- `trail_profile = "rocket_v1"` (агрессивный профиль)

### Расчет ATR (Average True Range)

Значение ATR рассчитывается или получается из следующих источников:

| Сервис                       | Файл                                                 | Метод/Функция                      | Описание                                                           |
| ---------------------------- | ---------------------------------------------------- | ---------------------------------- | ------------------------------------------------------------------ |
| **Base OrderFlow Handler**   | `python-worker/handlers/base_orderflow_handler.py`   | `_get_atr()`                       | Приоритет: локальный кэш → Redis → локальный расчет → оценка       |
| **ATR Calculator**           | `python-worker/signals/atr.py`                       | `ATR.feed_tick()` / `ATR.value()`  | Локальный расчет ATR на основе тиковых данных (Wilder's smoothing) |
| **ATR from Candles**         | `python-worker/services/atr_from_candles.py`         | `ATRState.feed()`                  | Расчет ATR из свечей (Wilder's smoothing)                          |
| **Go Gateway ATR**           | `go-gateway/internal/runtime/atr.go`                 | `ATRProvider.GetATR()`             | Расчет ATR в Go на основе баров из Redis                           |
| **Crypto OrderFlow Service** | `python-worker/services/crypto_orderflow_service.py` | `_calculate_levels()`              | Приоритет: индикаторы → кэш → fallback значение                    |
| **ATR Cache**                | `python-worker/utils/atr_cache.py`                   | `ATRCache.get_atr()` / `set_atr()` | Кэширование ATR значений для оптимизации                           |

#### Источники ATR (приоритет)

**1. Локальный кэш (ATRCache):**

```python
cached_atr = self._atr_cache.get_atr(symbol, timeframe)
```

**2. Redis кэш:**

- `ta:last:atr:{SYMBOL}` — JSON формат от go-gateway
- `atr:val:{SYMBOL}:{TF}` — новый формат
- `atr:{SYMBOL}:{TF}` — старый формат (legacy)

**3. Локальный расчет:**

```python
# На основе тиковых данных
atr_calculator.feed_tick(price, ts)
atr_val = atr_calculator.value()  # Wilder's smoothing
```

**4. Fallback значение:**

```python
atr = cfg.get("fallback_atr", 1.0)  # Значение по умолчанию из конфигурации
```

#### Конфигурация ATR

- `ATR_SOURCE`: `'redis'` | `'ticks'` | `'auto'` — источник ATR
- `ATR_TF`: таймфрейм для ATR (по умолчанию: `'1m'`)
- `ATR_PERIOD`: период для расчета ATR (по умолчанию: `14`)
- `ATR_STALE_SEC`: максимальный возраст ATR в секундах (по умолчанию: `90`)

### Расчет TP и SL уровней

Уровни TP и SL рассчитываются в следующих сервисах:

| Сервис                       | Файл                                                 | Метод/Функция         | Описание                                                                               |
| ---------------------------- | ---------------------------------------------------- | --------------------- | -------------------------------------------------------------------------------------- |
| **Risk Levels**              | `python-worker/signals/risk_levels.py`               | `compute_levels()`    | Универсальная функция расчета TP/SL на основе ATR, процентов или фиксированных пунктов |
| **Crypto OrderFlow**         | `python-worker/services/crypto_orderflow_service.py` | `_calculate_levels()` | Расчет для OrderFlow сигналов с использованием ATR из индикаторов или кэша             |
| **Unified Signal Generator** | `python-worker/core/unified_signal_generator.py`     | `_create_signal()`    | Расчет для универсальных сигналов (ATR-based с фиксированными множителями)             |
| **Trade Monitor**            | `python-worker/services/trade_monitor.py`            | `process_signal()`    | Fallback расчет, если уровни не предоставлены в сигнале                                |

#### Методы расчета

**1. ATR-based (наиболее распространенный):**

```python
stop_dist = atr * stop_atr_mult  # По умолчанию: 0.6 * ATR
sl = entry - stop_dist  # Для LONG
sl = entry + stop_dist  # Для SHORT

# TP на основе Risk-Reward
tp_levels = [entry + stop_dist * rr for rr in [1.3, 2.0, 2.7]]  # Для LONG
```

**2. Percentage-based:**

```python
stop_dist = entry * stop_pct / 100  # Например: 0.2% от цены входа
```

**3. Fixed points:**

```python
stop_dist = stop_points  # Фиксированное значение в пунктах
```

**Конфигурация:**

- `STOP_MODE`: `'ATR'` | `'PCT'` | `'POINTS'` — **текущее значение: `'ATR'`** (используется по умолчанию во всех сервисах)
- `STOP_ATR_MULT`: множитель ATR для стоп-лосса (по умолчанию: `0.6`)
- `STOP_PCT`: процент для SL при `STOP_MODE='PCT'` (по умолчанию: `0.2%`)
- `STOP_POINTS`: фиксированные пункты для SL при `STOP_MODE='POINTS'` (по умолчанию: `1.0`)
- `TP_MODE`: `'RR'` (Risk-Reward) | `'ATR'` (ATR multiples) — **текущее значение: `'RR'`**
- `TP_RR`: строки с RR коэффициентами (по умолчанию: `"1.3,2.0,2.7"` или `"1,2,3"`)
- `TP_ATR_MULTS`: множители ATR для TP (например: `"0.6,1.0,1.5"`)

**Текущая конфигурация в системе:**

- `STOP_MODE=ATR` (установлено в `docker-compose.yml` и используется по умолчанию во всех сервисах)
- `STOP_ATR_MULT=0.6` (стоп-лосс = ATR × 0.6)
- `TP_MODE=RR` (Take Profit на основе Risk-Reward)
- `TP_RR="1,2,3"` — используется в большинстве сервисов (по умолчанию)
- `TP_RR="1.3,2.0,2.7"` — используется только в `CryptoOrderflowService`

**Детализация по сервисам:**

| Сервис                       | Файл                                                 | Значение по умолчанию | Источник значения              |
| ---------------------------- | ---------------------------------------------------- | --------------------- | ------------------------------ |
| **Crypto OrderFlow Service** | `python-worker/services/crypto_orderflow_service.py` | `"1.3,2.0,2.7"`       | `DEFAULT_CONFIG["tp_rr"]`      |
| **XAU OrderFlow Handler**    | `python-worker/handlers/xau_orderflow_handler.py`    | `"1,2,3"`             | `os.getenv("TP_RR", "1,2,3")`  |
| **Instrument Config**        | `python-worker/core/instrument_config.py`            | `"1,2,3"`             | `tp_rr: str = "1,2,3"`         |
| **Risk Levels**              | `python-worker/signals/risk_levels.py`               | `"1,2,3"`             | `cfg.get("TP_RR", "1,2,3")`    |
| **Docker Compose**           | `docker-compose.yml`                                 | `"1,2,3"`             | `TP_RR=1,2,3` (env переменная) |

---

## 📤 Этап 2: Публикация и распространение

### Каналы публикации

После генерации сигнал публикуется в несколько каналов:

#### 1. Redis Streams для аналитики

```python
# signals:orderflow:<symbol>
redis.xadd("signals:orderflow:XAUUSD", {
    "sid": signal.sid,
    "symbol": signal.symbol,
    "direction": signal.direction,
    "entry": signal.entry,
    "tp": json.dumps(signal.tp),
    "sl": signal.sl,
    "confidence": signal.confidence,
    "trail_after_tp1": "1" if signal.trail_after_tp1 else "0",
    "trail_profile": signal.trail_profile,
    "atr": signal.atr,
    "strategy": signal.strategy,
    "source": signal.source,
    "tf": signal.tf,
    "ts": signal.ts
})

# signals:audit:<symbol> (расширенный payload)
redis.xadd("signals:audit:XAUUSD", {
    "sid": signal.sid,
    "payload": json.dumps(signal_payload),
    "ts": signal.ts
})
```

#### 2. Telegram уведомление

```python
# notify:telegram (type=signal)
redis.xadd("notify:telegram", {
    "type": "signal",
    "sid": signal.sid,
    "symbol": signal.symbol,
    "side": signal.direction,
    "entry": signal.entry,
    "sl": signal.sl,
    "tp_levels": json.dumps(signal.tp),
    "confidence": signal.confidence,
    "source": signal.source,
    "timestamp": str(signal.ts)
})
```

#### 3. Очередь ордеров (опционально)

```python
# orders:queue (если orders_queue_enabled=True)
redis.lpush("orders:queue", json.dumps({
    "action": "open",
    "signal_id": signal.sid,
    "symbol": signal.symbol,
    "direction": signal.direction,
    "entry": signal.entry,
    "tp": signal.tp,
    "sl": signal.sl,
    "lot": signal.lot
}))
```

#### 4. Manual signals stream (для внешних интеграций)

```python
# stream:manual-signals
redis.xadd("stream:manual-signals", {
    "sid": signal.sid,
    "symbol": signal.symbol,
    "direction": signal.direction,
    "entry": signal.entry,
    "tp": json.dumps(signal.tp),
    "sl": signal.sl,
    "source": signal.source,
    "ts": signal.ts
})
```

---

## 🎯 Этап 3: Создание виртуальной позиции

### Signal Performance Tracker

`Signal Performance Tracker` читает сигналы из Redis Streams и создает виртуальные позиции:

```python
# Чтение сигналов из streams
messages = redis.xreadgroup(
    group="tracker-group",
    consumer="tracker-1",
    streams={"signals:orderflow:XAUUSD": ">"},
    count=100,
    block=1000
)

for stream, msgs in messages:
    for msg_id, data in msgs:
        # Создание виртуальной позиции
        position = trade_monitor.create_position(
            signal_id=data["sid"],
            strategy=data["strategy"],
            symbol=data["symbol"],
            tf=data["tf"],
            source=data["source"],
            direction=data["direction"],
            entry_price=float(data["entry"]),
            lot=float(data.get("lot", 0.01)),
            sl=float(data["sl"]),
            tp_levels=json.loads(data["tp"]),
            signal_payload=data
        )
```

### Структура виртуальной позиции

```python
@dataclass
class Position:
    id: str                          # Уникальный ID позиции
    signal_id: str                   # ID сигнала
    strategy: str                    # Стратегия
    symbol: str                      # Символ
    tf: str                          # Таймфрейм
    source: str                      # Источник сигнала
    direction: str                   # LONG/SHORT
    entry_price: float              # Цена входа
    entry_time: float                # Время входа (timestamp)
    lot: float                       # Размер позиции
    sl: float                        # Стоп-лосс
    tp_levels: List[float]          # [TP1, TP2, TP3]
    remaining_lot: float            # Остаток позиции
    tp_hits: int                    # Количество достигнутых TP
    tp1_hit: bool                   # Флаг достижения TP1
    tp2_hit: bool                   # Флаг достижения TP2
    tp3_hit: bool                   # Флаг достижения TP3
    closed: bool                    # Флаг закрытия позиции
    close_time: Optional[float]     # Время закрытия
    realized_pnl: float             # Реализованная прибыль/убыток
    tp_before_sl: int               # Сколько TP было достигнуто до SL
    trailing_started: bool           # Флаг запуска трейлинг стопа
    trailing_active: bool            # Флаг активности трейлинг стопа
    signal_payload: Dict            # Полный payload сигнала
```

### Сохранение в Redis

```python
# order:{position_id}
redis.hset(f"order:{position.id}", mapping={
    "signal_id": position.signal_id,
    "strategy": position.strategy,
    "symbol": position.symbol,
    "direction": position.direction,
    "entry_price": position.entry_price,
    "entry_time": position.entry_time,
    "sl": position.sl,
    "tp_levels": json.dumps(position.tp_levels),
    "lot": position.lot,
    "remaining_lot": position.remaining_lot,
    "status": "open"
})
```

### Публикация события

```python
# events:trades
trade_events_logger.log_position_opened(
    signal_id=position.signal_id,
    position_id=position.id,
    symbol=position.symbol,
    entry_price=position.entry_price,
    direction=position.direction,
    lot=position.lot,
    tp_levels=position.tp_levels,
    sl=position.sl
)
```

---

## 📊 Этап 4: Отслеживание позиции

### Чтение тиков

`Trade Monitor` читает тики из `stream:tick_<symbol>` и обновляет P&L позиций:

```python
# Чтение тиков
messages = redis.xreadgroup(
    group="tracker-tick-group",
    consumer="tracker-tick-1",
    streams={"stream:tick_XAUUSD": ">"},
    count=200,
    block=1000
)

for stream, msgs in messages:
    for msg_id, data in msgs:
        tick_price = float(data["price"])
        tick_time = int(data["ts"])

        # Обновление всех открытых позиций по символу
        for position in open_positions[symbol]:
            if not position.closed:
                # Расчет текущего P&L
                current_pnl = calculate_pnl(
                    position=position,
                    current_price=tick_price
                )

                # Проверка достижения TP/SL
                check_tp_sl(position, tick_price, tick_time)
```

### Расчет P&L

```python
def calculate_pnl(position: Position, current_price: float) -> float:
    """Расчет текущего P&L для позиции."""
    if position.direction == "LONG":
        price_diff = current_price - position.entry_price
    else:  # SHORT
        price_diff = position.entry_price - current_price

    # Расчет P&L с учетом размера позиции
    # Упрощенная формула (для реальной системы нужен tick_value)
    pnl = price_diff * position.remaining_lot * 100  # Пример для XAUUSD

    return pnl
```

### Проверка TP/SL

```python
def check_tp_sl(position: Position, price: float, timestamp: int):
    """Проверка достижения уровней TP/SL."""
    if position.direction == "LONG":
        # Проверка TP
        if not position.tp1_hit and price >= position.tp_levels[0]:
            handle_tp1_hit(position, price, timestamp)
        elif not position.tp2_hit and price >= position.tp_levels[1]:
            handle_tp2_hit(position, price, timestamp)
        elif not position.tp3_hit and price >= position.tp_levels[2]:
            handle_tp3_hit(position, price, timestamp)

        # Проверка SL
        if price <= position.sl:
            handle_sl_hit(position, price, timestamp)
    else:  # SHORT
        # Аналогично для SHORT позиций (инвертированные условия)
        ...
```

---

## 🎯 Этап 5: Обработка событий

### Типы событий

| Событие            | Описание                                | Источник                  |
| ------------------ | --------------------------------------- | ------------------------- |
| `TP1_HIT`          | Достижение первой цели фиксации прибыли | Trade Monitor / MT5       |
| `TP2_HIT`          | Достижение второй цели                  | Trade Monitor / MT5       |
| `TP3_HIT`          | Достижение третьей цели                 | Trade Monitor / MT5       |
| `SL_HIT`           | Срабатывание стоп-лосса                 | Trade Monitor / MT5       |
| `TRAILING_STARTED` | Запуск трейлинг стопа после TP1         | TP1 Trailing Orchestrator |
| `TRAILING_MOVE`    | Перемещение трейлинг стопа              | MT5 / Trailing Dispatcher |
| `POSITION_CLOSED`  | Закрытие позиции                        | Trade Monitor / MT5       |

### Обработка TP1_HIT

```python
def handle_tp1_hit(position: Position, price: float, timestamp: int):
    """Обработка достижения TP1."""
    # Частичное закрытие (50% позиции)
    close_lot = position.remaining_lot * 0.5
    position.remaining_lot -= close_lot

    # Обновление флагов
    position.tp1_hit = True
    position.tp_hits = 1
    position.tp_before_sl = 1

    # Расчет реализованной прибыли
    if position.direction == "LONG":
        pnl = (price - position.entry_price) * close_lot * 100
    else:
        pnl = (position.entry_price - price) * close_lot * 100

    position.realized_pnl += pnl

    # Публикация события
    trade_events_logger.log_tp_hit(
        signal_id=position.signal_id,
        position_id=position.id,
        tp_level=1,
        price=price,
        lot=close_lot,
        pnl=pnl,
        timestamp=timestamp
    )

    # Запуск трейлинг стопа (если включен)
    if position.signal_payload.get("trail_after_tp1"):
        trailing_orchestrator.start_trailing(
            signal_id=position.signal_id,
            position_id=position.id,
            symbol=position.symbol,
            profile=position.signal_payload.get("trail_profile", "rocket_v1")
        )
```

### Обработка TRAILING_MOVE

```python
def handle_trailing_move(position: Position, new_sl: float, timestamp: int):
    """Обработка перемещения трейлинг стопа."""
    old_sl = position.sl
    position.sl = new_sl
    position.trailing_active = True

    # Публикация события
    trade_events_logger.log_trailing_move(
        signal_id=position.signal_id,
        position_id=position.id,
        old_sl=old_sl,
        new_sl=new_sl,
        timestamp=timestamp
    )

    # Обновление в Redis
    redis.hset(f"order:{position.id}", mapping={
        "sl": new_sl,
        "trailing_active": "1"
    })
```

### Обработка SL_HIT

```python
def handle_sl_hit(position: Position, price: float, timestamp: int):
    """Обработка срабатывания стоп-лосса."""
    # Закрытие остатка позиции
    close_lot = position.remaining_lot

    # Расчет убытка
    if position.direction == "LONG":
        pnl = (price - position.entry_price) * close_lot * 100
    else:
        pnl = (position.entry_price - price) * close_lot * 100

    position.realized_pnl += pnl
    position.remaining_lot = 0
    position.closed = True
    position.close_time = timestamp

    # Определение причины закрытия
    close_reason = "SL"
    if position.tp_before_sl > 0:
        close_reason = f"SL_AFTER_TP{position.tp_before_sl}"

    # Публикация события
    trade_events_logger.log_position_closed(
        signal_id=position.signal_id,
        position_id=position.id,
        close_price=price,
        close_reason=close_reason,
        pnl=position.realized_pnl,
        timestamp=timestamp
    )
```

---

## 🏁 Этап 6: Закрытие позиции

### Финальные расчеты

```python
def finalize_position(position: Position):
    """Финализация закрытой позиции."""
    # Расчет итогового P&L
    total_pnl = position.realized_pnl

    # Расчет процента P&L
    if position.direction == "LONG":
        pnl_pct = (position.close_price - position.entry_price) / position.entry_price * 100
    else:
        pnl_pct = (position.entry_price - position.close_price) / position.entry_price * 100

    # Определение результата
    trade_result = "win" if total_pnl > 0 else ("loss" if total_pnl < 0 else "breakeven")

    # Сохранение в trades:closed
    redis.hset(f"trades:closed:{position.signal_id}", mapping={
        "signal_id": position.signal_id,
        "position_id": position.id,
        "strategy": position.strategy,
        "symbol": position.symbol,
        "direction": position.direction,
        "entry_price": position.entry_price,
        "close_price": position.close_price,
        "pnl": total_pnl,
        "pnl_pct": pnl_pct,
        "trade_result": trade_result,
        "tp1_hit": "1" if position.tp1_hit else "0",
        "tp2_hit": "1" if position.tp2_hit else "0",
        "tp3_hit": "1" if position.tp3_hit else "0",
        "tp_before_sl": position.tp_before_sl,
        "trailing_started": "1" if position.trailing_started else "0",
        "trailing_active": "1" if position.trailing_active else "0",
        "close_reason": close_reason,
        "entry_time": position.entry_time,
        "close_time": position.close_time
    })
```

---

## 📈 Этап 7: Агрегация статистики

### Stats Aggregator

`Stats Aggregator` обновляет статистику в Redis Hash `stats:{strategy}:{symbol}:{tf}` и проверяет счетчик для автоматической отправки отчетов:

```python
def update_stats(position: Position):
    """Обновление статистики после закрытия позиции."""
    key = f"stats:{position.strategy}:{position.symbol}:{position.tf}"

    # Инкремент счетчиков
    redis.hincrby(key, "total_trades", 1)

    if position.realized_pnl > 0:
        redis.hincrby(key, "wins", 1)
    elif position.realized_pnl < 0:
        redis.hincrby(key, "losses", 1)

    # Обновление P&L
    redis.hincrbyfloat(key, "total_pnl", position.realized_pnl)

    # Обновление TP метрик
    if position.tp1_hit:
        redis.hincrby(key, "tp1_hits", 1)
    if position.tp2_hit:
        redis.hincrby(key, "tp2_hits", 1)
    if position.tp3_hit:
        redis.hincrby(key, "tp3_hits", 1)

    # Упущенная прибыль
    if position.tp_before_sl > 0:
        if position.tp_before_sl >= 1:
            redis.hincrby(key, "tp1_then_sl", 1)
        if position.tp_before_sl >= 2:
            redis.hincrby(key, "tp2_then_sl", 1)
        if position.tp_before_sl >= 3:
            redis.hincrby(key, "tp3_then_sl", 1)

    # Трейлинг метрики
    if position.trailing_started:
        redis.hincrby(key, "trailing_started", 1)
    if position.close_reason == "TRAILING_STOP":
        redis.hincrby(key, "trailing_stop_hits", 1)

    # Пересчет winrate
    total = int(redis.hget(key, "total_trades") or 0)
    wins = int(redis.hget(key, "wins") or 0)
    winrate = (wins / total * 100) if total > 0 else 0
    redis.hset(key, "winrate", winrate)

    # Пересчет среднего P&L
    total_pnl = float(redis.hget(key, "total_pnl") or 0)
    avg_pnl = total_pnl / total if total > 0 else 0
    redis.hset(key, "avg_pnl", avg_pnl)
```

### Структура статистики

```json
{
 "total_trades": 150,
 "wins": 95,
 "losses": 55,
 "winrate": 63.33,
 "tp1_hits": 120,
 "tp2_hits": 80,
 "tp3_hits": 45,
 "tp1_then_sl": 25,
 "tp2_then_sl": 15,
 "tp3_then_sl": 5,
 "total_pnl": 1250.5,
 "avg_pnl": 8.34,
 "trailing_started": 100,
 "trailing_stop_hits": 30
}
```

---

## 📝 Этап 8: Формирование отчета

### Periodic Reporter

`Periodic Reporter` автоматически отправляет отчеты каждые 100 сделок (настраивается через `REPORT_TRIGGER_COUNT`):

```python
# В StatsAggregator при закрытии каждой сделки
from services.periodic_reporter import check_and_trigger_report

# Увеличение счетчика и проверка лимита
check_and_trigger_report(source, symbol, counter_type="trades")

# Логика в PeriodicReporter:
counter_key = f"report_counter:trades:{source}:{symbol}"
count = redis.incr(counter_key)

if count >= REPORT_TRIGGER_COUNT:  # По умолчанию 100
    # Отправка отчета для пары source/symbol
    send_report_for_pair(source, symbol)
    # Сброс счетчика
    redis.delete(counter_key)
```

### Reporting Service

`Reporting Service` формирует HTML-отчеты на основе собранной статистики:

```python
def generate_report(strategy: str, symbol: str, tf: str) -> str:
    """Генерация HTML-отчета."""
    # Получение статистики
    stats = StatsAggregator.get_stats(redis, strategy, symbol, tf)

    # Формирование HTML
    html = f"""
    <b>📊 Отчет по стратегии: {strategy}</b>
    <b>Символ:</b> {symbol} | <b>Таймфрейм:</b> {tf}

    <b>Общие показатели:</b>
    • Всего сделок: {stats['total_trades']}
    • Прибыльных: {stats['wins']}
    • Убыточных: {stats['losses']}
    • Winrate: {stats['winrate']:.2f}%

    <b>TP метрики:</b>
    • TP1 достигнуто: {stats['tp1_hits']}
    • TP2 достигнуто: {stats['tp2_hits']}
    • TP3 достигнуто: {stats['tp3_hits']}

    <b>Упущенная прибыль:</b>
    • TP1 → SL: {stats['tp1_then_sl']}
    • TP2 → SL: {stats['tp2_then_sl']}
    • TP3 → SL: {stats['tp3_then_sl']}

    <b>P&L:</b>
    • Общий P&L: ${stats['total_pnl']:.2f}
    • Средний P&L: ${stats['avg_pnl']:.2f}

    <b>Трейлинг:</b>
    • Трейлинг запущен: {stats['trailing_started']}
    • Закрыто по трейлинг стопу: {stats['trailing_stop_hits']}
    """

    return html
```

### Автоматические отчеты по счетчику сделок

Отчеты отправляются автоматически каждые 100 сделок (настраивается через `REPORT_TRIGGER_COUNT`) через `PeriodicReporter`:

```python
# В StatsAggregator при закрытии каждой сделки
from services.periodic_reporter import check_and_trigger_report

# Увеличение счетчика для пары source/symbol
check_and_trigger_report(source, symbol, counter_type="trades")

# Логика в PeriodicReporter._check_and_trigger_report():
counter_key = f"report_counter:trades:{source}:{symbol}"
count = redis.incr(counter_key)  # Увеличение счетчика

if count >= REPORT_TRIGGER_COUNT:  # По умолчанию 100
    # Отправка отчета для конкретной пары source/symbol
    send_report_for_pair(source, symbol)
    # Сброс счетчика после отправки
    redis.delete(counter_key)
```

**Особенности:**

- Счетчик ведется отдельно для каждой пары `{source}/{symbol}` (например, `CryptoOrderFlow/BTCUSDT`, `OrderFlow/XAUUSD`)
- Счетчик увеличивается при каждой закрытой сделке
- Отчет отправляется при достижении лимита (100 сделок по умолчанию)
- После отправки счетчик сбрасывается

### Периодические отчеты по времени (опционально)

```python
def send_periodic_report():
    """Отправка периодического отчета по времени."""
    # Сбор статистики по всем стратегиям
    all_stats = {}

    for strategy in ["cryptoorderflow", "ta", "aggregated"]:
        for symbol in ["XAUUSD", "BTCUSDT", "ETHUSDT"]:
            for tf in ["M1", "M5", "M15"]:
                stats = StatsAggregator.get_stats(redis, strategy, symbol, tf)
                if stats.get("total_trades", 0) > 0:
                    all_stats[f"{strategy}:{symbol}:{tf}"] = stats

    # Формирование сводного отчета
    report = generate_summary_report(all_stats)

    # Публикация в notify:telegram
    redis.xadd("notify:telegram", {
        "type": "report",
        "text": report,
        "source": "ReportingService",
        "timestamp": str(int(time.time() * 1000))
    })
```

**Примечание:** Периодические отчеты по времени (каждые N часов) поддерживаются в `SignalPerformanceTracker`, но основная логика отправки отчетов работает по счетчику сделок через `PeriodicReporter`.

---

## 📱 Этап 9: Отправка в Telegram

### Telegram Worker

`Telegram Worker` читает сообщения из `notify:telegram` и отправляет их в Telegram:

```python
def process_notify_messages():
    """Обработка сообщений из notify:telegram."""
    messages = redis.xreadgroup(
        group="notify-group",
        consumer="telegram-worker-1",
        streams={"notify:telegram": ">"},
        count=100,
        block=1000
    )

    for stream, msgs in messages:
        for msg_id, data in msgs:
            msg_type = data.get("type")

            if msg_type == "signal":
                # Форматирование сигнала
                message = format_signal_message(data)
                send_telegram_message(message)

            elif msg_type == "report":
                # Отправка HTML-отчета
                html_text = data.get("text")
                send_html_to_telegram(html_text)

            # Подтверждение обработки
            redis.xack("notify:telegram", "notify-group", msg_id)
```

### Форматирование сообщений

```python
def format_signal_message(data: Dict) -> str:
    """Форматирование сообщения о сигнале."""
    direction_emoji = "🟢" if data["side"] == "LONG" else "🔴"

    message = f"""
    {direction_emoji} <b>Новый сигнал</b>

    <b>Символ:</b> {data['symbol']}
    <b>Направление:</b> {data['side']}
    <b>Вход:</b> {data['entry']}
    <b>SL:</b> {data['sl']}
    <b>TP:</b> {', '.join(json.loads(data['tp_levels']))}
    <b>Уверенность:</b> {data.get('confidence', 'N/A')}%
    <b>Источник:</b> {data.get('source', 'N/A')}
    """

    return message
```

---

## 🔄 Диаграмма последовательности

```
Signal Generator
    │
    ├─► Redis Streams (signals:orderflow:*)
    │
Signal Performance Tracker
    │
    ├─► Trade Monitor.create_position()
    │       │
    │       ├─► Redis (order:{position_id})
    │       └─► events:trades (POSITION_OPENED)
    │
    ├─► Чтение тиков (stream:tick_*)
    │       │
    │       ├─► Обновление P&L
    │       └─► Проверка TP/SL
    │
    ├─► Обработка событий
    │       │
    │       ├─► TP1_HIT → Частичное закрытие + Трейлинг
    │       ├─► TRAILING_MOVE → Обновление SL
    │       └─► SL_HIT → Закрытие позиции
    │
    ├─► Stats Aggregator.update_stats()
    │       │
    │       └─► Redis (stats:{strategy}:{symbol}:{tf})
    │
    ├─► Reporting Service.generate_report()
    │       │
    │       └─► notify:telegram (type=report)
    │
Telegram Worker
    │
    └─► Telegram Bot API
```

---

## ❓ FAQ

### Как отслеживается виртуальная позиция?

Виртуальная позиция создается в `Trade Monitor` при получении сигнала и отслеживается по тикам из `stream:tick_<symbol>`. P&L обновляется в реальном времени.

### Что происходит при достижении TP1?

При достижении TP1:

1. Закрывается 50% позиции
2. Фиксируется прибыль
3. Запускается трейлинг стоп (если `trail_after_tp1=True`)
4. Публикуется событие `TP1_HIT`

### Как рассчитывается статистика?

Статистика агрегируется в `Stats Aggregator` и сохраняется в Redis Hash `stats:{strategy}:{symbol}:{tf}`. Обновляется после каждого закрытия позиции.

### Как часто отправляются отчеты?

Отчеты отправляются:

- **Автоматически каждые 100 сделок** (настраивается через `REPORT_TRIGGER_COUNT`, по умолчанию 100)
  - Счетчик увеличивается при каждой закрытой сделке
  - Отчет отправляется при достижении лимита, счетчик сбрасывается
  - Отчеты формируются отдельно для каждой пары `{source}/{symbol}` (например, `CryptoOrderFlow/BTCUSDT`)
- **Ежедневно** (в заданный час UTC, настраивается через `DAILY_SUMMARY_HOUR`, по умолчанию 0:00)
  - Отправляется сводный отчет по всем стратегиям и символам
- **По запросу** (через API или команду `make send-real-report`)

**Примечание:** Периодические отчеты по времени (каждые N часов) также поддерживаются в `SignalPerformanceTracker`, но основная логика отправки отчетов работает по счетчику сделок через `PeriodicReporter`.

### Можно ли отслеживать только определенные стратегии?

Да, через переменную окружения `STRATEGY_WHITELIST` можно указать список стратегий для отслеживания.

---

## 🔗 Связанные документы

- **[trailing_stop_tracking.md](trailing_stop_tracking.md)** — детали трейлинг стопов
- **[pnl_analysis.md](pnl_analysis.md)** — расчет прибыли/убытков
- **[reporting.md](reporting.md)** — формирование отчетов
- **[trading_workflow/tp1_trailing.md](../trading_workflow/tp1_trailing.md)** — система трейлинг стопов

---

## ✅ Контроль версий

- **2025-11-26** — обновление документации по жизненному циклу сигнала
- **2025-11-21** — создание документации по жизненному циклу сигнала
- Ответственные: `@trading-analytics`, `@python-team`
