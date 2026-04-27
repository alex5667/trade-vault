# Формат хранения информации по сделкам в Redis

Документация описывает все ключи, типы данных и структуры полей, используемые для хранения информации о торговых сделках в Redis.

**Версия:** 1.0  
**Дата:** 2025-11-27  
**Команда:** Senior Go/Python Developer + Senior Trading Systems Analyst

---

## 📋 Содержание

1. [Основные ключи Redis](#основные-ключи-redis)
2. [Детальное описание структур](#детальное-описание-структур)
3. [События в streams](#события-в-streams)
4. [Примеры использования](#примеры-использования)

---

## 🔑 Основные ключи Redis

### 1. `order:{position_id}` (Hash)

**Тип:** Redis Hash  
**Описание:** Основная информация о позиции/сделке  
**TTL:** Без ограничений (сохраняется до закрытия)

### 2. `signals:{signal_id}` (String)

**Тип:** Redis String (JSON)  
**Описание:** Полный payload исходного сигнала  
**TTL:** 7 дней (604800 секунд)

### 3. `signal:{position_id}` (Hash)

**Тип:** Redis Hash  
**Описание:** Данные сигнала, связанные с позицией  
**TTL:** Без ограничений

### 4. `events:trades` (Stream)

**Тип:** Redis Stream  
**Описание:** Поток всех торговых событий  
**MaxLen:** 10000 сообщений

### 5. `trades:closed` (Stream)

**Тип:** Redis Stream  
**Описание:** Поток закрытых сделок (финальные результаты)  
**MaxLen:** 10000 сообщений

### 6. `trade:events:{signal_id}` (List)

**Тип:** Redis List  
**Описание:** История всех событий по конкретному сигналу  
**TTL:** 7 дней (604800 секунд)

### 7. `trade:timeline:{signal_id}` (Sorted Set)

**Тип:** Redis Sorted Set  
**Описание:** Временная последовательность событий (score = timestamp)  
**TTL:** 7 дней (604800 секунд)

### 8. `closed:{strategy}:{symbol}:{tf}` (List)

**Тип:** Redis List  
**Описание:** Список ID закрытых позиций для пагинации  
**TTL:** Без ограничений

### 9. `closed:{strategy}:{symbol}:{tf}:{source}` (List)

**Тип:** Redis List  
**Описание:** Список ID закрытых позиций по источнику  
**TTL:** Без ограничений

---

## 📊 Детальное описание структур

### 1. `order:{position_id}` (Hash)

#### Поля при открытии позиции

| Поле               | Тип    | Описание                     | Пример                                                     |
| ------------------ | ------ | ---------------------------- | ---------------------------------------------------------- |
| `id`               | string | Уникальный ID позиции (UUID) | `"a1b2c3d4-e5f6-..."`                                      |
| `sid`              | string | ID сигнала                   | `"signal-XAUUSD-1730222790"`                               |
| `strategy`         | string | Стратегия                    | `"orderflow"`, `"ta"`, `"aggregated"`                      |
| `symbol`           | string | Торговый символ              | `"XAUUSD"`, `"BTCUSDT"`                                    |
| `tf`               | string | Таймфрейм                    | `"tick"`, `"M1"`, `"H1"`                                   |
| `source`           | string | Источник сигнала             | `"OrderFlow"`, `"TechnicalAnalysis"`, `"AggregatedHub-V2"` |
| `direction`        | string | Направление                  | `"LONG"`, `"SHORT"`                                        |
| `entry_price`      | float  | Цена входа                   | `2769.50`                                                  |
| `entry_time`       | int    | Время входа (timestamp ms)   | `1730222790000`                                            |
| `lot`              | float  | Размер позиции               | `1.0`                                                      |
| `sl`               | float  | Стоп-лосс                    | `2760.00`                                                  |
| `tp1`              | float  | Take Profit уровень 1        | `2775.00`                                                  |
| `tp2`              | float  | Take Profit уровень 2        | `2780.00`                                                  |
| `tp3`              | float  | Take Profit уровень 3        | `2785.00`                                                  |
| `status`           | string | Статус позиции               | `"open"`                                                   |
| `tp1_hit`          | int    | Флаг достижения TP1          | `0` или `1`                                                |
| `tp2_hit`          | int    | Флаг достижения TP2          | `0` или `1`                                                |
| `tp3_hit`          | int    | Флаг достижения TP3          | `0` или `1`                                                |
| `trailing_started` | int    | Флаг запуска трейлинга       | `0` или `1`                                                |
| `trailing_active`  | int    | Флаг активности трейлинга    | `0` или `1`                                                |

#### Поля при обновлении трейлинга

| Поле                  | Тип    | Описание                   | Пример                   |
| --------------------- | ------ | -------------------------- | ------------------------ |
| `sl`                  | float  | Новый уровень SL           | `2765.00`                |
| `trailing_active`     | int    | Флаг активности            | `1`                      |
| `max_favorable_price` | float  | Пиковая цена (MFE)         | `2778.50`                |
| `max_favorable_ts`    | int    | Timestamp пика (ms)        | `1730223000000`          |
| `trailing_distance`   | float  | Дистанция трейлинга в цене | `5.0`                    |
| `trailing_point`      | float  | Размер point               | `0.01`                   |
| `tp_levels`           | string | JSON массив оставшихся TP  | `"[2775.00]"` или `"[]"` |

#### Поля при закрытии позиции

| Поле           | Тип    | Описание                      | Пример                             |
| -------------- | ------ | ----------------------------- | ---------------------------------- |
| `status`       | string | Статус                        | `"closed"`                         |
| `closed_time`  | int    | Время закрытия (timestamp ms) | `1730223600000`                    |
| `exit_price`   | float  | Цена выхода                   | `2765.00`                          |
| `pnl`          | float  | Реализованный P&L             | `-4.50`                            |
| `pnl_pct`      | float  | P&L в процентах               | `-0.16`                            |
| `tp_hits`      | int    | Количество достигнутых TP     | `1`                                |
| `result`       | string | Результат сделки              | `"win"`, `"loss"`, `"breakeven"`   |
| `close_reason` | string | Причина закрытия              | `"TP1"`, `"SL"`, `"TRAILING_STOP"` |

---

### 2. `signals:{signal_id}` (String - JSON)

**Формат:** JSON строка

```json
{
 "sid": "signal-XAUUSD-1730222790",
 "symbol": "XAUUSD",
 "side": "LONG",
 "entry": 2769.5,
 "sl": 2760.0,
 "tp_levels": [2775.0, 2780.0, 2785.0],
 "lot": 1.0,
 "source": "OrderFlow",
 "atr": 10.0,
 "confidence": 0.85,
 "reason": "Strong buy signal",
 "ts": 1730222790000,
 "trail_after_tp1": true,
 "trail_profile": "rocket_v1",
 "sl": 2765.0,
 "trailing_history": [
  {
   "ts": 1730223000000,
   "new_sl": 2765.0,
   "reason": "tp1_trailing_orchestrator",
   "tp_levels_cleared": true
  }
 ]
}
```

**Поля:**

| Поле               | Тип    | Описание                                            |
| ------------------ | ------ | --------------------------------------------------- |
| `sid`              | string | ID сигнала                                          |
| `symbol`           | string | Символ                                              |
| `side`             | string | Направление (`LONG`/`SHORT`)                        |
| `entry`            | float  | Цена входа                                          |
| `sl`               | float  | Стоп-лосс (обновляется при трейлинге)               |
| `tp_levels`        | array  | Массив TP уровней (может быть очищен для rocket_v1) |
| `lot`              | float  | Размер позиции                                      |
| `source`           | string | Источник сигнала                                    |
| `atr`              | float  | ATR значение                                        |
| `confidence`       | float  | Уровень уверенности (0-1)                           |
| `reason`           | string | Причина сигнала                                     |
| `ts`               | int    | Timestamp создания (ms)                             |
| `trail_after_tp1`  | bool   | Флаг включения трейлинга после TP1                  |
| `trail_profile`    | string | Профиль трейлинга (`rocket_v1`, и др.)              |
| `trailing_history` | array  | История обновлений SL                               |

---

### 3. `events:trades` (Stream)

**Типы событий:**

#### 3.1. Событие `OPEN`

```json
{
 "event": "OPEN",
 "order_id": "a1b2c3d4-e5f6-...",
 "strategy": "orderflow",
 "symbol": "XAUUSD",
 "tf": "tick",
 "source": "OrderFlow",
 "direction": "LONG",
 "entry_price": 2769.5,
 "lot": 1.0,
 "time": 1730222790000
}
```

#### 3.2. Событие `TP` (Take Profit)

```json
{
 "event": "TP",
 "order_id": "a1b2c3d4-e5f6-...",
 "strategy": "orderflow",
 "symbol": "XAUUSD",
 "source": "OrderFlow",
 "level": 1,
 "price": 2775.0,
 "volume_closed": 0.5,
 "pnl": 2.75,
 "time": 1730223000000
}
```

#### 3.3. Событие `TP1_HIT`

```json
{
 "event_type": "TP1_HIT",
 "sid": "signal-XAUUSD-1730222790",
 "symbol": "XAUUSD",
 "price": 2775.0,
 "position_id": "a1b2c3d4-e5f6-...",
 "volume": 0.5,
 "source": "OrderFlow",
 "ts": 1730223000000
}
```

#### 3.4. Событие `SL` (Stop Loss)

```json
{
 "event": "SL",
 "order_id": "a1b2c3d4-e5f6-...",
 "strategy": "orderflow",
 "symbol": "XAUUSD",
 "source": "OrderFlow",
 "price": 2760.0,
 "volume_closed": 0.5,
 "pnl": -4.75,
 "time": 1730223600000,
 "peak_price": "",
 "peak_ts": ""
}
```

#### 3.5. Событие `TRAILING_STOP`

```json
{
 "event": "TRAILING_STOP",
 "order_id": "a1b2c3d4-e5f6-...",
 "strategy": "orderflow",
 "symbol": "XAUUSD",
 "source": "OrderFlow",
 "price": 2765.0,
 "volume_closed": 0.5,
 "pnl": -2.25,
 "time": 1730223600000,
 "peak_price": 2778.5,
 "peak_ts": 1730223300000
}
```

#### 3.6. Событие `TRAILING_STARTED`

```json
{
 "event_type": "TRAILING_STARTED",
 "sid": "signal-XAUUSD-1730222790",
 "symbol": "XAUUSD",
 "profile": "rocket_v1",
 "ts": 1730223000000,
 "source": "tp1_trailing_orchestrator",
 "tp1_price": 2775.0,
 "position_id": "a1b2c3d4-e5f6-...",
 "new_sl": "2765.0000000000",
 "tp_levels_cleared": true,
 "clear_tp_levels": true
}
```

#### 3.7. Событие `TRAILING_MOVE`

```json
{
 "event_type": "TRAILING_MOVE",
 "event": "TRAILING_MOVE",
 "order_id": "a1b2c3d4-e5f6-...",
 "sid": "signal-XAUUSD-1730222790",
 "symbol": "XAUUSD",
 "direction": "LONG",
 "previous_sl": 2765.0,
 "new_sl": 2770.0,
 "price": 2778.5,
 "max_favorable_price": 2778.5,
 "max_favorable_ts": 1730223300000,
 "ts": 1730223300000,
 "source": "trade_monitor"
}
```

#### 3.8. Событие `TRAILING_SL_SYNC`

```json
{
 "event": "TRAILING_SL_SYNC",
 "order_id": "a1b2c3d4-e5f6-...",
 "signal_id": "signal-XAUUSD-1730222790",
 "strategy": "orderflow",
 "symbol": "XAUUSD",
 "source": "signal_performance_tracker",
 "profile": "rocket_v1",
 "new_sl": 2765.0,
 "previous_sl": 2760.0,
 "event_id": "1730223000000-0",
 "time": 1730223000000
}
```

#### 3.9. Событие `TP_LEVELS_CLEARED`

```json
{
 "event": "TP_LEVELS_CLEARED",
 "order_id": "a1b2c3d4-e5f6-...",
 "signal_id": "signal-XAUUSD-1730222790",
 "strategy": "orderflow",
 "symbol": "XAUUSD",
 "source": "signal_performance_tracker",
 "profile": "rocket_v1",
 "time": 1730223000000
}
```

---

### 4. `trades:closed` (Stream)

**Формат сообщения:**

```json
{
 "order_id": "a1b2c3d4-e5f6-...",
 "strategy": "orderflow",
 "symbol": "XAUUSD",
 "tf": "tick",
 "source": "OrderFlow",
 "direction": "LONG",
 "entry_time": 1730222790000,
 "close_time": 1730223600000,
 "entry_price": 2769.5,
 "exit_price": 2765.0,
 "pnl": -2.25,
 "pnl_pct": -0.08,
 "tp_count": 1,
 "tp1_hit": 1,
 "tp2_hit": 0,
 "tp3_hit": 0,
 "tp_before_sl": 1,
 "result": "loss",
 "close_reason": "TRAILING_STOP",
 "trailing_active": 1,
 "trailing_started": 1,
 "peak_price": 2778.5,
 "peak_ts": 1730223300000
}
```

**Поля:**

| Поле               | Тип    | Описание                             |
| ------------------ | ------ | ------------------------------------ |
| `order_id`         | string | ID позиции                           |
| `strategy`         | string | Стратегия                            |
| `symbol`           | string | Символ                               |
| `tf`               | string | Таймфрейм                            |
| `source`           | string | Источник                             |
| `direction`        | string | Направление                          |
| `entry_time`       | int    | Время входа (ms)                     |
| `close_time`       | int    | Время закрытия (ms)                  |
| `entry_price`      | float  | Цена входа                           |
| `exit_price`       | float  | Цена выхода                          |
| `pnl`              | float  | Реализованный P&L                    |
| `pnl_pct`          | float  | P&L в процентах                      |
| `tp_count`         | int    | Количество достигнутых TP            |
| `tp1_hit`          | int    | Флаг TP1                             |
| `tp2_hit`          | int    | Флаг TP2                             |
| `tp3_hit`          | int    | Флаг TP3                             |
| `tp_before_sl`     | int    | Количество TP до SL                  |
| `result`           | string | Результат (`win`/`loss`/`breakeven`) |
| `close_reason`     | string | Причина закрытия                     |
| `trailing_active`  | int    | Флаг трейлинга                       |
| `trailing_started` | int    | Флаг запуска трейлинга               |
| `peak_price`       | float  | Пиковая цена (MFE)                   |
| `peak_ts`          | int    | Timestamp пика (ms)                  |

---

### 5. `trade:events:{signal_id}` (List)

**Формат:** Список JSON строк, каждая строка - событие

```json
[
 "{\"event_type\":\"TP1_HIT\",\"sid\":\"signal-XAUUSD-1730222790\",\"symbol\":\"XAUUSD\",\"ts\":1730223000000}",
 "{\"event_type\":\"TRAILING_STARTED\",\"sid\":\"signal-XAUUSD-1730222790\",\"symbol\":\"XAUUSD\",\"ts\":1730223001000}",
 "{\"event_type\":\"TRAILING_MOVE\",\"sid\":\"signal-XAUUSD-1730222790\",\"new_sl\":2770.00,\"ts\":1730223300000}",
 "{\"event_type\":\"TRAILING_STOP\",\"sid\":\"signal-XAUUSD-1730222790\",\"price\":2765.00,\"ts\":1730223600000}"
]
```

---

### 6. `trade:timeline:{signal_id}` (Sorted Set)

**Формат:** Sorted Set, где:

- **Score:** Timestamp события (ms)
- **Value:** JSON строка с данными события

```json
{
 "event_type": "TP1_HIT",
 "ts": 1730223000000,
 "price": 2775.0,
 "new_sl": null
}
```

---

## 💡 Примеры использования

### Чтение открытой позиции

```python
import redis
r = redis.from_url("redis://localhost:6379/0", decode_responses=True)

position_id = "a1b2c3d4-e5f6-..."
order_data = r.hgetall(f"order:{position_id}")

print(f"Symbol: {order_data['symbol']}")
print(f"Direction: {order_data['direction']}")
print(f"Entry Price: {order_data['entry_price']}")
print(f"Status: {order_data['status']}")
```

### Чтение событий по сигналу

```python
signal_id = "signal-XAUUSD-1730222790"

# Из списка событий
events = r.lrange(f"trade:events:{signal_id}", 0, -1)
for event_json in events:
    event = json.loads(event_json)
    print(f"{event['event_type']} at {event['ts']}")

# Из временной последовательности
timeline = r.zrange(f"trade:timeline:{signal_id}", 0, -1, withscores=True)
for event_json, timestamp in timeline:
    event = json.loads(event_json)
    print(f"{event['event_type']} at {timestamp}")
```

### Чтение закрытых сделок

```python
# Из stream
messages = r.xread({"trades:closed": "0"}, count=10)
for stream, msgs in messages:
    for msg_id, data in msgs:
        print(f"Closed trade: {data['order_id']}, P&L: {data['pnl']}")

# Из списка по стратегии
closed_ids = r.lrange("closed:orderflow:XAUUSD:tick", 0, -1)
for pos_id in closed_ids:
    order_data = r.hgetall(f"order:{pos_id}")
    print(f"Trade {pos_id}: {order_data['result']} {order_data['pnl']}")
```

### Мониторинг трейлинга

```python
# Читаем события трейлинга из stream
messages = r.xread({"events:trades": "0"}, count=100)
for stream, msgs in messages:
    for msg_id, data in msgs:
        if data.get("event") == "TRAILING_MOVE":
            print(f"Trailing move: {data['previous_sl']} → {data['new_sl']}")
            print(f"Peak price: {data.get('max_favorable_price', 'N/A')}")
```

---

## 🔄 Жизненный цикл данных

1. **Открытие позиции:**

   - Создается `order:{position_id}` (Hash)
   - Создается `signals:{signal_id}` (String)
   - Публикуется событие `OPEN` в `events:trades`

2. **Достижение TP1:**

   - Обновляется `order:{position_id}` (`tp1_hit = 1`)
   - Публикуется событие `TP` и `TP1_HIT` в `events:trades`
   - Для `rocket_v1`: очищаются TP2/TP3

3. **Запуск трейлинга:**

   - Обновляется `order:{position_id}` (`trailing_active = 1`, `sl`)
   - Обновляется `signals:{signal_id}` (`sl`, `trailing_history`)
   - Публикуется событие `TRAILING_STARTED` в `events:trades`

4. **Движение трейлинга:**

   - Обновляется `order:{position_id}` (`sl`, `max_favorable_price`, `max_favorable_ts`)
   - Публикуется событие `TRAILING_MOVE` в `events:trades`

5. **Закрытие позиции:**
   - Обновляется `order:{position_id}` (`status = "closed"`, все финальные поля)
   - Публикуется событие `SL` или `TRAILING_STOP` в `events:trades`
   - Публикуется финальная запись в `trades:closed`
   - Добавляется ID в списки `closed:{strategy}:{symbol}:{tf}`

---

## 📝 Примечания

1. **Типы данных:**

   - Все числовые значения хранятся как строки (Redis Hash)
   - JSON структуры сериализуются в строки
   - Timestamps в миллисекундах

2. **TTL:**

   - `signals:{signal_id}` - 7 дней
   - `trade:events:{signal_id}` - 7 дней
   - `trade:timeline:{signal_id}` - 7 дней
   - `order:{position_id}` - без TTL (сохраняется навсегда)

3. **Streams:**

   - `events:trades` - ограничен 10000 сообщений (FIFO)
   - `trades:closed` - ограничен 10000 сообщений (FIFO)

4. **Особенности для rocket_v1:**
   - После TP1 поле `tp_levels` в `order:{position_id}` очищается
   - В `signals:{signal_id}` поле `tp_levels` также очищается
   - Флаг `clear_tp_levels` передается в событиях

---

**Последнее обновление:** 2025-11-27
