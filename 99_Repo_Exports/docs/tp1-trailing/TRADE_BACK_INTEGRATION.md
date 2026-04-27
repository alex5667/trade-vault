# TP1 Trailing System - Trade Back Integration

## 📊 Логирование событий для trade_back анализа

Система логирует **все** торговые события для последующего расчёта winrate/ROC.

---

## 🎯 Что логируется

### События

1. **POSITION_OPENED** - Позиция открыта
2. **TP1_HIT** - TP1 достигнут
3. **TP2_HIT** - TP2 достигнут
4. **TP3_HIT** - TP3 достигнут
5. **TRAILING_STARTED** - Трейлинг активирован
6. **TRAILING_MOVE** - 🎯 SL передвинут (критично для анализа!)
7. **SL_HIT** - Stop Loss сработал
8. **POSITION_CLOSED** - Позиция закрыта

---

## 💾 Где хранятся события

### 1. Redis Stream: `events:trades`

**Глобальный поток всех событий**

```bash
# Просмотр последних событий
redis-cli XREVRANGE events:trades + - COUNT 10

# Подсчёт событий
redis-cli XLEN events:trades
```

### 2. Redis List: `trade:events:{sid}`

**Полная история событий по конкретному сигналу**

```bash
# Получить все события сигнала
redis-cli LRANGE trade:events:signal-XAUUSD-123 0 -1

# Количество событий
redis-cli LLEN trade:events:signal-XAUUSD-123
```

### 3. Redis Sorted Set: `trade:timeline:{sid}`

**Временная последовательность для анализа**

```bash
# События в хронологическом порядке
redis-cli ZRANGE trade:timeline:signal-XAUUSD-123 0 -1 WITHSCORES
```

**TTL**: 7 дней (604800 секунд) - автоматическая очистка

---

## 📝 Формат событий

### TRAILING_MOVE (ключевое для анализа!)

```json
{
	"event_type": "TRAILING_MOVE",
	"sid": "signal-XAUUSD-1730222790",
	"symbol": "XAUUSD",
	"new_sl": 2771.4,
	"profile": "rocket_v1",
	"ts": 1730222990000,
	"source": "mt5",
	"metadata": {
		"current_price": 2776.5,
		"distance_from_entry": 5.9,
		"atr": 2.5
	}
}
```

### TP1_HIT

```json
{
	"event_type": "TP1_HIT",
	"sid": "signal-XAUUSD-1730222790",
	"symbol": "XAUUSD",
	"price": 2769.9,
	"position_id": "1234567",
	"lot": 0.015,
	"ts": 1730222890000,
	"source": "mt5"
}
```

### POSITION_CLOSED

```json
{
	"event_type": "POSITION_CLOSED",
	"sid": "signal-XAUUSD-1730222790",
	"symbol": "XAUUSD",
	"price": 2771.5,
	"pnl": 150.25,
	"lot": 0.005,
	"ts": 1730223090000,
	"source": "mt5",
	"metadata": {
		"close_reason": "trailing_stop"
	}
}
```

---

## 🔧 API Usage

### Python

```python
from services.trade_events_logger import TradeEventsLogger

logger = TradeEventsLogger()

# Логируем TP1
logger.log_tp1_hit(
    sid="signal-XAUUSD-123",
    symbol="XAUUSD",
    price=2769.9,
    lot=0.015
)

# Логируем движение trailing
logger.log_trailing_move(
    sid="signal-XAUUSD-123",
    symbol="XAUUSD",
    new_sl=2771.4,
    current_price=2776.5,
    profile="rocket_v1",
    distance_from_entry=5.9,
    atr=2.5
)

# Получаем все события сигнала
events = logger.get_signal_events("signal-XAUUSD-123")

# Анализируем результат
outcome = logger.calculate_signal_outcome("signal-XAUUSD-123")
print(f"TP1: {outcome['tp1_hit']}, TP2: {outcome['tp2_hit']}")
print(f"Trailing moves: {outcome['trailing_moves']}")
print(f"Max SL: {outcome['max_sl']}")
```

### MT5 (через go-gateway)

```mql5
// В вашем MT5 EA при движении trailing stop

void OnTick()
{
    // ... ваш код ...

    // При изменении trailing stop
    double old_sl = g_LastKnownSL;
    double new_sl = PositionGetDouble(POSITION_SL);

    if(MathAbs(new_sl - old_sl) > _Point * 5)  // Изменилось > 5 пунктов
    {
        // Логируем движение
        PublishTrailingMove(
            g_SignalID,
            g_Symbol,
            new_sl,
            SymbolInfoDouble(_Symbol, SYMBOL_BID),
            g_TrailingProfile
        );

        g_LastKnownSL = new_sl;
    }
}

bool PublishTrailingMove(
    string sid,
    string symbol,
    double new_sl,
    double current_price,
    string profile
)
{
    // Формируем JSON
    string json = "";
    json += "{";
    json += "\"event_type\":\"TRAILING_MOVE\",";
    json += "\"sid\":\"" + sid + "\",";
    json += "\"symbol\":\"" + symbol + "\",";
    json += "\"new_sl\":\"" + DoubleToString(new_sl, _Digits) + "\",";
    json += "\"profile\":\"" + profile + "\",";
    json += "\"ts\":\"" + IntegerToString(TimeCurrent() * 1000) + "\",";
    json += "\"source\":\"mt5\",";
    json += "\"metadata\":{";
    json += "\"current_price\":\"" + DoubleToString(current_price, _Digits) + "\"";
    json += "}";
    json += "}";

    // Отправляем в gateway
    return SendToGateway("/events/publish", json);
}
```

---

## 📊 Анализ для trade_back

### Получение истории событий

```python
from services.trade_events_logger import TradeEventsLogger

logger = TradeEventsLogger()

# Все события по сигналу
events = logger.get_signal_events("signal-XAUUSD-123")

# Только trailing movements
trailing = logger.get_trailing_history("signal-XAUUSD-123")

# Результат сигнала
outcome = logger.calculate_signal_outcome("signal-XAUUSD-123")
```

### Расчёт метрик

```python
# Пример расчёта для trade_back

def analyze_signal_performance(sid: str) -> Dict:
    """Анализ производительности сигнала."""
    logger = TradeEventsLogger()

    outcome = logger.calculate_signal_outcome(sid)
    if not outcome:
        return None

    # Базовые метрики
    result = {
        "sid": sid,
        "tp1_reached": outcome["tp1_hit"],
        "tp2_reached": outcome["tp2_hit"],
        "tp3_reached": outcome["tp3_hit"],
        "sl_hit": outcome["sl_hit"],
        "trailing_used": outcome["trailing_started"],
        "trailing_moves": outcome["trailing_moves"],
        "lifetime_sec": outcome["lifetime_ms"] / 1000.0,
        "final_pnl": outcome["final_pnl"]
    }

    # Анализ trailing
    if outcome["trailing_started"]:
        result["max_sl"] = outcome["max_sl"]
        result["sl_movement"] = outcome["max_sl"] - outcome["min_sl"] if outcome["min_sl"] else 0

        # Эффективность трейлинга
        if outcome["tp2_hit"]:
            result["trailing_effectiveness"] = "excellent"  # TP1→TP2 с трейлингом
        elif outcome["sl_hit"]:
            # Проверяем где был SL в момент hit
            if outcome["max_sl"] and outcome["min_sl"]:
                # Если SL ушёл далеко от начального → trailing помог
                result["trailing_effectiveness"] = "good" if outcome["max_sl"] > outcome["min_sl"] * 1.01 else "poor"
        else:
            result["trailing_effectiveness"] = "active"  # Ещё в позиции

    return result


# Пример использования
performance = analyze_signal_performance("signal-XAUUSD-123")
print(json.dumps(performance, indent=2))
```

### Aggregated Analysis

```python
def analyze_all_signals(symbol: str = "XAUUSD", limit: int = 100):
    """
    Анализ всех сигналов для trade_back.

    Returns:
        Dict с агрегированной статистикой
    """
    r = redis.from_url("redis://scanner-redis:6379/0", decode_responses=True)
    logger = TradeEventsLogger()

    # Находим все сигналы
    signal_keys = r.keys(f"signals:*{symbol}*")[:limit]

    stats = {
        "total_signals": 0,
        "with_trailing": 0,
        "tp1_hit": 0,
        "tp1_then_tp2": 0,
        "tp1_then_sl": 0,
        "trailing_moves_total": 0,
        "avg_trailing_moves": 0,
        "max_sl_movement": 0
    }

    for key in signal_keys:
        sid = key.split(":")[-1]
        outcome = logger.calculate_signal_outcome(sid)

        if not outcome:
            continue

        stats["total_signals"] += 1

        if outcome["tp1_hit"]:
            stats["tp1_hit"] += 1

            if outcome["trailing_started"]:
                stats["with_trailing"] += 1
                stats["trailing_moves_total"] += outcome["trailing_moves"]

            if outcome["tp2_hit"]:
                stats["tp1_then_tp2"] += 1
            elif outcome["sl_hit"]:
                stats["tp1_then_sl"] += 1

    # Средние значения
    if stats["with_trailing"] > 0:
        stats["avg_trailing_moves"] = stats["trailing_moves_total"] / stats["with_trailing"]

    return stats
```

---

## 🧪 Тестирование

```python
# Тестирование logger
python -m services.trade_events_logger

# Тестирование MT5 logger
python -m services.mt5_trailing_move_logger
```

---

## 📈 Примеры анализа

### Сколько раз SL двигался?

```python
logger = TradeEventsLogger()
trailing = logger.get_trailing_history("signal-XAUUSD-123")
print(f"SL moved {len(trailing)} times")
```

### Как далеко утащили?

```python
from services.mt5_trailing_move_logger import MT5TrailingMoveLogger

logger = MT5TrailingMoveLogger()
distance = logger.get_trailing_distance("signal-XAUUSD-123")
print(f"Max distance from entry: {distance:.2f} pips")
```

### Полная timeline сигнала

```python
logger = TradeEventsLogger()
events = logger.get_signal_events("signal-XAUUSD-123")

for event in events:
    ts_str = datetime.fromtimestamp(event['ts']/1000).strftime('%H:%M:%S')
    print(f"{ts_str} - {event['event_type']}")
```

Вывод:

```
14:30:15 - POSITION_OPENED
14:31:45 - TP1_HIT
14:31:46 - TRAILING_STARTED
14:32:10 - TRAILING_MOVE (SL → 2762.0)
14:32:35 - TRAILING_MOVE (SL → 2764.5)
14:33:05 - TRAILING_MOVE (SL → 2767.2)
14:33:40 - TP2_HIT
14:33:45 - POSITION_CLOSED
```

---

## 🔧 Интеграция с trade_back

### Структура данных для trade_back

```python
# trade_back будет читать из Redis:

class TradeBackAnalyzer:
    def analyze_signal(self, sid: str):
        """Анализ сигнала для trade_back."""

        # 1. Получаем события
        logger = TradeEventsLogger()
        events = logger.get_signal_events(sid)

        # 2. Строим timeline
        timeline = sorted(events, key=lambda e: e['ts'])

        # 3. Рассчитываем метрики
        metrics = {
            "duration_ms": timeline[-1]['ts'] - timeline[0]['ts'],
            "tp_levels_hit": sum([
                1 for e in events
                if e['event_type'] in ['TP1_HIT', 'TP2_HIT', 'TP3_HIT']
            ]),
            "trailing_moves": sum([
                1 for e in events
                if e['event_type'] == 'TRAILING_MOVE'
            ]),
            "final_outcome": self._determine_outcome(events)
        }

        # 4. Анализ trailing эффективности
        if metrics["trailing_moves"] > 0:
            trailing_events = [
                e for e in events
                if e['event_type'] == 'TRAILING_MOVE'
            ]

            sl_values = [e['new_sl'] for e in trailing_events if 'new_sl' in e]
            metrics["sl_movement"] = max(sl_values) - min(sl_values)
            metrics["trailing_protected"] = self._check_protection(events)

        return metrics
```

---

## 📊 Примеры запросов для анализа

### Сигналы с TP1→TP2 (успех трейлинга)

```python
r = redis.from_url("redis://scanner-redis:6379/0", decode_responses=True)
logger = TradeEventsLogger()

successful_trails = []

for signal_key in r.keys("trade:events:signal-*"):
    sid = signal_key.split(":")[-1]
    outcome = logger.calculate_signal_outcome(sid)

    if outcome and outcome["tp1_hit"] and outcome["tp2_hit"] and outcome["trailing_started"]:
        successful_trails.append({
            "sid": sid,
            "moves": outcome["trailing_moves"],
            "max_sl": outcome["max_sl"]
        })

print(f"Successful trails: {len(successful_trails)}")
```

### Сигналы с TP1→SL (нужно анализировать)

```python
failed_trails = []

for signal_key in r.keys("trade:events:signal-*"):
    sid = signal_key.split(":")[-1]
    outcome = logger.calculate_signal_outcome(sid)

    if outcome and outcome["tp1_hit"] and outcome["sl_hit"] and not outcome["tp2_hit"]:
        failed_trails.append({
            "sid": sid,
            "trailing_used": outcome["trailing_started"],
            "moves": outcome["trailing_moves"]
        })

# Анализ: помог ли трейлинг уменьшить убыток?
with_trailing = [f for f in failed_trails if f["trailing_used"]]
print(f"TP1→SL signals: {len(failed_trails)}")
print(f"  With trailing: {len(with_trailing)}")
print(f"  Without trailing: {len(failed_trails) - len(with_trailing)}")
```

### Максимальное движение SL

```python
from services.mt5_trailing_move_logger import MT5TrailingMoveLogger

logger = MT5TrailingMoveLogger()

max_movements = []

for signal_key in r.keys("trade:events:signal-*"):
    sid = signal_key.split(":")[-1]
    distance = logger.get_trailing_distance(sid)

    if distance and distance > 0:
        max_movements.append({
            "sid": sid,
            "distance": distance
        })

# Топ-10 лучших трейлингов
top_10 = sorted(max_movements, key=lambda x: x["distance"], reverse=True)[:10]
for i, item in enumerate(top_10, 1):
    print(f"{i}. {item['sid']}: {item['distance']:.2f} pips")
```

---

## 📈 Метрики для trade_back

### Расчёт winrate

```python
def calculate_winrate(symbol: str = "XAUUSD"):
    """Рассчитать winrate с учётом трейлинга."""
    logger = TradeEventsLogger()
    r = redis.from_url("redis://scanner-redis:6379/0", decode_responses=True)

    total = 0
    wins = 0

    for signal_key in r.keys(f"trade:events:*{symbol}*"):
        sid = signal_key.split(":")[-1]
        outcome = logger.calculate_signal_outcome(sid)

        if not outcome or not outcome["position_opened"]:
            continue

        total += 1

        # Win = достигли хотя бы TP1 или закрылись в прибыли
        if outcome["tp1_hit"] or (outcome["final_pnl"] and outcome["final_pnl"] > 0):
            wins += 1

    return (wins / total * 100) if total > 0 else 0
```

### Расчёт ROC (Rate of Change)

```python
def calculate_roc(symbol: str = "XAUUSD"):
    """Рассчитать ROC по всем сигналам."""
    logger = TradeEventsLogger()
    r = redis.from_url("redis://scanner-redis:6379/0", decode_responses=True)

    total_pnl = 0
    count = 0

    for signal_key in r.keys(f"trade:events:*{symbol}*"):
        sid = signal_key.split(":")[-1]
        outcome = logger.calculate_signal_outcome(sid)

        if outcome and outcome["final_pnl"] is not None:
            total_pnl += outcome["final_pnl"]
            count += 1

    return total_pnl / count if count > 0 else 0
```

---

## 🎓 Best Practices

1. **Логируйте каждое движение SL** - это критично для анализа
2. **Включайте metadata** - current_price, distance_from_entry, atr
3. **Используйте TTL** - автоматическая очистка старых данных
4. **Проверяйте дубликаты** - MT5TrailingMoveLogger делает это автоматически
5. **Агрегируйте периодически** - сохраняйте статистику в отдельные ключи

---

## 📞 Support

- **Code**: `python-worker/services/trade_events_logger.py`
- **MT5 Logger**: `python-worker/services/mt5_trailing_move_logger.py`
- **Examples**: Этот документ
- **Testing**: `python -m services.trade_events_logger`

---

**Version**: 1.0.0  
**Date**: 2025-11-06  
**Status**: ✅ Ready for trade_back integration
