# TP1 Trailing System - Events Logging для trade_back

## 🎯 Что добавлено

Полноценное логирование всех торговых событий для будущего анализа через trade_back.

---

## 📦 Новые модули

### 1. trade_events_logger.py
**Главный logger всех торговых событий**

Функции:
- `log_tp1_hit()` - TP1 достигнут
- `log_tp2_hit()` - TP2 достигнут
- `log_tp3_hit()` - TP3 достигнут
- `log_sl_hit()` - SL сработал
- `log_trailing_started()` - Трейлинг начат
- `log_trailing_move()` - 🎯 SL передвинут (КРИТИЧНО!)
- `log_position_opened()` - Позиция открыта
- `log_position_closed()` - Позиция закрыта

Анализ:
- `get_signal_events()` - Все события по сигналу
- `get_trailing_history()` - История движения SL
- `calculate_signal_outcome()` - Итоговый результат

### 2. mt5_trailing_move_logger.py
**Специализированный logger для MT5 trailing moves**

Функции:
- `log_move()` - Логировать движение SL (с проверкой дубликатов)
- `get_trailing_distance()` - Максимальное расстояние от entry
- `get_trailing_stats()` - Статистика по trailing

---

## 💾 Структура хранения в Redis

### events:trades (Stream)
**Глобальный поток всех событий**
```bash
XADD events:trades * event_type "TRAILING_MOVE" sid "signal-123" new_sl "2771.4" ...
```

### trade:events:{sid} (List)
**История событий по сигналу**
```bash
RPUSH trade:events:signal-XAUUSD-123 '{"event_type": "TRAILING_MOVE", ...}'
```

### trade:timeline:{sid} (Sorted Set)
**Временная последовательность**
```bash
ZADD trade:timeline:signal-XAUUSD-123 1730222990000 '{"event_type": "TRAILING_MOVE", ...}'
```

**TTL**: 7 дней - автоматическая очистка

---

## 🔧 Использование

### В TP1 Trailing Orchestrator

```python
# Уже интегрировано!
# При активации трейлинга автоматически логируется TRAILING_STARTED

orchestrator = TP1TrailingOrchestrator()
# events_logger уже внутри
```

### В MT5 EA

```mql5
// При каждом движении trailing stop
if(new_sl != old_sl)
{
    PublishTrailingMove(
        signal_id,
        new_sl,
        current_price,
        "rocket_v1"
    );
}
```

### В Paper Executor

```python
from services.mt5_trailing_move_logger import MT5TrailingMoveLogger

logger = MT5TrailingMoveLogger()

# При симуляции движения SL
logger.log_move(
    sid="signal-XAUUSD-123",
    symbol="XAUUSD",
    new_sl=new_sl,
    current_price=current_price,
    profile="rocket_v1"
)
```

---

## 📊 Что можно анализировать

### 1. Эффективность профилей

```python
# Сколько раз SL двигался для каждого профиля
stats_by_profile = {}

for event in all_trailing_moves:
    profile = event.get("profile", "unknown")
    stats_by_profile[profile] = stats_by_profile.get(profile, 0) + 1

# rocket_v1: 450 moves
# lock_and_trail: 320 moves
# wide_swing: 180 moves
```

### 2. Максимальное движение

```python
# Как далеко удалось утащить SL от entry
from services.mt5_trailing_move_logger import MT5TrailingMoveLogger

logger = MT5TrailingMoveLogger()
distance = logger.get_trailing_distance("signal-XAUUSD-123")

# distance = 12.5 pips (утащили на 12.5 пунктов от entry!)
```

### 3. TP1→TP2 vs TP1→SL

```python
logger = TradeEventsLogger()

tp1_then_tp2 = 0
tp1_then_sl = 0

for sid in all_signals:
    outcome = logger.calculate_signal_outcome(sid)
    
    if outcome["tp1_hit"]:
        if outcome["tp2_hit"]:
            tp1_then_tp2 += 1
        elif outcome["sl_hit"]:
            tp1_then_sl += 1

success_rate = tp1_then_tp2 / (tp1_then_tp2 + tp1_then_sl) * 100
# Ожидаем: 70-80% (было 50-60% без трейлинга)
```

---

## ✅ Integration Complete

**Создано:**
- ✅ `trade_events_logger.py` - Полный logger событий
- ✅ `mt5_trailing_move_logger.py` - Специализированный для MT5
- ✅ Интеграция в `tp1_trailing_orchestrator.py`
- ✅ MT5 example обновлён (PublishTrailingMove)
- ✅ Документация (этот файл)

**Готово для trade_back!** 📊

---

**Version**: 1.0.0  
**Date**: 2025-11-06
