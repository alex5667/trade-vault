# TP1 Trailing - ATR to Points Conversion

## 🎯 Почему конвертируем ATR в пункты?

**Проблема старого подхода:**

- Отправляли `mode="ATR", atr_mult=0.6` в MT5
- MT5 сам считал свой ATR
- Несоответствие: ATR при входе ≠ ATR при трейлинге
- Сложно анализировать в отчётах

**Решение нового подхода:**

- Берём ATR из исходного сигнала (уже сохранён)
- Умножаем на профиль (0.6, 0.8, 1.2)
- Конвертируем в конкретные пункты
- Отправляем `mode="POINTS", trail_points=X`

**Преимущества:**
✅ Видим в логах: "трейлили ровно 0.6×того ATR, на котором входили"  
✅ Консистентность с аналитикой  
✅ MT5 не нужно считать свой ATR  
✅ Лучше для отчётов trade_back

---

## 📐 Формула конвертации

```python
# 1. Расстояние трейлинга в единицах цены
trail_dist_price = atr_value * atr_mult

# 2. Конвертация в пункты MT5
trail_points = trail_dist_price / point

# Где:
# atr_value - ATR из сигнала (например, 2.5)
# atr_mult - множитель профиля (0.6, 0.8, 1.2)
# point - размер пункта для символа (0.1 для XAUUSD)
```

### Пример для XAUUSD

```python
# Дано:
atr_value = 2.5        # ATR при входе (из сигнала)
atr_mult = 0.6         # Профиль rocket_v1
point = 0.1            # Пункт XAUUSD

# Расчёт:
trail_dist_price = 2.5 * 0.6 = 1.5    # В единицах цены (USD)
trail_points = 1.5 / 0.1 = 15.0       # В пунктах MT5

# Результат:
# Отправляем в MT5: mode="POINTS", trail_points=15.0
```

---

## 🔧 Реализация

### В order_trailing_dispatcher.py

```python
def send_trailing_command_from_atr(
    self,
    sid: str,
    symbol: str,
    position_id: Optional[str],
    atr_value: float,      # ATR из сигнала
    atr_mult: float,       # Множитель профиля
    point: Optional[float] = None,  # Автоматически из Redis
    metadata: Optional[dict] = None
) -> bool:
    # Получаем point из symbol specs
    if point is None:
        point = self._get_symbol_point(symbol)

    # Конвертируем ATR в пункты
    trail_dist_price = atr_value * atr_mult
    trail_points = trail_dist_price / point

    payload = {
        "action": "trail",
        "sid": sid,
        "symbol": symbol,
        "position_id": position_id,
        "mode": "POINTS",           # 🎯 Готовое значение
        "trail_points": trail_points,
        "source": "tp1_trailing_orchestrator",
        "metadata": {
            "atr_value": atr_value,
            "atr_mult": atr_mult,
            "trail_dist_price": trail_dist_price,
            "calculated_from_signal_atr": True
        }
    }

    # Отправка...
```

### В tp1_trailing_orchestrator.py

```python
# Получаем ATR из исходного сигнала
atr_from_signal = signal.get("atr")

if atr_from_signal and float(atr_from_signal) > 0:
    # 🎯 Используем ATR из сигнала (РЕКОМЕНДУЕТСЯ)
    success = self.dispatcher.send_trailing_command_from_atr(
        sid=sid,
        symbol=symbol,
        position_id=position_id,
        atr_value=float(atr_from_signal),
        atr_mult=profile.atr_mult,
        point=None  # Автоматически из symbol specs
    )
else:
    # Fallback: mode=ATR (MT5 сам считает)
    success = self.dispatcher.send_trailing_command(...)
```

---

## 📊 Symbol Specs в Redis

### Структура

```json
{
	"symbol": "XAUUSD",
	"point": 0.1,
	"digits": 2,
	"tick_value_per_lot": 1.0,
	"contract_size": 100,
	"lot_step": 0.01,
	"min_lot": 0.01,
	"max_lot": 100.0
}
```

### Redis Key

```bash
symbol_specs:XAUUSD
```

### Получение в коде

```python
def _get_symbol_point(self, symbol: str) -> float:
    """Получить размер пункта из Redis."""
    try:
        specs_key = f"symbol_specs:{symbol}"
        specs_data = self.r.get(specs_key)

        if specs_data:
            specs = json.loads(specs_data)
            point = specs.get("point")
            if point and point > 0:
                return float(point)
    except Exception as e:
        log.debug("Could not get specs: %s", e)

    # Fallback
    defaults = {
        "XAUUSD": 0.1,
        "XAGUSD": 0.01,
        "BTCUSD": 1.0,
        "ETHUSD": 0.1,
    }
    return defaults.get(symbol, 0.1)
```

---

## 📈 Примеры расчётов

### XAUUSD с разными профилями

```python
atr = 2.5  # ATR из сигнала
point = 0.1  # XAUUSD

# rocket_v1 (0.6)
trail_dist = 2.5 * 0.6 = 1.5
trail_points = 1.5 / 0.1 = 15.0 пунктов

# lock_and_trail (0.8)
trail_dist = 2.5 * 0.8 = 2.0
trail_points = 2.0 / 0.1 = 20.0 пунктов

# wide_swing (1.2)
trail_dist = 2.5 * 1.2 = 3.0
trail_points = 3.0 / 0.1 = 30.0 пунктов
```

### BTCUSD

```python
atr = 250.0  # ATR из сигнала
point = 1.0  # BTCUSD

# rocket_v1 (0.6)
trail_dist = 250.0 * 0.6 = 150.0
trail_points = 150.0 / 1.0 = 150.0 пунктов

# lock_and_trail (0.8)
trail_dist = 250.0 * 0.8 = 200.0
trail_points = 200.0 / 1.0 = 200.0 пунктов
```

---

## 🔍 Логи и отчёты

### Что видим в логах

**Старый подход:**

```
Trailing command sent: mode=ATR atr_mult=0.6
```

**Новый подход:**

```
Trailing command sent: mode=POINTS trail=15.0 pts (ATR 2.50 × 0.6 = 1.50)
```

**Преимущество:** Видим точное расстояние, которое использовали!

### Что видим в trade_back

```python
# Получаем metadata из события
event = {
    "event_type": "TRAILING_STARTED",
    "sid": "signal-XAUUSD-123",
    "metadata": {
        "atr_value": 2.5,
        "atr_mult": 0.6,
        "trail_dist_price": 1.5,
        "calculated_from_signal_atr": True
    }
}

# Анализ:
# "Этот сигнал использовал трейлинг 1.5 USD (0.6 × ATR 2.5)"
```

---

## 🎓 Best Practices

### 1. Всегда сохраняйте ATR в сигнале

```python
# В aggregated_hub_v2.py
signal = XAUUSDSignal(
    # ...
    atr=atr,  # 🎯 ВАЖНО! Сохраняем ATR
    trail_after_tp1=True,
    trail_profile="rocket_v1"
)

# В filtered_signal_writer.py
signal_data = {
    # ...
    "atr": atr,  # 🎯 Сохраняем в Redis
}
self.r.set(f"signals:{sid}", json.dumps(signal_data), ex=86400)
```

### 2. Проверяйте наличие ATR

```python
# В orchestrator
atr = signal.get("atr")
if atr and float(atr) > 0:
    # Используем ATR из сигнала (лучше!)
    use_atr_mode()
else:
    # Fallback на MT5 ATR
    use_points_mode()
```

### 3. Логируйте расчёты

```python
log.info(
    "Trailing: ATR %.2f × %.2f = %.2f (%.1f points)",
    atr_value, atr_mult, trail_dist_price, trail_points
)
```

---

## 🧪 Тестирование

```python
# Тест конвертации
from services.order_trailing_dispatcher import OrderTrailingDispatcher

dispatcher = OrderTrailingDispatcher()

# XAUUSD
success = dispatcher.send_trailing_command_from_atr(
    sid="test-123",
    symbol="XAUUSD",
    position_id="1234567",
    atr_value=2.5,
    atr_mult=0.6,
    point=None  # Автоматически = 0.1
)

# Должно отправить: trail_points = 15.0
```

---

## 📊 Преимущества для анализа

### До (mode=ATR)

```
Сигнал: ATR=2.5
TP1: ATR=2.8 (изменился!)
Трейлинг: использован ATR=2.8 × 0.6 = 1.68
Анализ: 🤔 Какой ATR использовался?
```

### После (mode=POINTS)

```
Сигнал: ATR=2.5
TP1: ATR=2.8 (изменился!)
Трейлинг: 2.5 × 0.6 = 1.5 = 15 пунктов
Анализ: ✅ Точно знаем: 0.6 × ATR входа
```

---

## ✅ Integration Status

**Реализовано:**

- ✅ `send_trailing_command_from_atr()` - конвертация ATR→пункты
- ✅ `_get_symbol_point()` - получение specs из Redis
- ✅ Приоритет на ATR из сигнала в orchestrator
- ✅ Fallback на mode=ATR если ATR не найден
- ✅ Metadata с полной информацией о расчётах
- ✅ Логирование всех расчётов

**Готово к использованию!** 🚀

---

**Version**: 1.0.0  
**Date**: 2025-11-06  
**Team**: Senior Go/Python Developer + Senior Trading Systems Analyst
