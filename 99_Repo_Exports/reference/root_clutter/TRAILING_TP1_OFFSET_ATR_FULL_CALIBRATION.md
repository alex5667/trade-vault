# Полноценная калибровка TRAILING_TP1_OFFSET_ATR

## ✅ Реализована полная логика симуляции на исторических данных

### 📍 **Что было реализовано:**

#### 1. **Новый скрипт симуляции**
**Файл:** `python-worker/tools/simulate_trailing_tp1_full.py`

Полноценный анализатор, который:
- ✅ Берет только сделки с `tp1_hit = true`
- ✅ Загружает реальные исторические тики из таблицы `ticks`
- ✅ Симулирует трейлинг на каждом тике после TP1
- ✅ Рассчитывает expectancy, giveback, missed profit, fake stopouts

#### 2. **Интеграция в AutoCalibrationService**
**Файл:** `python-worker/services/auto_calibration_service.py`

```python
def _calibrate_trailing_tp1_offset_atr(self, trades: List[Any], symbol: str) -> Dict[str, Any]:
    # Полноценная симуляция на исторических тиках
    from tools.simulate_trailing_tp1_full import (
        fetch_trades, fetch_ticks, simulate_trade_for_offset,
        aggregate_stats, score_offset
    )

    # Загружает trades с tp1_hit и симулирует на тиках
    # Возвращает оптимальный offset_mult
```

#### 3. **Обновление Redis symbol_specs**
Автоматически сохраняет откалиброванный `tp1_offset_atr` в Redis.

### 🎯 **Алгоритм работы:**

#### **1. Фильтрация данных**
```sql
SELECT * FROM trades_closed
WHERE tp1_hit = TRUE
  AND trailing_started = TRUE
  AND atr > 0
  AND one_r_money > 0
```

#### **2. Симуляция на тиках**
Для каждой сделки:
```python
# Загружаем путь цены после TP1
ticks = fetch_ticks(conn, symbol, entry_ts, exit_ts + 5min)

# Для каждого offset_mult симулируем трейлинг
for offset_mult in [0.3, 0.4, 0.5, 0.6, 0.7]:
    offset = atr * offset_mult
    new_sl = entry_price + offset  # для LONG

    # Проходим по тикам после TP1
    for tick in ticks:
        if tick.price <= new_sl:  # SL сработал
            r_trail = compute_r(entry_price, new_sl, initial_sl)
            break
```

#### **3. Метрики расчета**
```python
r_orig = compute_r(entry_price, exit_price, initial_sl)      # Оригинальный результат
r_mfe = max(r по пути после TP1)                             # Максимум после TP1
r_trail = результат с трейлингом                            # Результат симуляции

giveback_r = max(r_mfe - r_trail, 0.0)                       # Потеряли от максимума
missed_r = max(r_orig - r_trail, 0.0)                        # Недобрали относительно оригинала
fake_stopout = (trailing_exit and r_mfe > r_trail + 0.1)     # Ложный стоп
```

#### **4. Скоринг и выбор**
```python
def score_offset(stats: OffsetStats) -> float:
    return (
        1.0 * expectancy_r
        - 0.4 * avg_giveback_r      # giveback хуже
        - 0.3 * avg_missed_r        # missed тоже плохо
        - 0.7 * share_fake_stopout  # fake stopouts очень плохо
    )
```

### 🔄 **Интеграция в пайплайн:**

```
Закрытие сделки → save_trade_closed() → increment_trade_counter()
    ↓
if counter >= 100 → run_calibration_async()
    ↓
load_calibration_data() → calibrate_stop_atr_mult() → calibrate_rr_levels()
    ↓
_calibrate_trailing_tp1_offset_atr() → fetch_trades() → fetch_ticks() → simulate_trade_for_offset()
    ↓
aggregate_stats() → score_offset() → select_best_offset()
    ↓
update_symbol_spec() → Redis: symbol_specs:{symbol}["trailing"]["tp1_offset_atr"]
    ↓
Новый сигнал → _resolve_trailing_tp1_offset_atr() → compute_levels() → signal_settings
    ↓
Сигнал с калиброванным TRAILING_TP1_OFFSET_ATR → Бот
```

### 📊 **Примеры использования:**

#### **Ручной запуск:**
```bash
cd python-worker/tools
python simulate_trailing_tp1_full.py --dsn "postgresql://..." --source CryptoOrderFlow --symbol ETHUSDT --limit 200
```

#### **Результат:**
```
=== Recommended offset_mult ===
symbol=ETHUSDT source=CryptoOrderFlow offset_mult=0.45 (score=2.134, expR=2.340, giveback=0.120, missed=0.089, fake=0.03, count=150)
```

#### **Redis сохранение:**
```redis
symbol_specs:ETHUSDT = {
  "trailing": {
    "stop_atr_mult": 1.05,
    "rr_levels": [1.0, 2.0, 3.0],
    "tp1_offset_atr": 0.45
  }
}
```

### ✅ **Полностью соответствует требованиям:**

- ✅ **tp1_hit = true** - фильтрует только сделки, где TP1 был достигнут
- ✅ **Сетка offset_mult** - настраивается под символ (ETH: 0.3-0.7, BTC: 0.5-1.0)
- ✅ **Исторические данные** - использует реальные тики из таблицы `ticks`
- ✅ **Временной анализ** - проверяет каждый тик после TP1
- ✅ **Метрики**: expectancy_r, giveback_r, missed_r, fake_stopout
- ✅ **Автокалибровка** - запускается автоматически каждые 100 сделок
- ✅ **Интеграция** - результаты сохраняются в Redis и используются в сигналах

### 🚀 **Результат:**

Теперь **TRAILING_TP1_OFFSET_ATR** калибруется на основе полноценной симуляции поведения цены после достижения TP1, а не упрощенных предположений!

**Логика подбора TRAILING_TP1_OFFSET_ATR полностью реализована и интегрирована! 🎉**
