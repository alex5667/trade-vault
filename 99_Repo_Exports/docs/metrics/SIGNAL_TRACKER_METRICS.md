# 📊 Signal Performance Tracker - Полный список метрик

## 🎯 Senior Developer + Trading Analyst (40 лет опыта)

---

## Метрики которые рассчитывает `signal_performance_tracker.py`

### ✅ БАЗОВЫЕ МЕТРИКИ (Fundamental)

| Метрика           | Описание                       | Тип       | Redis Key                                |
| ----------------- | ------------------------------ | --------- | ---------------------------------------- |
| **total_trades**  | Общее количество сделок        | Integer   | `hincrby`                                |
| **wins**          | Количество прибыльных сделок   | Integer   | `hincrby`                                |
| **losses**        | Количество убыточных сделок    | Integer   | `hincrby`                                |
| **winrate**       | Процент прибыльных сделок      | Float (%) | Calculated: `wins/total*100`             |
| **total_pnl**     | Суммарный P&L (прибыль/убыток) | Float     | `hincrbyfloat`                           |
| **total_pnl_pct** | Суммарный P&L в процентах      | Float (%) | `hincrbyfloat`                           |
| **avg_pnl**       | Средний P&L на сделку          | Float     | Calculated: `total_pnl/total_trades`     |
| **avg_pnl_pct**   | Средний P&L в процентах        | Float (%) | Calculated: `total_pnl_pct/total_trades` |

**Пример из кода (строка 88-92):**

```python
pipe.hincrby(stats_key, "total_trades", 1)
pipe.hincrby(stats_key, "wins", win)
pipe.hincrby(stats_key, "losses", loss)
pipe.hincrbyfloat(stats_key, "total_pnl", pnl)
pipe.hincrbyfloat(stats_key, "total_pnl_pct", pnl_pct)
```

---

### ✅ TP/SL МЕТРИКИ (Take Profit / Stop Loss)

#### Частичное закрытие позиций:

- **TP1:** 50% объема
- **TP2:** 30% объема
- **TP3:** 20% объема

| Метрика      | Описание                        | Формула                     |
| ------------ | ------------------------------- | --------------------------- |
| **tp1_hits** | Количество сделок достигших TP1 | Counter                     |
| **tp2_hits** | Количество сделок достигших TP2 | Counter                     |
| **tp3_hits** | Количество сделок достигших TP3 | Counter                     |
| **tp1_rate** | Процент сделок достигших TP1    | `tp1_hits/total_trades*100` |
| **tp2_rate** | Процент сделок достигших TP2    | `tp2_hits/total_trades*100` |
| **tp3_rate** | Процент сделок достигших TP3    | `tp3_hits/total_trades*100` |

**Пример из кода (строка 93-95):**

```python
pipe.hincrby(stats_key, "tp1_hits", tp1_hit)
pipe.hincrby(stats_key, "tp2_hits", tp2_hit)
pipe.hincrby(stats_key, "tp3_hits", tp3_hit)
```

---

### ⭐ МЕТРИКИ УПУЩЕННОЙ ПРИБЫЛИ (Advanced - 40 years exp)

**Критично для оптимизации:** Показывают где TP был достигнут, но потом цена вернулась и сработал SL.

| Метрика              | Описание                           | Значение                |
| -------------------- | ---------------------------------- | ----------------------- |
| **tp1_then_sl**      | Сделок где TP1 достигнут, затем SL | Counter                 |
| **tp2_then_sl**      | Сделок где TP2 достигнут, затем SL | Counter                 |
| **tp3_then_sl**      | Сделок где TP3 достигнут, затем SL | Counter                 |
| **tp1_then_sl_rate** | Процент упущенной прибыли на TP1   | `tp1_then_sl/total*100` |
| **tp2_then_sl_rate** | Процент упущенной прибыли на TP2   | `tp2_then_sl/total*100` |
| **tp3_then_sl_rate** | Процент упущенной прибыли на TP3   | `tp3_then_sl/total*100` |

**Пример из кода (строка 97-99):**

```python
# Метрики упущенной прибыли
pipe.hincrby(stats_key, "tp1_then_sl", tp1_then_sl)
pipe.hincrby(stats_key, "tp2_then_sl", tp2_then_sl)
pipe.hincrby(stats_key, "tp3_then_sl", tp3_then_sl)
```

**Использование:**

- Если `tp1_then_sl_rate` высокий → TP1 слишком близко, нужно увеличить
- Если `tp3_then_sl_rate` > 0 → trailing stop может помочь

---

### 📈 СТАТИСТИКА ПО ИСТОЧНИКАМ (Source-level metrics)

**Уникальная feature!** Разбивка ВСЕХ метрик по источникам сигналов.

**Источники:**

- `OrderFlow` - delta spikes, absorption, breakouts
- `TechnicalAnalysis` - EMA/RSI/MACD из signal-generator
- `AggregatedHub-V2` - weighted blending (delta+speed+cluster+legacy)

**Redis ключи:**

```
stats:orderflow:XAUUSD:tick                       # Общая статистика
stats:orderflow:XAUUSD:tick:OrderFlow             # По источнику
stats:aggregated:XAUUSD:tick:AggregatedHub-V2     # Aggregated Hub
stats:ta:XAUUSD:tick:TechnicalAnalysis            # TA signals
```

**Все метрики дублируются для каждого источника:**

- total_trades, wins, losses, winrate
- total_pnl, avg_pnl
- tp1/tp2/tp3 hits и rates
- tp1/tp2/tp3_then_sl и rates

**Пример из кода (строка 102-113):**

```python
# Статистика по источникам (strategy:symbol:tf:source)
pipe.hincrby(stats_key_source, "total_trades", 1)
pipe.hincrby(stats_key_source, "wins", win)
pipe.hincrby(stats_key_source, "losses", loss)
pipe.hincrbyfloat(stats_key_source, "total_pnl", pnl)
pipe.hincrbyfloat(stats_key_source, "total_pnl_pct", pnl_pct)
pipe.hincrby(stats_key_source, "tp1_hits", tp1_hit)
pipe.hincrby(stats_key_source, "tp2_hits", tp2_hit)
pipe.hincrby(stats_key_source, "tp3_hits", tp3_hit)
pipe.hincrby(stats_key_source, "tp1_then_sl", tp1_then_sl)
pipe.hincrby(stats_key_source, "tp2_then_sl", tp2_then_sl)
pipe.hincrby(stats_key_source, "tp3_then_sl", tp3_then_sl)
```

---

### 🎯 ДОПОЛНИТЕЛЬНЫЕ МЕТРИКИ

| Метрика          | Описание                          | Откуда               |
| ---------------- | --------------------------------- | -------------------- |
| **strategy**     | Название стратегии                | Из сигнала           |
| **symbol**       | Торговый инструмент               | Из сигнала           |
| **tf**           | Таймфрейм                         | Из сигнала           |
| **source**       | Источник сигнала                  | Из сигнала           |
| **last_update**  | Timestamp последнего обновления   | `time.time() * 1000` |
| **close_reason** | Причина закрытия (TP1/TP2/TP3/SL) | TradeMonitor         |
| **tp_before_sl** | Сколько TP достигнуто до SL       | TradeMonitor         |
| **realized_pnl** | Реализованная прибыль/убыток      | TradeMonitor         |

---

### 📊 ИНДЕКСЫ ДЛЯ БЫСТРОГО ПОИСКА

Redis Sets для эффективной навигации:

| Set                             | Описание              | Пример                                             |
| ------------------------------- | --------------------- | -------------------------------------------------- |
| `stats:strategies`              | Все стратегии         | `{orderflow, ta, aggregated}`                      |
| `stats:symbols:{strategy}`      | Символы для стратегии | `{XAUUSD, BTCUSD}`                                 |
| `stats:tfs:{strategy}:{symbol}` | Таймфреймы            | `{tick, 1m, 5m}`                                   |
| `stats:sources:{s}:{sym}:{tf}`  | Источники             | `{OrderFlow, TechnicalAnalysis, AggregatedHub-V2}` |

**Пример из кода (строка 184-187):**

```python
redis_client.sadd("stats:strategies", strategy)
redis_client.sadd(f"stats:symbols:{strategy}", symbol)
redis_client.sadd(f"stats:tfs:{strategy}:{symbol}", tf)
redis_client.sadd(f"stats:sources:{strategy}:{symbol}:{tf}", source)
```

---

## 🔍 КАК ПОЛУЧИТЬ МЕТРИКИ

### 1. Общая статистика по стратегии

```bash
docker exec scanner-redis-worker-1 redis-cli HGETALL stats:aggregated:XAUUSD:tick
```

**Вывод:**

```
total_trades: 94
wins: 36
losses: 58
winrate: 38.30
total_pnl: -204.12
avg_pnl: -2.17
tp1_hits: 45
tp1_rate: 47.9
tp2_hits: 28
tp2_rate: 29.8
tp3_hits: 15
tp3_rate: 16.0
tp1_then_sl: 12
tp1_then_sl_rate: 12.8
tp2_then_sl: 8
tp2_then_sl_rate: 8.5
tp3_then_sl: 3
tp3_then_sl_rate: 3.2
```

### 2. Статистика по конкретному источнику

```bash
docker exec scanner-redis-worker-1 redis-cli HGETALL stats:orderflow:XAUUSD:tick:AggregatedHub-V2
```

**Вывод:**

```
source: AggregatedHub-V2
total_trades: 94
wins: 36
winrate: 38.30
avg_pnl: -2.17
tp1_rate: 47.9
tp2_rate: 29.8
tp3_rate: 16.0
```

### 3. Список всех источников

```bash
docker exec scanner-redis-worker-1 redis-cli SMEMBERS stats:sources:orderflow:XAUUSD:tick
```

**Вывод:**

```
OrderFlow
AggregatedHub-V2
TechnicalAnalysis
```

### 4. Сравнение источников

```bash
# OrderFlow
docker exec scanner-redis-worker-1 redis-cli HGET stats:orderflow:XAUUSD:tick:OrderFlow winrate

# AggregatedHub-V2
docker exec scanner-redis-worker-1 redis-cli HGET stats:aggregated:XAUUSD:tick:AggregatedHub-V2 winrate

# TechnicalAnalysis
docker exec scanner-redis-worker-1 redis-cli HGET stats:ta:XAUUSD:tick:TechnicalAnalysis winrate
```

---

## 📈 РАСЧЁТ ПРОИЗВОДНЫХ МЕТРИК

### WinRate (код строка 131)

```python
winrate = (wins / total_trades * 100.0) if total_trades > 0 else 0.0
```

### Average P/L (код строка 148)

```python
avg_pnl = total_pnl / total_trades if total_trades > 0 else 0.0
```

### TP Rates (код строка 226-228)

```python
for lvl in (1, 2, 3):
    hits = int(stats.get(f"tp{lvl}_hits", 0))
    rate = (hits / total * 100.0) if total else 0.0
    stats[f"tp{lvl}_rate"] = f"{rate:.1f}"
```

### TP→SL Rates (код строка 344-347)

```python
tp_then_sl = int(stats.get(f"tp{lvl}_then_sl", 0))
tp_then_sl_rate = (tp_then_sl / total * 100.0) if total else 0.0
stats[f"tp{lvl}_then_sl_rate"] = f"{tp_then_sl_rate:.1f}"
```

---

## 🔧 API МЕТОДЫ (StatsAggregator)

### Статические методы для работы с метриками:

```python
from services.stats_aggregator import StatsAggregator

# 1. Обновление статистики (вызывается при закрытии позиции)
StatsAggregator.update_stats(redis, pos, trade_summary)

# 2. Получение статистики
stats = StatsAggregator.get_stats(redis, "aggregated", "XAUUSD", "tick")

# 3. Статистика по источнику
stats = StatsAggregator.get_stats_by_source(
    redis, "orderflow", "XAUUSD", "tick", "AggregatedHub-V2"
)

# 4. Сводка по стратегии (агрегация всех символов/TF)
summary = StatsAggregator.get_strategy_summary(redis, "aggregated")

# 5. Список стратегий
strategies = StatsAggregator.get_all_strategies(redis)

# 6. Список источников
sources = StatsAggregator.get_strategy_sources(
    redis, "orderflow", "XAUUSD", "tick"
)

# 7. Постраничный список сделок
trades = StatsAggregator.get_trades_page(
    redis, "aggregated", "XAUUSD", "tick", page=1, page_size=50
)

# 8. Сброс статистики
StatsAggregator.reset_stats(redis, "aggregated", "XAUUSD", "tick")
```

---

## 📊 СТРУКТУРА ДАННЫХ В REDIS

### Hashes (основные метрики):

```
stats:aggregated:XAUUSD:tick {
    total_trades: 94
    wins: 36
    losses: 58
    winrate: 38.30
    total_pnl: -204.12
    total_pnl_pct: -0.52
    avg_pnl: -2.17
    avg_pnl_pct: -0.0055
    tp1_hits: 45
    tp2_hits: 28
    tp3_hits: 15
    tp1_rate: 47.9
    tp2_rate: 29.8
    tp3_rate: 16.0
    tp1_then_sl: 12
    tp2_then_sl: 8
    tp3_then_sl: 3
    tp1_then_sl_rate: 12.8
    tp2_then_sl_rate: 8.5
    tp3_then_sl_rate: 3.2
    strategy: aggregated
    symbol: XAUUSD
    tf: tick
    last_update: 1762403156234
}
```

### Hashes по источникам:

```
stats:orderflow:XAUUSD:tick:AggregatedHub-V2 {
    source: AggregatedHub-V2
    total_trades: 94
    wins: 36
    winrate: 38.30
    avg_pnl: -2.17
    ... (все те же метрики)
}
```

### Lists (закрытые сделки):

```
closed:aggregated:XAUUSD:tick [
    "position-id-1",
    "position-id-2",
    ...
]

closed:aggregated:XAUUSD:tick:AggregatedHub-V2 [
    "position-id-3",
    ...
]
```

### Streams (события):

```
events:trades {
    event_type: "OPEN" | "TP1" | "TP2" | "TP3" | "SL" | "CLOSE"
    position_id: "..."
    price: 3986.50
    lot: 0.50
    pnl: 12.45
    timestamp: 1762403156234
}

trades:closed {
    position_id: "..."
    entry: 3986.50
    exit: 3988.20
    pnl: 12.45
    close_reason: "TP2"
    duration: 3600  # seconds
}
```

---

## 🎓 SENIOR DEVELOPER INSIGHTS

### 1. Почему tp_then_sl важна?

**Проблема:** Вы видите `winrate: 38%`, но не понимаете почему низкая.

**Анализ с tp_then_sl:**

```
tp1_rate: 47.9%        ← 47% позиций достигли TP1
tp1_then_sl_rate: 12.8% ← Но 12.8% потом вернулись и закрылись по SL!
```

**Вывод:**

- **Реальная эффективность TP1: 35.1%** (47.9% - 12.8%)
- **Проблема:** Trailing stop не установлен, цена возвращается
- **Решение:** Переносить SL в breakeven после TP1

### 2. Почему по источникам?

**Пример:**

```
AggregatedHub-V2:    winrate: 38.3%, avg_pnl: -2.17
OrderFlow:           winrate: 52.1%, avg_pnl: +3.45
TechnicalAnalysis:   winrate: 28.7%, avg_pnl: -5.21
```

**Вывод:** OrderFlow лучшая стратегия! Можно:

- Увеличить вес OrderFlow в aggregated blending
- Уменьшить вес TA
- Оптимизировать параметры каждого источника независимо

### 3. Profit Factor (можно добавить)

```python
# Расчёт из существующих метрик:
gross_profit = sum(win_pnl for all wins)
gross_loss = abs(sum(loss_pnl for all losses))
profit_factor = gross_profit / gross_loss if gross_loss > 0 else 0
```

Currently NOT implemented, но легко добавить через:

```python
pipe.hincrbyfloat(stats_key, "gross_profit", pnl if pnl > 0 else 0)
pipe.hincrbyfloat(stats_key, "gross_loss", abs(pnl) if pnl < 0 else 0)
```

---

## 📋 ПОЛНЫЙ СПИСОК ВСЕХ МЕТРИК

### Категория 1: ОСНОВНЫЕ (8 метрик)

1. total_trades
2. wins
3. losses
4. winrate (%)
5. total_pnl
6. total_pnl_pct (%)
7. avg_pnl
8. avg_pnl_pct (%)

### Категория 2: TP/SL (12 метрик)

9. tp1_hits
10. tp2_hits
11. tp3_hits
12. tp1_rate (%)
13. tp2_rate (%)
14. tp3_rate (%)
15. tp1_then_sl
16. tp2_then_sl
17. tp3_then_sl
18. tp1_then_sl_rate (%)
19. tp2_then_sl_rate (%)
20. tp3_then_sl_rate (%)

### Категория 3: МЕТА (5 метрик)

21. strategy
22. symbol
23. tf
24. source
25. last_update

### Категория 4: ИНДЕКСЫ (4 типа)

26. stats:strategies (Set)
27. stats:symbols:{strategy} (Set)
28. stats:tfs:{strategy}:{symbol} (Set)
29. stats:sources:{s}:{sym}:{tf} (Set)

---

## 💡 ПРИМЕРЫ ИСПОЛЬЗОВАНИЯ

### Пример 1: Сравнение стратегий

```python
from services.stats_aggregator import StatsAggregator

redis = get_redis()

# Получаем все стратегии
strategies = StatsAggregator.get_all_strategies(redis)

for strategy in strategies:
    summary = StatsAggregator.get_strategy_summary(redis, strategy)
    print(f"{strategy}: WinRate={summary['winrate']:.1f}%, Avg P/L={summary['avg_pnl']:+.2f}")
```

**Вывод:**

```
aggregated: WinRate=38.3%, Avg P/L=-2.17
orderflow: WinRate=52.1%, Avg P/L=+3.45
ta: WinRate=28.7%, Avg P/L=-5.21
```

### Пример 2: Анализ упущенной прибыли

```python
stats = StatsAggregator.get_stats(redis, "aggregated", "XAUUSD", "tick")

print(f"TP1 достигнут: {stats['tp1_rate']}%")
print(f"TP1→SL упущено: {stats['tp1_then_sl_rate']}%")
print(f"Реальная эффективность TP1: {float(stats['tp1_rate']) - float(stats['tp1_then_sl_rate']):.1f}%")
```

**Вывод:**

```
TP1 достигнут: 47.9%
TP1→SL упущено: 12.8%
Реальная эффективность TP1: 35.1%  ← Нужен trailing stop!
```

### Пример 3: Сравнение источников

```python
sources = StatsAggregator.get_strategy_sources(redis, "orderflow", "XAUUSD", "tick")

for source in sources:
    stats = StatsAggregator.get_stats_by_source(
        redis, "orderflow", "XAUUSD", "tick", source
    )
    print(f"{source}: WinRate={stats['winrate']}%, Trades={stats['total_trades']}")
```

**Вывод:**

```
OrderFlow: WinRate=52.1%, Trades=120
AggregatedHub-V2: WinRate=38.3%, Trades=94
```

---

## 🚀 ПРОИЗВОДСТВЕННОЕ ИСПОЛЬЗОВАНИЕ

### Dashboard запросы:

```bash
# 1. Общая статистика всех стратегий
for s in aggregated orderflow ta; do
  echo "$s:"
  docker exec scanner-redis-worker-1 redis-cli HGETALL stats:$s:XAUUSD:tick | grep -E "(winrate|avg_pnl|total_trades)"
  echo ""
done

# 2. TP эффективность
docker exec scanner-redis-worker-1 redis-cli HMGET stats:aggregated:XAUUSD:tick \
  tp1_rate tp2_rate tp3_rate \
  tp1_then_sl_rate tp2_then_sl_rate tp3_then_sl_rate

# 3. Последние закрытые сделки
docker exec scanner-redis-worker-1 redis-cli LRANGE closed:aggregated:XAUUSD:tick 0 10
```

---

## ✅ ИТОГО: 25+ метрик

**signal_performance_tracker.py** рассчитывает:

### Основные (8):

✅ total_trades, wins, losses, winrate  
✅ total_pnl, total_pnl_pct, avg_pnl, avg_pnl_pct

### TP/SL (12):

✅ tp1/2/3_hits, tp1/2/3_rate  
✅ tp1/2/3_then_sl, tp1/2/3_then_sl_rate ⭐ (unique!)

### Мета (5):

✅ strategy, symbol, tf, source, last_update

### Индексы (4 типа):

✅ strategies, symbols, timeframes, sources

### Всего: **25+ метрик** + индексы

---

**Senior Developer Note:**  
Метрика `tp_then_sl` - это профессиональный уровень trading analytics.  
Показывает где trailing stop мог бы сохранить прибыль. Редко встречается в open-source проектах.

---

**Дата:** 2025-11-06 04:35 UTC  
**Статус:** ✅ PRODUCTION  
**Документация:** Полная

