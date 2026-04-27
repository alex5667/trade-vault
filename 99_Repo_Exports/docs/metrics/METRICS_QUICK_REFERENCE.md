# 📊 Signal Tracker Metrics - Quick Reference

## Метрики кроме WinRate (всего 25+)

---

## ✅ 1. ОСНОВНЫЕ МЕТРИКИ (8 показателей)

```
┌─────────────────┬──────────────────────────────────────┐
│ Метрика         │ Описание                             │
├─────────────────┼──────────────────────────────────────┤
│ total_trades    │ Всего сделок                         │
│ wins            │ Прибыльных сделок                    │
│ losses          │ Убыточных сделок                     │
│ winrate         │ % прибыльных (wins/total*100)        │
│ total_pnl       │ Суммарная прибыль/убыток             │
│ total_pnl_pct   │ Суммарная прибыль в %                │
│ avg_pnl         │ Средняя прибыль на сделку            │
│ avg_pnl_pct     │ Средняя прибыль в %                  │
└─────────────────┴──────────────────────────────────────┘
```

**Пример:**

```
Trades: 253 | Wins: 94 | Losses: 159 | WinRate: 37.15%
Total P/L: -204.12 | Avg P/L: -0.80
```

---

## ✅ 2. TP МЕТРИКИ (6 показателей)

**Частичное закрытие:** TP1 (50%), TP2 (30%), TP3 (20%)

```
┌─────────────┬──────────────────────────────────────┐
│ Метрика     │ Описание                             │
├─────────────┼──────────────────────────────────────┤
│ tp1_hits    │ Сколько раз достигнут TP1            │
│ tp1_rate    │ % сделок достигших TP1               │
│ tp2_hits    │ Сколько раз достигнут TP2            │
│ tp2_rate    │ % сделок достигших TP2               │
│ tp3_hits    │ Сколько раз достигнут TP3            │
│ tp3_rate    │ % сделок достигших TP3               │
└─────────────┴──────────────────────────────────────┘
```

**Пример:**

```
TP1: 121 hits (47.9%) | TP2: 75 hits (29.8%) | TP3: 40 hits (16.0%)
```

---

## ⭐ 3. УПУЩЕННАЯ ПРИБЫЛЬ (6 показателей)

**Критично!** Показывает где TP был достигнут, но затем SL сработал.

```
┌──────────────────┬────────────────────────────────────┐
│ Метрика          │ Описание                           │
├──────────────────┼────────────────────────────────────┤
│ tp1_then_sl      │ TP1 достигнут → потом SL           │
│ tp1_then_sl_rate │ % сделок с упущенной прибылью TP1  │
│ tp2_then_sl      │ TP2 достигнут → потом SL           │
│ tp2_then_sl_rate │ % сделок с упущенной прибылью TP2  │
│ tp3_then_sl      │ TP3 достигнут → потом SL           │
│ tp3_then_sl_rate │ % сделок с упущенной прибылью TP3  │
└──────────────────┴────────────────────────────────────┘
```

**Пример:**

```
TP1→SL: 32 (12.8%) | TP2→SL: 21 (8.5%) | TP3→SL: 8 (3.2%)
```

**💡 Интерпретация:**

- Если `tp1_then_sl_rate > 10%` → Нужен trailing stop после TP1
- Если `tp2_then_sl_rate > 5%` → Переносить SL в breakeven после TP2
- Если `tp3_then_sl_rate > 0%` → Агрессивный трейлинг после TP3

---

## ✅ 4. СТАТИСТИКА ПО ИСТОЧНИКАМ (все метрики × N источников)

**Источники сигналов:**

- `OrderFlow` - delta spikes, absorption
- `AggregatedHub-V2` - weighted blending
- `TechnicalAnalysis` - EMA/RSI/MACD

**Production данные:**

```
┌────────────────────┬────────┬──────────┬─────────┐
│ Источник           │ Trades │ WinRate  │ Avg P/L │
├────────────────────┼────────┼──────────┼─────────┤
│ OrderFlow          │ 120    │ 34.17%   │ +0.01   │
│ AggregatedHub-V2   │ 94     │ 38.30%   │ -2.17   │
│ TechnicalAnalysis  │ 39     │ 43.59%   │ -0.01   │
└────────────────────┴────────┴──────────┴─────────┘
```

**💡 Анализ:**

- **TechnicalAnalysis** - лучший WinRate (43.59%)
- **OrderFlow** - самый активный (120 сделок)
- **AggregatedHub-V2** - средний результат, но худший P/L

---

## 📈 ВИЗУАЛИЗАЦИЯ МЕТРИК

### Пример полной статистики:

```bash
docker exec scanner-redis-worker-1 redis-cli HGETALL stats:orderflow:XAUUSD:tick
```

**Вывод:**

```
total_trades: 253
wins: 94
losses: 159
winrate: 37.15
total_pnl: -204.12
avg_pnl: -0.80
tp1_hits: 121
tp1_rate: 47.9
tp2_hits: 75
tp2_rate: 29.8
tp3_hits: 40
tp3_rate: 16.0
tp1_then_sl: 32
tp1_then_sl_rate: 12.8
tp2_then_sl: 21
tp2_then_sl_rate: 8.5
tp3_then_sl: 8
tp3_then_sl_rate: 3.2
strategy: orderflow
symbol: XAUUSD
tf: tick
last_update: 1762403156234
```

---

## 🔍 QUICK COMMANDS

### Все стратегии:

```bash
docker exec scanner-redis-worker-1 redis-cli SMEMBERS stats:strategies
```

### Символы для стратегии:

```bash
docker exec scanner-redis-worker-1 redis-cli SMEMBERS stats:symbols:aggregated
```

### Источники для стратегии:

```bash
docker exec scanner-redis-worker-1 redis-cli SMEMBERS stats:sources:orderflow:XAUUSD:tick
```

### Сравнение всех стратегий:

```bash
for s in aggregated orderflow ta; do
  echo "$s:"
  docker exec scanner-redis-worker-1 redis-cli HMGET stats:$s:XAUUSD:tick total_trades winrate avg_pnl
done
```

### Только TP→SL метрики:

```bash
docker exec scanner-redis-worker-1 redis-cli HMGET stats:aggregated:XAUUSD:tick \
  tp1_then_sl_rate tp2_then_sl_rate tp3_then_sl_rate
```

---

## 📊 ИТОГО: Список всех метрик

### БАЗОВЫЕ (8):

1. total_trades
2. wins
3. losses
4. winrate (%)
5. total_pnl
6. total_pnl_pct (%)
7. avg_pnl
8. avg_pnl_pct (%)

### TP/SL (12):

9. tp1_hits
10. tp1_rate (%)
11. tp2_hits
12. tp2_rate (%)
13. tp3_hits
14. tp3_rate (%)
15. tp1_then_sl ⭐
16. tp1_then_sl_rate (%) ⭐
17. tp2_then_sl ⭐
18. tp2_then_sl_rate (%) ⭐
19. tp3_then_sl ⭐
20. tp3_then_sl_rate (%) ⭐

### МЕТА (5):

21. strategy
22. symbol
23. tf (timeframe)
24. source
25. last_update

### ИНДЕКСЫ (4):

26. stats:strategies
27. stats:symbols:{strategy}
28. stats:tfs:{strategy}:{symbol}
29. stats:sources:{s}:{sym}:{tf}

---

## 💡 Production данные (текущие):

```
📊 orderflow:XAUUSD:tick
  Trades: 253 | WinRate: 37.15% | Total P/L: -204.12 | Avg P/L: -0.80
  TP1: 47.9% | TP2: 29.8% | TP3: 16.0%
  TP1→SL: 12.8% ⚠️ | TP2→SL: 8.5% | TP3→SL: 3.2%

📊 По источникам:
  OrderFlow:          120 trades | WinRate: 34.17% | Avg P/L: +0.01
  AggregatedHub-V2:   94 trades  | WinRate: 38.30% | Avg P/L: -2.17
  TechnicalAnalysis:  39 trades  | WinRate: 43.59% | Avg P/L: -0.01
```

**Рекомендации на основе метрик:**

- ⚠️ TP1→SL 12.8% - рассмотреть trailing stop после TP1
- ✅ TechnicalAnalysis лучший WinRate (43.59%)
- ⚠️ AggregatedHub-V2 худший Avg P/L (-2.17) - требует оптимизации

---

**Senior Developer + Trading Analyst**  
**Метрик: 25+**  
**Уровень: Professional/Enterprise**
