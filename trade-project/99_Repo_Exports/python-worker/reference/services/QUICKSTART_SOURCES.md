# Быстрый старт: Статистика по источникам

## 🎯 Что это?

Система Signal Performance Tracker теперь ведёт **раздельную статистику** для каждого источника сигналов:

- **OrderFlow** - чистые сигналы на основе order flow
- **AggregatedHub-V2** - агрегированные сигналы
- **TechnicalAnalysis** - технический анализ

Это позволяет:

- ✅ Сравнивать эффективность источников
- ✅ Выбирать лучший источник для торговли
- ✅ Отслеживать деградацию качества
- ✅ A/B тестирование

## ⚡ Быстрый старт

### 1. Получение списка источников

```python
from services.stats_aggregator import StatsAggregator
from core.redis_client import get_redis

redis_client = get_redis()

sources = StatsAggregator.get_strategy_sources(
    redis_client, "orderflow", "XAUUSD", "tick"
)

print(f"Доступные источники: {sources}")
# → ['OrderFlow', 'AggregatedHub-V2', 'TechnicalAnalysis']
```

### 2. Статистика по источнику

```python
stats = StatsAggregator.get_stats_by_source(
    redis_client, "orderflow", "XAUUSD", "tick", "OrderFlow"
)

print(f"OrderFlow:")
print(f"  Сделок: {stats['total_trades']}")
print(f"  WinRate: {stats['winrate']}%")
print(f"  Total P/L: {stats['total_pnl']}")
```

### 3. Сравнение всех источников

```python
from services.reporting_service import ReportingService

reporting = ReportingService()
sources_summary = reporting.get_sources_summary()

for source, data in sources_summary.items():
    print(f"{source}: {data['total_trades']} trades, WR {data['winrate']}%")
```

### 4. Определение лучшего источника

```python
# Фильтруем надёжные источники (10+ сделок)
MIN_TRADES = 10
qualified = {
    s: d for s, d in sources_summary.items()
    if d['total_trades'] >= MIN_TRADES
}

# Находим лучший по WinRate
best = max(qualified.items(), key=lambda x: x[1]['winrate'])
print(f"🏆 Лучший: {best[0]} ({best[1]['winrate']:.1f}%)")
```

## 📊 Примеры уведомлений

### Периодическая сводка (каждые 3 часа)

```
🗓 Итоги за 3ч

• orderflow: 8 сделок, WR 75.0%, P/L +67.40

📊 По источникам:
  • OrderFlow: 3 сделки, WR 100.0%, P/L +45.20
  • AggregatedHub-V2: 5 сделок, WR 60.0%, P/L +22.20
```

### Ежедневная сводка

```
📊 Ежедневная сводка

Всего сделок: 25
WinRate: 72.0%
Общий P/L: +245.80

📊 По источникам:
  • OrderFlow: 10 сделок, WR 80.0%, P/L +120.50
  • AggregatedHub-V2: 12 сделок, WR 66.7%, P/L +98.30
  • TechnicalAnalysis: 3 сделки, WR 66.7%, P/L +27.00
```

## 🔧 Запросы к Redis

### Получение статистики

```bash
# Общая статистика
redis-cli HGETALL stats:orderflow:XAUUSD:tick

# По источнику OrderFlow
redis-cli HGETALL stats:orderflow:XAUUSD:tick:OrderFlow

# По источнику AggregatedHub-V2
redis-cli HGETALL stats:orderflow:XAUUSD:tick:AggregatedHub-V2
```

### Список источников

```bash
redis-cli SMEMBERS stats:sources:orderflow:XAUUSD:tick
```

## 🧪 Тестирование

### Запуск примеров

```bash
cd /home/alex/front/trade/scanner_infra/python-worker/services

# Сравнение источников
python example_sources_analysis.py 1

# Сводка по источникам
python example_sources_analysis.py 2

# Определение лучшего
python example_sources_analysis.py 4

# Мониторинг в реальном времени
python example_sources_analysis.py 5
```

## 📚 Полная документация

Детальная информация в `SOURCE_STATISTICS.md`
