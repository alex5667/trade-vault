# Статистика по источникам сигналов

## 📊 Обзор

Система отслеживания эффективности теперь ведёт статистику **раздельно по источникам** сигналов.

### Поддерживаемые источники

1. **OrderFlow** - сигналы на основе order flow анализа
2. **AggregatedHub-V2** - агрегированные сигналы из нескольких источников
3. **TechnicalAnalysis** - сигналы на основе технического анализа

## 🗄️ Схема хранения в Redis

### Общая статистика (без разбивки по источникам)

```
stats:{strategy}:{symbol}:{tf}
```

Пример: `stats:orderflow:XAUUSD:tick`

### Статистика по источникам

```
stats:{strategy}:{symbol}:{tf}:{source}
```

Примеры:

- `stats:orderflow:XAUUSD:tick:OrderFlow`
- `stats:orderflow:XAUUSD:tick:AggregatedHub-V2`
- `stats:orderflow:XAUUSD:tick:TechnicalAnalysis`

### Индексы

```
stats:sources:{strategy}:{symbol}:{tf}
```

Set со списком источников для данной комбинации

### Списки сделок по источникам

```
closed:{strategy}:{symbol}:{tf}:{source}
```

Примеры:

- `closed:orderflow:XAUUSD:tick:OrderFlow`
- `closed:orderflow:XAUUSD:tick:AggregatedHub-V2`

## 🔧 API для работы со статистикой по источникам

### Получение списка источников

```python
from services.stats_aggregator import StatsAggregator
from core.redis_client import get_redis

redis_client = get_redis()

# Получить список всех источников для стратегии
sources = StatsAggregator.get_strategy_sources(
    redis_client,
    "orderflow",
    "XAUUSD",
    "tick"
)

print(f"Доступные источники: {sources}")
# Вывод: ['OrderFlow', 'AggregatedHub-V2', 'TechnicalAnalysis']
```

### Получение статистики по источнику

```python
# Статистика для конкретного источника
stats = StatsAggregator.get_stats_by_source(
    redis_client,
    strategy="orderflow",
    symbol="XAUUSD",
    tf="tick",
    source="OrderFlow"
)

print(f"OrderFlow - Сделок: {stats['total_trades']}")
print(f"OrderFlow - WinRate: {stats['winrate']}%")
print(f"OrderFlow - Total P/L: {stats['total_pnl']}")
print(f"OrderFlow - TP1 Rate: {stats.get('tp1_rate', 0)}%")
```

### Сравнение источников

```python
sources = ["OrderFlow", "AggregatedHub-V2", "TechnicalAnalysis"]

for source in sources:
    stats = StatsAggregator.get_stats_by_source(
        redis_client, "orderflow", "XAUUSD", "tick", source
    )

    if stats:
        print(f"\n{source}:")
        print(f"  Сделок: {stats['total_trades']}")
        print(f"  WinRate: {stats['winrate']}%")
        print(f"  Avg P/L: {stats.get('avg_pnl', 0)}")
```

### Получение сводки по всем источникам

```python
from services.reporting_service import ReportingService

reporting = ReportingService()

# Агрегированная статистика по всем источникам
sources_summary = reporting.get_sources_summary()

for source, data in sources_summary.items():
    print(f"\n{source}:")
    print(f"  Всего сделок: {data['total_trades']}")
    print(f"  Выигрышей: {data['wins']}")
    print(f"  Проигрышей: {data['losses']}")
    print(f"  WinRate: {data['winrate']}%")
    print(f"  Total P/L: {data['total_pnl']}")
    print(f"  Avg P/L: {data['avg_pnl']}")
```

## 📱 Уведомления с разбивкой по источникам

### Ежедневная сводка

```
📊 Ежедневная сводка

Всего сделок: 25
Выигрышей: 18
Проигрышей: 7
WinRate: 72.0%
Общий P/L: +245.80

orderflow: 25 сделок, WR 72.0%, P/L +245.80

📊 По источникам:
  • OrderFlow: 10 сделок, WR 80.0%, P/L +120.50
  • AggregatedHub-V2: 12 сделок, WR 66.7%, P/L +98.30
  • TechnicalAnalysis: 3 сделок, WR 66.7%, P/L +27.00
```

### Периодическая сводка (каждые 3 часа)

```
🗓 Итоги за 3ч

• orderflow: 8 сделок, WR 75.0%, P/L +67.40

📊 По источникам:
  • OrderFlow: 3 сделки, WR 100.0%, P/L +45.20
  • AggregatedHub-V2: 5 сделок, WR 60.0%, P/L +22.20
```

## 🔍 Анализ эффективности источников

### Пример анализа

```python
from services.stats_aggregator import StatsAggregator
from core.redis_client import get_redis

redis_client = get_redis()

# Получаем источники
sources = StatsAggregator.get_strategy_sources(
    redis_client, "orderflow", "XAUUSD", "tick"
)

# Собираем метрики для сравнения
comparison = []

for source in sources:
    stats = StatsAggregator.get_stats_by_source(
        redis_client, "orderflow", "XAUUSD", "tick", source
    )

    if stats and int(stats.get("total_trades", 0)) > 0:
        comparison.append({
            "source": source,
            "trades": int(stats.get("total_trades", 0)),
            "winrate": float(stats.get("winrate", 0)),
            "avg_pnl": float(stats.get("avg_pnl", 0)),
            "tp1_rate": float(stats.get("tp1_rate", 0)),
            "tp2_rate": float(stats.get("tp2_rate", 0)),
            "tp3_rate": float(stats.get("tp3_rate", 0))
        })

# Сортировка по WinRate
comparison.sort(key=lambda x: x["winrate"], reverse=True)

# Вывод рейтинга
print("🏆 Рейтинг источников по WinRate:\n")
for i, item in enumerate(comparison, 1):
    print(f"{i}. {item['source']}")
    print(f"   WinRate: {item['winrate']:.1f}%")
    print(f"   Avg P/L: {item['avg_pnl']:+.2f}")
    print(f"   TP3 Rate: {item['tp3_rate']:.1f}%")
    print(f"   Сделок: {item['trades']}")
    print()
```

### Вывод лучшего источника

```python
# Находим лучший источник по различным критериям

# По WinRate
best_by_winrate = max(comparison, key=lambda x: x["winrate"])
print(f"🏆 Лучший по WinRate: {best_by_winrate['source']} ({best_by_winrate['winrate']:.1f}%)")

# По Average P/L
best_by_pnl = max(comparison, key=lambda x: x["avg_pnl"])
print(f"💰 Лучший по Avg P/L: {best_by_pnl['source']} ({best_by_pnl['avg_pnl']:+.2f})")

# По TP3 достижениям
best_by_tp3 = max(comparison, key=lambda x: x["tp3_rate"])
print(f"🎯 Лучший по TP3: {best_by_tp3['source']} ({best_by_tp3['tp3_rate']:.1f}%)")
```

## 📈 Отчёты с источниками

### Детальный отчёт с разбивкой

```python
from services.reporting_service import ReportingService

reporting = ReportingService()

# Получение отчёта с разбивкой по источникам
report = reporting.get_strategy_report(
    "orderflow",
    "XAUUSD",
    "tick",
    include_sources=True
)

# Общая статистика
print(f"Всего сделок: {report.get('total_trades', 0)}")
print(f"WinRate: {report.get('winrate', 0)}%")
print(f"Total P/L: {report.get('total_pnl', 0)}")

# Статистика по источникам
print("\nПо источникам:")
for source, stats in report.get('sources', {}).items():
    print(f"\n  {source}:")
    print(f"    Сделок: {stats.get('total_trades', 0)}")
    print(f"    WinRate: {stats.get('winrate', 0)}%")
    print(f"    Total P/L: {stats.get('total_pnl', 0)}")
    print(f"    TP1/TP2/TP3: {stats.get('tp1_rate', 0)}% / {stats.get('tp2_rate', 0)}% / {stats.get('tp3_rate', 0)}%")
```

## 💾 Экспорт данных по источникам

### Сохранение в JSON

```python
import json

sources_summary = reporting.get_sources_summary()

# Сохранение в файл
with open("/tmp/sources_performance.json", "w") as f:
    json.dump(sources_summary, f, indent=2)

print("✅ Данные сохранены в /tmp/sources_performance.json")
```

### Формат экспорта

```json
{
	"OrderFlow": {
		"total_trades": 45,
		"wins": 32,
		"losses": 13,
		"total_pnl": 234.5,
		"winrate": 71.1,
		"avg_pnl": 5.21
	},
	"AggregatedHub-V2": {
		"total_trades": 38,
		"wins": 24,
		"losses": 14,
		"total_pnl": 187.3,
		"winrate": 63.2,
		"avg_pnl": 4.93
	},
	"TechnicalAnalysis": {
		"total_trades": 12,
		"wins": 7,
		"losses": 5,
		"total_pnl": 45.8,
		"winrate": 58.3,
		"avg_pnl": 3.82
	}
}
```

## 🎯 Use Cases

### 1. Определение наиболее эффективного источника

```python
sources_summary = reporting.get_sources_summary()

# Фильтруем источники с достаточным количеством сделок
min_trades = 10
qualified_sources = {
    source: data
    for source, data in sources_summary.items()
    if data.get("total_trades", 0) >= min_trades
}

# Находим лучший
if qualified_sources:
    best = max(qualified_sources.items(), key=lambda x: x[1]["winrate"])
    print(f"🏆 Лучший источник: {best[0]}")
    print(f"   WinRate: {best[1]['winrate']}%")
    print(f"   Avg P/L: {best[1]['avg_pnl']}")
```

### 2. A/B тестирование источников

```python
# Сравнение двух источников
source_a = "OrderFlow"
source_b = "AggregatedHub-V2"

stats_a = StatsAggregator.get_stats_by_source(
    redis_client, "orderflow", "XAUUSD", "tick", source_a
)

stats_b = StatsAggregator.get_stats_by_source(
    redis_client, "orderflow", "XAUUSD", "tick", source_b
)

print(f"Сравнение {source_a} vs {source_b}:")
print(f"\n{source_a}:")
print(f"  WinRate: {stats_a.get('winrate', 0)}%")
print(f"  Avg P/L: {stats_a.get('avg_pnl', 0)}")

print(f"\n{source_b}:")
print(f"  WinRate: {stats_b.get('winrate', 0)}%")
print(f"  Avg P/L: {stats_b.get('avg_pnl', 0)}")

# Определение победителя
wr_a = float(stats_a.get("winrate", 0))
wr_b = float(stats_b.get("winrate", 0))

if wr_a > wr_b:
    print(f"\n🏆 Победитель: {source_a} (+{wr_a - wr_b:.1f}% WR)")
else:
    print(f"\n🏆 Победитель: {source_b} (+{wr_b - wr_a:.1f}% WR)")
```

### 3. Мониторинг деградации источника

```python
import time

# Проверяем производительность источника каждые 30 минут
while True:
    stats = StatsAggregator.get_stats_by_source(
        redis_client, "orderflow", "XAUUSD", "tick", "OrderFlow"
    )

    winrate = float(stats.get("winrate", 0))

    # Алерт если WinRate падает ниже порога
    if winrate < 50.0:
        print(f"⚠️ АЛЕРТ: WinRate источника OrderFlow упал до {winrate:.1f}%")
        # Можно отправить уведомление

    time.sleep(1800)  # 30 минут
```

## 📊 Примеры отчётов

### Детальный отчёт с источниками

```python
from services.reporting_service import ReportingService

reporting = ReportingService()

report = reporting.get_strategy_report(
    "orderflow",
    "XAUUSD",
    "tick",
    include_sources=True
)

# Общая статистика
print("=" * 60)
print(f"Стратегия: {report.get('strategy', 'N/A')}")
print(f"Символ: {report.get('symbol', 'N/A')}")
print(f"Таймфрейм: {report.get('tf', 'N/A')}")
print("=" * 60)
print(f"Всего сделок: {report.get('total_trades', 0)}")
print(f"WinRate: {report.get('winrate', 0):.1f}%")
print(f"Total P/L: {report.get('total_pnl', 0):+.2f}")
print(f"Avg P/L: {report.get('avg_pnl', 0):+.2f}")

# Разбивка по источникам
print("\nРазбивка по источникам:")
print("-" * 60)

for source, stats in report.get('sources', {}).items():
    print(f"\n{source}:")
    print(f"  Сделок: {stats.get('total_trades', 0)}")
    print(f"  WinRate: {stats.get('winrate', 0)}%")
    print(f"  Total P/L: {stats.get('total_pnl', 0):+.2f}")
    print(f"  Avg P/L: {stats.get('avg_pnl', 0):+.2f}")
    print(f"  TP1 Rate: {stats.get('tp1_rate', 0)}%")
    print(f"  TP2 Rate: {stats.get('tp2_rate', 0)}%")
    print(f"  TP3 Rate: {stats.get('tp3_rate', 0)}%")
```

### Вывод:

```
============================================================
Стратегия: orderflow
Символ: XAUUSD
Таймфрейм: tick
============================================================
Всего сделок: 45
WinRate: 71.1%
Total P/L: +234.50
Avg P/L: +5.21

Разбивка по источникам:
------------------------------------------------------------

OrderFlow:
  Сделок: 18
  WinRate: 83.3%
  Total P/L: +156.80
  Avg P/L: +8.71
  TP1 Rate: 94.4%
  TP2 Rate: 77.8%
  TP3 Rate: 55.6%

AggregatedHub-V2:
  Сделок: 20
  WinRate: 65.0%
  Total P/L: +67.40
  Avg P/L: +3.37
  TP1 Rate: 85.0%
  TP2 Rate: 55.0%
  TP3 Rate: 30.0%

TechnicalAnalysis:
  Сделок: 7
  WinRate: 57.1%
  Total P/L: +10.30
  Avg P/L: +1.47
  TP1 Rate: 71.4%
  TP2 Rate: 42.9%
  TP3 Rate: 14.3%
```

## 📉 Графический анализ (будущее)

### Сравнительная таблица источников

```python
import pandas as pd

sources_summary = reporting.get_sources_summary()

# Конвертация в DataFrame
df = pd.DataFrame(sources_summary).T

# Вывод таблицы
print(df[['total_trades', 'winrate', 'avg_pnl']])
```

### Визуализация (планируется)

```python
import matplotlib.pyplot as plt

# Bar chart по WinRate
sources = list(sources_summary.keys())
winrates = [data['winrate'] for data in sources_summary.values()]

plt.bar(sources, winrates)
plt.title('WinRate по источникам')
plt.ylabel('WinRate (%)')
plt.show()
```

## 🔧 Прямые запросы к Redis

### Получение статистики

```bash
# Общая статистика
redis-cli HGETALL stats:orderflow:XAUUSD:tick

# Статистика по источнику OrderFlow
redis-cli HGETALL stats:orderflow:XAUUSD:tick:OrderFlow

# Статистика по источнику AggregatedHub-V2
redis-cli HGETALL stats:orderflow:XAUUSD:tick:AggregatedHub-V2
```

### Получение списка источников

```bash
redis-cli SMEMBERS stats:sources:orderflow:XAUUSD:tick
```

### Получение сделок по источнику

```bash
# Последние 10 сделок от OrderFlow
redis-cli LRANGE closed:orderflow:XAUUSD:tick:OrderFlow -10 -1

# Детали конкретной сделки
redis-cli HGETALL order:{order_id}
```

## 💡 Best Practices

### 1. Фильтрация по минимальному объёму

```python
# Рассматриваем только источники с достаточным количеством сделок
MIN_TRADES = 20

sources_summary = reporting.get_sources_summary()
reliable_sources = {
    source: data
    for source, data in sources_summary.items()
    if data.get("total_trades", 0) >= MIN_TRADES
}
```

### 2. Взвешенная оценка

```python
# Учитываем и WinRate и объём сделок
for source, data in sources_summary.items():
    trades = data.get("total_trades", 0)
    winrate = data.get("winrate", 0)

    # Weighted score (больше сделок = больше вес)
    confidence = min(trades / 100.0, 1.0)  # макс при 100+ сделках
    weighted_score = winrate * confidence

    print(f"{source}: WR={winrate:.1f}%, Trades={trades}, Score={weighted_score:.1f}")
```

### 3. Автоматическое переключение источников

```python
# Выбираем лучший источник динамически
sources_summary = reporting.get_sources_summary()

best_source = max(
    sources_summary.items(),
    key=lambda x: (
        x[1].get("winrate", 0) *
        min(x[1].get("total_trades", 0) / 50.0, 1.0)  # учитываем объём
    )
)

print(f"🎯 Рекомендуемый источник: {best_source[0]}")
print(f"   WinRate: {best_source[1]['winrate']}%")
print(f"   Сделок: {best_source[1]['total_trades']}")
```

## 🚀 Интеграция в уведомления

По умолчанию все периодические сводки (3ч) и ежедневные сводки включают разбивку по источникам:

```python
# Периодические сводки автоматически включают источники
tracker = SignalPerformanceTracker(config)
tracker.start()

# Будет отправляться каждые 3 часа с разбивкой по источникам
```

Для отключения разбивки:

```python
# В ReportingService
reporting.send_daily_summary(include_sources=False)
```

## 📝 Заметки

- Статистика по источникам обновляется **атомарно** вместе с общей статистикой
- Использует **Redis pipeline** для производительности
- **Обратно совместима** с системами без source (используется "unknown")
- **Автоматическое индексирование** источников
