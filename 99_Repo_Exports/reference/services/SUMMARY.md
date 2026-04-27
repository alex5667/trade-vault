# Итоговая сводка: Signal Performance Tracker v1.0

## 🎉 Что было создано

Полнофункциональная система отслеживания эффективности торговых сигналов с поддержкой **разбивки по источникам**.

## 📦 Компоненты системы

### 1. **Основные сервисы**

| Файл                            | Описание             | Особенности                                                                    |
| ------------------------------- | -------------------- | ------------------------------------------------------------------------------ |
| `trade_monitor.py`              | Мониторинг позиций   | Частичное закрытие 50%/30%/20%, отслеживание по source                         |
| `stats_aggregator.py`           | Агрегация статистики | Статические методы, Redis pipeline, **двойная статистика** (общая + по source) |
| `reporting_service.py`          | Отчёты и уведомления | Telegram интеграция, **разбивка по источникам** в отчётах                      |
| `signal_performance_tracker.py` | Главный оркестратор  | Consumer groups, graceful shutdown, **периодические сводки каждые 3ч**         |

### 2. **Скрипты**

| Файл                          | Назначение                        |
| ----------------------------- | --------------------------------- |
| `run_performance_tracker.py`  | Standalone запуск с ENV vars      |
| `example_usage.py`            | 6 примеров базового использования |
| `example_sources_analysis.py` | 7 примеров анализа источников ⭐  |

### 3. **Документация**

| Файл                          | Содержание                                |
| ----------------------------- | ----------------------------------------- |
| `README_SIGNAL_TRACKER.md`    | Полная документация                       |
| `INTEGRATION_GUIDE.md`        | Руководство по интеграции                 |
| `NOTIFICATION_INTEGRATION.md` | Настройка Telegram                        |
| `DEPLOYMENT.md`               | Развёртывание (standalone/docker/systemd) |
| `SOURCE_STATISTICS.md`        | Работа со статистикой по источникам ⭐    |
| `QUICKSTART_SOURCES.md`       | Быстрый старт с источниками ⭐            |
| `CHANGELOG.md`                | История изменений                         |
| `SUMMARY.md`                  | Итоговая сводка (этот файл)               |

### 4. **Конфигурация**

- `config/signal_tracker_config.json` - основной конфиг

## ⭐ Ключевые особенности

### 1. Частичное закрытие позиций

```
TP1 (R:R 1.0): 50% позиции
TP2 (R:R 2.0): 30% позиции
TP3 (R:R 3.0): 20% позиции
```

### 2. Статистика по источникам сигналов

Полная разбивка метрик для каждого источника:

- **OrderFlow** - чистые order flow сигналы
- **AggregatedHub-V2** - агрегированные сигналы
- **TechnicalAnalysis** - технический анализ

**Redis схема:**

```
stats:{strategy}:{symbol}:{tf}             - общая статистика
stats:{strategy}:{symbol}:{tf}:{source}    - по источнику
```

**API:**

```python
# Список источников
sources = StatsAggregator.get_strategy_sources(redis, "orderflow", "XAUUSD", "tick")

# Статистика по источнику
stats = StatsAggregator.get_stats_by_source(redis, "orderflow", "XAUUSD", "tick", "OrderFlow")

# Сводка по всем источникам
summary = reporting.get_sources_summary()
```

### 3. Периодические сводки каждые 3 часа

**По умолчанию ВКЛЮЧЕНЫ** для избежания спама:

```json
{
	"reporting": {
		"periodic_summary_enabled": true,
		"periodic_summary_interval_hours": 3
	}
}
```

**Формат уведомления:**

```
🗓 Итоги за 3ч

• orderflow: 8 сделок, WR 75.0%, P/L +67.40

📊 По источникам:
  • OrderFlow: 3 сделки, WR 100.0%, P/L +45.20
  • AggregatedHub-V2: 5 сделок, WR 60.0%, P/L +22.20
```

### 4. Атомарные операции Redis

Использование **Redis pipeline** для:

- Инкрементальных обновлений счётчиков
- Одновременного обновления общей статистики и статистики по источникам
- Избежания race conditions

### 5. Интеграция с существующей инфраструктурой

- ✅ Совместимость с `telegram-worker`, `bot-nest`, `notify-bridge`
- ✅ Использование существующих Redis Streams
- ✅ Следование архитектуре `python-worker`
- ✅ Consumer groups для масштабирования

## 🚀 Использование

### Запуск системы

```bash
# Standalone
cd python-worker
python run_performance_tracker.py

# С переменными окружения
export SYMBOLS=XAUUSD
export STRATEGIES=orderflow
export PERIODIC_SUMMARY=true
export PERIODIC_SUMMARY_HOURS=3
python run_performance_tracker.py
```

### Получение статистики

```python
from services.stats_aggregator import StatsAggregator
from services.reporting_service import ReportingService
from core.redis_client import get_redis

redis_client = get_redis()

# Общая статистика
stats = StatsAggregator.get_stats(redis_client, "orderflow", "XAUUSD", "tick")

# По источнику
stats_of = StatsAggregator.get_stats_by_source(
    redis_client, "orderflow", "XAUUSD", "tick", "OrderFlow"
)

# Сводка по всем источникам
reporting = ReportingService()
sources = reporting.get_sources_summary()
```

### Анализ источников

```bash
# Сравнение источников
python services/example_sources_analysis.py 1

# Определение лучшего источника
python services/example_sources_analysis.py 4

# Мониторинг в реальном времени
python services/example_sources_analysis.py 5
```

## 📊 Структура данных

### Общая схема

```
┌─────────────────────────────────────────┐
│  Signals (входящие)                     │
│  signals:{strategy}:{symbol}            │
│  + field: source                        │
└──────────────┬──────────────────────────┘
               │
               ▼
┌──────────────────────────────────────────┐
│  Trade Monitor                           │
│  - Создаёт Position с source             │
│  - Отслеживает по тикам                  │
│  - Частичное закрытие TP1/TP2/TP3        │
└──────────────┬───────────────────────────┘
               │
               ▼
┌──────────────────────────────────────────┐
│  Stats Aggregator                        │
│  - Обновляет stats:{s}:{sym}:{tf}       │
│  - Обновляет stats:{s}:{sym}:{tf}:{src} │
│  - Redis pipeline (атомарно)             │
└──────────────┬───────────────────────────┘
               │
               ▼
┌──────────────────────────────────────────┐
│  Reporting Service                       │
│  - Отчёты с разбивкой по источникам      │
│  - Telegram уведомления каждые 3ч        │
│  - Сводки по источникам                  │
└──────────────────────────────────────────┘
```

### Redis ключи

#### Статистика

```
# Общая
stats:orderflow:XAUUSD:tick

# По источникам
stats:orderflow:XAUUSD:tick:OrderFlow
stats:orderflow:XAUUSD:tick:AggregatedHub-V2
stats:orderflow:XAUUSD:tick:TechnicalAnalysis
```

#### Сделки

```
# Общий список
closed:orderflow:XAUUSD:tick

# По источникам
closed:orderflow:XAUUSD:tick:OrderFlow
closed:orderflow:XAUUSD:tick:AggregatedHub-V2
closed:orderflow:XAUUSD:tick:TechnicalAnalysis
```

#### Индексы

```
stats:strategies                          - Set всех стратегий
stats:symbols:{strategy}                  - Set символов
stats:tfs:{strategy}:{symbol}             - Set таймфреймов
stats:sources:{strategy}:{symbol}:{tf}    - Set источников ⭐
```

## 📈 Метрики

### Доступны для каждого источника отдельно:

- ✅ Total Trades
- ✅ Wins / Losses
- ✅ WinRate (%)
- ✅ Total P/L
- ✅ Average P/L
- ✅ TP1/TP2/TP3 Hit Rates
- ✅ Max Win/Loss (планируется)
- ✅ Profit Factor (планируется)

## 🎯 Use Cases

### 1. Сравнение эффективности источников

```python
from services.reporting_service import ReportingService

reporting = ReportingService()
sources = reporting.get_sources_summary()

for source, data in sources.items():
    print(f"{source}: WR {data['winrate']}%, P/L {data['total_pnl']}")
```

**Результат:**

```
OrderFlow: WR 80.0%, P/L +156.80
AggregatedHub-V2: WR 65.0%, P/L +67.40
TechnicalAnalysis: WR 57.1%, P/L +10.30
```

### 2. Выбор лучшего источника

```python
best = max(sources.items(), key=lambda x: x[1]['winrate'])
print(f"🏆 Лучший источник: {best[0]}")
```

### 3. A/B тестирование

```python
# Сравнение двух источников
stats_a = StatsAggregator.get_stats_by_source(redis, "orderflow", "XAUUSD", "tick", "OrderFlow")
stats_b = StatsAggregator.get_stats_by_source(redis, "orderflow", "XAUUSD", "tick", "AggregatedHub-V2")

print(f"OrderFlow WR: {stats_a['winrate']}%")
print(f"AggregatedHub WR: {stats_b['winrate']}%")
```

### 4. Мониторинг деградации

```python
# Отслеживание падения производительности
for source in sources:
    stats = StatsAggregator.get_stats_by_source(redis, "orderflow", "XAUUSD", "tick", source)
    if float(stats.get('winrate', 100)) < 50.0:
        print(f"⚠️ АЛЕРТ: {source} WinRate упал до {stats['winrate']}%")
```

## 🔧 Конфигурация

### Рекомендуемая (production)

```json
{
	"monitor": {
		"notify_on_trade_close": false
	},
	"reporting": {
		"daily_summary_enabled": true,
		"periodic_summary_enabled": true,
		"periodic_summary_interval_hours": 3
	}
}
```

### Для тестирования

```json
{
	"monitor": {
		"notify_on_trade_close": true
	},
	"reporting": {
		"periodic_summary_enabled": true,
		"periodic_summary_interval_hours": 1
	}
}
```

## 📱 Уведомления

### Расписание (по умолчанию)

| Время      | Уведомление            | Включает источники |
| ---------- | ---------------------- | ------------------ |
| Каждые 3ч  | Периодическая сводка   | ✅ Да              |
| 00:00 UTC  | Ежедневная сводка      | ✅ Да              |
| По запросу | Отчёт по стратегии     | ✅ Опционально     |
| При сделке | Уведомление о закрытии | ❌ Выключено       |

### Пример периодической сводки

```
🗓 Итоги за 3ч

• orderflow: 12 сделок, WR 75.0%, P/L +89.40

📊 По источникам:
  • OrderFlow: 5 сделок, WR 80.0%, P/L +45.20
  • AggregatedHub-V2: 5 сделок, WR 60.0%, P/L +34.10
  • TechnicalAnalysis: 2 сделки, WR 100.0%, P/L +10.10
```

## 🎁 Дополнительные возможности

- ✅ **Сравнительный анализ** источников (7 примеров в `example_sources_analysis.py`)
- ✅ **Экспорт данных** по источникам в JSON
- ✅ **Мониторинг в реальном времени** производительности каждого источника
- ✅ **Автоматическое индексирование** источников в Redis
- ✅ **Постраничная выборка** сделок по источникам
- ✅ **Обратная совместимость** (если source не указан, используется "unknown")

## 📚 Документация

### Быстрый старт

1. `README_SIGNAL_TRACKER.md` - начните здесь
2. `QUICKSTART_SOURCES.md` - быстрый старт с источниками
3. `INTEGRATION_GUIDE.md` - интеграция в проект

### Детальная информация

- `SOURCE_STATISTICS.md` - полная документация по источникам
- `NOTIFICATION_INTEGRATION.md` - настройка уведомлений
- `DEPLOYMENT.md` - развёртывание

### Примеры кода

- `example_usage.py` - базовые примеры
- `example_sources_analysis.py` - анализ источников

## 🚀 Быстрый запуск

### 1. Базовый запуск

```bash
cd /home/alex/front/trade/scanner_infra/python-worker
python run_performance_tracker.py
```

### 2. С настройкой периодических сводок

```bash
export PERIODIC_SUMMARY=true
export PERIODIC_SUMMARY_HOURS=3
export TELEGRAM_BOT_TOKEN=your_token
export TELEGRAM_CHAT_ID=your_chat_id
python run_performance_tracker.py
```

### 3. Анализ источников

```bash
# Сравнение источников
python services/example_sources_analysis.py 1

# Лучший источник
python services/example_sources_analysis.py 4
```

## 💡 Примеры использования

### Получение статистики по источнику

```python
from services.stats_aggregator import StatsAggregator
from core.redis_client import get_redis

redis_client = get_redis()

# Статистика OrderFlow
stats = StatsAggregator.get_stats_by_source(
    redis_client, "orderflow", "XAUUSD", "tick", "OrderFlow"
)

print(f"OrderFlow - Сделок: {stats['total_trades']}")
print(f"OrderFlow - WinRate: {stats['winrate']}%")
print(f"OrderFlow - Total P/L: {stats['total_pnl']}")
```

### Сравнение источников

```python
from services.reporting_service import ReportingService

reporting = ReportingService()
sources = reporting.get_sources_summary()

for source, data in sources.items():
    print(f"{source}:")
    print(f"  Сделок: {data['total_trades']}")
    print(f"  WinRate: {data['winrate']:.1f}%")
    print(f"  P/L: {data['total_pnl']:+.2f}")
```

### Отчёт с разбивкой по источникам

```python
report = reporting.get_strategy_report(
    "orderflow", "XAUUSD", "tick",
    include_sources=True
)

# Общая статистика
print(f"Total: {report['total_trades']} trades")

# По источникам
for source, stats in report.get('sources', {}).items():
    print(f"{source}: WR {stats['winrate']}%")
```

## 🎯 Рекомендации

### Настройки уведомлений

✅ **Рекомендуется:**

- Периодические сводки каждые 3 часа (включены по умолчанию)
- Ежедневные сводки в 00:00 UTC
- Разбивка по источникам в сводках

❌ **Не рекомендуется:**

- Уведомления при каждой сделке (спам)

### Анализ эффективности источников

1. **Начните с минимума** - 10+ сделок для надёжности
2. **Сравнивайте** - используйте `example_sources_analysis.py`
3. **Мониторьте** - отслеживайте деградацию
4. **Оптимизируйте** - выбирайте лучший источник

## 🔮 Что дальше?

### Планируется в v1.1

- [ ] WebSocket API для real-time обновлений
- [ ] Web Dashboard с графиками по источникам
- [ ] Сравнительные графики источников
- [ ] Автоматическое переключение на лучший источник

### Планируется в v1.2

- [ ] ML-анализ качества источников
- [ ] Precision/Recall по источникам
- [ ] Correlation analysis между источниками
- [ ] Ensemble методы (комбинация источников)

## ✅ Checklist использования

- [ ] Система запущена (`run_performance_tracker.py`)
- [ ] Telegram настроен (токен и chat_id)
- [ ] Периодические сводки включены (3ч)
- [ ] Сигналы содержат поле `source`
- [ ] Проверена статистика по источникам
- [ ] Настроен мониторинг эффективности
- [ ] Выбран оптимальный источник

## 📞 Дополнительная информация

Полная документация в файлах:

- `README_SIGNAL_TRACKER.md`
- `SOURCE_STATISTICS.md`
- `DEPLOYMENT.md`

Примеры:

- `example_usage.py`
- `example_sources_analysis.py`

## 🎊 Готово к использованию!

Система полностью интегрирована и готова отслеживать эффективность ваших торговых сигналов с **разбивкой по источникам**! 🚀
