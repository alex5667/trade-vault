# Signal Performance Tracker - Главный README

## 🎯 Что это?

**Signal Performance Tracker** - система отслеживания эффективности торговых сигналов на основе реальных тиковых данных.

## ⭐ Ключевые особенности

- ✅ **Частичное закрытие**: TP1(50%), TP2(30%), TP3(20%)
- ✅ **Статистика по источникам**: OrderFlow, AggregatedHub-V2, TechnicalAnalysis
- ✅ **Метрики упущенной прибыли**: TP1→SL, TP2→SL, TP3→SL
- ✅ **Автоматические сводки**: каждые 3 часа + ежедневные
- ✅ **Production-ready**: атомарность, масштабирование, graceful shutdown

## 🚀 Быстрый старт

```bash
cd /home/alex/front/trade/scanner_infra/python-worker

# Запуск системы
python run_performance_tracker.py

# Тестирование
python test_performance_tracker.py

# Анализ упущенной прибыли
python services/analyze_missed_profit.py

# Сравнение источников
python services/example_sources_analysis.py 1
```

## 📚 Документация

### Начните здесь

- **`services/00_START_HERE.md`** ⭐ - начальная точка
- **`services/INDEX.md`** - навигация по документации
- **`services/README_SIGNAL_TRACKER.md`** - полное руководство

### Специализированная

- **`services/SOURCE_STATISTICS.md`** - статистика по источникам
- **`services/MISSED_PROFIT_ANALYSIS.md`** - анализ TP→SL метрик
- **`services/DEPLOYMENT.md`** - развёртывание
- **`services/NOTIFICATION_INTEGRATION.md`** - Telegram

### Справочная

- **`services/FINAL_SUMMARY.md`** - обзор системы
- **`services/CHANGELOG.md`** - история изменений
- **`services/CHECKLIST.md`** - проверочный список

## 📊 Компоненты

| Компонент         | Файл                                     | Назначение           |
| ----------------- | ---------------------------------------- | -------------------- |
| Trade Monitor     | `services/trade_monitor.py`              | Отслеживание позиций |
| Stats Aggregator  | `services/stats_aggregator.py`           | Подсчёт метрик       |
| Reporting Service | `services/reporting_service.py`          | Отчёты и уведомления |
| Orchestrator      | `services/signal_performance_tracker.py` | Координация          |

## 🎯 Метрики

### Базовые

Total Trades, Wins/Losses, WinRate, P/L, Average P/L

### TP метрики

TP1/TP2/TP3 Hit Rates

### Упущенная прибыль ⭐

TP1→SL, TP2→SL, TP3→SL (count + rate)

### По источникам ⭐

Разбивка всех метрик для OrderFlow, AggregatedHub-V2, TechnicalAnalysis

## 📱 Уведомления

**По умолчанию:**

- ✅ Каждые 3 часа - периодическая сводка с источниками
- ✅ 00:00 UTC - ежедневная сводка
- ❌ При каждой сделке - выключено (спам)

## 🔧 Конфигурация

### Переменные окружения

```bash
export SYMBOLS=XAUUSD
export STRATEGIES=orderflow
export PERIODIC_SUMMARY_HOURS=3
export TELEGRAM_BOT_TOKEN=your_token
export TELEGRAM_CHAT_ID=your_chat_id
```

### Конфигурационный файл

`config/signal_tracker_config.json`

## 💡 Примеры использования

```python
# Получение статистики
from services.stats_aggregator import StatsAggregator
from core.redis_client import get_redis

redis = get_redis()
stats = StatsAggregator.get_stats(redis, "orderflow", "XAUUSD", "tick")

# По источнику
stats_of = StatsAggregator.get_stats_by_source(
    redis, "orderflow", "XAUUSD", "tick", "OrderFlow"
)

# Сводка по всем источникам
from services.reporting_service import ReportingService
reporting = ReportingService()
sources = reporting.get_sources_summary()
```

## 🧪 Тестирование

```bash
# Запуск тестов
python test_performance_tracker.py

# Примеры
python services/example_usage.py 1
python services/example_sources_analysis.py 1
python services/analyze_missed_profit.py
```

## 📦 Структура проекта

```
python-worker/
├── run_performance_tracker.py        # Запуск
├── test_performance_tracker.py       # Тестирование
├── config/
│   └── signal_tracker_config.json    # Конфигурация
└── services/
    ├── trade_monitor.py              # Trade Monitor
    ├── stats_aggregator.py           # Stats Aggregator
    ├── reporting_service.py          # Reporting
    ├── signal_performance_tracker.py # Orchestrator
    ├── example_usage.py              # Примеры
    ├── example_sources_analysis.py   # Анализ источников
    ├── analyze_missed_profit.py      # Анализ TP→SL
    └── [11 файлов документации]
```

## 🎊 Готово!

**Вся система реализована и готова к использованию.**

**Начните с:**

```bash
python run_performance_tracker.py
```

**Документация:**

- `services/00_START_HERE.md` - начните здесь
- `services/INDEX.md` - навигация
- `services/README_SIGNAL_TRACKER.md` - полное руководство

**Успешного трейдинга! 🚀**
