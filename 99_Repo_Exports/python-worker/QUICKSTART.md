# ⚡ Signal Performance Tracker - Шпаргалка

## 🚀 Запуск (одна команда)

```bash
cd /home/alex/front/trade/scanner_infra/python-worker && python run_performance_tracker.py
```

## 📊 Что умеет?

✅ Отслеживание эффективности сигналов  
✅ Частичное закрытие: TP1(50%), TP2(30%), TP3(20%)  
✅ Статистика по источникам: OrderFlow, AggregatedHub-V2, TechnicalAnalysis  
✅ Метрики упущенной прибыли: TP1→SL, TP2→SL  
✅ Автоматические сводки каждые 3 часа

## 💡 Полезные команды

```bash
# Запуск
python run_performance_tracker.py

# Тестирование
python test_performance_tracker.py

# Анализ упущенной прибыли
python services/analyze_missed_profit.py

# Сравнение источников
python services/example_sources_analysis.py 1

# Статистика через Redis
redis-cli HGETALL stats:orderflow:XAUUSD:tick

# По источнику OrderFlow
redis-cli HGETALL stats:orderflow:XAUUSD:tick:OrderFlow
```

## 📚 Документация

**Начните с:** `services/00_START_HERE.md`  
**Навигация:** `services/INDEX.md`  
**Полная:** `services/README_SIGNAL_TRACKER.md`

## 🎯 Python API

```python
# Статистика
from services.stats_aggregator import StatsAggregator
from core.redis_client import get_redis

redis = get_redis()
stats = StatsAggregator.get_stats(redis, "orderflow", "XAUUSD", "tick")

# По источнику
stats_of = StatsAggregator.get_stats_by_source(
    redis, "orderflow", "XAUUSD", "tick", "OrderFlow"
)

# Сводка по источникам
from services.reporting_service import ReportingService
reporting = ReportingService()
sources = reporting.get_sources_summary()
```

## ⚙️ Конфигурация

```bash
# ENV переменные
export SYMBOLS=XAUUSD
export STRATEGIES=orderflow
export PERIODIC_SUMMARY_HOURS=3
export TELEGRAM_BOT_TOKEN=your_token
export TELEGRAM_CHAT_ID=your_chat_id

# Запуск
python run_performance_tracker.py
```

## 📁 Файлы проекта: 25

**Код:** 8 файлов (~4,000 строк)  
**Документация:** 14 файлов (~5,100 строк)  
**Утилиты:** 3 файла

**ИТОГО:** 13,025+ строк

## ✅ Всё готово!

Начните с `python run_performance_tracker.py` 🚀
