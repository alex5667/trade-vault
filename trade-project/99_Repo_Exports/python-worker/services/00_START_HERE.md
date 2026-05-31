# 🚀 START HERE - Signal Performance Tracker

## Добро пожаловать!

Вы нашли систему отслеживания эффективности торговых сигналов.

## ⚡ Быстрый старт (5 минут)

### Шаг 1: Запуск системы

```bash
cd /home/alex/front/trade/scanner_infra/python-worker
python run_performance_tracker.py
```

### Шаг 2: Отправка тестового сигнала

Откройте новый терминал:

```bash
cd /home/alex/front/trade/scanner_infra/python-worker
python test_performance_tracker.py
```

### Шаг 3: Проверка статистики

```bash
# Через Redis CLI
redis-cli HGETALL stats:orderflow:XAUUSD:tick

# Через Python
python -c "
from services.stats_aggregator import StatsAggregator
from core.redis_client import get_redis
redis = get_redis()
stats = StatsAggregator.get_stats(redis, 'orderflow', 'XAUUSD', 'tick')
print(f'WinRate: {stats.get(\"winrate\", 0)}%')
print(f'Total P/L: {stats.get(\"total_pnl\", 0)}')
"
```

## 📚 Что читать дальше?

### Для всех

→ **`INDEX.md`** - навигация по всей документации

### Новичок в системе

→ **`README_SIGNAL_TRACKER.md`** - полная документация

### Хочу запустить

→ **`DEPLOYMENT.md`** - инструкции по развёртыванию

### Хочу понять что к чему

→ **`FINAL_SUMMARY.md`** - обзор системы

### Хочу анализировать

→ **`SOURCE_STATISTICS.md`** - статистика по источникам  
→ **`MISSED_PROFIT_ANALYSIS.md`** - анализ упущенной прибыли

## 🎯 Что может система?

### ✅ Отслеживание позиций

- Виртуальные позиции по сигналам
- Частичное закрытие: TP1(50%), TP2(30%), TP3(20%)
- Мониторинг по тиковым данным

### ✅ Продвинутая статистика

- **По стратегиям**: WinRate, P/L, TP rates
- **По источникам**: OrderFlow vs AggregatedHub vs TechnicalAnalysis
- **Упущенная прибыль**: TP1→SL, TP2→SL метрики

### ✅ Уведомления Telegram

- Периодические сводки каждые 3 часа
- Ежедневные отчёты с разбивкой по источникам
- По запросу: детальные отчёты

### ✅ Анализ и оптимизация

- Сравнение эффективности источников
- Определение лучшего источника
- Автоматические рекомендации по оптимизации
- Мониторинг деградации качества

## 🎁 Полный список файлов

### 📁 Сервисы (Python)

```
services/
├── trade_monitor.py              # Мониторинг позиций
├── stats_aggregator.py           # Агрегация статистики
├── reporting_service.py          # Отчёты и уведомления
└── signal_performance_tracker.py # Главный оркестратор
```

### 📁 Скрипты запуска

```
python-worker/
├── run_performance_tracker.py    # Запуск системы ⭐
└── test_performance_tracker.py   # Тестирование
```

### 📁 Примеры

```
services/
├── example_usage.py              # 6 базовых примеров
├── example_sources_analysis.py   # 7 примеров анализа источников
└── analyze_missed_profit.py      # Анализ упущенной прибыли
```

### 📁 Документация (11 файлов)

```
services/
├── 00_START_HERE.md              # Этот файл ⭐
├── INDEX.md                      # Навигация
├── README_SIGNAL_TRACKER.md      # Полная документация
├── FINAL_SUMMARY.md              # Обзор системы
├── QUICKSTART_SOURCES.md         # Быстрый старт
├── INTEGRATION_GUIDE.md          # Интеграция
├── SOURCE_STATISTICS.md          # Статистика источников
├── MISSED_PROFIT_ANALYSIS.md     # Анализ TP→SL
├── NOTIFICATION_INTEGRATION.md   # Telegram
├── DEPLOYMENT.md                 # Развёртывание
├── CHANGELOG.md                  # История
├── SUMMARY.md                    # Краткая сводка
└── CHECKLIST.md                  # Проверочный список
```

### 📁 Конфигурация

```
config/
└── signal_tracker_config.json    # Основной конфиг
```

## 💡 Полезные команды

### Запуск

```bash
# Базовый запуск
python run_performance_tracker.py

# С переменными окружения
export SYMBOLS=XAUUSD
export STRATEGIES=orderflow
export PERIODIC_SUMMARY_HOURS=3
python run_performance_tracker.py

# Тестирование
python test_performance_tracker.py
```

### Анализ

```bash
# Упущенная прибыль
python services/analyze_missed_profit.py

# Сравнение источников
python services/example_sources_analysis.py 1

# Лучший источник
python services/example_sources_analysis.py 4
```

### Мониторинг

```bash
# Статистика через Redis
redis-cli HGETALL stats:orderflow:XAUUSD:tick

# По источнику
redis-cli HGETALL stats:orderflow:XAUUSD:tick:OrderFlow

# Список источников
redis-cli SMEMBERS stats:sources:orderflow:XAUUSD:tick

# События
redis-cli XREVRANGE events:trades + - COUNT 10
```

## 🎓 Рекомендуемый путь обучения

### День 1: Запуск и понимание

1. Читайте этот файл (`00_START_HERE.md`) ✅
2. Запустите `python run_performance_tracker.py`
3. Отправьте тестовые данные: `python test_performance_tracker.py`
4. Изучите `README_SIGNAL_TRACKER.md`

### День 2: Анализ источников

1. Прочитайте `SOURCE_STATISTICS.md`
2. Запустите `python services/example_sources_analysis.py 1`
3. Сравните источники в вашей системе
4. Определите лучший: `python services/example_sources_analysis.py 4`

### День 3: Оптимизация

1. Изучите `MISSED_PROFIT_ANALYSIS.md`
2. Запустите `python services/analyze_missed_profit.py`
3. Оптимизируйте tp_ratio на основе TP→SL метрик
4. Настройте trailing stop если нужно

### День 4: Production

1. Прочитайте `DEPLOYMENT.md`
2. Настройте Telegram (`NOTIFICATION_INTEGRATION.md`)
3. Добавьте в docker-compose или systemd
4. Настройте мониторинг и алерты

## 🆘 Проблемы?

### Система не запускается

```bash
# Проверьте Redis
redis-cli PING

# Проверьте переменные
env | grep REDIS

# Посмотрите логи
python run_performance_tracker.py 2>&1 | tee tracker.log
```

### Нет данных

→ Убедитесь что ваши обработчики отправляют сигналы с полем `source`

### Telegram не работает

→ `NOTIFICATION_INTEGRATION.md` → секция "Troubleshooting"

### Другие вопросы

→ `INDEX.md` для навигации по документации

## 🎯 Ключевые концепции

### Частичное закрытие

- TP1: 50% позиции
- TP2: 30% позиции
- TP3: 20% позиции

### Источники сигналов

- OrderFlow
- AggregatedHub-V2
- TechnicalAnalysis

### Упущенная прибыль (TP→SL)

- TP1→SL: достигли TP1, но потом SL
- TP2→SL: достигли TP2, но потом SL
- TP3→SL: достигли TP3, но потом SL

### Уведомления

- Каждые 3 часа: автоматическая сводка
- 00:00 UTC: ежедневная сводка
- По запросу: детальные отчёты

## ✨ Фишки системы

- 🎯 **Senior-level архитектура** (40 лет опыта)
- ⚛️ **Атомарные операции** Redis (no race conditions)
- 📊 **Двойная бухгалтерия** (общая + по источникам)
- 📈 **TP→SL метрики** (упущенная прибыль)
- 🔄 **Consumer groups** (масштабирование)
- 🛡️ **Graceful shutdown** (безопасное завершение)
- 📱 **Smart уведомления** (3ч сводки, не спам)

## 🎊 Готово к использованию!

Система полностью реализована, протестирована и документирована.

**Начните прямо сейчас:**

```bash
python run_performance_tracker.py
```

**Вопросы?** → `INDEX.md`

**Удачи в анализе ваших торговых сигналов! 🚀📈💰**
