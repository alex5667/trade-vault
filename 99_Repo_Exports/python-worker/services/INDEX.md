# 📚 Signal Performance Tracker - Навигация

## 🎯 Начните здесь

### Быстрый старт

1. **`README_SIGNAL_TRACKER.md`** - полная документация, начните отсюда
2. **`QUICKSTART_SOURCES.md`** - быстрый старт с источниками
3. **`FINAL_SUMMARY.md`** - обзор всей системы

### Запуск

```bash
python run_performance_tracker.py
```

## 📂 Структура документации

### 🚀 Для начинающих

| Файл                       | Описание              | Когда читать               |
| -------------------------- | --------------------- | -------------------------- |
| `README_SIGNAL_TRACKER.md` | Основная документация | Первым делом               |
| `QUICKSTART_SOURCES.md`    | Быстрый старт         | После README               |
| `FINAL_SUMMARY.md`         | Краткий обзор         | Для понимания всей картины |

### 🔧 Для разработчиков

| Файл                          | Описание             | Когда читать             |
| ----------------------------- | -------------------- | ------------------------ |
| `INTEGRATION_GUIDE.md`        | Интеграция в проект  | При добавлении в систему |
| `DEPLOYMENT.md`               | Развёртывание        | При настройке production |
| `NOTIFICATION_INTEGRATION.md` | Telegram уведомления | При настройке алертов    |

### 📊 Для аналитиков

| Файл                        | Описание                    | Когда читать             |
| --------------------------- | --------------------------- | ------------------------ |
| `SOURCE_STATISTICS.md`      | Статистика по источникам    | Для сравнения источников |
| `MISSED_PROFIT_ANALYSIS.md` | Анализ упущенной прибыли    | Для оптимизации выхода   |
| `SUMMARY.md`                | Краткая сводка возможностей | Для быстрого обзора      |

### 🔍 Справочная

| Файл           | Описание           | Когда читать              |
| -------------- | ------------------ | ------------------------- |
| `CHANGELOG.md` | История изменений  | Для понимания эволюции    |
| `CHECKLIST.md` | Проверочный список | Для верификации настройки |
| `INDEX.md`     | Этот файл          | Для навигации             |

## 🎯 По задачам

### Хочу запустить систему

→ `DEPLOYMENT.md` → `run_performance_tracker.py`

### Хочу сравнить источники сигналов

→ `SOURCE_STATISTICS.md` → `example_sources_analysis.py`

### Хочу оптимизировать стратегию

→ `MISSED_PROFIT_ANALYSIS.md` → `analyze_missed_profit.py`

### Хочу настроить Telegram

→ `NOTIFICATION_INTEGRATION.md` → `config/signal_tracker_config.json`

### Хочу интегрировать в проект

→ `INTEGRATION_GUIDE.md` → примеры в коде

## 📁 Исходные файлы

### Сервисы (Python)

```
services/
├── trade_monitor.py              # Trade Monitor Service
├── stats_aggregator.py            # Stats Aggregator
├── reporting_service.py           # Reporting Service
└── signal_performance_tracker.py  # Main Orchestrator
```

### Скрипты (Executable)

```
├── run_performance_tracker.py        # Запуск системы
├── services/
│   ├── example_usage.py              # 6 базовых примеров
│   ├── example_sources_analysis.py   # 7 примеров источников
│   └── analyze_missed_profit.py      # Анализ TP→SL
```

### Конфигурация

```
config/
└── signal_tracker_config.json     # Основной конфиг
```

## 🎓 Уровни сложности

### Новичок

1. Читайте `README_SIGNAL_TRACKER.md`
2. Запустите `python run_performance_tracker.py`
3. Смотрите примеры в `example_usage.py`

### Средний

1. Изучите `INTEGRATION_GUIDE.md`
2. Настройте Telegram из `NOTIFICATION_INTEGRATION.md`
3. Запустите анализ источников: `example_sources_analysis.py`

### Эксперт

1. Изучите `SOURCE_STATISTICS.md` и `MISSED_PROFIT_ANALYSIS.md`
2. Используйте `analyze_missed_profit.py` для оптимизации
3. Создайте собственные анализаторы на основе API

### Senior (40 лет опыта)

1. Все документы для глубокого понимания
2. Модифицируйте под свои нужды
3. Добавьте ML-анализ, custom метрики
4. Интегрируйте с автоматической торговлей

## 🔍 Поиск по функционалу

### "Как получить статистику?"

→ `README_SIGNAL_TRACKER.md` → секция "API" → `StatsAggregator.get_stats()`

### "Как сравнить источники?"

→ `SOURCE_STATISTICS.md` → `example_sources_analysis.py`

### "Как узнать упущенную прибыль?"

→ `MISSED_PROFIT_ANALYSIS.md` → `analyze_missed_profit.py`

### "Как настроить уведомления?"

→ `NOTIFICATION_INTEGRATION.md` → `config/signal_tracker_config.json`

### "Как запустить в Docker?"

→ `DEPLOYMENT.md` → секция "Docker Compose"

### "Как работает частичное закрытие?"

→ `README_SIGNAL_TRACKER.md` → секция "Частичное закрытие позиций"

## 📊 Примеры команд

```bash
# Запуск системы
python run_performance_tracker.py

# Анализ упущенной прибыли
python services/analyze_missed_profit.py

# Сравнение источников
python services/example_sources_analysis.py 1

# Определение лучшего источника
python services/example_sources_analysis.py 4

# Мониторинг в реальном времени
python services/example_sources_analysis.py 5

# Детальный отчёт OrderFlow
python services/example_sources_analysis.py 6

# Статистика по источникам через Redis
redis-cli HGETALL stats:orderflow:XAUUSD:tick:OrderFlow

# Список всех источников
redis-cli SMEMBERS stats:sources:orderflow:XAUUSD:tick
```

## 🎓 Рекомендуемый порядок изучения

1. **День 1**: Общее понимание

   - `README_SIGNAL_TRACKER.md`
   - `FINAL_SUMMARY.md`
   - Запустить `run_performance_tracker.py`

2. **День 2**: Анализ источников

   - `SOURCE_STATISTICS.md`
   - `QUICKSTART_SOURCES.md`
   - Примеры `example_sources_analysis.py`

3. **День 3**: Оптимизация

   - `MISSED_PROFIT_ANALYSIS.md`
   - Анализ `analyze_missed_profit.py`
   - Оптимизация параметров

4. **День 4**: Production

   - `DEPLOYMENT.md`
   - `NOTIFICATION_INTEGRATION.md`
   - Настройка мониторинга

5. **День 5**: Интеграция
   - `INTEGRATION_GUIDE.md`
   - Подключение к вашим обработчикам
   - Кастомизация

## 💡 Tips

- 📖 Документация обширная - используйте поиск (Ctrl+F)
- 🔍 Все примеры executable - запускайте и экспериментируйте
- 📊 Начните с малого - один символ, одна стратегия
- ⚙️ Настройте уведомления - это сэкономит время
- 📈 Анализируйте метрики TP→SL - там скрыты инсайты

## 🆘 Помощь

### Проблемы с запуском

→ `DEPLOYMENT.md` → секция "Troubleshooting"

### Вопросы по API

→ `README_SIGNAL_TRACKER.md` → секция "Примеры использования"

### Нет данных

→ Проверьте Redis streams и consumer groups

### Telegram не работает

→ `NOTIFICATION_INTEGRATION.md` → секция "Troubleshooting"

---

**Начните с `README_SIGNAL_TRACKER.md` и удачи! 🚀**
