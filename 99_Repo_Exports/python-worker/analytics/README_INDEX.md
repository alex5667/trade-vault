# 📑 Analytics v2.0 - Documentation Index

Навигация по документации Analytics Package v2.0.

---

## 🎯 С чего начать?

### Для новичков

1. **[00_START_HERE_V2.md](./00_START_HERE_V2.md)** ⭐ **НАЧНИТЕ ЗДЕСЬ**
   - Обзор возможностей
   - Быстрый старт за 5 минут
   - Основные команды

2. **[QUICKSTART_V2.md](./QUICKSTART_V2.md)** ⚡ Быстрые примеры
   - Топ-5 команд
   - Docker примеры
   - Cron настройка

### Для продвинутых

3. **[ANALYTICS_V2_README.md](./ANALYTICS_V2_README.md)** 📖 Полная документация
   - API референс всех модулей
   - Примеры использования
   - Redis схема
   - Best practices

4. **[INTEGRATION_GUIDE.md](./INTEGRATION_GUIDE.md)** 🔗 Интеграция
   - Архитектура интеграции
   - Signal Performance Tracker
   - Grafana setup
   - Docker Compose

---

## 📚 Документация по темам

### Основные возможности

| Тема | Файл | Описание |
|------|------|----------|
| **Обзор проекта** | [ANALYTICS_V2_COMPLETE.md](./ANALYTICS_V2_COMPLETE.md) | Полная сводка проекта |
| **Быстрый старт** | [00_START_HERE_V2.md](./00_START_HERE_V2.md) | Первые шаги |
| **API документация** | [ANALYTICS_V2_README.md](./ANALYTICS_V2_README.md) | Полный API референс |
| **Интеграция** | [INTEGRATION_GUIDE.md](./INTEGRATION_GUIDE.md) | Интеграция с системой |
| **Примеры** | [QUICKSTART_V2.md](./QUICKSTART_V2.md) | Готовые примеры |

---

## 🔧 Модули

### Core Modules

| Модуль | Файл | Описание |
|--------|------|----------|
| **Repository** | `repository.py` | Доступ к данным в Redis |
| **Metrics** | `metrics.py` | Вычисление ROC/AUC, Precision/Recall |
| **Parquet Sink** | `parquet_sink.py` | Запись в Parquet |
| **Tiles Service** | `tiles_service.py` | Фоновая запись тайлов |

### v2.0 Modules (Новые)

| Модуль | Файл | Описание |
|--------|------|----------|
| **Dataset Export** | `dataset_export.py` | Партиционированный экспорт |
| **ROC Store** | `roc_store.py` | Хранение ROC кривых |
| **Threshold Tuner** | `threshold_tuner.py` | Автоподбор порогов |
| **Metrics Publisher** | `metrics_publisher.py` | Публикация в Grafana |
| **Telegram Reporter** | `telegram_reporter_ext.py` | Отчёты с графиками |

### CLI Tools

| Инструмент | Файл | Описание |
|------------|------|----------|
| **Multi Threshold** | `multi_publish_best_threshold.py` | Мульти-тюнинг порогов |
| **Nightly Pipeline** | `nightly_pipeline.py` | Полный ночной прогон |

---

## 🎯 По задачам

### Экспорт датасетов

📖 **Документация:** [ANALYTICS_V2_README.md § Dataset Export](./ANALYTICS_V2_README.md#dataset-export)  
⚡ **Примеры:** [QUICKSTART_V2.md § Экспорт датасета](./QUICKSTART_V2.md#3-экспорт-датасета-для-ml)  
🔗 **Интеграция:** [INTEGRATION_GUIDE.md § Dataset Export](./INTEGRATION_GUIDE.md)

### Тюнинг порогов

📖 **Документация:** [ANALYTICS_V2_README.md § Threshold Tuner](./ANALYTICS_V2_README.md#threshold-tuner)  
⚡ **Примеры:** [QUICKSTART_V2.md § Тюнинг порога](./QUICKSTART_V2.md#1-мульти-тюнинг-порогов)  
🔗 **Интеграция:** [INTEGRATION_GUIDE.md § Threshold Tuner](./INTEGRATION_GUIDE.md#1-автоматическое-обновление-порогов)

### ROC анализ

📖 **Документация:** [ANALYTICS_V2_README.md § ROC Store](./ANALYTICS_V2_README.md#roc-store)  
⚡ **Примеры:** [QUICKSTART_V2.md § ROC отчёт](./QUICKSTART_V2.md#5-отправка-roc-отчёта-в-telegram)

### Telegram отчёты

📖 **Документация:** [ANALYTICS_V2_README.md § Telegram Reporter](./ANALYTICS_V2_README.md#telegram-reporter-extended)  
⚡ **Примеры:** [QUICKSTART_V2.md § Telegram](./QUICKSTART_V2.md#5-отправка-roc-отчёта-в-telegram)  
🔗 **Интеграция:** [INTEGRATION_GUIDE.md § Telegram](./INTEGRATION_GUIDE.md#3-интеграция-с-telegram-notifier)

### Grafana мониторинг

📖 **Документация:** [ANALYTICS_V2_README.md § Grafana](./ANALYTICS_V2_README.md#интеграция-с-grafana)  
🔗 **Интеграция:** [INTEGRATION_GUIDE.md § Grafana](./INTEGRATION_GUIDE.md#интеграция-с-grafana)

### Автоматизация

📖 **Документация:** [ANALYTICS_V2_README.md § Automation](./ANALYTICS_V2_README.md)  
⚡ **Примеры:** [QUICKSTART_V2.md § Cron](./QUICKSTART_V2.md#автоматизация-cron)  
🔗 **Интеграция:** [INTEGRATION_GUIDE.md § Docker](./INTEGRATION_GUIDE.md#docker-compose-интеграция)

---

## 🚀 Быстрые ссылки

### Команды

```bash
# Мульти-тюнинг
python -m analytics.multi_publish_best_threshold --symbols XAUUSD --strategies aggregated --days 7

# Полный прогон
python -m analytics.nightly_pipeline --symbols XAUUSD --strategies aggregated --days 7
```

### Примеры кода

**Dataset Export:**
```python
from analytics.dataset_export import export_dataset_partitioned
path = export_dataset_partitioned(repo, orders, signals)
```

**Threshold Tuning:**
```python
from analytics.threshold_tuner import ThresholdTuner
tuner = ThresholdTuner(repo)
result = tuner.tune_and_publish(strategy="aggregated", symbol="XAUUSD", signals=signals, orders=orders)
```

**Telegram Report:**
```python
from analytics.telegram_reporter_ext import TelegramReporterExt
reporter = TelegramReporterExt()
reporter.send_roc_report(strategy="aggregated", symbol="XAUUSD", roc_points=points, auc=0.72, summary={})
```

---

## 📖 Последовательность чтения

### Для начинающих

1. [00_START_HERE_V2.md](./00_START_HERE_V2.md) - Первое знакомство
2. [QUICKSTART_V2.md](./QUICKSTART_V2.md) - Практические примеры
3. [ANALYTICS_V2_README.md](./ANALYTICS_V2_README.md) - Детальное изучение

### Для опытных разработчиков

1. [ANALYTICS_V2_COMPLETE.md](./ANALYTICS_V2_COMPLETE.md) - Обзор проекта
2. [ANALYTICS_V2_README.md](./ANALYTICS_V2_README.md) - API референс
3. [INTEGRATION_GUIDE.md](./INTEGRATION_GUIDE.md) - Интеграция

---

## 🔍 Поиск по темам

- **Партиционирование:** [ANALYTICS_V2_README.md § Partitioned Dataset](./ANALYTICS_V2_README.md)
- **ROC/AUC:** [ANALYTICS_V2_README.md § ROC Store](./ANALYTICS_V2_README.md)
- **Docker:** [INTEGRATION_GUIDE.md § Docker](./INTEGRATION_GUIDE.md)
- **Cron:** [QUICKSTART_V2.md § Cron](./QUICKSTART_V2.md)
- **Grafana:** [INTEGRATION_GUIDE.md § Grafana](./INTEGRATION_GUIDE.md)
- **Telegram:** [ANALYTICS_V2_README.md § Telegram](./ANALYTICS_V2_README.md)

---

**Выберите нужный раздел и начинайте!** 🚀

