# Log Sampling System

Система сэмплинга логов для уменьшения шума в логах путем вывода только каждого N-го повторяющегося сообщения.

## Проблема

В высоконагруженных системах некоторые типы логов повторяются очень часто, создавая шум:
- Grafana update checks: "Update check succeeded"
- PeriodicReporter summaries: "📊 Итого собрано 0 сделок..."
- Trade veto messages: "Data-quality veto", "Confidence threshold veto"
- Health checks и метрики

## Решение

Система `LogSampler` позволяет выводить только каждое N-е сообщение определенного типа, значительно снижая объем логов при сохранении возможности мониторинга.

## Использование

### Базовое использование

```python
from handlers.crypto_orderflow.utils.log_sampler import sampled_info

# Вместо: logger.info("Update check succeeded")
sampled_info(logger, "update_check", "Update check succeeded")
```

### Переменные окружения

```bash
# Grafana-like логи
LOG_SAMPLE_UPDATE_CHECK_RATE=1000        # Каждое 1000-е сообщение (default)
LOG_SAMPLE_METRICS_RATE=100              # Каждое 100-е сообщение метрик

# PeriodicReporter логи
LOG_SAMPLE_PERIODIC_REPORTER_SUMMARY_RATE=10000     # Каждое 10000-е summary (default)
LOG_SAMPLE_PERIODIC_REPORTER_TRIGGER_RATE=5000      # Каждое 5000-е trigger
LOG_SAMPLE_PERIODIC_REPORTER_SEND_REPORT_RATE=500   # Каждое 500-е send report

# Trade veto логи
LOG_SAMPLE_DQ_VETO_RATE=500               # Каждое 500-е data quality veto
LOG_SAMPLE_CONFIDENCE_THRESHOLD_VETO_RATE=200  # Каждое 200-е confidence veto
LOG_SAMPLE_EV_GATE_VETO_RATE=300          # Каждое 300-е EV gate veto
LOG_SAMPLE_COST_EDGE_VETO_RATE=300        # Каждое 300-е cost edge veto

# Отключение threading для debugging
LOG_SAMPLE_UPDATE_CHECK_THREADING=0       # Без thread safety
```

## Интегрированные компоненты

### CryptoOrderFlow Handler
- ✅ Data quality veto messages
- ✅ Consistency veto messages
- ✅ Confidence threshold veto messages
- ✅ EV-gate veto messages
- ✅ Cost-edge veto messages

### PeriodicReporter Service
- ✅ Summary messages: "📊 Итого собрано X сделок..."
- ✅ Trigger messages: "🚀 Триггер отчета для..."
- ✅ Formation messages: "📤 Формирование отчета для..."
- ✅ Metrics collection messages
- ✅ Send report messages
- ✅ Skip insufficient messages

## Мониторинг сэмплинга

```python
from handlers.crypto_orderflow.utils.log_sampler import LogSamplerFactory

# Получить статистику по всем сэмплерам
stats = LogSamplerFactory.get_stats()
print(f"Sampling counters: {stats}")
# {'UPDATE_CHECK': {'update_check': 15000}, 'PERIODIC_REPORTER_SUMMARY': {'PERIODIC_REPORTER_SUMMARY': 50000}, ...}
```

## Производительность

- Thread-safe по умолчанию
- Минимальный overhead на проверку сэмплинга
- Использует collections.defaultdict для эффективного хранения счетчиков
- Поддерживает отключение threading для single-threaded приложений

## Примеры

См. `grafana_log_sampler_example.py` для демонстрации работы системы.

## Архитектура

- `LogSampler`: Основной класс сэмплера
- `LogSamplerFactory`: Фабрика для создания и управления сэмплерами
- `sampled_info/warning/error/debug`: Удобные функции для логирования
- Конфигурация через environment variables с fallback на defaults
