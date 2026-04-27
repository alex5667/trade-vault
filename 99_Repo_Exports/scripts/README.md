# 📂 Scripts Directory

Эта папка содержит все утилитные скрипты проекта Scanner Infrastructure.

## 📊 Статистика

**Всего скриптов:** 107 файлов

## 📁 Категории скриптов

### 🔧 Redis Management

- `redis-*.sh` - управление Redis (мониторинг, оптимизация, подключение)
- `redis_*.py` - Python скрипты для работы с Redis
- `clear_candles_data*.sh` - очистка данных свечей
- `sync_redis_*.py` - синхронизация данных между Redis

### 📡 Telegram Worker

- `*telegram*.sh/py` - скрипты для работы с Telegram
- `load_telegram_channels.py` - загрузка каналов Telegram
- `create_telegram_session_auto.py` - автоматическое создание сессии
- `setup-telegram-session.py` - настройка сессии

### ✅ Health Checks & Monitoring

- `check_*.sh/py` - скрипты проверки компонентов
- `health_check_all.sh` - общая проверка здоровья системы
- `monitor_*.sh/py` - мониторинг различных компонентов
- `verify_*.sh/py` - скрипты верификации

### 🚀 Deployment & Management

- `start-scanner.sh` - запуск сканера
- `stop-scanner.sh` - остановка сканера
- `restart_*.sh` - перезапуск компонентов
- `docker-compose-wrapper.sh` - обертка для docker-compose

### 🛠️ Configuration & Setup

- `apply-*.sh` - применение конфигураций
- `backup_config.sh` - резервное копирование конфигурации
- `setup-*.sh` - скрипты настройки
- `initialize-*.sh` - инициализация компонентов

### 🧹 Cleanup & Maintenance

- `clear_trades_and_signals.sh` - очистка всех данных по сделкам и сигналам (упрощенная версия с использованием массивов и циклов)
- `cleanup_*.py` - очистка данных
- `clear-ports*.sh` - очистка портов
- `manual-cleanup.sh` - ручная очистка
- `redis-streams-cleanup.sh` - очистка Redis Streams
- `clear_candles_data*.sh` - очистка данных свечей

### 📊 Data Processing

- `parse-and-transfer.py` - парсинг и передача данных
- `export_to_6380.py` - экспорт данных
- `transfer-candles-data.sh` - передача данных свечей
- `send_signal_to_redis.py` - отправка сигналов в Redis

### 🔍 Debugging & Testing

- `demo_full_cycle.py` - демонстрация полного цикла
- `send_test_signal_to_bot.py` - отправка тестового сигнала
- `view_alerts.py` - просмотр алертов
- `check_and_fix_duplicates.sh` - проверка и исправление дубликатов

### 📝 Logging & Monitoring Setup

- `add_*_logs.py` - добавление логирования
- `add_monitoring_*.py` - добавление мониторинга
- `comprehensive_timestamp_audit.py` - аудит временных меток
- `precise_timestamp_audit.py` - точный аудит времени

### 🔄 Migration & Updates

- `commit_redis_migration.sh` - коммит миграции Redis
- `update_*.py` - обновление компонентов
- `sync_redis_data.py` - синхронизация данных

### 🎯 Regime & Quantiles

- `check_regime_flow.sh` - проверка потока режимов
- `reset_regime_state.sh` - сброс состояния режима

## 🚀 Часто используемые скрипты

### Запуск и остановка

```bash
./scripts/start-scanner.sh          # Запуск сканера
./scripts/stop-scanner.sh           # Остановка сканера
./scripts/restart_all_properly.sh   # Корректный перезапуск всех компонентов
```

### Проверка здоровья

```bash
./scripts/health_check_all.sh           # Общая проверка
./scripts/check_workers.sh              # Проверка воркеров
./scripts/check_telegram_worker.sh      # Проверка Telegram worker
./scripts/check_and_fix_duplicates.sh   # Проверка дубликатов сообщений
```

### Redis

```bash
./scripts/redis-health-check.sh         # Проверка здоровья Redis
./scripts/redis-quick-check.sh          # Быстрая проверка Redis
./scripts/redis-monitor.sh              # Мониторинг Redis
./scripts/redis-streams-cleanup.sh      # Очистка Redis Streams
```

### Мониторинг

```bash
./scripts/monitor_redis_performance.sh  # Мониторинг производительности Redis
./scripts/monitor_telegram_events.sh    # Мониторинг событий Telegram
./scripts/monitor-goroutines.sh         # Мониторинг горутин
```

### Очистка

```bash
./scripts/clear_trades_and_signals.sh   # Очистка всех данных по сделкам и сигналам
./scripts/clear_trades_and_signals.sh --yes  # Автоматическое подтверждение
./scripts/clear_candles_data.sh         # Очистка данных свечей
./scripts/clear_duplicate_streams.sh    # Очистка дубликатов в streams
./scripts/clear-ports.sh                # Очистка портов
```

**Примечание:** Скрипт `clear_trades_and_signals.sh` очищает данные из всех Redis контейнеров (scanner-redis, scanner-redis-worker-1, scanner-redis-worker-2), удаляя:
- Все сигналы (signals:*, signal:*)
- Все ордера (order:*)
- Все сделки (trade:*)
- Все события (events:trades, trades:closed)

## 📝 Примечания

- Все скрипты должны запускаться из **корня проекта**: `./scripts/script_name.sh`
- Большинство скриптов требуют Docker и docker-compose
- Перед запуском скриптов убедитесь, что они исполняемые: `chmod +x scripts/*.sh`
- Скрипты с префиксом `add_*` - это утилиты для добавления функциональности
- Скрипты с префиксом `check_*` - это проверочные утилиты
- Скрипты с префиксом `monitor_*` - это утилиты мониторинга

## ⚠️ Важные скрипты

### Критические для работы системы:

- `start-scanner.sh` - запуск всей системы
- `stop-scanner.sh` - корректная остановка
- `health_check_all.sh` - диагностика проблем

### Для отладки:

- `check_and_fix_duplicates.sh` - исправление дубликатов сообщений
- `redis-health-comprehensive.sh` - подробная диагностика Redis
- `check_workers.sh` - проверка всех воркеров

### Для обслуживания:

- `backup_config.sh` - резервное копирование конфигурации
- `cleanup_invalid_channels.py` - очистка невалидных каналов
- `redis-streams-cleanup.sh` - очистка старых данных

## 🔄 Последнее обновление

**Дата:** 25 октября 2025  
**Версия:** 1.0  
**Изменения:** Все скрипты перенесены из корня проекта в папку `/scripts/` для лучшей организации.

## 📚 Связанные документы

- [QUICK_FIX_GUIDE.md](../QUICK_FIX_GUIDE.md) - Быстрое исправление дубликатов
- [DUPLICATE_MESSAGES_FIX.md](../DUPLICATE_MESSAGES_FIX.md) - Исправление дубликатов сообщений
- [docker-compose.yml](../docker-compose.yml) - Основная конфигурация

---

💡 **Совет:** Используйте `./scripts/health_check_all.sh` для быстрой диагностики проблем!
