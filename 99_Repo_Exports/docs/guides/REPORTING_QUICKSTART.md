# 📊 Быстрый старт: Система отчетов

## Проблема решена ✅

**Что было**: Отчеты статистики не приходили в Telegram бот

**Что сделано**:
1. ✅ ReportingService теперь отправляет через Redis stream `notify:telegram`
2. ✅ Создан сервис `periodic-reporter` для автоматической отправки отчетов
3. ✅ Единый путь доставки для сигналов и отчетов
4. ✅ Добавлены команды в Makefile

## Запуск

### 1. Пересобрать и запустить

```bash
cd /home/alex/front/trade/scanner_infra

# Пересобрать periodic-reporter
docker-compose build periodic-reporter

# Запустить сервис
docker-compose up -d periodic-reporter
```

### 2. Проверить работу

```bash
# Проверить что сервис запущен
docker ps | grep periodic-reporter

# Посмотреть логи
make logs-reporter
# или
docker-compose logs -f periodic-reporter
```

### 3. Протестировать

```bash
# Тест системы отчетов
make test-reporting

# Отправить отчет вручную
make send-report-now
```

## Ожидаемое поведение

### При старте сервиса

```
📊 Periodic Reporter Service
══════════════════════════════════════════════════════════════════════
Периодические отчеты: каждые 3 часа
Ежедневный отчет: 00:00 UTC
Redis: redis://scanner-redis-worker-1:6379/0
══════════════════════════════════════════════════════════════════════
🚀 Запуск Periodic Reporter...
✅ Расписание настроено:
   - Периодический отчет: каждые 3ч
   - Ежедневный отчет: 00:00 UTC
📤 Отправка стартового отчета...
✅ Отчет опубликован в notify:telegram: 1730736542123-0
🔄 Вход в главный цикл...
```

### В Telegram

Вы будете получать:

**Каждые 3 часа**:
```
📊 Периодический отчет
🕐 2025-11-04 15:30 UTC

Всего сделок: 45
Выигрышей: 28 (62.2%)
Проигрышей: 17
Общий P/L: +125.50
Средний P/L: +2.79

📡 По источникам сигналов:
  • OrderFlow: 20 сделок, WR 65.0%, P/L +68.00
  • AggregatedHub-V2: 15 сделок, WR 60.0%, P/L +42.50
  • TechnicalAnalysis: 10 сделок, WR 60.0%, P/L +15.00
```

**Ежедневно в 00:00 UTC**:
```
📊 Ежедневная сводка

Всего сделок: 145
Выигрышей: 92
Проигрышей: 53
WinRate: 63.4%
Общий P/L: +456.70

orderflow: 60 сделок, WR 65.0%, P/L +234.00
aggregated: 50 сделок, WR 62.0%, P/L +156.70
ta: 35 сделок, WR 60.0%, P/L +66.00

📊 По источникам:
  • OrderFlow: 80 сделок, WR 65.0%, P/L +312.00
  • AggregatedHub-V2: 42 сделок, WR 61.9%, P/L +102.50
  • TechnicalAnalysis: 23 сделок, WR 60.9%, P/L +42.20
```

## Команды Makefile

```bash
# Тест системы отчетов
make test-reporting

# Просмотр логов
make logs-reporter

# Перезапуск сервиса
make restart-reporter

# Отправить отчет сейчас (вручную)
make send-report-now
```

## Проверка Redis

```bash
# Подключиться к Redis
docker exec -it scanner-redis-worker-1 redis-cli

# Посмотреть последние сообщения
XREVRANGE notify:telegram + - COUNT 5

# Длина stream
XLEN notify:telegram

# Consumer groups
XINFO GROUPS notify:telegram
```

## Настройка

### Изменить периодичность

Отредактируйте `docker-compose.yml`:

```yaml
periodic-reporter:
  environment:
    - PERIODIC_REPORT_INTERVAL_HOURS=1  # Каждый час вместо 3
    - DAILY_REPORT_TIME=09:00           # В 09:00 UTC вместо 00:00
```

Перезапустите:
```bash
make restart-reporter
```

## Архитектура

```
СИГНАЛЫ                         ОТЧЕТЫ
   │                               │
   │  signal-generator             │  periodic-reporter
   │  OrderFlow                    │  (каждые 3ч + ежедневно)
   │  AggregatedHub-V2             │
   │                               │
   └────────┬──────────────────────┘
            │
            ▼
     notify:telegram
      (Redis Stream)
            │
            ▼
      bot-nest.ts
    или notify-worker
            │
            ▼
     Telegram Bot API
            │
            ▼
         👤 USER
```

## Troubleshooting

### Отчеты не приходят

```bash
# 1. Проверить что сервис запущен
docker ps | grep periodic-reporter

# 2. Проверить логи на ошибки
docker-compose logs periodic-reporter | grep ERROR

# 3. Проверить Redis stream
docker exec scanner-redis-worker-1 redis-cli XLEN notify:telegram

# 4. Проверить bot-nest
docker-compose logs bot-nest | grep "Sent message"

# 5. Запустить тест
make test-reporting
```

### Нет статистики

Если отчет пустой, нет закрытых сделок:

```bash
# Проверить статистику в Redis
docker exec scanner-redis-worker-1 redis-cli KEYS "stats:*"

# Посмотреть конкретную статистику
docker exec scanner-redis-worker-1 redis-cli HGETALL "stats:orderflow:XAUUSD:tick"
```

### Отключить отчеты

```bash
docker-compose stop periodic-reporter
```

### Включить обратно

```bash
docker-compose start periodic-reporter
```

## Дополнительные файлы

- **Полная документация**: `python-worker/services/REPORTING_FIX.md`
- **Исходный код**: `python-worker/services/periodic_reporter.py`
- **ReportingService**: `python-worker/services/reporting_service.py`
- **Тестовый скрипт**: `scripts/test_reporting.py`

## Контакты

При проблемах проверьте:
1. Логи periodic-reporter: `make logs-reporter`
2. Логи bot-nest: `docker-compose logs bot-nest`
3. Redis stream: `docker exec scanner-redis-worker-1 redis-cli XLEN notify:telegram`
4. Запустите тест: `make test-reporting`

