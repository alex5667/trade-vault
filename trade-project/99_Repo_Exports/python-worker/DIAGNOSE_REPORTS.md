# 🔍 Диагностика: Почему отчеты не приходят в бот

## Быстрая диагностика

Запустите диагностический скрипт:

```bash
cd python-worker
python diagnose_reports.py
```

Скрипт проверит:
1. ✅ Подключение к Redis
2. ✅ Запущен ли сервис `periodic-reporter`
3. ✅ Наличие пар `source/symbol` для отчетов
4. ✅ Наличие сделок в окне времени
5. ✅ Состояние `notify:telegram` stream
6. ✅ Работает ли `notify_worker`
7. ✅ Тестовая отправка отчета

## Основные причины отсутствия отчетов

### 1. Сервис `periodic-reporter` не запущен

**Проверка:**
```bash
docker ps | grep periodic-reporter
```

**Решение:**
```bash
docker-compose up -d periodic-reporter
docker-compose logs -f periodic-reporter
```

### 2. Не найдены пары `source/symbol`

**Причины:**
- Нет данных в `stats:strategies`
- Нет записей в `trades:closed` stream
- Нет открытых позиций в `orders:open`

**Проверка:**
```bash
docker exec scanner-redis-worker-1 redis-cli SMEMBERS stats:strategies
docker exec scanner-redis-worker-1 redis-cli XLEN trades:closed
docker exec scanner-redis-worker-1 redis-cli SCARD orders:open
```

**Решение:**
- Убедитесь, что сделки закрываются и попадают в Redis
- Проверьте, что `source` и `symbol` корректно записываются

### 3. Нет сделок в окне времени

**Причины:**
- Окно времени слишком маленькое (`PERIODIC_REPORT_WINDOW_SECONDS`)
- Сделки закрыты вне окна
- Неправильный `source` или `symbol` в данных

**Проверка:**
```bash
# Проверить настройку окна
docker exec scanner-periodic-reporter env | grep PERIODIC_REPORT_WINDOW_SECONDS

# Проверить последние сделки
docker exec scanner-redis-worker-1 redis-cli XREVRANGE trades:closed + - COUNT 10
```

**Решение:**
- Увеличьте `PERIODIC_REPORT_WINDOW_SECONDS` (по умолчанию 3600s = 1 час)
- Установите `PERIODIC_REPORT_SEND_EMPTY=true` для отправки пустых отчетов

### 4. Сообщения не публикуются в `notify:telegram` stream

**Проверка:**
```bash
# Проверить длину stream
docker exec scanner-redis-worker-1 redis-cli XLEN notify:telegram

# Проверить последние сообщения
docker exec scanner-redis-worker-1 redis-cli XREVRANGE notify:telegram + - COUNT 10
```

**Решение:**
- Проверьте логи `periodic-reporter` на ошибки публикации
- Убедитесь, что Redis доступен

### 5. `notify_worker` не читает сообщения

**Проверка:**
```bash
docker ps | grep notify
docker-compose logs notify-worker | tail -50
```

**Решение:**
- Убедитесь, что `notify_worker` или `bot-nest` запущены
- Проверьте логи на ошибки обработки сообщений
- Проверьте настройки `TELEGRAM_BOT_TOKEN` и `TELEGRAM_CHAT_ID`

## Улучшенное логирование

После обновления кода `periodic_reporter.py` логи стали более детальными:

- ✅ Логируется публикация каждого отчета в Redis stream
- ✅ Проверяется длина stream после публикации
- ✅ Логируются причины пропуска отчетов (нет сделок, lock занят и т.д.)
- ✅ Диагностические сообщения при отсутствии данных

## Просмотр логов

```bash
# Логи periodic-reporter
docker-compose logs -f periodic-reporter

# Логи notify-worker
docker-compose logs -f notify-worker

# Логи reporting_service
docker-compose logs python-worker | grep ReportingService
```

## Ручная отправка отчета

Для тестирования можно отправить отчет вручную:

```python
from services.periodic_reporter import PeriodicReporter

reporter = PeriodicReporter()
reporter.send_report_for_pair("OrderFlow", "XAUUSD")
```

Или использовать тестовый скрипт:

```bash
cd python-worker
python test_report_send.py
```

## Переменные окружения

Проверьте настройки в `docker-compose.yml`:

- `PERIODIC_REPORT_WINDOW_SECONDS` - окно времени для сбора сделок (по умолчанию 3600s)
- `PERIODIC_REPORT_SEND_EMPTY` - отправлять ли пустые отчеты (по умолчанию false)
- `REPORT_TRIGGER_COUNT` - количество сделок для триггера отчета (по умолчанию 100)
- `PERIODIC_REPORT_CHECK_INTERVAL_SEC` - интервал проверки пар (по умолчанию 300s)
- `NOTIFY_STREAM` - название Redis stream (по умолчанию `notify:telegram`)

## Цепочка доставки

```
PeriodicReporter
    ↓
ReportingService.send_telegram_message()
    ↓
Redis Stream: notify:telegram
    ↓
notify_worker (telegram-worker)
    ↓
Telegram Bot API
    ↓
Telegram Chat
```

Проверьте каждый этап цепочки с помощью диагностического скрипта.



