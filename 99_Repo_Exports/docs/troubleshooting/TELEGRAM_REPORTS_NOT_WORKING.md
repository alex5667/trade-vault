# 🔍 Диагностика: Отчеты не приходят в Telegram

## Проблема

Отчеты по сигналам/сделкам не приходят в Telegram бот, хотя сигналы приходят нормально.

## Архитектура доставки отчетов

```
ReportingService → notify:telegram (Redis Stream) → notify-worker → Telegram Bot API → Telegram
```

### Компоненты:

1. **ReportingService** (`python-worker/services/reporting_service.py`)
   - Формирует отчеты
   - Публикует в `notify:telegram` stream с `type="report"`

2. **notify-worker** (`telegram-worker/notify_worker.py`)
   - Читает из `notify:telegram` stream
   - Обрабатывает сообщения с `type="report"`
   - Отправляет через `send_html_to_telegram()`

3. **ImprovedTelegramNotifier** (`telegram-worker/improved_notifier.py`)
   - Отправляет сообщения в Telegram Bot API
   - Требует `TELEGRAM_BOT_TOKEN` и `TELEGRAM_CHAT_ID`

## Возможные причины

### 1. ❌ notify-worker не запущен

**Симптомы:**
- Сообщения накапливаются в `notify:telegram` stream
- Pending сообщения в consumer group

**Решение:**
```bash
# Проверка статуса
docker-compose ps notify-worker

# Запуск
docker-compose up -d notify-worker

# Проверка логов
docker logs scanner-notify-worker --tail 50 -f
```

### 2. ❌ Не настроены Telegram credentials

**Симптомы:**
- В логах notify-worker: "Telegram бот не настроен"
- Ошибки отправки в Telegram API

**Решение:**
```bash
# Проверка переменных окружения
docker exec scanner-notify-worker env | grep TELEGRAM

# Установка в .env файл telegram-worker/.env
TELEGRAM_BOT_TOKEN=your_bot_token
TELEGRAM_CHAT_ID=your_chat_id
```

### 3. ❌ ReportingService не публикует отчеты

**Симптомы:**
- Нет сообщений с `type="report"` в `notify:telegram` stream
- Логи signal-performance-tracker показывают ошибки

**Решение:**
```bash
# Проверка логов signal-performance-tracker
docker logs scanner-signal-tracker --tail 100 | grep -i report

# Проверка stream вручную
docker exec scanner-redis redis-cli XREVRANGE notify:telegram + - COUNT 10
```

### 4. ❌ Consumer group не создана или удалена

**Симптомы:**
- Ошибки "NOGROUP" в логах notify-worker
- Сообщения не обрабатываются

**Решение:**
```bash
# notify-worker автоматически создает группу при запуске
# Но можно проверить вручную:
docker exec scanner-redis redis-cli XINFO GROUPS notify:telegram
```

### 5. ❌ Сообщения застряли в pending

**Симптомы:**
- Pending сообщения в consumer group
- notify-worker не обрабатывает их

**Решение:**
```bash
# Проверка pending
docker exec scanner-redis redis-cli XPENDING notify:telegram notify-group

# Очистка старых pending (если нужно)
docker exec scanner-redis redis-cli XAUTOCLAIM notify:telegram notify-group notify-consumer 0 0
```

## Диагностика

### Автоматическая диагностика

Запустите диагностический скрипт:

```bash
cd /home/alex/front/trade/scanner_infra
python3 scripts/diagnose_reports.py
```

Скрипт проверит:
- ✅ Подключение к Redis
- ✅ Состояние `notify:telegram` stream
- ✅ Наличие сообщений с `type="report"`
- ✅ Конфигурацию Telegram
- ✅ Статус notify-worker контейнера
- ✅ Pending сообщения

### Ручная диагностика

#### 1. Проверка stream

```bash
# Длина stream
docker exec scanner-redis redis-cli XLEN notify:telegram

# Последние сообщения
docker exec scanner-redis redis-cli XREVRANGE notify:telegram + - COUNT 10

# Поиск отчетов
docker exec scanner-redis redis-cli XREVRANGE notify:telegram + - COUNT 100 | grep -A 5 "type.*report"
```

#### 2. Проверка notify-worker

```bash
# Статус контейнера
docker-compose ps notify-worker

# Логи
docker logs scanner-notify-worker --tail 100 -f

# Проверка переменных окружения
docker exec scanner-notify-worker env | grep TELEGRAM
```

#### 3. Проверка signal-performance-tracker

```bash
# Логи
docker logs scanner-signal-tracker --tail 100 | grep -i report

# Проверка, что сервис запущен
docker-compose ps signal-performance-tracker
```

#### 4. Тестовая отправка отчета

```python
# В контейнере signal-tracker
docker exec -it scanner-signal-tracker python3

from services.reporting_service import ReportingService
import os

reporting = ReportingService(
    redis_url=os.getenv("REDIS_URL", "redis://scanner-redis:6379/0")
)

# Тестовая отправка
reporting.send_telegram_message("🧪 Тестовый отчет из диагностики")
```

## Решения по компонентам

### ReportingService

**Файл:** `python-worker/services/reporting_service.py`

**Метод:** `send_telegram_message()`

**Проверка:**
- Публикует в `notify:telegram` с `type="report"`
- Поле `text` содержит HTML-форматированный отчет
- Использует правильный Redis клиент

### notify-worker

**Файл:** `telegram-worker/notify_worker.py`

**Проверка:**
- Обрабатывает сообщения с `type="report"` (строки 82-97)
- Вызывает `send_html_to_telegram()` для отчетов
- Consumer group создана и работает

### ImprovedTelegramNotifier

**Файл:** `telegram-worker/improved_notifier.py`

**Проверка:**
- `TELEGRAM_BOT_TOKEN` установлен
- `TELEGRAM_CHAT_ID` установлен
- Функция `send_notification()` работает корректно

## Частые проблемы и решения

### Проблема: "NOGROUP" ошибка

**Причина:** Consumer group удалена или не создана

**Решение:**
```bash
# notify-worker автоматически создаст группу при запуске
# Или вручную:
docker exec scanner-redis redis-cli XGROUP CREATE notify:telegram notify-group $ MKSTREAM
```

### Проблема: Отчеты публикуются, но не отправляются

**Причина:** notify-worker не читает stream или не обрабатывает `type="report"`

**Решение:**
1. Проверьте логи notify-worker
2. Убедитесь, что обработка `type="report"` включена (строки 82-97)
3. Проверьте, что `send_html_to_telegram()` вызывается

### Проблема: Telegram API ошибки

**Причина:** Неверный токен, chat_id или rate limiting

**Решение:**
1. Проверьте `TELEGRAM_BOT_TOKEN` и `TELEGRAM_CHAT_ID`
2. Проверьте, что бот добавлен в чат
3. Проверьте rate limits Telegram API

### Проблема: Отчеты не формируются

**Причина:** Signal Performance Tracker не вызывает ReportingService

**Решение:**
1. Проверьте логи signal-performance-tracker
2. Убедитесь, что `_send_periodic_report()` вызывается
3. Проверьте конфигурацию `PERIODIC_REPORT_HOURS`

## Конфигурация

### Environment Variables

```yaml
# docker-compose.yml
notify-worker:
  environment:
    - REDIS_URL=redis://scanner-redis:6379/0
    - NOTIFY_STREAM=notify:telegram
    - NOTIFY_GROUP=notify-group
    - TELEGRAM_BOT_TOKEN=${TELEGRAM_BOT_TOKEN}
    - TELEGRAM_CHAT_ID=${TELEGRAM_CHAT_ID}

signal-performance-tracker:
  environment:
    - REDIS_URL=redis://scanner-redis-worker-1:6379/0
    - PERIODIC_REPORT_HOURS=3
    - DAILY_SUMMARY=true
    - DAILY_SUMMARY_HOUR=0
```

## Мониторинг

### Проверка работы в реальном времени

```bash
# Логи notify-worker
docker logs scanner-notify-worker -f

# Логи signal-tracker
docker logs scanner-signal-tracker -f | grep -i report

# Мониторинг stream
watch -n 1 'docker exec scanner-redis redis-cli XLEN notify:telegram'
```

### Метрики

- Количество сообщений в `notify:telegram` stream
- Pending сообщения в consumer group
- Успешные отправки в Telegram (логи notify-worker)
- Ошибки отправки (логи notify-worker)

## Контакты и дополнительная информация

- Документация: `docs/fixes/FIX_SUMMARY_REPORTING.md`
- Документация: `python-worker/services/REPORTING_FIX.md`
- Диагностический скрипт: `scripts/diagnose_reports.py`













