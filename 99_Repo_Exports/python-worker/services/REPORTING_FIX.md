# Исправление отправки отчетов в Telegram

## Проблема

Отчеты статистики не приходили в Telegram бот, хотя сигналы приходили нормально.

## Причины

1. **ReportingService** пытался отправлять напрямую через Telegram Bot API, требуя TELEGRAM_BOT_TOKEN и TELEGRAM_CHAT_ID
2. **Не было сервиса** для периодической отправки отчетов
3. **Разные пути доставки**: сигналы шли через `notify:telegram` stream, а отчеты пытались идти напрямую

## Решение

### 1. Изменен ReportingService

**Файл**: `python-worker/services/reporting_service.py`

**Изменения**:

- Метод `send_telegram_message()` теперь отправляет через Redis stream `notify:telegram`
- Убрана зависимость от TELEGRAM_BOT_TOKEN и TELEGRAM_CHAT_ID
- Единый путь доставки для сигналов и отчетов

```python
def send_telegram_message(self, text: str, parse_mode: str = "HTML") -> bool:
    """
    Отправка сообщения в Telegram через Redis stream notify:telegram.
    """
    try:
        notify_stream = os.getenv("NOTIFY_STREAM", "notify:telegram")

        message_data = {
            "type": "report",
            "text": text,
            "source": "ReportingService",
            "timestamp": str(int(time.time() * 1000))
        }

        msg_id = self.redis.xadd(notify_stream, message_data, maxlen=1000)
        self.logger.info(f"✅ Отчет опубликован в {notify_stream}: {msg_id}")
        return True
    except Exception as e:
        self.logger.error(f"❌ Ошибка отправки отчета в Redis stream: {e}")
        return False
```

### 2. Создан Periodic Reporter Service

**Файл**: `python-worker/services/periodic_reporter.py`

**Функционал**:

- Периодические отчеты каждые 3 часа (настраивается через `PERIODIC_REPORT_INTERVAL_HOURS`)
- Ежедневный детальный отчет в 00:00 UTC (настраивается через `DAILY_REPORT_TIME`)
- Разбивка статистики по источникам сигналов (OrderFlow, AggregatedHub-V2, TechnicalAnalysis)
- Автоматическая отправка стартового отчета при запуске

**Параметры окружения**:

```bash
PERIODIC_REPORT_INTERVAL_HOURS=3  # Периодичность отчетов (часы)
DAILY_REPORT_TIME=00:00           # Время ежедневного отчета (UTC)
NOTIFY_STREAM=notify:telegram     # Redis stream для уведомлений
```

### 3. Добавлен в Docker Compose

**Файл**: `docker-compose.yml`

**Сервис**: `periodic-reporter`

```yaml
periodic-reporter:
  profiles:
    - default
  build:
    context: .
    dockerfile: python-worker/Dockerfile
  container_name: scanner-periodic-reporter
  environment:
    - PERIODIC_REPORT_INTERVAL_HOURS=3
    - DAILY_REPORT_TIME=00:00
    - NOTIFY_STREAM=notify:telegram
  depends_on:
    - redis
    - redis-worker-1
    - signal-performance-tracker
```

## Архитектура

```
┌─────────────────────────────────────────────────────────────┐
│                    SIGNAL GENERATION                        │
├─────────────────────────────────────────────────────────────┤
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐     │
│  │ signal-gen   │  │ OrderFlow    │  │ Aggregated   │     │
│  │              │  │ Handler      │  │ Hub-V2       │     │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘     │
│         │                 │                 │              │
│         └─────────────────┴─────────────────┘              │
│                           │                                │
│                           ▼                                │
│                  ╔════════════════════╗                    │
│                  ║ notify:telegram    ║  Redis Stream     │
│                  ║    (Redis)         ║                    │
│                  ╚═════════╤══════════╝                    │
│                           │                                │
└───────────────────────────┼────────────────────────────────┘
                            │
┌───────────────────────────┼────────────────────────────────┐
│                    REPORTING                               │
├───────────────────────────┼────────────────────────────────┤
│  ┌────────────────────────┴───────────────────┐           │
│  │ Periodic Reporter                          │           │
│  │ - Отправка каждые 3ч                       │           │
│  │ - Ежедневный отчет 00:00 UTC               │           │
│  │ - Статистика по источникам                 │           │
│  └────────────────────────┬───────────────────┘           │
│                           │                                │
│                           ▼                                │
│                  ╔════════════════════╗                    │
│                  ║ ReportingService   ║                    │
│                  ║ - get_stats()      ║                    │
│                  ║ - format_message() ║                    │
│                  ║ - send_to_stream() ║                    │
│                  ╚═════════╤══════════╝                    │
│                           │                                │
│                           └────────────────────────────────┼─┐
│                                                            │ │
└────────────────────────────────────────────────────────────┼─┘
                                                             │
┌────────────────────────────────────────────────────────────┼─┐
│                    TELEGRAM DELIVERY                       │ │
├────────────────────────────────────────────────────────────┼─┤
│                           ▼                                │ │
│                  ╔════════════════════╗                    │ │
│                  ║ notify:telegram    ║                    │ │
│                  ║    (Redis)         ║◄───────────────────┘ │
│                  ╚═════════╤══════════╝                      │
│                           │                                  │
│                           ▼                                  │
│                  ╔════════════════════╗                      │
│                  ║ bot-nest/main.ts   ║                      │
│                  ║ или notify-worker  ║                      │
│                  ╚═════════╤══════════╝                      │
│                           │                                  │
│                           ▼                                  │
│                  ╔════════════════════╗                      │
│                  ║ Telegram Bot API   ║                      │
│                  ╚═════════╤══════════╝                      │
│                           │                                  │
│                           ▼                                  │
│                     👤 USER in Bot                           │
└──────────────────────────────────────────────────────────────┘
```

## Запуск и проверка

### 1. Пересоберите образ

```bash
cd /home/alex/front/trade/scanner_infra
docker-compose build periodic-reporter
```

### 2. Запустите сервис

```bash
docker-compose up -d periodic-reporter
```

### 3. Проверьте логи

```bash
# Логи periodic-reporter
docker-compose logs -f periodic-reporter

# Проверка что сообщения попадают в Redis
docker exec scanner-redis-worker-1 redis-cli XLEN notify:telegram

# Логи bot-nest (Node.js bot)
docker-compose logs -f bot-nest

# Или логи notify-worker (Python)
docker-compose logs -f notify-worker
```

### 4. Проверка работы

**Ожидаемое поведение**:

- При старте сервис отправит первый отчет сразу
- Затем отчеты будут приходить каждые 3 часа
- Ежедневный отчет в 00:00 UTC

**Формат отчета**:

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

### 5. Ручная отправка тестового отчета

```bash
# Запустить Python интерпретатор в контейнере
docker exec -it scanner-periodic-reporter python

# В Python:
from services.reporting_service import ReportingService
reporting = ReportingService()
reporting.send_telegram_message("🧪 Тестовый отчет от ReportingService")
```

## Проверка Redis Stream

```bash
# Подключиться к Redis
docker exec -it scanner-redis-worker-1 redis-cli

# Посмотреть последние 10 сообщений в notify:telegram
XREVRANGE notify:telegram + - COUNT 10

# Посмотреть длину stream
XLEN notify:telegram

# Посмотреть информацию о consumer groups
XINFO GROUPS notify:telegram
```

## Troubleshooting

### Отчеты не приходят

1. **Проверьте что periodic-reporter запущен**:

   ```bash
   docker ps | grep periodic-reporter
   ```

2. **Проверьте логи на ошибки**:

   ```bash
   docker-compose logs periodic-reporter | grep ERROR
   ```

3. **Проверьте что сообщения попадают в Redis**:

   ```bash
   docker exec scanner-redis-worker-1 redis-cli XLEN notify:telegram
   ```

4. **Проверьте bot-nest/notify-worker**:
   ```bash
   docker-compose logs bot-nest | grep "Sent message"
   # или
   docker-compose logs notify-worker | tail -20
   ```

### Нет статистики

Если отчет пустой, значит нет закрытых сделок:

```bash
# Проверить наличие статистики в Redis
docker exec scanner-redis-worker-1 redis-cli KEYS "stats:*"
docker exec scanner-redis-worker-1 redis-cli HGETALL "stats:orderflow:XAUUSD:tick"
```

### Изменить периодичность

Отредактируйте `docker-compose.yml`:

```yaml
environment:
  - PERIODIC_REPORT_INTERVAL_HOURS=1 # Каждый час
  - DAILY_REPORT_TIME=09:00 # В 09:00 UTC
```

Перезапустите:

```bash
docker-compose up -d periodic-reporter
```

## Связь с другими сервисами

### signal-performance-tracker

Собирает статистику в Redis (`stats:*` ключи)

### StatsAggregator

Читает и агрегирует статистику

### ReportingService

Форматирует отчеты и отправляет в stream

### periodic-reporter

Периодически вызывает ReportingService

### bot-nest / notify-worker

Читают из `notify:telegram` и отправляют в Telegram

## Дополнительно

### Отключить periodic-reporter

```bash
docker-compose stop periodic-reporter
```

### Просмотреть конфигурацию

```bash
docker exec scanner-periodic-reporter env | grep REPORT
```

### Перезапустить с обновленной конфигурацией

```bash
docker-compose restart periodic-reporter
```
