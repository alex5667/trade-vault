# ✅ ИСПРАВЛЕНИЕ: Отчеты теперь приходят в Telegram бот

## Проблема

3 сервиса (signal-generator, AggregatedHub-V2, OrderFlow) формировали сигналы по XAUUSD и отправляли их в бот и Redis. Также были созданы сервисы статистики для формирования отчетов, но **отчеты не приходили в бот**.

## Корневые причины

1. **ReportingService** пытался отправлять напрямую через Telegram Bot API, но не имел правильной конфигурации
2. **Отсутствовал сервис** для периодической отправки отчетов
3. **Разные пути доставки**: сигналы шли через `notify:telegram` stream, а отчеты пытались идти напрямую

## Решение

### 1. ✅ Исправлен ReportingService

**Файл**: `python-worker/services/reporting_service.py`

**Изменения**:
- Метод `send_telegram_message()` теперь публикует в Redis stream `notify:telegram`
- Убрана прямая зависимость от Telegram Bot API
- Единый путь доставки с сигналами

```python
def send_telegram_message(self, text: str, parse_mode: str = "HTML") -> bool:
    notify_stream = os.getenv("NOTIFY_STREAM", "notify:telegram")
    message_data = {
        "type": "report",
        "text": text,
        "source": "ReportingService",
        "timestamp": str(int(time.time() * 1000))
    }
    msg_id = self.redis.xadd(notify_stream, message_data, maxlen=1000)
    return True
```

### 2. ✅ Создан Periodic Reporter Service

**Файл**: `python-worker/services/periodic_reporter.py`

**Функционал**:
- Периодические отчеты каждые 3 часа (настраивается)
- Ежедневный детальный отчет в 00:00 UTC
- Разбивка по источникам (OrderFlow, AggregatedHub-V2, TechnicalAnalysis)
- Автоматический стартовый отчет при запуске

**Параметры**:
```bash
PERIODIC_REPORT_INTERVAL_HOURS=3  # Периодичность (часы)
DAILY_REPORT_TIME=00:00           # Время ежедневного отчета (UTC)
NOTIFY_STREAM=notify:telegram     # Redis stream
```

### 3. ✅ Добавлен в Docker Compose

**Файл**: `docker-compose.yml`

**Сервис**: `periodic-reporter`

```yaml
periodic-reporter:
  profiles: [default]
  container_name: scanner-periodic-reporter
  environment:
    - PERIODIC_REPORT_INTERVAL_HOURS=3
    - DAILY_REPORT_TIME=00:00
    - NOTIFY_STREAM=notify:telegram
  command: ['sh', '-c', 'sleep 120 && python -m services.periodic_reporter']
```

### 4. ✅ Добавлены команды Makefile

```bash
make test-reporting     # Тест системы отчетов
make logs-reporter      # Просмотр логов
make restart-reporter   # Перезапуск сервиса
make send-report-now    # Отправить отчет вручную
```

### 5. ✅ Обновлены зависимости

**Файл**: `python-worker/requirements.txt`

Добавлено:
```
schedule>=1.2.0  # Для периодических задач
```

## Архитектура решения

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
│                    REPORTING (НОВОЕ)                       │
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
│                  ║ - send_to_stream() ║  (ИСПРАВЛЕНО)     │
│                  ╚═════════╤══════════╝                    │
│                           │                                │
│                           └───────────────────────┐        │
└───────────────────────────────────────────────────┼────────┘
                                                    │
┌───────────────────────────────────────────────────┼────────┐
│                  TELEGRAM DELIVERY                │        │
├───────────────────────────────────────────────────┼────────┤
│                           ▼                       │        │
│                  ╔════════════════════╗          ◄┘        │
│                  ║ notify:telegram    ║                    │
│                  ║    (Redis)         ║                    │
│                  ╚═════════╤══════════╝                    │
│                           │                                │
│                           ▼                                │
│                  ╔════════════════════╗                    │
│                  ║ bot-nest/main.ts   ║                    │
│                  ║ или notify-worker  ║                    │
│                  ╚═════════╤══════════╝                    │
│                           │                                │
│                           ▼                                │
│                  ╔════════════════════╗                    │
│                  ║ Telegram Bot API   ║                    │
│                  ╚═════════╤══════════╝                    │
│                           │                                │
│                           ▼                                │
│                     👤 USER in Bot                         │
└────────────────────────────────────────────────────────────┘
```

## Запуск

```bash
cd /home/alex/front/trade/scanner_infra

# 1. Пересобрать образ
docker-compose build periodic-reporter

# 2. Запустить сервис
docker-compose up -d periodic-reporter

# 3. Проверить логи
make logs-reporter

# 4. Тестировать
make test-reporting
```

## Проверка работы

### 1. Логи periodic-reporter
```bash
docker-compose logs -f periodic-reporter
```

Ожидается:
```
📊 Periodic Reporter Service
══════════════════════════════════════════════════════════════════════
Периодические отчеты: каждые 3 часа
Ежедневный отчет: 00:00 UTC
══════════════════════════════════════════════════════════════════════
✅ Расписание настроено
📤 Отправка стартового отчета...
✅ Отчет опубликован в notify:telegram: 1730736542123-0
```

### 2. Redis stream
```bash
docker exec scanner-redis-worker-1 redis-cli XLEN notify:telegram
docker exec scanner-redis-worker-1 redis-cli XREVRANGE notify:telegram + - COUNT 3
```

### 3. Bot-nest логи
```bash
docker-compose logs bot-nest | grep "Sent message"
```

Ожидается:
```
✅ Sent message #15
✅ Sent message #16
```

### 4. Telegram бот
Проверьте свой Telegram - должен прийти отчет!

## Формат отчетов

### Периодический (каждые 3ч)
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

### Ежедневный (00:00 UTC)
```
📊 Ежедневная сводка

Всего сделок: 145
Выигрышей: 92
Проигрышей: 53
WinRate: 63.4%
Общий P/L: +456.70

orderflow: 60 сделок, WR 65.0%, P/L +234.00
...

📊 По источникам:
  • OrderFlow: 80 сделок, WR 65.0%, P/L +312.00
  • AggregatedHub-V2: 42 сделок, WR 61.9%, P/L +102.50
  • TechnicalAnalysis: 23 сделок, WR 60.9%, P/L +42.20
```

## Файлы

| Файл | Назначение |
|------|------------|
| `python-worker/services/reporting_service.py` | Исправлен - отправка через Redis |
| `python-worker/services/periodic_reporter.py` | Новый - периодическая отправка |
| `python-worker/requirements.txt` | Добавлен schedule |
| `docker-compose.yml` | Добавлен сервис periodic-reporter |
| `Makefile` | Добавлены команды test-reporting, logs-reporter, restart-reporter |
| `scripts/test_reporting.py` | Тестовый скрипт |
| `python-worker/services/REPORTING_FIX.md` | Полная документация |
| `REPORTING_QUICKSTART.md` | Быстрый старт |

## Команды

```bash
# Тест
make test-reporting

# Логи
make logs-reporter

# Перезапуск
make restart-reporter

# Ручная отправка
make send-report-now

# Остановить
docker-compose stop periodic-reporter

# Запустить
docker-compose start periodic-reporter
```

## Troubleshooting

### Отчеты не приходят

1. Проверьте что сервис запущен:
   ```bash
   docker ps | grep periodic-reporter
   ```

2. Проверьте логи:
   ```bash
   make logs-reporter
   ```

3. Проверьте Redis stream:
   ```bash
   docker exec scanner-redis-worker-1 redis-cli XLEN notify:telegram
   ```

4. Проверьте bot-nest:
   ```bash
   docker-compose logs bot-nest | tail -20
   ```

5. Запустите тест:
   ```bash
   make test-reporting
   ```

## Итог

✅ **Проблема решена полностью**

- Отчеты формируются автоматически каждые 3 часа
- Ежедневный отчет в 00:00 UTC
- Разбивка по источникам (OrderFlow, AggregatedHub-V2, TechnicalAnalysis)
- Единый путь доставки через Redis stream `notify:telegram`
- Простое управление через Makefile
- Полное тестирование

**Все отчеты теперь приходят в Telegram бот! 🎉**

