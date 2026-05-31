# Цепочка отправки отчетов в Telegram

## Обзор

Документ описывает полную цепочку отправки отчетов из системы в Telegram бот.

## Архитектура цепочки

```
PeriodicReporter
    ↓
ReportingService.send_telegram_message()
    ↓
Redis Stream: notify:telegram
    ↓
notify_worker (telegram-worker)
    ↓
notifier.send_html_to_telegram()
    ↓
ImprovedTelegramNotifier.send_notification()
    ↓
Telegram Bot API
    ↓
Telegram Chat
```

## Компоненты

### 1. PeriodicReporter
**Файл:** `python-worker/services/periodic_reporter.py`

**Функции:**
- Собирает метрики из Redis stream `trades:closed`
- Формирует отчет с статистикой по парам source/symbol
- Вызывает `ReportingService.send_telegram_message()`

**Методы:**
- `_gather_window_metrics_stream()` - сбор метрик из stream
- `_send_report()` - формирование и отправка отчета
- `send_report_for_pair()` - отправка отчета для конкретной пары

### 2. ReportingService
**Файл:** `python-worker/services/reporting_service.py`

**Функции:**
- Публикует сообщения в Redis stream `notify:telegram`
- Не отправляет напрямую в Telegram (только в Redis)

**Метод:**
```python
send_telegram_message(
    text: str,
    parse_mode: str = "HTML",
    tags: Optional[List[str]] = None,
    severity: str = "info",
    dedup_key: Optional[str] = None,
    meta: Optional[Dict[str, Any]] = None
) -> bool
```

**Публикует в Redis:**
- Stream: `notify:telegram` (или из env `NOTIFY_STREAM`)
- Поля:
  - `type`: "report"
  - `text`: HTML текст сообщения
  - `parse_mode`: "HTML"
  - `source`: "ReportingService"
  - `severity`: уровень (info/warn/error)
  - `timestamp`: timestamp в ms
  - `tags`: список тегов (опционально)
  - `dedup_key`: ключ дедупликации (опционально)
  - `meta`: JSON с метаданными (опционально)

### 3. Redis Stream: notify:telegram
**Назначение:** Очередь сообщений для отправки в Telegram

**Consumer Group:** `notify-group`
**Consumer:** `notify-consumer-{pid}`

### 4. notify_worker
**Файл:** `telegram-worker/notify_worker.py`

**Функции:**
- Читает сообщения из `notify:telegram` через consumer group
- Обрабатывает сообщения с `type="report"`
- Вызывает `send_html_to_telegram()` для отчетов

**Обработка отчетов:**
```python
if msg_type == "report":
    text = entry.get("text", "")
    await send_html_to_telegram(text)
```

### 5. notifier
**Файл:** `telegram-worker/notifier.py`

**Функции:**
- Обертка над `ImprovedTelegramNotifier`
- Метод `send_html_to_telegram()` для отчетов

### 6. ImprovedTelegramNotifier
**Файл:** `telegram-worker/improved_notifier.py`

**Функции:**
- Отправка сообщений через Telegram Bot API
- Rate limiting
- Retry логика
- Статистика отправки

**Конфигурация:**
- `TELEGRAM_BOT_TOKEN` - токен бота
- `TELEGRAM_CHAT_ID` или `TELEGRAM_NOTIFY_CHAT_IDS` - ID чатов

**Отправка:**
```
POST https://api.telegram.org/bot{BOT_TOKEN}/sendMessage
{
    "chat_id": "{CHAT_ID}",
    "text": "{HTML_TEXT}",
    "parse_mode": "HTML",
    "disable_web_page_preview": true
}
```

## Формат отчета

Отчет формируется в `PeriodicReporter._send_report()`:

```
📊 <b>Отчет: {source} / {symbol}</b>
🕐 {timestamp}
🪟 Окно: последние {window_minutes} мин
========================================

<b>📈 ОСНОВНОЕ (net по PnL)</b>
Сделок: {total}
W/L/BE: {wins}/{losses}/{be} | WR: {winrate}%
P/L net: {total_pnl} | Avg: {avg_pnl}
Avg P/L %: {avg_pct}
Fees: {fees}
ProfitFactor: {pf}
Avg duration: {avg_dur}s

<b>🎯 TP / Trailing</b>
TP1/TP2/TP3 hits: {tp1}/{tp2}/{tp3}
Trailing started: {trailing_started} | Trailing stops: {trailing_stop_hits}

<b>🧪 Диагностика (strict по close_reason)</b>
W/L/BE(strict): {ws}/{ls}/{bs} | WR(strict): {wrs}%

<b>🔍 Диагностика данных</b>
Убыточных сделок: {neg_pnl_count}
Min PnL: {min_pnl} | Max PnL: {max_pnl}
Пропущено fees: {missing_fees} | Пропущено duration: {missing_duration}
```

## Тестирование

### Тестовый скрипт

**Файл:** `scripts/test_report_send.py`

**Запуск из контейнера:**
```bash
docker exec -it scanner-python-worker python3 /app/scripts/test_report_send.py
```

**Запуск локально (если Redis доступен):**
```bash
REDIS_URL=redis://localhost:6379/0 python3 scripts/test_report_send.py
```

**Что делает скрипт:**
1. Проверяет подключение к Redis
2. Ищет доступные пары source/symbol
3. Собирает метрики для выбранной пары
4. Формирует и отправляет отчет через всю цепочку
5. Проверяет сообщение в Redis stream

### Ручная проверка

1. **Проверка Redis stream:**
```bash
docker exec -it scanner-redis-worker-1 redis-cli XREVRANGE notify:telegram + - COUNT 5
```

2. **Проверка логов notify_worker:**
```bash
docker logs scanner-telegram-worker --tail 50 -f
```

3. **Проверка Telegram:**
   - Проверьте чат, указанный в `TELEGRAM_CHAT_ID`
   - Должно прийти HTML-форматированное сообщение

## Устранение неполадок

### Отчеты не приходят в Telegram

1. **Проверьте переменные окружения:**
   - `TELEGRAM_BOT_TOKEN` - должен быть установлен
   - `TELEGRAM_CHAT_ID` или `TELEGRAM_NOTIFY_CHAT_IDS` - должен быть установлен

2. **Проверьте, что notify_worker запущен:**
```bash
docker ps | grep telegram-worker
docker logs scanner-telegram-worker --tail 100
```

3. **Проверьте Redis stream:**
```bash
docker exec -it scanner-redis-worker-1 redis-cli XINFO STREAM notify:telegram
```

4. **Проверьте consumer group:**
```bash
docker exec -it scanner-redis-worker-1 redis-cli XINFO GROUPS notify:telegram
```

### Сообщения накапливаются в stream

1. Проверьте, что `notify_worker` обрабатывает сообщения
2. Проверьте логи на ошибки
3. Убедитесь, что consumer group активна

### Rate limiting

`ImprovedTelegramNotifier` автоматически обрабатывает rate limiting от Telegram API:
- Максимум 30 сообщений в секунду
- Автоматическое ожидание при превышении лимита

## Настройки

### Переменные окружения

**PeriodicReporter:**
- `PERIODIC_REPORT_WINDOW_SECONDS` - окно для сбора метрик (по умолчанию 3600)
- `REPORT_TRIGGER_COUNT` - количество сделок для триггера отчета (по умолчанию 100)
- `PERIODIC_REPORT_CHECK_INTERVAL_SEC` - интервал проверки для периодических отчетов (по умолчанию 300)

**ReportingService:**
- `NOTIFY_STREAM` - имя Redis stream (по умолчанию "notify:telegram")

**ImprovedTelegramNotifier:**
- `TELEGRAM_BOT_TOKEN` - токен Telegram бота (обязательно)
- `TELEGRAM_CHAT_ID` - ID чата для отправки
- `TELEGRAM_NOTIFY_CHAT_IDS` - список ID чатов через запятую

## Примеры использования

### Отправка отчета для конкретной пары

```python
from services.periodic_reporter import PeriodicReporter

reporter = PeriodicReporter()
reporter.send_report_for_pair("OrderFlow", "XAUUSD")
```

### Отправка кастомного сообщения

```python
from services.reporting_service import ReportingService

reporting = ReportingService()
reporting.send_telegram_message(
    "<b>Тест</b>",
    tags=["test"],
    severity="info"
)
```

## Мониторинг

### Метрики

`ImprovedTelegramNotifier` ведет статистику:
- Количество отправленных сообщений
- Количество ошибок
- Количество rate limit случаев
- Время последней отправки

### Логирование

Все компоненты используют структурированное логирование:
- `PeriodicReporter` - логирует сбор метрик и отправку отчетов
- `ReportingService` - логирует публикацию в Redis stream
- `notify_worker` - логирует обработку сообщений из stream
- `ImprovedTelegramNotifier` - логирует отправку в Telegram API

