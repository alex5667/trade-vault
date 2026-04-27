# Интеграция уведомлений Telegram

## 📋 Обзор

Система Signal Performance Tracker интегрирована с существующей инфраструктурой Telegram уведомлений в проекте.

## 🏗️ Существующая инфраструктура

В проекте уже есть несколько систем уведомлений:

### 1. **telegram-worker** (Python)
- `telegram-worker/improved_notifier.py` - основной нотификатор
- `telegram-worker/notifier.py` - обёртка
- `telegram-worker/notify_worker.py` - воркер для обработки потока `notify:telegram`

### 2. **bot-nest** (Node.js)
- Основной бот для Telegram
- Читает из `notify:telegram` stream
- Поддержка inline кнопок

### 3. **notify-bridge** (Python FastAPI)
- `python-worker/services/notify_bridge.py`
- HTTP API для отправки уведомлений
- Прямой вызов Telegram API

### 4. **ReportingService** в Signal Tracker
- `python-worker/services/reporting_service.py`
- Встроенная поддержка Telegram уведомлений
- Отправка отчётов и сводок

## 🔧 Использование уведомлений в Signal Tracker

### Автоматические уведомления при закрытии сделки

```python
config = {
    "monitor": {
        "notify_on_trade_close": True  # Включить автоуведомления
    },
    "telegram": {
        "bot_token": "YOUR_BOT_TOKEN",
        "chat_id": "YOUR_CHAT_ID"
    }
}

tracker = SignalPerformanceTracker(config)
tracker.start()
```

### Ручная отправка уведомлений

```python
from services.reporting_service import ReportingService

reporting = ReportingService(telegram_config={
    "bot_token": "YOUR_TOKEN",
    "chat_id": "YOUR_CHAT_ID"
})

# Уведомление о закрытии сделки
trade_summary = {
    "strategy": "orderflow",
    "symbol": "XAUUSD",
    "direction": "LONG",
    "result": "win",
    "pnl": 45.50,
    "pnl_pct": 1.8,
    "tp_count": 2
}
reporting.notify_trade_closed(trade_summary)

# Ежедневная сводка
reporting.send_daily_summary()

# Отчёт по стратегии
reporting.send_strategy_report("orderflow")

# Периодическая сводка (гибкий формат)
from services.stats_aggregator import StatsAggregator
from core.redis_client import get_redis

redis_client = get_redis()
stats = StatsAggregator.get_stats(redis_client, "orderflow", "XAUUSD", "tick")
reporting.notify_periodic_summary(stats, period="week")
```

## 📨 Типы уведомлений

### 1. При закрытии сделки
```
✅ Сделка закрыта

Стратегия: orderflow
Инструмент: XAUUSD (tick)
Направление: 📈 LONG
Результат: WIN
P/L: +45.50 (+1.80%)
Причина: TP2
TP достигнуто: 2/3
```

### 2. Ежедневная сводка
```
📊 Ежедневная сводка

Всего сделок: 15
Выигрышей: 9
Проигрышей: 6
WinRate: 60.0%
Общий P/L: +125.50

orderflow: 10 сделок, WR 70.0%, P/L +98.20
deltaSpikeB: 5 сделок, WR 40.0%, P/L +27.30
```

### 3. Отчёт по стратегии
```
📊 Отчёт: orderflow

Всего сделок: 25
Выигрышей: 18
Проигрышей: 7
WinRate: 72.0%
Общий P/L: +245.80
Средний P/L: +9.83
```

### 4. Периодическая сводка (гибкая)
```
🗓 Итоги за week

• orderflow: 45 сделок, WR 68.9%, P/L +342.50
• deltaSpikeB: 23 сделок, WR 56.5%, P/L +89.40

Итого: 68 сделок, WR 64.7%, P/L +431.90
```

## ⚙️ Конфигурация

### Переменные окружения

```bash
export TELEGRAM_BOT_TOKEN=123456789:ABCdefGHIjklMNOpqrsTUVwxyz
export TELEGRAM_CHAT_ID=987654321
```

### Конфигурационный файл

```json
{
  "telegram": {
    "bot_token": "${TELEGRAM_BOT_TOKEN}",
    "chat_id": "${TELEGRAM_CHAT_ID}",
    "notify_on_trade_close": true,
    "notify_on_error": false
  }
}
```

### Программная конфигурация

```python
from services.trade_monitor import TradeMonitor
from services.reporting_service import ReportingService

# Создание монитора с автоуведомлениями
monitor = TradeMonitor(config={
    "notify_on_trade_close": True
})

# Создание reporting service
reporting = ReportingService(telegram_config={
    "bot_token": "YOUR_TOKEN",
    "chat_id": "YOUR_CHAT_ID"
})

# Связывание
monitor.set_reporting_service(reporting)
```

## 🔀 Выбор канала уведомлений

### Вариант 1: Прямая отправка через ReportingService
**Преимущества:**
- Простая настройка
- Прямой контроль
- Минимальные зависимости

**Использование:**
```python
reporting = ReportingService(telegram_config={...})
reporting.notify_trade_closed(trade_summary)
```

### Вариант 2: Через notify:telegram stream + bot-nest
**Преимущества:**
- Асинхронная обработка
- Rate limiting
- Поддержка кнопок
- Централизованная очередь

**Использование:**
```python
import redis

r = redis.Redis(...)
r.xadd("notify:telegram", {
    "text": "Сообщение",
    "buttons": json.dumps([...])
})
```

### Вариант 3: Через notify-bridge HTTP API
**Преимущества:**
- HTTP интерфейс
- Независимость от Redis
- REST API

**Использование:**
```python
import requests

requests.post("http://localhost:8080/notify", json={
    "text": "Сообщение"
})
```

## 🎯 Рекомендации

### Для Signal Performance Tracker

**По умолчанию: отключены автоуведомления**
```json
{
  "notify_on_trade_close": false
}
```

**Причины:**
- Избежать спама при большом количестве сделок
- Пользователь сам выбирает когда получать уведомления
- Ежедневные сводки более информативны

**Включать автоуведомления:**
- При тестировании новой стратегии
- Для критически важных сигналов
- На production с малым объёмом сделок

### Оптимальная стратегия

1. **Автоматически:** Ежедневные сводки (00:00 UTC)
2. **По запросу:** Отчёты по конкретным стратегиям
3. **Опционально:** Уведомления при каждой сделке

```python
config = {
    "telegram": {
        "notify_on_trade_close": False,  # Отключено
    },
    "reporting": {
        "daily_summary_enabled": True,   # Включено
        "daily_summary_hour": 0          # 00:00 UTC
    }
}
```

## 🛠️ Troubleshooting

### Уведомления не приходят

1. **Проверить переменные окружения**
```bash
echo $TELEGRAM_BOT_TOKEN
echo $TELEGRAM_CHAT_ID
```

2. **Проверить токен бота**
```bash
curl https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/getMe
```

3. **Проверить права бота**
- Бот должен быть добавлен в чат
- Бот должен иметь права на отправку сообщений

4. **Проверить конфигурацию**
```python
reporting = ReportingService(telegram_config={...})
print(f"Telegram enabled: {reporting.telegram_enabled}")
```

### Дублирование уведомлений

Если уведомления дублируются:
1. Проверить что `notify_on_trade_close = False` в конфиге
2. Убедиться что нет других воркеров отправляющих в Telegram
3. См. `docs/BUGFIX_DUPLICATE_NOTIFICATIONS.md`

### Rate limiting

Telegram API имеет лимиты:
- 30 сообщений в секунду
- 20 сообщений в минуту в группу

При превышении использовать:
- `bot-nest` с встроенным rate limiter
- Очередь через Redis stream
- Батчинг сообщений

## 📚 Дополнительные ресурсы

- [Telegram Bot API](https://core.telegram.org/bots/api)
- [python-telegram-bot](https://python-telegram-bot.org/)
- `docs/SETUP_TELEGRAM_BOT.md` - настройка бота
- `docs/BUGFIX_DUPLICATE_NOTIFICATIONS.md` - решение проблем дублирования

