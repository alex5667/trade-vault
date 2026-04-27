# ✅ Исправление отправки отчетов в Telegram - Signal Performance Tracker

**Дата**: 2025-11-06  
**Проблема**: Отчеты по XAUUSD не отправляются в Telegram бот  
**Статус**: ✅ ИСПРАВЛЕНО

---

## 🐛 Проблема

### Симптомы

- Периодические отчеты НЕ приходили в Telegram
- Ежедневные сводки НЕ приходили в Telegram
- Сигналы приходили нормально (OrderFlow, TA)

### Причина

В файле `python-worker/services/signal_performance_tracker.py`:

```python
# ❌ БЫЛО: Вызывался несуществующий скрипт
def _send_periodic_report(self) -> None:
    result = subprocess.run(
        ['python3', '/app/send_real_report.py'],  # ❌ Файл не существует!
        ...
    )
```

**Проблемы:**

1. `self.reporting_service` создавался, но **НИКОГДА не использовался**
2. Вызывался внешний скрипт `/app/send_real_report.py`, который **не существует**
3. Ошибки не логировались должным образом

---

## ✅ Решение

### Что исправлено

#### 1. `_send_periodic_report()` - теперь использует ReportingService

```python
# ✅ ТЕПЕРЬ: Используется self.reporting_service
def _send_periodic_report(self) -> None:
    """Отправка периодического отчета в Telegram через ReportingService."""
    try:
        # ✅ Используем self.reporting_service для отправки отчета
        self.logger.info("📊 Generating periodic report...")

        # Отправляем ежедневную сводку через ReportingService
        self.reporting_service.send_daily_summary(include_sources=True)

        self.logger.info("📤 Periodic report sent successfully via ReportingService")

    except Exception as e:
        self.logger.error(f"❌ Error in periodic report: {e}")
```

#### 2. `_send_daily_summary()` - расширенная функциональность

```python
# ✅ ТЕПЕРЬ: Отправляет полную сводку + детальные отчеты
def _send_daily_summary(self) -> None:
    """Отправка ежедневной сводки в Telegram через ReportingService."""
    try:
        self.logger.info("📅 Generating daily summary...")

        # 1. Отправляем полную ежедневную сводку с разбивкой по источникам
        self.reporting_service.send_daily_summary(include_sources=True)

        # 2. Также отправляем детальный отчет по каждому символу/стратегии
        for symbol in self.symbols:
            for strategy in self.strategies:
                if strategy != "aggregated":
                    self.reporting_service.send_strategy_report(
                        strategy=strategy,
                        symbol=symbol,
                        tf="tick"
                    )

        self.logger.info("📅 Daily summary sent successfully via ReportingService")

    except Exception as e:
        self.logger.error(f"❌ Error in daily summary: {e}")
```

---

## 📊 Как это работает

### Архитектура отправки отчетов

```
Signal Performance Tracker
    ↓
self.reporting_service
    ↓
ReportingService.send_telegram_message()
    ↓
Redis Stream: notify:telegram
    ↓
Notify Worker
    ↓
Telegram Bot
```

### Методы ReportingService

#### 1. `send_daily_summary(include_sources=True)`

Отправляет полную ежедневную сводку:

```
📅 Ежедневная сводка (полная)
🗓️ 2025-11-06
========================================

📈 ОБЩИЕ ПОКАЗАТЕЛИ
Всего сделок: 45
Выигрышей: 30 (66.7%)
Проигрышей: 15
Общий P/L: +127.50

📊 ORDERFLOW
Сделок: 25 | WR: 68.0% | P/L: +85.20
TP: 20 (80%) / 15 (60%) / 10 (40%)

📊 TA
Сделок: 20 | WR: 65.0% | P/L: +42.30
TP: 15 (75%) / 10 (50%) / 5 (25%)

📡 ПО ИСТОЧНИКАМ
• OrderFlow: 15 сделок, WR 73.3%, P/L +65.40
• AggregatedHub-V2: 10 сделок, WR 60.0%, P/L +19.80
```

#### 2. `send_strategy_report(strategy, symbol, tf)`

Отправляет детальный отчет по стратегии:

```
📊 Отчёт: orderflow:XAUUSD:tick
========================================

📈 ОСНОВНЫЕ
Сделок: 25
Wins/Losses: 17/8
WinRate: 68.0%
Total P/L: +85.20
Avg P/L: +3.41

🎯 TP МЕТРИКИ
TP1: 20 (80.0%)
TP2: 15 (60.0%)
TP3: 10 (40.0%)

⭐ УПУЩЕННАЯ ПРИБЫЛЬ
TP1→SL: 3 (12.0%) ⚠️
💡 Trailing stop после TP1!

📡 ПО ИСТОЧНИКАМ
• OrderFlow: 15 сделок, WR 73.3%, P/L +65.40
• AggregatedHub-V2: 10 сделок, WR 60.0%, P/L +19.80
```

#### 3. `send_telegram_message(text)`

Публикует сообщение в Redis Stream:

```python
# Внутри ReportingService
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

---

## 🕐 Расписание отправки

### Периодические отчеты

```python
# Настройка в config
"reporting": {
    "periodic_interval_hours": 3,  # Каждые 3 часа
    ...
}
```

**Отправляется:**

- Каждые 3 часа (по умолчанию)
- Содержит: ежедневную сводку с разбивкой

### Ежедневные сводки

```python
# Настройка в config
"reporting": {
    "daily_summary_enabled": true,
    "daily_summary_hour": 0  # 00:00 UTC
}
```

**Отправляется:**

- Раз в день в 00:00 UTC (по умолчанию)
- Содержит:
  - Полную ежедневную сводку
  - Детальные отчеты по каждой стратегии (orderflow, ta)
  - Разбивку по источникам

---

## 🚀 Проверка работы

### 1. Проверка логов Signal Performance Tracker

```bash
# Просмотр логов
docker logs scanner-signal-tracker -f

# Должны видеть:
# ✅ Signal Performance Tracker инициализирован
# 📊 Generating periodic report...
# ✅ Отчет опубликован в notify:telegram: 1730...
# 📤 Periodic report sent successfully via ReportingService
```

### 2. Проверка Redis Stream

```bash
# Подключение к Redis
docker exec -it scanner-redis redis-cli

# Проверка stream notify:telegram
XLEN notify:telegram

# Последние сообщения
XREVRANGE notify:telegram + - COUNT 5

# Должны видеть сообщения с type="report"
```

### 3. Проверка Telegram бота

- Отчеты должны приходить в Telegram чат
- Формат: HTML с разметкой
- Содержание: полная статистика с метриками

### 4. Тест вручную

```python
# Запуск Python в контейнере
docker exec -it scanner-signal-tracker python3

# Тестирование
from services.reporting_service import ReportingService

reporting = ReportingService(
    redis_url="redis://scanner-redis:6379/0"
)

# Отправка тестового сообщения
reporting.send_telegram_message("🧪 Test report from Signal Performance Tracker")

# Отправка ежедневной сводки
reporting.send_daily_summary(include_sources=True)

# Отправка отчета по стратегии
reporting.send_strategy_report("orderflow", "XAUUSD", "tick")
```

---

## ⚙️ Конфигурация

### Environment Variables

```yaml
# docker-compose.yml
signal-performance-tracker:
  environment:
    # Redis
    - REDIS_URL=redis://scanner-redis-worker-1:6379/0

    # Символы и стратегии
    - SYMBOLS=XAUUSD,BTCUSD,ETHUSD
    - STRATEGIES=orderflow,ta,aggregated

    # Отчеты
    - PERIODIC_REPORT_HOURS=3
    - DAILY_SUMMARY=true
    - DAILY_SUMMARY_HOUR=0

    # Telegram (не требуется, отправка через notify:telegram)
    # - TELEGRAM_BOT_TOKEN=...  # НЕ НУЖНО!
    # - TELEGRAM_CHAT_ID=...    # НЕ НУЖНО!
```

### Config File

```json
{
	"streams": {
		"symbols": ["XAUUSD", "BTCUSD", "ETHUSD"],
		"strategies": ["orderflow", "ta", "aggregated"]
	},
	"reporting": {
		"periodic_interval_hours": 3,
		"daily_summary_enabled": true,
		"daily_summary_hour": 0
	},
	"monitor": {
		"notify_on_trade_close": true
	}
}
```

---

## 📝 Что отправляется

### Периодический отчет (каждые 3 часа)

1. **Ежедневная сводка** с:
   - Общие показатели (сделки, winrate, P/L)
   - По каждой стратегии (orderflow, ta)
   - TP метрики (TP1, TP2, TP3)
   - Упущенная прибыль (TP→SL)
   - Разбивка по источникам

### Ежедневная сводка (00:00 UTC)

1. **Полная ежедневная сводка** (см. выше)
2. **Детальные отчеты** по каждой комбинации:
   - orderflow:XAUUSD:tick
   - orderflow:BTCUSD:tick
   - orderflow:ETHUSD:tick
   - ta:XAUUSD:tick
   - ta:BTCUSD:tick
   - ta:ETHUSD:tick

---

## ✅ Преимущества нового подхода

1. **Единый путь доставки**: Все через `notify:telegram` stream
2. **Надежность**: Не зависит от внешних скриптов
3. **Логирование**: Полное логирование отправки
4. **Метрики**: Полные TP метрики и разбивка по источникам
5. **Гибкость**: Легко расширять функциональность

---

## 🔍 Troubleshooting

### Отчеты не приходят

```bash
# 1. Проверка Signal Performance Tracker
docker logs scanner-signal-tracker -f | grep "report"

# 2. Проверка Redis stream
docker exec -it scanner-redis redis-cli XLEN notify:telegram

# 3. Проверка Notify Worker
docker logs scanner-notify-worker -f

# 4. Проверка Telegram бота
# Убедитесь что notify-worker получает сообщения и отправляет в TG
```

### Нет данных в отчетах

```bash
# Проверка наличия статистики в Redis
docker exec -it scanner-redis redis-cli

# Проверка ключей статистики
KEYS stats:*

# Пример: stats:orderflow:XAUUSD:tick
HGETALL stats:orderflow:XAUUSD:tick

# Если пусто - сигналы не обрабатывались или не закрывались
```

### Ошибки в логах

```bash
# Детальные логи ReportingService
docker logs scanner-signal-tracker -f | grep "ReportingService"

# Должны видеть:
# ✅ ReportingService инициализирован (отправка через notify:telegram stream)
# ✅ Отчет опубликован в notify:telegram: ...
```

---

**Исправление завершено**: 2025-11-06  
**Файл**: `python-worker/services/signal_performance_tracker.py`  
**Тестировано**: ✅ Работает корректно
