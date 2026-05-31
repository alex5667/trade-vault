# Руководство по интеграции Signal Performance Tracker

## 📋 Что было создано

Полноценная система отслеживания эффективности торговых сигналов, состоящая из:

### Основные компоненты

1. **`trade_monitor.py`** - Trade Monitor Service

   - Отслеживание виртуальных позиций по сигналам
   - Частичное закрытие на TP1 (50%), TP2 (30%), TP3 (20%)
   - Расчёт уровней на основе ATR
   - Логирование всех событий в Redis

2. **`stats_aggregator.py`** - Stats Aggregator

   - Подсчёт метрик: WinRate, P/L, TP rates
   - Агрегация по стратегиям/символам/таймфреймам
   - Инкрементное обновление статистики
   - Хранение в Redis для быстрого доступа

3. **`reporting_service.py`** - Reporting Service

   - API для получения отчётов и статистики
   - Постраничная выборка сделок
   - Telegram уведомления
   - Ежедневные сводки
   - Экспорт в JSON

4. **`signal_performance_tracker.py`** - Главный оркестратор
   - Координация всех компонентов
   - Чтение сигналов и тиков из Redis Streams
   - Graceful shutdown
   - Мониторинг в реальном времени

### Вспомогательные файлы

- **`config/signal_tracker_config.json`** - Конфигурационный файл
- **`README_SIGNAL_TRACKER.md`** - Подробная документация
- **`example_usage.py`** - Примеры использования

## 🎯 Ключевые особенности

### Частичное закрытие позиций

```
TP1: 50% позиции (при R:R = 1.0)
TP2: 30% позиции (при R:R = 2.0)
TP3: 20% позиции (при R:R = 3.0)
```

### Расчёт уровней

- **SL**: entry_price ± (ATR × 1.0)
- **TP1**: entry_price ± (ATR × 1.0)
- **TP2**: entry_price ± (ATR × 2.0)
- **TP3**: entry_price ± (ATR × 3.0)

### Redis структура данных

```
Streams:
  signals:{strategy}:{symbol}     - входящие сигналы
  stream:tick_{symbol}            - тиковые данные
  events:trades                   - события (OPEN/TP/SL/CLOSE)
  trades:closed                   - закрытые сделки

Hashes:
  order:{id}                      - данные позиции
  stats:{strategy}:{symbol}:{tf}  - статистика

Lists:
  closed:{strategy}:{symbol}:{tf} - ID сделок для пагинации
```

## 🚀 Быстрый старт

### 1. Запуск как standalone сервис

```bash
cd python-worker/services
python signal_performance_tracker.py
```

### 2. Запуск с кастомной конфигурацией

```python
from services.signal_performance_tracker import SignalPerformanceTracker

config = {
    "symbols": ["XAUUSD"],
    "strategies": ["orderflow"],
    "monitor": {
        "default_lot": 1.0,
        "stop_atr_mult": 1.0,
        "rr_levels": [1.0, 2.0, 3.0],
        "tp_ratio": [0.50, 0.30, 0.20]
    },
    "telegram": {
        "bot_token": "YOUR_TOKEN",
        "chat_id": "YOUR_CHAT_ID"
    }
}

tracker = SignalPerformanceTracker(config)
tracker.run_forever()
```

### 3. Использование отдельных компонентов

```python
from services.trade_monitor import TradeMonitor
from services.stats_aggregator import StatsAggregator
from services.reporting_service import ReportingService

# Создание компонентов
monitor = TradeMonitor()
aggregator = StatsAggregator()
reporting = ReportingService(stats_aggregator=aggregator)

# Связывание
monitor.set_stats_aggregator(aggregator)

# Обработка сигнала
signal = {
    "strategy": "orderflow",
    "symbol": "XAUUSD",
    "tf": "tick",
    "direction": "LONG",
    "price": 2650.50,
    "atr": 1.2,
    "timestamp": int(time.time() * 1000)
}

pos_id = monitor.process_signal(signal)

# Обработка тика
tick = {
    "symbol": "XAUUSD",
    "last": 2651.70,
    "bid": 2651.68,
    "ask": 2651.72
}

monitor.process_tick(tick)

# Получение статистики
stats = aggregator.get_stats("orderflow", "XAUUSD", "tick")
print(f"WinRate: {stats['winrate']}%")
```

## 🔧 Интеграция с существующей системой

### Вариант 1: Автоматическое отслеживание из потоков

Система автоматически читает сигналы из Redis Streams:

```
signals:orderflow:XAUUSD
```

Просто публикуйте сигналы в этот поток, и они будут автоматически обработаны.

### Вариант 2: Прямая отправка из обработчиков

Модифицируйте существующие обработчики для прямой отправки:

```python
from services.trade_monitor import TradeMonitor

class XAUUSDOrderFlowHandlerV2(BaseOrderFlowHandler):
    def __init__(self, config=None):
        super().__init__("XAUUSD", config)
        self.trade_monitor = TradeMonitor()

    def _publish_signal(self, signal):
        # Публикация в стандартный поток
        super()._publish_signal(signal)

        # Дополнительно: прямая отправка в монитор
        self.trade_monitor.process_signal(signal)
```

### Вариант 3: Docker Compose сервис

Добавьте в `docker-compose.yml`:

```yaml
signal-performance-tracker:
  build:
    context: ./python-worker
  command: python services/signal_performance_tracker.py
  environment:
    - REDIS_HOST=scanner-redis-worker-1
    - REDIS_PORT=6379
    - TELEGRAM_BOT_TOKEN=${TELEGRAM_BOT_TOKEN}
    - TELEGRAM_CHAT_ID=${TELEGRAM_CHAT_ID}
  depends_on:
    - scanner-redis-worker-1
  restart: unless-stopped
```

## 📊 Получение отчётов

### Через Python API

```python
from services.stats_aggregator import StatsAggregator
from services.reporting_service import ReportingService

aggregator = StatsAggregator()
reporting = ReportingService(stats_aggregator=aggregator)

# Общая сводка
all_report = reporting.get_all_strategies_report()

# По стратегии
strategy_report = reporting.get_strategy_report("orderflow")

# Последние сделки
recent_trades = reporting.get_recent_trades(
    "orderflow", "XAUUSD", "tick",
    limit=50, offset=0
)

# Детали конкретной сделки
details = reporting.get_trade_details(order_id)
```

### Через Redis напрямую

```bash
# Статистика по стратегии
redis-cli HGETALL stats:orderflow:XAUUSD:tick

# Список закрытых сделок
redis-cli LRANGE closed:orderflow:XAUUSD:tick 0 49

# Детали ордера
redis-cli HGETALL order:{order_id}
```

## 📱 Telegram уведомления

### Настройка

1. Создайте бота через [@BotFather](https://t.me/BotFather)
2. Получите `bot_token`
3. Получите `chat_id` (отправьте боту сообщение и проверьте через API)
4. Установите переменные окружения:

```bash
export TELEGRAM_BOT_TOKEN=123456789:ABCdefGHIjklMNOpqrsTUVwxyz
export TELEGRAM_CHAT_ID=987654321
```

### Типы уведомлений

- **При закрытии каждой сделки** (опционально)
- **Ежедневная сводка** (по умолчанию в 00:00 UTC)
- **Отчёт по стратегии** (по запросу)

### Пример уведомления

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

## 🧪 Тестирование

### Отправка тестового сигнала

```python
import redis
import json
import time

r = redis.Redis(host='localhost', port=6379, decode_responses=True)

signal = {
    "strategy": "orderflow",
    "symbol": "XAUUSD",
    "tf": "tick",
    "direction": "LONG",
    "price": 2650.50,
    "atr": 1.2,
    "timestamp": int(time.time() * 1000)
}

r.xadd("signals:orderflow:XAUUSD", {"data": json.dumps(signal)})
```

### Симуляция тиков

```python
# Тик достигающий TP1
tick_tp1 = {
    "symbol": "XAUUSD",
    "last": 2651.70,  # entry + ATR*1.0
    "bid": 2651.68,
    "ask": 2651.72,
    "volume": 100,
    "flags": 1
}

r.xadd("stream:tick_XAUUSD", tick_tp1)
```

## 📈 Метрики и KPI

### Базовые метрики

- **Total Trades** - общее количество сделок
- **WinRate** - процент прибыльных сделок
- **Total P/L** - суммарная прибыль/убыток
- **Average P/L** - средняя прибыль на сделку

### TP метрики

- **TP1/TP2/TP3 Rate** - процент достижения каждой цели
- **Average TP Latency** - среднее время до достижения TP

### Риск метрики

- **Max Win/Loss** - максимальная прибыль/убыток
- **Profit Factor** - отношение прибылей к убыткам
- **Sharpe Ratio** (планируется)

## 🔍 Мониторинг и логи

### Вывод статуса

```python
status = tracker.get_status()
print(f"""
Uptime: {status['uptime_sec']}s
Сигналов: {status['signals_read']}
Тиков: {status['ticks_processed']}
Открыто: {status['monitor']['open_positions']}
Закрыто: {status['monitor']['positions_closed']}
""")
```

### Логи

Все события логируются в stdout с уровнями:

- **INFO** - обычные события
- **WARNING** - предупреждения
- **ERROR** - ошибки

Пример:

```
2025-11-02 12:34:56 - TradeMonitor - INFO - 📈 Позиция открыта: abc12345
2025-11-02 12:35:10 - TradeMonitor - INFO - 🎯 TP1 достигнут: abc12345
```

## 🛠️ Troubleshooting

### Сигналы не обрабатываются

**Причина**: Consumer group не создана или неправильное имя потока

**Решение**:

```bash
redis-cli XINFO GROUPS signals:orderflow:XAUUSD
redis-cli XGROUP CREATE signals:orderflow:XAUUSD signal-tracker-group 0 MKSTREAM
```

### Статистика не обновляется

**Причина**: Stats Aggregator не подключен к Trade Monitor

**Решение**:

```python
tracker.trade_monitor.set_stats_aggregator(tracker.stats_aggregator)
```

### Telegram не работает

**Причина**: Неверные credentials или API недоступен

**Решение**:

```bash
# Проверить токен
curl https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/getMe

# Проверить переменные окружения
echo $TELEGRAM_BOT_TOKEN
echo $TELEGRAM_CHAT_ID
```

## 🔮 Roadmap

- [ ] WebSocket API для real-time обновлений
- [ ] Web Dashboard с графиками
- [ ] Backtesting на исторических данных
- [ ] ML-анализ quality score vs результаты
- [ ] Экспорт в ClickHouse/TimescaleDB
- [ ] А/B тестирование стратегий
- [ ] Auto-optimization параметров

## 📞 Поддержка

Для вопросов и предложений создавайте issue или обращайтесь к команде разработки.

## 📄 Файлы проекта

```
python-worker/services/
├── trade_monitor.py              # Trade Monitor Service
├── stats_aggregator.py            # Stats Aggregator
├── reporting_service.py           # Reporting Service
├── signal_performance_tracker.py  # Главный оркестратор
├── example_usage.py               # Примеры использования
├── README_SIGNAL_TRACKER.md       # Подробная документация
└── INTEGRATION_GUIDE.md           # Это руководство

python-worker/config/
└── signal_tracker_config.json     # Конфигурация
```
