# Signal Performance Tracker

Система отслеживания эффективности торговых сигналов на основе тиковых данных.

## 🏗️ Архитектура

Система состоит из четырёх основных компонентов:

### 1. **Trade Monitor** (`trade_monitor.py`)

Отслеживает виртуальные позиции по сигналам:

- Обрабатывает поступающие сигналы
- Формирует виртуальные позиции с уровнями TP/SL
- Отслеживает выполнение условий по тиковым данным
- Частичное закрытие на TP1/TP2/TP3
- Логирование событий в Redis

### 2. **Stats Aggregator** (`stats_aggregator.py`)

Подсчитывает метрики эффективности:

- WinRate, P/L, Average P/L
- TP hit rates (TP1/TP2/TP3)
- Max win/loss
- Временные метрики (duration)
- Profit Factor

### 3. **Reporting Service** (`reporting_service.py`)

Генерирует отчёты и уведомления:

- API для получения статистики
- Постраничная выборка сделок
- Telegram уведомления
- Ежедневные/еженедельные сводки
- Экспорт в JSON

### 4. **Signal Performance Tracker** (`signal_performance_tracker.py`)

Главный оркестратор:

- Координация всех компонентов
- Чтение сигналов из Redis Streams
- Чтение тиков для обновления позиций
- Периодические задачи
- Graceful shutdown

## 📊 Схема данных Redis

### Streams (потоки)

```
signals:{strategy}:{symbol}     - входящие сигналы
stream:tick_{symbol}            - тиковые данные
events:trades                   - события по сделкам (OPEN/TP/SL/CLOSE)
trades:closed                   - закрытые сделки
```

### Hashes (хэши)

```
signal:{id}                                - исходный сигнал
order:{id}                                 - данные позиции/ордера
stats:{strategy}:{symbol}:{tf}             - общая статистика
stats:{strategy}:{symbol}:{tf}:{source}    - статистика по источнику ⭐
```

### Lists (списки)

```
closed:{strategy}:{symbol}:{tf}            - ID закрытых сделок (для пагинации)
closed:{strategy}:{symbol}:{tf}:{source}   - ID сделок по источнику ⭐
```

### Sets (множества)

```
stats:strategies                           - список всех стратегий
stats:symbols:{strategy}                   - символы по стратегии
stats:tfs:{strategy}:{symbol}              - таймфреймы
stats:sources:{strategy}:{symbol}:{tf}     - источники сигналов ⭐
```

## 🚀 Быстрый старт

### 1. Установка зависимостей

```bash
cd python-worker
pip install -r requirements.txt
```

### 2. Настройка переменных окружения

```bash
export REDIS_HOST=scanner-redis-worker-1
export REDIS_PORT=6379
export TELEGRAM_BOT_TOKEN=your_bot_token
export TELEGRAM_CHAT_ID=your_chat_id
```

### 3. Запуск сервиса

```python
from services.signal_performance_tracker import SignalPerformanceTracker

# Конфигурация
config = {
    "symbols": ["XAUUSD"],
    "strategies": ["orderflow"],
    "monitor": {
        "default_lot": 1.0,
        "stop_atr_mult": 1.0,
        "rr_levels": [1.0, 2.0, 3.0],
        "tp_ratio": [0.50, 0.30, 0.20]
    }
}

# Создание и запуск
tracker = SignalPerformanceTracker(config)
tracker.run_forever()
```

### 4. Запуск как standalone сервис

```bash
cd python-worker/services
python signal_performance_tracker.py
```

## 📖 Примеры использования

### Получение статистики по стратегии

```python
from services.stats_aggregator import StatsAggregator
from core.redis_client import get_redis

redis_client = get_redis()

# Общая статистика
stats = StatsAggregator.get_stats(redis_client, "orderflow", "XAUUSD", "tick")
print(f"WinRate: {stats['winrate']}%")
print(f"Total P/L: {stats['total_pnl']}")

# Статистика по источнику ⭐
stats_orderflow = StatsAggregator.get_stats_by_source(
    redis_client, "orderflow", "XAUUSD", "tick", "OrderFlow"
)
print(f"OrderFlow WinRate: {stats_orderflow['winrate']}%")

# Сводка по всей стратегии
summary = StatsAggregator.get_strategy_summary(redis_client, "orderflow")
print(summary)
```

### Получение отчётов

```python
from services.reporting_service import ReportingService

reporting = ReportingService()

# Отчёт по стратегии с разбивкой по источникам ⭐
report = reporting.get_strategy_report(
    "orderflow",
    "XAUUSD",
    "tick",
    include_sources=True
)

# Общая статистика
print(f"Total: {report['total_trades']} trades, WR {report['winrate']}%")

# По источникам
for source, stats in report.get('sources', {}).items():
    print(f"{source}: {stats['total_trades']} trades, WR {stats['winrate']}%")

# Список последних сделок
trades = reporting.get_recent_trades("orderflow", "XAUUSD", "tick", limit=50)

# Сводка по всем источникам ⭐
sources_summary = reporting.get_sources_summary()
print(sources_summary)
```

### Отправка уведомлений

```python
from services.reporting_service import ReportingService

telegram_config = {
    "bot_token": "your_token",
    "chat_id": "your_chat_id"
}

reporting = ReportingService(telegram_config=telegram_config)

# Ежедневная сводка
reporting.send_daily_summary()

# Отчёт по стратегии
reporting.send_strategy_report("orderflow")
```

### Мониторинг в реальном времени

```python
from services.signal_performance_tracker import SignalPerformanceTracker

tracker = SignalPerformanceTracker(config)
tracker.start()

# Получение текущего статуса
while True:
    status = tracker.get_status()
    print(f"Open positions: {status['monitor']['open_positions']}")
    print(f"Closed positions: {status['monitor']['positions_closed']}")
    time.sleep(60)
```

## 🔧 Конфигурация

Конфигурационный файл: `config/signal_tracker_config.json`

### Основные параметры

#### Monitor (Trade Monitor)

```json
{
	"monitor": {
		"default_lot": 1.0, // Размер позиции по умолчанию
		"risk_pct": 1.0, // Риск на сделку (%)
		"stop_atr_mult": 1.0, // Множитель ATR для SL
		"rr_levels": [1.0, 2.0, 3.0], // R:R для TP1/TP2/TP3
		"tp_ratio": [0.5, 0.3, 0.2] // Доли позиции для TP1/TP2/TP3 (50%/30%/20%)
	}
}
```

#### Telegram

```json
{
	"telegram": {
		"bot_token": "${TELEGRAM_BOT_TOKEN}",
		"chat_id": "${TELEGRAM_CHAT_ID}",
		"notify_on_trade_close": true
	}
}
```

#### Reporting

```json
{
	"reporting": {
		"daily_summary_enabled": true,
		"daily_summary_hour": 0,
		"periodic_summary_enabled": true,
		"periodic_summary_interval_hours": 3
	}
}
```

## 📈 Метрики

### Базовые метрики

- **Total Trades**: Общее количество сделок
- **Wins / Losses**: Количество прибыльных/убыточных
- **WinRate**: Процент прибыльных сделок
- **Total P/L**: Суммарная прибыль/убыток
- **Average P/L**: Средняя прибыль на сделку

### TP метрики

- **TP1/TP2/TP3 Hits**: Количество достигнутых целей
- **TP1/TP2/TP3 Rate**: Процент сделок, дошедших до каждой цели

### Временные метрики

- **Average Duration**: Среднее время удержания
- **Min/Max Duration**: Минимальное/максимальное время
- **TP Latency**: Время до достижения каждого TP

### Метрики по источникам ⭐

- **Source**: Источник сигнала (OrderFlow, AggregatedHub-V2, TechnicalAnalysis)
- Все метрики доступны с разбивкой по источникам
- Автоматическое включение в отчёты и сводки

### Расширенные метрики (будущее)

- **Profit Factor**: Отношение прибылей к убыткам
- **Sharpe Ratio**: Риск-скорректированная доходность
- **Max Drawdown**: Максимальная просадка
- **Recovery Factor**: Прибыль / макс. просадка

## 🔄 Интеграция с существующей системой

### 1. Подключение к обработчикам сигналов

```python
from services.trade_monitor import TradeMonitor

# В обработчике сигналов
class XAUUSDOrderFlowHandlerV2(BaseOrderFlowHandler):
    def __init__(self, config=None):
        super().__init__("XAUUSD", config)
        self.trade_monitor = TradeMonitor()

    def _publish_signal(self, signal):
        # Публикация в стандартный поток
        super()._publish_signal(signal)

        # Опционально: отправка напрямую в монитор
        # self.trade_monitor.process_signal(signal)
```

### 2. Автоматический запуск с docker-compose

Добавить в `docker-compose.yml`:

```yaml
signal-tracker:
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
```

## 🧪 Тестирование

### Запуск тестов

```bash
cd python-worker
pytest tests/test_signal_tracker.py -v
```

### Ручное тестирование

```python
# Отправка тестового сигнала
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

## 📝 Логи и мониторинг

Логи пишутся в stdout/stderr и могут быть перенаправлены в файл:

```bash
python services/signal_performance_tracker.py 2>&1 | tee tracker.log
```

Формат логов:

```
2025-11-02 12:34:56 - TradeMonitor - INFO - 📈 Позиция открыта: abc12345 | orderflow | XAUUSD | ...
2025-11-02 12:35:10 - TradeMonitor - INFO - 🎯 TP1 достигнут: abc12345 | ...
2025-11-02 12:36:00 - StatsAggregator - INFO - 📊 Статистика обновлена: orderflow/XAUUSD/tick | ...
```

## 🛠️ Troubleshooting

### Проблема: Сигналы не обрабатываются

**Решение**: Проверить consumer groups

```bash
redis-cli XINFO GROUPS signals:orderflow:XAUUSD
```

### Проблема: Статистика не обновляется

**Решение**: Проверить подключение Stats Aggregator

```python
tracker.trade_monitor.set_stats_aggregator(tracker.stats_aggregator)
```

### Проблема: Telegram уведомления не приходят

**Решение**: Проверить переменные окружения и доступность API

```bash
curl https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/getMe
```

## 🔮 Дальнейшее развитие

- [ ] WebSocket API для real-time обновлений
- [ ] Dashboard с визуализацией метрик
- [ ] Backtesting на исторических данных
- [ ] ML-анализ эффективности сигналов
- [ ] Экспорт в ClickHouse/TimescaleDB
- [ ] А/B тестирование стратегий
- [ ] Автоматическая оптимизация параметров

## 📄 Лицензия

Внутренний проект

## 👥 Контакты

Для вопросов и предложений обращайтесь к команде разработки.
