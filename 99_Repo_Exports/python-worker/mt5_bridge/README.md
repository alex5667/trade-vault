# MT5 Bridge - Signal Execution Engine

Мост между scanner_infra и MetaTrader5 для автоматического исполнения сигналов.

## Обзор

MT5 Bridge реализует полный цикл исполнения сигналов:
- **Чтение планов**: ExecutionPlan из `stream:signals:plans`
- **Исполнение**: Автоматическое открытие позиций по правилам TTD + entry zones
- **Отслеживание**: Реальных сделок MT5 с публикацией execution events
- **Команды**: Обработка команд управления (закрытие позиций и т.п.)

## Архитектура

```
scanner_infra (SignalDetector)
    ↓ publishes ExecutionPlan
stream:signals:plans
    ↓ MT5 Bridge reads & executes
MT5 Bridge (main.py)
├── PlanExecutor (TTD + entry zones)
├── Mt5DealsWatcher (реальные сделки)
└── ExecCommandsConsumer (команды)
    ↓ executes via MetaTrader5 Terminal
    ↓ publishes execution events
stream:signals:exec_events
    ↓ SignalPerformanceTracker reads
```

## 🚀 Быстрый Старт

### 1. Установка зависимостей
```bash
pip install MetaTrader5 redis
```

### 2. Настройка Environment
```bash
# MT5 Credentials (обязательно)
export MT5_LOGIN=12345678
export MT5_PASSWORD=your_password
export MT5_SERVER=YourBroker-Server

# Redis (опционально, по умолчанию localhost)
export REDIS_DSN=redis://localhost:6379/0

# Symbol Mapping (опционально)
export MT5_SYMBOL_MAP='{"XAUUSD": "XAUUSD.m", "BTCUSDT": "BTCUSDT.m"}'

# Performance Tuning (опционально)
export POLL_BLOCK_MS=500
export POLL_COUNT=20
export STEP_INTERVAL=0.2
```

### 3. Запуск
```bash
cd python-worker
python -m mt5_bridge.main
```

## 📋 Формат Данных

### Redis Stream Message
```json
{
  "signal_id": "XAUUSD-breakout-123",
  "symbol": "XAUUSD",
  "setup_type": "breakout_R1",
  "side": "long",
  "ts_signal": "2025-12-15T12:34:56.123456+00:00",
  "payload": "{\"ctx\": {...}, \"plan\": {...}}"
}
```

### ExecutionPlan Payload
```json
{
  "signal_id": "XAUUSD-breakout-123",
  "symbol": "XAUUSD",
  "side": "long",
  "ts_signal": "2025-12-15T12:34:56.123456+00:00",
  "price_at_signal": 2615.3,
  "entry_zone_low": 2610.0,
  "entry_zone_high": 2616.0,
  "stop_price": 2600.0,
  "tp_levels": [2625.0, 2640.0],
  "partials": [0.5, 0.5],
  "risk_usd": 100.0,
  "position_size": 0.2,
  "expiry_bars": 3,
  "created_at": "2025-12-15T12:34:56.123456+00:00"
}
```

## ⚙️ Компоненты

### Новые компоненты v2.0

#### ExecEventsPublisher & ExecutionEvent
```python
# Публикация execution events в Redis
publisher = ExecEventsPublisher(redis_dsn)
event = ExecutionEvent(
    signal_id="XAUUSD-123",
    kind="fill",  # или "command"
    event_type="OPEN",  # OPEN/CLOSE/SL/TP/DEAL
    price=2615.5,
    qty_lots=0.2,
    pnl_ccy=15.50,
    account_ccy="USD"
)
publisher.publish(event)
```

#### Mt5DealsWatcher
```python
# Отслеживание реальных сделок MT5
watcher = Mt5DealsWatcher(mt5_client, publisher)
watcher.step()  # Проверяет новые сделки и публикует events
```

#### ExecCommandsConsumer
```python
# Обработка команд управления
consumer = ExecCommandsConsumer(redis_dsn, mt5_client)
consumer.step()  # Обрабатывает CLOSE_REQUEST и др.
```

### Mt5ExecutionPlan
Упрощенная модель плана для MT5:
```python
@dataclass
class Mt5ExecutionPlan:
    signal_id: str
    symbol: str
    side: str  # "long"/"short"
    entry_zone_low: float
    entry_zone_high: float
    stop_price: float
    tp_levels: List[float]
    partials: List[float]
    position_size_lots: float
    expiry_bars: int
    # ... другие поля
```

### PlanExecutor
Основная логика исполнения:
- **TTD Check**: `(now - ts_signal) > expiry_bars * 60`
- **Entry Zone**: `entry_zone_low <= price <= entry_zone_high`
- **Partial Orders**: Разбиение объема по partials с соответствующими TP

### PlansStreamConsumer
Чтение из Redis Streams:
- Блокирующее чтение новых планов
- JSON парсинг и конвертация
- Graceful error handling

## 🔧 Конфигурация

### Environment Variables

#### Обязательные
- `MT5_LOGIN` - Номер счета MT5
- `MT5_PASSWORD` - Пароль счета
- `MT5_SERVER` - Сервер брокера

#### Опциональные
- `REDIS_DSN` - Redis connection string (default: `redis://localhost:6379/0`)
- `MT5_SYMBOL_MAP` - JSON маппинг символов (default: `{}`)
- `POLL_BLOCK_MS` - Время блокировки Redis polling (default: `500`)
- `POLL_COUNT` - Макс. сообщений за poll (default: `20`)
- `STEP_INTERVAL` - Интервал между execution steps (default: `0.2`)

### Symbol Mapping
```json
{
  "XAUUSD": "XAUUSD.m",
  "BTCUSDT": "BTCUSDT.m",
  "EURUSD": "EURUSD."
}
```

## 📊 Мониторинг

### Логи
```
[mt5_bridge] ✅ MT5 connected successfully
[mt5_bridge] ✅ Redis consumer initialized
[mt5_bridge] 🚀 Bridge started - waiting for signals...
[mt5_bridge] 📋 New plan: XAUUSD-breakout-123 XAUUSD long zone=(2610.00..2616.00) stop=2600.00 size=0.200 expiry=3 bars
[mt5_bridge] 📊 Status: 5 active plans, 2 positions entered
```

### Metrics
- **Active Plans**: Количество планов в обработке
- **Entered Positions**: Количество открытых позиций
- **Expired Plans**: Автоматически удаляются при TTL истечении

## 🛠️ Расширение

### Добавление Symbol Mapping
```python
# В Mt5Config
symbol_map = {
    "XAUUSD": "XAUUSD.m",  # MetaTrader suffix
    "BTCUSDT": "BTCUSDT",  # No change
}
```

### Кастомная Логика Входа
```python
# В PlanExecutor._price_in_zone()
def _price_in_zone(self, plan, price):
    # Добавить spread filter
    bid, ask = self.mt5.get_tick(plan.symbol)
    spread = ask - bid
    if spread > plan.max_spread:
        return False
    return super()._price_in_zone(plan, price)
```

### Сопровождение Позиций
```python
# В PlanExecutor.step() для entered positions
if st.entered:
    # Trailing stop
    self._update_trailing_stop(st)
    # Break-even
    self._check_break_even(st)
```

## 🚨 Troubleshooting

### MT5 Connection Issues
```bash
# Check MT5 terminal is running
# Verify credentials
# Check firewall/network
```

### Redis Connection
```bash
# Test Redis: redis-cli ping
# Check REDIS_DSN format
# Verify network connectivity
```

### Symbol Not Found
```bash
# Add to MT5_SYMBOL_MAP
# Check symbol exists in MT5
# Verify broker symbol naming
```

### No Signals Received
```bash
# Check Redis stream: redis-cli XREAD STREAMS stream:signals:plans 0
# Verify scanner_infra is publishing
# Check network between services
```

## 📈 Production Deployment

### Docker
```dockerfile
FROM python:3.9-slim

# Install MT5 dependencies
RUN apt-get update && apt-get install -y wine

# Install Python deps
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy code
COPY . .

# Run
CMD ["python", "-m", "mt5_bridge.main"]
```

### Systemd Service
```ini
[Unit]
Description=MT5 Bridge Service
After=network.target

[Service]
Type=simple
User=mt5user
EnvironmentFile=/etc/mt5-bridge.env
ExecStart=/usr/bin/python -m mt5_bridge.main
Restart=always

[Install]
WantedBy=multi-user.target
```

### Monitoring
- **Health Checks**: Периодическая проверка MT5 connection
- **Metrics**: Prometheus/Grafana для active plans/positions
- **Alerts**: При потере связи с MT5/Redis

## 🔒 Безопасность

- **Credentials**: Хранить в environment, не в коде
- **Network**: Использовать VPN для MT5 connection
- **Access**: Ограничить доступ к Redis/MT5
- **Logs**: Не логировать чувствительную информацию

---

**MT5 Bridge готов к production использованию! 🚀**
