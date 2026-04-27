# Развёртывание Signal Performance Tracker

## 🚀 Способы запуска

### 1. Standalone скрипт (рекомендуется)

Простой запуск из командной строки:

```bash
cd /home/alex/front/trade/scanner_infra/python-worker
python run_performance_tracker.py
```

#### С переменными окружения

```bash
# Базовые настройки
export REDIS_HOST=scanner-redis-worker-1
export REDIS_PORT=6379

# Символы и стратегии (через запятую)
export SYMBOLS=XAUUSD,BTCUSD
export STRATEGIES=orderflow,deltaSpikeB

# Trading параметры
export DEFAULT_LOT=1.0
export RISK_PCT=1.0
export STOP_ATR_MULT=1.0

# Telegram
export TELEGRAM_BOT_TOKEN=your_token
export TELEGRAM_CHAT_ID=your_chat_id
export NOTIFY_ON_TRADE_CLOSE=false
export DAILY_SUMMARY=true
export DAILY_SUMMARY_HOUR=0

# Запуск
python run_performance_tracker.py
```

#### С кастомной конфигурацией

```bash
export TRACKER_CONFIG=/path/to/my_config.json
python run_performance_tracker.py
```

### 2. Через Docker Compose

Добавьте в `docker-compose.yml`:

```yaml
signal-performance-tracker:
  build:
    context: ./python-worker
    dockerfile: Dockerfile
  container_name: signal-tracker
  command: python run_performance_tracker.py
  environment:
    - REDIS_HOST=scanner-redis-worker-1
    - REDIS_PORT=6379
    - SYMBOLS=XAUUSD,BTCUSD
    - STRATEGIES=orderflow
    - TELEGRAM_BOT_TOKEN=${TELEGRAM_BOT_TOKEN}
    - TELEGRAM_CHAT_ID=${TELEGRAM_CHAT_ID}
    - NOTIFY_ON_TRADE_CLOSE=false
    - DAILY_SUMMARY=true
  depends_on:
    - scanner-redis-worker-1
  restart: unless-stopped
  networks:
    - scanner-network
```

Запуск:

```bash
docker-compose up -d signal-performance-tracker
```

### 3. Как systemd сервис

Создайте файл `/etc/systemd/system/signal-tracker.service`:

```ini
[Unit]
Description=Signal Performance Tracker
After=network.target redis.service

[Service]
Type=simple
User=alex
WorkingDirectory=/home/alex/front/trade/scanner_infra/python-worker
Environment="REDIS_HOST=localhost"
Environment="REDIS_PORT=6379"
Environment="SYMBOLS=XAUUSD"
Environment="STRATEGIES=orderflow"
Environment="TELEGRAM_BOT_TOKEN=your_token"
Environment="TELEGRAM_CHAT_ID=your_chat_id"
ExecStart=/usr/bin/python3 /home/alex/front/trade/scanner_infra/python-worker/run_performance_tracker.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

Управление:

```bash
sudo systemctl daemon-reload
sudo systemctl enable signal-tracker
sudo systemctl start signal-tracker
sudo systemctl status signal-tracker
sudo journalctl -u signal-tracker -f
```

### 4. Программный запуск (Python API)

```python
from services.signal_performance_tracker import SignalPerformanceTracker

config = {
    "streams": {
        "symbols": ["XAUUSD"],
        "strategies": ["orderflow"]
    },
    "monitor": {
        "default_lot": 1.0,
        "tp_ratio": [0.50, 0.30, 0.20]
    },
    "telegram": {
        "bot_token": "YOUR_TOKEN",
        "chat_id": "YOUR_CHAT_ID",
        "notify_on_trade_close": False
    }
}

tracker = SignalPerformanceTracker(config)
tracker.run_forever()
```

## 📝 Конфигурация

### Структура конфигурационного файла

```json
{
	"streams": {
		"symbols": ["XAUUSD", "BTCUSD"],
		"strategies": ["orderflow", "deltaSpikeB"]
	},

	"consumer_group": "signal-tracker-group",
	"consumer_name": "tracker-main",

	"monitor": {
		"default_lot": 1.0,
		"risk_pct": 1.0,
		"stop_atr_mult": 1.0,
		"rr_levels": [1.0, 2.0, 3.0],
		"tp_ratio": [0.5, 0.3, 0.2],
		"notify_on_trade_close": false
	},

	"telegram": {
		"bot_token": "${TELEGRAM_BOT_TOKEN}",
		"chat_id": "${TELEGRAM_CHAT_ID}",
		"notify_on_trade_close": false
	},

	"reporting": {
		"daily_summary_enabled": true,
		"daily_summary_hour": 0
	}
}
```

### Переменные окружения

| Переменная               | Описание                  | Значение по умолчанию               |
| ------------------------ | ------------------------- | ----------------------------------- |
| `REDIS_HOST`             | Хост Redis                | `scanner-redis-worker-1`            |
| `REDIS_PORT`             | Порт Redis                | `6379`                              |
| `SYMBOLS`                | Символы (через запятую)   | `XAUUSD`                            |
| `STRATEGIES`             | Стратегии (через запятую) | `orderflow`                         |
| `CONSUMER_GROUP`         | Consumer group            | `signal-tracker-group`              |
| `DEFAULT_LOT`            | Размер позиции            | `1.0`                               |
| `STOP_ATR_MULT`          | Множитель ATR для SL      | `1.0`                               |
| `TELEGRAM_BOT_TOKEN`     | Токен Telegram бота       | -                                   |
| `TELEGRAM_CHAT_ID`       | ID чата Telegram          | -                                   |
| `NOTIFY_ON_TRADE_CLOSE`  | Уведомления при закрытии  | `false`                             |
| `DAILY_SUMMARY`          | Ежедневная сводка         | `true`                              |
| `DAILY_SUMMARY_HOUR`     | Час отправки сводки (UTC) | `0`                                 |
| `PERIODIC_SUMMARY`       | Периодическая сводка      | `true`                              |
| `PERIODIC_SUMMARY_HOURS` | Интервал сводки (часы)    | `3`                                 |
| `TRACKER_CONFIG`         | Путь к JSON конфигу       | `config/signal_tracker_config.json` |

## 🔄 Управление процессом

### Проверка статуса

```python
# Через Python API
status = tracker.get_status()
print(f"Running: {status['is_running']}")
print(f"Uptime: {status['uptime_sec']}s")
print(f"Signals: {status['signals_read']}")
print(f"Open positions: {status['monitor']['open_positions']}")
```

### Логи

```bash
# Standalone
python run_performance_tracker.py 2>&1 | tee tracker.log

# Docker
docker logs -f signal-performance-tracker

# Systemd
journalctl -u signal-tracker -f
```

### Graceful shutdown

Трекер корректно обрабатывает сигналы `SIGINT` (Ctrl+C) и `SIGTERM`:

- Останавливает все потоки обработки
- Очищает закрытые позиции из памяти
- Выводит финальную статистику

```bash
# Отправка SIGTERM
kill -TERM <PID>

# Docker
docker stop signal-performance-tracker

# Systemd
sudo systemctl stop signal-tracker
```

## 🧪 Тестирование развёртывания

### 1. Проверка конфигурации

```bash
python -c "
from run_performance_tracker import load_config
import json
config = load_config()
print(json.dumps(config, indent=2))
"
```

### 2. Проверка подключения к Redis

```bash
redis-cli -h scanner-redis-worker-1 -p 6379 PING
```

### 3. Проверка Consumer Groups

```bash
# Для сигналов
redis-cli XINFO GROUPS signals:orderflow:XAUUSD

# Для тиков
redis-cli XINFO GROUPS stream:tick_XAUUSD
```

### 4. Сухой запуск (dry run)

```python
from services.signal_performance_tracker import SignalPerformanceTracker

config = {"streams": {"symbols": ["XAUUSD"], "strategies": ["orderflow"]}}
tracker = SignalPerformanceTracker(config)

# Проверяем инициализацию
assert tracker.is_running == False
assert tracker.trade_monitor is not None
assert tracker.reporting_service is not None

print("✅ Инициализация успешна")
```

## 📊 Мониторинг

### Метрики работы

```python
status = tracker.get_status()

# Системные метрики
uptime = status['uptime_sec']
errors = status['errors']

# Обработка данных
signals_read = status['signals_read']
ticks_processed = status['ticks_processed']

# Позиции
open_positions = status['monitor']['open_positions']
closed_positions = status['monitor']['positions_closed']
tp_events = status['monitor']['tp_events']
sl_events = status['monitor']['sl_events']

# Производительность
performance = status['performance']
total_trades = performance['total_trades']
winrate = performance['winrate']
total_pnl = performance['total_pnl']
```

### Проверка health

```bash
# Redis streams существуют
redis-cli EXISTS signals:orderflow:XAUUSD
redis-cli EXISTS stream:tick_XAUUSD

# Consumer groups активны
redis-cli XINFO GROUPS signals:orderflow:XAUUSD | grep signal-tracker-group

# Статистика доступна
redis-cli HGETALL stats:orderflow:XAUUSD:tick
```

## 🐛 Troubleshooting

### Проблема: Сервис не запускается

**Решение:**

```bash
# Проверить Redis
redis-cli -h $REDIS_HOST -p $REDIS_PORT PING

# Проверить переменные окружения
env | grep -E "REDIS|TELEGRAM|SYMBOLS"

# Проверить логи
tail -f tracker.log
```

### Проблема: Сигналы не обрабатываются

**Решение:**

```bash
# Проверить наличие сигналов в потоке
redis-cli XLEN signals:orderflow:XAUUSD

# Проверить consumer group
redis-cli XINFO GROUPS signals:orderflow:XAUUSD

# Проверить pending messages
redis-cli XPENDING signals:orderflow:XAUUSD signal-tracker-group
```

### Проблема: Высокое потребление памяти

**Причина:** Много открытых позиций в памяти

**Решение:**

```python
# Периодическая очистка
tracker.trade_monitor.cleanup_closed_positions()

# Или настроить автоочистку в конфиге
```

### Проблема: Потеря сообщений

**Причина:** Consumer не подтверждает обработку

**Решение:**

```bash
# Проверить pending messages
redis-cli XPENDING signals:orderflow:XAUUSD signal-tracker-group

# Claim старые сообщения
redis-cli XAUTOCLAIM signals:orderflow:XAUUSD signal-tracker-group tracker-main 3600000 0-0
```

## 🔐 Безопасность

### Redis аутентификация

```bash
export REDIS_URL=redis://:password@host:port/db
```

### Telegram токены

**Не** храните токены в git:

```bash
# .gitignore
.env
config/production.json
```

Используйте `.env` файл:

```bash
# .env
TELEGRAM_BOT_TOKEN=your_token_here
TELEGRAM_CHAT_ID=your_chat_id_here

# Загрузка
export $(cat .env | xargs)
python run_performance_tracker.py
```

### Ограничение доступа

```bash
# Файл конфигурации
chmod 600 config/signal_tracker_config.json

# Скрипт запуска
chmod 700 run_performance_tracker.py
```

## 📈 Масштабирование

### Множественные экземпляры

Для обработки большой нагрузки можно запустить несколько экземпляров:

```bash
# Экземпляр 1
CONSUMER_NAME=tracker-1 python run_performance_tracker.py &

# Экземпляр 2
CONSUMER_NAME=tracker-2 python run_performance_tracker.py &

# Экземпляр 3
CONSUMER_NAME=tracker-3 python run_performance_tracker.py &
```

Redis автоматически распределит нагрузку между consumers в группе.

### Разделение по символам

```bash
# Трекер для XAUUSD
SYMBOLS=XAUUSD CONSUMER_NAME=tracker-xau python run_performance_tracker.py &

# Трекер для BTCUSD
SYMBOLS=BTCUSD CONSUMER_NAME=tracker-btc python run_performance_tracker.py &
```

### Мониторинг производительности

```bash
# CPU и память
ps aux | grep run_performance_tracker

# Сетевой трафик
nethogs

# Redis операции
redis-cli --latency
redis-cli --stat
```

## 🔄 Обновление

### Без downtime

```bash
# 1. Запустить новый экземпляр
CONSUMER_NAME=tracker-new python run_performance_tracker.py &

# 2. Остановить старый
kill -TERM <OLD_PID>

# 3. Переименовать новый consumer (опционально)
```

### С коротким downtime

```bash
# 1. Остановить сервис
sudo systemctl stop signal-tracker

# 2. Обновить код
git pull

# 3. Запустить сервис
sudo systemctl start signal-tracker
```

## 📋 Checklist развёртывания

- [ ] Redis доступен и работает
- [ ] Переменные окружения установлены
- [ ] Конфигурационный файл создан
- [ ] Consumer groups созданы
- [ ] Telegram бот настроен (опционально)
- [ ] Логирование настроено
- [ ] Автозапуск настроен (systemd/docker)
- [ ] Мониторинг настроен
- [ ] Backup стратегия определена
- [ ] Тестовый запуск выполнен успешно
