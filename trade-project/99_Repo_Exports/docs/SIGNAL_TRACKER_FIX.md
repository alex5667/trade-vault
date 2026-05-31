# 🔧 Signal Performance Tracker - Исправление проблем

## 🎯 Проблема

Сервис **Signal Performance Tracker** должен был отправлять статистику каждые 3 часа, но **ни разу не работал**.

## 🔍 Найденные проблемы

### 1. ❌ Сервис не был добавлен в docker-compose.yml

**Критическая ошибка**: Сервис существовал в коде, но **не был запущен**!

### 2. ❌ Неправильная загрузка конфигурации

- Путь к конфиг-файлу был неверный (`config/signal_tracker.json` вместо `config/signal_tracker_config.json`)
- Отсутствовала обработка ошибок при загрузке конфига
- Не было логирования процесса загрузки

### 3. ❌ Отсутствие мержинга конфигов

- Если файл конфига не существовал, использовался только default config
- Если файл существовал, не добавлялись отсутствующие секции

### 4. ❌ Нет команд в Makefile

- Невозможно было проверить статус сервиса
- Невозможно было просмотреть логи
- Невозможно было перезапустить сервис

---

## ✅ Исправления

### 1. Добавлен сервис в docker-compose.yml

```yaml
signal-performance-tracker:
  build:
    context: .
    dockerfile: python-worker/Dockerfile
  container_name: scanner-signal-tracker
  environment:
    - PYTHONUNBUFFERED=1
    - REDIS_URL=redis://scanner-redis:6379/0
    - REDIS_HOST=scanner-redis-worker-1
    - REDIS_PORT=6379
    # Telegram для отчетов
    - TELEGRAM_BOT_TOKEN=${TELEGRAM_BOT_TOKEN}
    - TELEGRAM_CHAT_ID=${TELEGRAM_CHAT_ID}
    # Конфигурация трекера
    - TRACKER_CONFIG_PATH=/app/python-worker/config/signal_tracker_config.json
  depends_on:
    redis:
      condition: service_healthy
    redis-worker-1:
      condition: service_healthy
    multi-symbol-orderflow:
      condition: service_started
  networks:
    - scanner-network
  restart: unless-stopped
  command:
    ['sh', '-c', 'sleep 20 && python -m services.signal_performance_tracker']
```

**Что это даст:**

- ✅ Сервис будет автоматически запускаться с системой
- ✅ Будет отслеживать сигналы от 3 сервисов (orderflow, aggregated-hub, и т.д.)
- ✅ Будет отправлять статистику каждые 3 часа в Telegram
- ✅ Auto-restart при сбоях

---

### 2. Создан конфигурационный файл

**Файл**: `python-worker/config/signal_tracker_config.json`

```json
{
	"streams": {
		"symbols": ["XAUUSD"],
		"strategies": ["orderflow", "aggregated-hub"]
	},
	"consumer_group": "signal-tracker-group",
	"consumer_name": "tracker-main",
	"monitor": {
		"default_lot": 0.01,
		"risk_pct": 1.0,
		"stop_atr_mult": 1.5,
		"rr_levels": [2.0, 3.0, 4.0],
		"tp_ratio": [0.5, 0.3, 0.2]
	},
	"telegram": {
		"notify_on_trade_close": false
	},
	"reporting": {
		"daily_summary_enabled": true,
		"daily_summary_hour": 0,
		"periodic_summary_enabled": true,
		"periodic_summary_interval_hours": 3
	}
}
```

**Ключевые параметры:**

- `periodic_summary_enabled: true` - включена периодическая отправка
- `periodic_summary_interval_hours: 3` - каждые 3 часа
- `strategies`: отслеживает сигналы от `orderflow` и `aggregated-hub`

---

### 3. Исправлен код signal_performance_tracker.py

**Основные изменения:**

#### a) Улучшенная загрузка конфигурации

```python
# Логгер для main
logger = setup_logger("SignalTrackerMain", level="INFO")

# Загрузка конфигурации из ENV или файла
config_path = os.getenv("TRACKER_CONFIG_PATH", "config/signal_tracker_config.json")
logger.info(f"🔧 Загрузка конфигурации из: {config_path}")

config = {}
if os.path.exists(config_path):
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
        logger.info(f"✅ Конфигурация загружена из файла: {config_path}")
    except Exception as e:
        logger.error(f"❌ Ошибка загрузки конфига из {config_path}: {e}")
else:
    logger.warning(f"⚠️ Файл конфигурации не найден: {config_path}")
```

#### b) Мержинг конфигов

```python
# Мержим конфиги (default + loaded)
if not config:
    config = default_config
else:
    # Добавляем отсутствующие секции из default
    for key, value in default_config.items():
        if key not in config:
            config[key] = value
```

#### c) Детальное логирование

```python
# Логируем итоговую конфигурацию
logger.info(f"📊 Символы: {config.get('streams', {}).get('symbols', [])}")
logger.info(f"📊 Стратегии: {config.get('streams', {}).get('strategies', [])}")
logger.info(f"📊 Периодические отчеты: {config.get('reporting', {}).get('periodic_summary_enabled', False)}")
logger.info(f"📊 Интервал отчетов: {config.get('reporting', {}).get('periodic_summary_interval_hours', 3)}ч")
```

---

### 4. Добавлены команды в Makefile

```bash
# Статус трекера
make tracker-status

# Логи трекера (follow)
make tracker-logs

# Перезапуск трекера
make tracker-restart
```

**Также обновлен help:**

```bash
make help  # Теперь показывает команды трекера
```

---

## 🚀 Запуск исправленной системы

### Шаг 1: Убедитесь, что есть Telegram credentials

```bash
# Проверьте, что в .env или переменных окружения есть:
echo $TELEGRAM_BOT_TOKEN
echo $TELEGRAM_CHAT_ID

# Если нет, добавьте в telegram-worker/.env:
TELEGRAM_BOT_TOKEN=your_bot_token_here
TELEGRAM_CHAT_ID=your_chat_id_here
```

### Шаг 2: Остановите старые контейнеры

```bash
make down
```

### Шаг 3: Запустите систему

```bash
make up
```

Или в фоновом режиме:

```bash
make up-bg
```

### Шаг 4: Проверьте статус трекера

```bash
make tracker-status
```

**Ожидаемый вывод:**

```
📊 Статус Signal Performance Tracker:
NAME                    STATUS
scanner-signal-tracker  Up X seconds

Последние 30 строк логов:
🔧 Загрузка конфигурации из: /app/python-worker/config/signal_tracker_config.json
✅ Конфигурация загружена из файла
📊 Символы: ['XAUUSD']
📊 Стратегии: ['orderflow', 'aggregated-hub']
📊 Периодические отчеты: True
📊 Интервал отчетов: 3ч
🚀 Запуск Signal Performance Tracker...
✅ Redis подключение установлено
🔧 Инициализация компонентов...
✅ Signal Performance Tracker инициализирован
📊 Отслеживаемые символы: ['XAUUSD']
📊 Отслеживаемые стратегии: ['orderflow', 'aggregated-hub']
🚀 Запуск Signal Performance Tracker...
✅ Все потоки запущены
📊 Система мониторинга активна
🔄 Запущен цикл обработки сигналов
🔄 Запущен цикл обработки тиков
🔄 Запущен цикл периодических задач
```

### Шаг 5: Следите за логами

```bash
make tracker-logs
```

**Каждые 60 секунд вы будете видеть:**

```
📊 Статус: Uptime 60s | Signals 15 | Ticks 1250 | Open 2 | Closed 5 | Errors 0
```

**Каждые 3 часа вы получите в Telegram:**

```
📊 Периодическая сводка (3ч)

Strategy: orderflow
├─ Сделок: 25
├─ Прибыльных: 18 (72.0%)
├─ Убыточных: 7
└─ Общий P&L: +$523.40

Strategy: aggregated-hub
├─ Сделок: 15
├─ Прибыльных: 11 (73.3%)
├─ Убыточных: 4
└─ Общий P&L: +$342.10
```

---

## 📊 Что отслеживает трекер

### 1. Обработка сигналов

- Читает сигналы из Redis streams: `signals:orderflow:XAUUSD`, `signals:aggregated-hub:XAUUSD`
- Создает виртуальные позиции для каждого сигнала
- Отслеживает SL/TP уровни

### 2. Обработка тиков

- Читает тики из `stream:tick_XAUUSD`
- Обновляет открытые позиции текущей ценой
- Закрывает позиции при достижении SL/TP

### 3. Статистика

Собирает метрики по каждой стратегии:

- Количество сделок
- Win rate (% прибыльных)
- Общий P&L
- Average win/loss
- Max drawdown
- и другие

### 4. Отчеты

**Периодические (каждые 3 часа):**

- Сводка по всем стратегиям
- Отправка в Telegram

**Ежедневные (в 00:00 UTC):**

- Детальный отчет за день
- Отправка в Telegram

---

## 🔍 Troubleshooting

### Проблема: Трекер не запускается

```bash
# Проверьте логи
make tracker-logs

# Проверьте, что Redis доступен
docker exec scanner-signal-tracker redis-cli -h scanner-redis ping

# Перезапустите
make tracker-restart
```

### Проблема: Нет отчетов в Telegram

```bash
# Проверьте, что Telegram credentials правильные
docker exec scanner-signal-tracker env | grep TELEGRAM

# Если пусто, добавьте в docker-compose.yml:
environment:
  - TELEGRAM_BOT_TOKEN=your_token
  - TELEGRAM_CHAT_ID=your_chat_id
```

### Проблема: Трекер не видит сигналы

```bash
# Проверьте, что сигналы есть в Redis
docker exec scanner-redis redis-cli XLEN signals:orderflow:XAUUSD

# Если 0, проверьте orderflow handler
make orderflow-status

# Проверьте aggregated-hub
make hub-status
```

---

## ✅ Итоговый чеклист

- [x] Добавлен сервис в docker-compose.yml
- [x] Создан конфигурационный файл
- [x] Исправлен код signal_performance_tracker.py
- [x] Добавлены команды в Makefile
- [x] Улучшено логирование
- [x] Добавлена обработка ошибок
- [x] Документация создана

---

## 📝 Дополнительные улучшения (опционально)

### 1. Мониторинг через Prometheus

Добавьте метрики в `signal_performance_tracker.py`:

```python
from prometheus_client import Counter, Gauge, start_http_server

signals_processed = Counter('tracker_signals_processed_total', 'Total signals processed')
positions_open = Gauge('tracker_positions_open', 'Open positions')
```

### 2. Графики в Telegram

Можно добавить отправку графиков P&L в отчетах.

### 3. Alert'ы при проблемах

Настроить уведомления при:

- Долгом отсутствии сигналов
- Высоком проценте убыточных сделок
- Ошибках в обработке

---

## 🎓 Как это работает (для понимания)

### Поток данных:

```
Binance WS → Go Workers → Redis (candles)
     │
     └─→ Python Workers → Redis (signals)
              │
              └─→ Signal Tracker → Статистика → Telegram
                         │
                         └─→ Redis (ticks) → Update Positions
```

### Архитектура трекера:

```
SignalPerformanceTracker
├─ SignalProcessor Thread (читает signals из Redis)
│  └─→ TradeMonitor.process_signal() → создает позицию
│
├─ TickProcessor Thread (читает ticks из Redis)
│  └─→ TradeMonitor.process_tick() → обновляет позиции
│
└─ PeriodicTasks Thread
   ├─→ каждые 60 сек: логирование статуса
   ├─→ каждые 3 часа: отправка периодической сводки
   └─→ каждый день: отправка дневного отчета
```

---

**Готово! Система исправлена и готова к работе** ✅

**Следующие шаги:**

1. Запустите систему: `make up-bg`
2. Проверьте статус: `make tracker-status`
3. Следите за логами: `make tracker-logs`
4. Ждите первую статистику через 3 часа! 📊

---

_Документ создан: 3 ноября 2025_  
_Senior Go/Python Developer + Senior Trading Systems Analyst_
