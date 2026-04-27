# 🔗 Analytics v2.0 - Integration Guide

Руководство по интеграции Analytics Package v2.0 с существующей инфраструктурой.

---

## 🏗️ Архитектура интеграции

```
┌─────────────────────────────────────────────────────────────────┐
│                    Trading Infrastructure                        │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Signals → TradeMonitor → StatsAggregator → ReportingService   │
│              │                   │                               │
│              ↓                   ↓                               │
│         trades:closed      stats:*:*:*                          │
│              │                   │                               │
│              └───────┬───────────┘                               │
│                      ↓                                           │
│              ┌───────────────┐                                   │
│              │  Redis Data   │                                   │
│              └───────┬───────┘                                   │
│                      ↓                                           │
└──────────────────────┼───────────────────────────────────────────┘
                       │
┌──────────────────────┼───────────────────────────────────────────┐
│              Analytics v2.0 Layer                                │
├──────────────────────┼───────────────────────────────────────────┤
│                      ↓                                           │
│         ┌────────────────────────┐                               │
│         │    Repository API       │                               │
│         └────────┬───────────────┘                               │
│                  │                                                │
│         ┌────────┴────────┬──────────────┬──────────────┐       │
│         ↓                 ↓              ↓              ↓        │
│  ┌──────────────┐  ┌─────────────┐ ┌──────────┐ ┌─────────────┐│
│  │Dataset Export│  │Threshold    │ │ROC Store │ │Metrics Pub  ││
│  │(Parquet)     │  │Tuner        │ │          │ │(Grafana)    ││
│  └──────┬───────┘  └──────┬──────┘ └────┬─────┘ └──────┬──────┘│
│         │                 │              │              │        │
│         ↓                 ↓              ↓              ↓        │
│  ┌──────────────┐  ┌─────────────┐ ┌──────────┐ ┌─────────────┐│
│  │ML Analysis   │  │Aggregated   │ │Telegram  │ │Grafana      ││
│  │(Offline)     │  │Hub Reload   │ │Reports   │ │Dashboard    ││
│  └──────────────┘  └─────────────┘ └──────────┘ └─────────────┘│
└──────────────────────────────────────────────────────────────────┘
```

---

## 🔌 Интеграция с Signal Performance Tracker

### 1. Автоматическое обновление порогов

Analytics v2.0 автоматически обновляет пороги фильтрации в aggregated hub:

```python
from analytics.threshold_tuner import ThresholdTuner
from analytics.repository import Repository, RepoConfig

# Инициализация
repo = Repository(RepoConfig())
tuner = ThresholdTuner(repo)

# Тюнинг порога
result = tuner.tune_and_publish(
    strategy="aggregated",
    symbol="XAUUSD",
    signals=signals,
    orders=orders,
    emit_telegram=True
)

# Результат:
# 1. hub:threshold:aggregated:XAUUSD = {"thr": 0.55, "auc": 0.72, ...}
# 2. aggregated_hub:control stream <- {"action": "reload", "scope": "aggregated:XAUUSD"}
# 3. Aggregated Hub перечитывает порог и обновляет фильтрацию
```

**Aggregated Hub должен:**

```python
# В aggregated_hub_v2.py добавить consumer для control stream

def _watch_control_stream(self):
    """Слушаем команды перезагрузки"""
    while self.running:
        messages = self.redis.xread(
            {"aggregated_hub:control": "$"},
            count=10,
            block=1000
        )
        
        for stream, msgs in messages:
            for msg_id, data in msgs:
                action = data.get("action")
                scope = data.get("scope")  # "strategy:symbol"
                
                if action == "reload" and scope:
                    strategy, symbol = scope.split(":")
                    self._reload_threshold(strategy, symbol)

def _reload_threshold(self, strategy: str, symbol: str):
    """Перезагрузка порога из Redis"""
    key = f"hub:threshold:{strategy}:{symbol}"
    data = self.redis.get(key)
    
    if data:
        threshold_data = json.loads(data)
        threshold = threshold_data.get("thr")
        
        # Обновляем в runtime
        self.thresholds[(strategy, symbol)] = threshold
        
        self.logger.info(
            f"🔧 Threshold updated: {strategy}/{symbol} = {threshold:.2f}"
        )
```

---

### 2. Интеграция с ReportingService

Reporting Service может использовать метрики из Analytics v2.0:

```python
# В services/reporting_service.py

from analytics.metrics_publisher import MetricsPublisher
from analytics.roc_store import ROCStore

class ReportingService:
    def __init__(self, redis_url: str):
        # ... существующая инициализация
        self.metrics_publisher = MetricsPublisher(redis_url)
        self.roc_store = ROCStore(redis_url)
    
    def get_extended_report(self, strategy: str, symbol: str, tf: str):
        """Расширенный отчёт с метриками из Analytics v2.0"""
        
        # Базовые метрики из StatsAggregator
        stats = StatsAggregator.get_stats(self.redis, strategy, symbol, tf)
        
        # Дополнительные метрики из Analytics v2.0
        analytics_metrics = self.metrics_publisher.get_latest(strategy, symbol)
        
        if analytics_metrics:
            stats["auc"] = analytics_metrics.get("auc")
            stats["threshold"] = analytics_metrics.get("thr")
            stats["youdenJ"] = analytics_metrics.get("youdenJ")
        
        # ROC данные
        roc_data = self.roc_store.load(strategy, symbol)
        
        if roc_data:
            stats["roc_points"] = len(roc_data.get("points", []))
            stats["roc_updated"] = roc_data.get("ts")
        
        return stats
```

---

### 3. Интеграция с Telegram Notifier

Используйте `notify:telegram` stream для отправки графиков:

**Python-worker notify service:**

```python
# В telegram-worker или notify service

def _process_telegram_message(self, msg_data: dict):
    """Обработка сообщений из notify:telegram stream"""
    
    text = msg_data.get("text")
    photo_path = msg_data.get("photo_path")
    caption = msg_data.get("caption")
    parse_mode = msg_data.get("parse_mode", "HTML")
    group_id = msg_data.get("group_id")
    
    if photo_path and os.path.exists(photo_path):
        # Отправка фото
        with open(photo_path, "rb") as photo:
            self.bot.send_photo(
                chat_id=self.chat_id,
                photo=photo,
                caption=caption,
                parse_mode=parse_mode
            )
    elif text:
        # Отправка текста
        self.bot.send_message(
            chat_id=self.chat_id,
            text=text,
            parse_mode=parse_mode
        )
```

---

## 📊 Интеграция с Grafana

### 1. Настройка Redis Data Source

1. Установите Redis Data Source:
```bash
grafana-cli plugins install redis-datasource
```

2. Добавьте data source в Grafana:
- **Name:** Redis Scanner Worker 1
- **Address:** `scanner-redis-worker-1:6379`
- **Database:** `0`

### 2. Создание Dashboard

**Panel: Winrate по стратегиям**

```sql
-- Query Type: Stream
XREVRANGE metrics:strategy_perf + - COUNT 1000
```

**Transformation:**
- Group by: `strategy`, `symbol`
- Aggregate: Last value of `winrate`

**Panel: Average P/L Trend**

```sql
XREVRANGE metrics:strategy_perf + - COUNT 1000
```

**Transformation:**
- Time series от поля `ts`
- Value от поля `avg_pnl`
- Group by: `strategy`

**Panel: AUC Quality**

```sql
XREVRANGE metrics:roc + - COUNT 100
```

**Visualization:** Time series с thresholds:
- Green: AUC > 0.7
- Yellow: AUC 0.6-0.7
- Red: AUC < 0.6

### 3. Variables для фильтрации

```sql
-- Variable: strategy
SMEMBERS strategies

-- Variable: symbol  
SMEMBERS symbols

-- Filtered query
XREVRANGE metrics:strategy_perf + - COUNT 1000 
WHERE strategy=$strategy AND symbol=$symbol
```

---

## 🐳 Docker Compose интеграция

Добавьте сервис в `docker-compose.yml`:

```yaml
services:
  # ... существующие сервисы
  
  analytics-nightly:
    build:
      context: ./python-worker
      dockerfile: Dockerfile
    container_name: analytics-nightly
    restart: unless-stopped
    environment:
      - REDIS_URL=redis://scanner-redis-worker-1:6379/0
      - DATASET_DIR=/data/datasets_partitioned
      - REPORT_IMG_DIR=/data/reports
      - TELEGRAM_BOT_TOKEN=${TELEGRAM_BOT_TOKEN}
      - NOTIFY_STREAM=notify:telegram
    volumes:
      - ./data/datasets_partitioned:/data/datasets_partitioned
      - ./data/reports:/data/reports
    command: >
      bash -c "
        echo 'Starting Analytics Nightly Pipeline...';
        while true; do
          echo '[$(date)] Running nightly pipeline...';
          python -m analytics.nightly_pipeline \
            --symbols XAUUSD,XAGUSD \
            --strategies aggregated,orderflow,ta \
            --days 7 || echo 'Pipeline failed';
          echo '[$(date)] Sleeping for 24h...';
          sleep 86400;
        done
      "
    depends_on:
      - scanner-redis-worker-1
    networks:
      - scanner-network
    logging:
      driver: "json-file"
      options:
        max-size: "10m"
        max-file: "3"

  analytics-threshold-tuner:
    build:
      context: ./python-worker
      dockerfile: Dockerfile
    container_name: analytics-threshold-tuner
    restart: unless-stopped
    environment:
      - REDIS_URL=redis://scanner-redis-worker-1:6379/0
      - TELEGRAM_BOT_TOKEN=${TELEGRAM_BOT_TOKEN}
    command: >
      bash -c "
        echo 'Starting Threshold Tuner (every 12h)...';
        while true; do
          echo '[$(date)] Tuning thresholds...';
          python -m analytics.multi_publish_best_threshold \
            --symbols XAUUSD,XAGUSD \
            --strategies aggregated,orderflow \
            --days 3 \
            --emit-telegram 0 || echo 'Tuning failed';
          echo '[$(date)] Sleeping for 12h...';
          sleep 43200;
        done
      "
    depends_on:
      - scanner-redis-worker-1
    networks:
      - scanner-network
    logging:
      driver: "json-file"
      options:
        max-size: "10m"
        max-file: "3"
```

### Запуск сервисов

```bash
# Запуск всех сервисов
docker-compose up -d

# Только analytics
docker-compose up -d analytics-nightly analytics-threshold-tuner

# Логи
docker-compose logs -f analytics-nightly

# Остановка
docker-compose stop analytics-nightly
```

---

## 🔄 Workflow примеры

### Пример 1: Ежедневный автоматический цикл

```
02:00 - Nightly Pipeline запускается
  │
  ├─► Экспорт датасета за последние 7 дней
  │   └─► /data/datasets_partitioned/symbol=XAUUSD/...
  │
  ├─► Тюнинг порогов для всех стратегий
  │   ├─► Вычисление ROC/AUC
  │   ├─► Youden Index оптимизация
  │   ├─► Публикация порогов в Redis
  │   └─► Уведомление aggregated hub
  │
  ├─► Публикация метрик в Grafana
  │   └─► metrics:strategy_perf stream
  │
  └─► Telegram отчёты
      ├─► ROC кривые (PNG)
      ├─► Confusion Matrix
      └─► Текстовые сводки

10:00 - Threshold Tuner запускается (каждые 12ч)
  │
  └─► Быстрый тюнинг без экспорта датасета
      └─► Обновление порогов на основе последних 3 дней
```

### Пример 2: Manual on-demand анализ

```bash
# 1. Экспорт датасета для конкретной стратегии
python -c "
from analytics.repository import Repository, RepoConfig
from analytics.dataset_export import export_dataset_partitioned
import time

repo = Repository(RepoConfig())
since = time.time() - 30*86400  # 30 дней

orders = [o for o in repo.read_closed_trades(50000) 
          if o.symbol == 'XAUUSD' and o.strategy == 'orderflow']
signals = list(repo.iter_signals(symbol='XAUUSD', strategy='orderflow', since_ts=since))

export_dataset_partitioned(repo, orders, signals)
"

# 2. Тюнинг порога
python -m analytics.multi_publish_best_threshold \
  --symbols XAUUSD \
  --strategies orderflow \
  --days 30 \
  --emit-telegram 1

# 3. Проверка результата
redis-cli GET "hub:threshold:orderflow:XAUUSD" | jq
```

### Пример 3: A/B сравнение стратегий

```python
from analytics.repository import Repository, RepoConfig
from analytics.metrics_publisher import MetricsPublisher
import time

repo = Repository(RepoConfig())
publisher = MetricsPublisher()

since = time.time() - 7*86400

strategies = ["aggregated", "orderflow", "ta"]
results = {}

for strategy in strategies:
    orders = [o for o in repo.read_closed_trades(10000)
              if o.symbol == "XAUUSD" and o.strategy == strategy
              and o.entry_time and o.entry_time >= since]
    
    if not orders:
        continue
    
    wins = sum(1 for o in orders if (o.pnl_usd or 0) > 0)
    winrate = wins / len(orders)
    avg_pnl = sum([o.pnl_usd or 0 for o in orders]) / len(orders)
    
    results[strategy] = {
        "trades": len(orders),
        "winrate": winrate,
        "avg_pnl": avg_pnl
    }

# Вывод сравнения
print("A/B Comparison (7 days):")
for strategy, metrics in sorted(results.items(), key=lambda x: x[1]["winrate"], reverse=True):
    print(f"{strategy:15s} | WR: {metrics['winrate']:.1%} | Avg P/L: ${metrics['avg_pnl']:+.2f} | N={metrics['trades']}")
```

**Output:**
```
A/B Comparison (7 days):
aggregated      | WR: 62.5% | Avg P/L: $+9.23 | N=158
orderflow       | WR: 58.3% | Avg P/L: $+7.45 | N=142
ta              | WR: 54.2% | Avg P/L: $+5.67 | N=96
```

---

## 🧪 Тестирование интеграции

### Проверка всей цепочки

```bash
#!/bin/bash
# test_analytics_integration.sh

echo "🧪 Testing Analytics v2.0 Integration..."

# 1. Проверка Redis доступности
echo "1. Checking Redis..."
redis-cli -h scanner-redis-worker-1 PING || exit 1

# 2. Проверка наличия данных
echo "2. Checking data availability..."
TRADES=$(redis-cli -h scanner-redis-worker-1 XLEN trades:closed)
echo "   Closed trades: $TRADES"

# 3. Запуск мини-тюнинга
echo "3. Running threshold tuning..."
python -m analytics.multi_publish_best_threshold \
  --symbols XAUUSD \
  --strategies aggregated \
  --days 1 \
  --emit-telegram 0 || exit 1

# 4. Проверка результатов
echo "4. Checking results..."
THRESHOLD=$(redis-cli -h scanner-redis-worker-1 GET "hub:threshold:aggregated:XAUUSD")
echo "   Threshold: $THRESHOLD"

# 5. Проверка ROC данных
echo "5. Checking ROC data..."
ROC=$(redis-cli -h scanner-redis-worker-1 GET "analytics:roc:aggregated:XAUUSD")
echo "   ROC data: ${ROC:0:100}..."

# 6. Проверка метрик
echo "6. Checking metrics..."
METRICS=$(redis-cli -h scanner-redis-worker-1 GET "metrics:last:aggregated:XAUUSD")
echo "   Metrics: ${METRICS:0:100}..."

echo "✅ Integration test completed!"
```

---

## 📚 Best Practices

### 1. Частота обновлений

- **Nightly Pipeline:** 1 раз в день (02:00)
- **Threshold Tuner:** Каждые 12 часов
- **Metrics Publisher:** Real-time (по мере закрытия сделок)

### 2. Размер выборки

- Минимум: 50-100 сделок
- Оптимум: 200-500 сделок
- Maximum: 30 дней истории (для избежания overfitting)

### 3. Мониторинг

Создайте алерты в Grafana:

```yaml
alerts:
  - name: Low AUC Warning
    condition: auc < 0.6
    duration: 1h
    action: telegram
    
  - name: Threshold Drift
    condition: abs(thr_current - thr_previous) > 0.2
    duration: 30m
    action: telegram
```

### 4. Backup

Регулярное резервное копирование:

```bash
# Backup датасетов
rsync -av /data/datasets_partitioned/ /backup/datasets/

# Backup Redis keys
redis-cli -h scanner-redis-worker-1 --rdb /backup/redis/dump_$(date +%Y%m%d).rdb
```

---

## 🎉 Готово!

Analytics v2.0 полностью интегрирован с вашей инфраструктурой! 🚀

**Что дальше?**

1. Настройте Grafana дашборды
2. Добавьте cron jobs для автоматизации
3. Настройте алерты для критических метрик
4. Экспериментируйте с ML моделями на экспортированных датасетах

**Дополнительные ресурсы:**

- [ANALYTICS_V2_README.md](./ANALYTICS_V2_README.md)
- [QUICKSTART_V2.md](./QUICKSTART_V2.md)
- [Signal Performance Tracker](./README_SIGNAL_TRACKER.md)

