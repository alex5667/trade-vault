# 🚀 Analytics v2.0 - Quickstart Guide

Быстрый старт для Analytics Package v2.0.

---

## ⚡ Установка

```bash
# 1. Установите зависимости
pip install pandas pyarrow matplotlib redis

# 2. Настройте переменные окружения
export REDIS_URL="redis://scanner-redis-worker-1:6379/0"
export DATASET_DIR="/data/datasets_partitioned"
export REPORT_IMG_DIR="/data/reports"
```

---

## 🎯 Топ-5 команд

### 1. Мульти-тюнинг порогов

Подбор оптимальных порогов для нескольких символов:

```bash
python -m analytics.multi_publish_best_threshold \
  --symbols XAUUSD,XAGUSD \
  --strategies aggregated,orderflow \
  --days 7 \
  --emit-telegram 1
```

**Результат:**
- ✅ Пороги в `hub:threshold:{strategy}:{symbol}`
- ✅ ROC точки в `analytics:roc:{strategy}:{symbol}`
- ✅ Telegram уведомления
- ✅ Aggregated hub перезагружен

---

### 2. Полный ночной прогон

Экспорт + тюнинг + отчёты за один запуск:

```bash
python -m analytics.nightly_pipeline \
  --symbols XAUUSD \
  --strategies aggregated,orderflow,ta \
  --days 7
```

**Выполняет:**
- 📦 Экспорт партиционированного датасета
- 🔧 Тюнинг порогов
- 📊 Сохранение ROC точек
- 📈 Публикация метрик для Grafana
- 📱 Telegram отчёты с графиками

---

### 3. Экспорт датасета для ML

```python
from analytics.repository import Repository, RepoConfig
from analytics.dataset_export import export_dataset_partitioned

repo = Repository(RepoConfig())
orders = list(repo.read_closed_trades(10000))
signals = list(repo.iter_signals(limit=10000))

path = export_dataset_partitioned(repo, orders, signals)
print(f"Датасет: {path}")
```

**Структура:**
```
/data/datasets_partitioned/
  symbol=XAUUSD/
    strategy=orderflow/
      year=2025/
        month=11/
          part-1730000000.parquet
```

---

### 4. Тюнинг порога для одной стратегии

```python
from analytics.repository import Repository, RepoConfig
from analytics.threshold_tuner import ThresholdTuner

repo = Repository(RepoConfig())
tuner = ThresholdTuner(repo)

# Получаем данные за 7 дней
import time
since = time.time() - 7*86400

orders = [o for o in repo.read_closed_trades(5000) 
          if o.symbol == "XAUUSD" and o.strategy == "aggregated"]
signals = list(repo.iter_signals(symbol="XAUUSD", strategy="aggregated", since_ts=since))

# Тюнинг
result = tuner.tune_and_publish(
    strategy="aggregated",
    symbol="XAUUSD",
    signals=signals,
    orders=orders,
    emit_telegram=True
)

print(f"Threshold: {result['thr']:.2f}")
print(f"AUC: {result['auc']:.3f}")
print(f"Youden J: {result['youdenJ']:.3f}")
```

---

### 5. Отправка ROC отчёта в Telegram

```python
from analytics.roc_store import ROCStore
from analytics.telegram_reporter_ext import TelegramReporterExt
import json

# Загрузка ROC данных
roc_store = ROCStore()
roc_data = roc_store.load("aggregated", "XAUUSD")

# Отправка отчёта
reporter = TelegramReporterExt()
reporter.send_roc_report(
    strategy="aggregated",
    symbol="XAUUSD",
    roc_points=roc_data["points"],
    auc=roc_data["auc"],
    summary={
        "thr": 0.55,
        "youdenJ": 0.42,
        "f1_at_thr": 0.78,
        "support": 150
    }
)
```

---

## 📅 Автоматизация (Cron)

### Ежедневный прогон в 2:00

```bash
# Добавьте в crontab
crontab -e
```

```cron
# Ночной аналитический прогон
0 2 * * * cd /home/alex/front/trade/scanner_infra/python-worker && \
  python -m analytics.nightly_pipeline \
  --symbols XAUUSD,XAGUSD \
  --strategies aggregated,orderflow \
  --days 7 \
  >> /var/log/nightly_analytics.log 2>&1

# Мульти-тюнинг порогов каждые 12 часов
0 */12 * * * cd /home/alex/front/trade/scanner_infra/python-worker && \
  python -m analytics.multi_publish_best_threshold \
  --symbols XAUUSD \
  --strategies aggregated \
  --days 3 \
  --emit-telegram 0 \
  >> /var/log/threshold_tuning.log 2>&1
```

---

## 🐳 Docker Integration

### Добавление в docker-compose.yml

```yaml
services:
  analytics-nightly:
    build:
      context: ./python-worker
      dockerfile: Dockerfile
    container_name: analytics-nightly
    environment:
      - REDIS_URL=redis://scanner-redis-worker-1:6379/0
      - DATASET_DIR=/data/datasets_partitioned
      - REPORT_IMG_DIR=/data/reports
    volumes:
      - ./data/datasets_partitioned:/data/datasets_partitioned
      - ./data/reports:/data/reports
    command: >
      bash -c "
        while true; do
          python -m analytics.nightly_pipeline \
            --symbols XAUUSD,XAGUSD \
            --strategies aggregated,orderflow \
            --days 7;
          sleep 86400;
        done
      "
    depends_on:
      - scanner-redis-worker-1
    networks:
      - scanner-network
```

---

## 📊 Мониторинг метрик

### Просмотр последних метрик

```bash
# Redis CLI
redis-cli -h scanner-redis-worker-1 GET "metrics:last:aggregated:XAUUSD"
```

```json
{
  "strategy": "aggregated",
  "symbol": "XAUUSD",
  "total_trades": 150,
  "winrate": 0.6,
  "avg_pnl_usd": 8.34,
  "auc": 0.72,
  "thr": 0.55,
  "ts": 1730000000.0
}
```

### Просмотр stream метрик

```bash
# Последние 10 событий
redis-cli -h scanner-redis-worker-1 XREVRANGE "metrics:strategy_perf" + - COUNT 10
```

---

## 🔍 Проверка ROC данных

### Python

```python
from analytics.roc_store import ROCStore

roc_store = ROCStore()
roc_data = roc_store.load("aggregated", "XAUUSD")

if roc_data:
    print(f"AUC: {roc_data['auc']:.3f}")
    print(f"Points: {roc_data['num_points']}")
    print(f"Updated: {roc_data['ts']}")
```

### Redis CLI

```bash
redis-cli -h scanner-redis-worker-1 GET "analytics:roc:aggregated:XAUUSD" | jq
```

---

## 🎨 Просмотр сгенерированных графиков

```bash
# Список последних графиков
ls -lht /data/reports/*.png | head -10

# Просмотр (если есть GUI)
eog /data/reports/roc_aggregated_XAUUSD_1730000000.png
```

---

## 🐛 Troubleshooting

### Проблема: "Недостаточно данных"

```bash
# Проверьте количество сделок
redis-cli -h scanner-redis-worker-1 XLEN "trades:closed"

# Проверьте сигналы
redis-cli -h scanner-redis-worker-1 KEYS "signals:*" | head -5
redis-cli -h scanner-redis-worker-1 XLEN "signals:XAUUSD:aggregated"
```

**Решение:** Уменьшите `--days` или дождитесь накопления данных.

---

### Проблема: "PyArrow недоступен"

```bash
pip install pyarrow

# Или используйте FastParquet
pip install fastparquet
```

---

### Проблема: "Matplotlib графики не генерируются"

```bash
pip install matplotlib

# Для headless серверов
export MPLBACKEND=Agg
```

---

### Проблема: "Telegram уведомления не приходят"

**Проверьте:**

1. notify-worker запущен?
```bash
docker ps | grep notify
```

2. Stream содержит сообщения?
```bash
redis-cli -h scanner-redis-worker-1 XLEN "notify:telegram"
```

3. Bot token корректен?
```bash
echo $TELEGRAM_BOT_TOKEN
```

---

## 📚 Дополнительные ресурсы

- [Полная документация](./ANALYTICS_V2_README.md)
- [Signal Performance Tracker](./README_SIGNAL_TRACKER.md)
- [Deployment Guide](./DEPLOYMENT.md)

---

**Analytics v2.0** - Готово к production! 🚀

