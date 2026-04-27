# 📊 Analytics Package v2.0

Полный аналитический пакет для Signal Performance Tracker с автоматизированными отчётами, тюнингом порогов и интеграцией с Grafana.

---

## 🎯 Основные возможности

### 1. **Партиционированный экспорт датасетов**
- Экспорт сигналов и ордеров в Parquet
- Партиционирование по `symbol/strategy/year/month`
- Поддержка PyArrow и FastParquet
- Готовность для ML-анализа

### 2. **Автоматический тюнинг порогов**
- ROC анализ с вычислением AUC
- Youden Index для оптимального порога
- F1-score оптимизация
- Публикация порогов в Redis
- Автоматическое обновление aggregated hub

### 3. **ROC кривые и метрики**
- Сохранение ROC точек в Redis
- Публикация в metrics:roc stream
- Визуализация в Grafana
- Исторические данные

### 4. **Telegram отчёты с графиками**
- "Карусель" сообщений с group_id
- PNG графики ROC кривых
- Confusion matrix визуализация
- Периодические сводки

### 5. **Метрики для Grafana**
- Публикация в metrics:strategy_perf stream
- Временные ряды для дашбордов
- Агрегация по стратегиям/символам
- Real-time мониторинг

### 6. **Автоматизированные пайплайны**
- Ночной прогон (nightly pipeline)
- Мульти-тюнинг для нескольких символов
- Cron-совместимость
- Batch обработка

---

## 📦 Структура модулей

```
analytics/
├── __init__.py                           # Экспорты модулей
├── repository.py                         # Доступ к данным в Redis
├── metrics.py                            # Вычисление метрик (WR, P/L, ROC)
│
├── dataset_export.py                     # ✨ Экспорт Parquet датасетов
├── roc_store.py                          # ✨ Хранение ROC кривых
├── threshold_tuner.py                    # ✨ Автоподбор порогов
├── metrics_publisher.py                  # ✨ Публикация метрик
├── telegram_reporter_ext.py              # ✨ Telegram отчёты с графиками
│
├── multi_publish_best_threshold.py       # 🚀 CLI: мульти-тюнинг
├── nightly_pipeline.py                   # 🚀 CLI: полный ночной прогон
│
└── ANALYTICS_V2_README.md                # Эта документация
```

✨ = Новые модули v2.0  
🚀 = CLI скрипты

---

## 🚀 Быстрый старт

### 1. Установка зависимостей

```bash
pip install pandas pyarrow matplotlib redis
```

### 2. Мульти-тюнинг порогов

Подбор оптимальных порогов для нескольких символов/стратегий:

```bash
python -m analytics.multi_publish_best_threshold \
  --symbols XAUUSD,XAGUSD \
  --strategies aggregated,orderflow \
  --days 7 \
  --emit-telegram 1
```

**Результат:**
- Пороги опубликованы в `hub:threshold:{strategy}:{symbol}`
- ROC точки сохранены в `analytics:roc:{strategy}:{symbol}`
- Telegram уведомления отправлены
- Aggregated hub перезагружен

### 3. Ночной прогон (Nightly Pipeline)

Полный аналитический прогон:

```bash
python -m analytics.nightly_pipeline \
  --symbols XAUUSD \
  --strategies aggregated,orderflow,ta \
  --days 7
```

**Выполняет:**
1. ✅ Экспорт партиционированного датасета
2. ✅ Тюнинг порогов для всех комбинаций
3. ✅ Сохранение ROC точек
4. ✅ Публикация метрик для Grafana
5. ✅ Telegram отчёты с ROC графиками

### 4. Запуск по расписанию (Cron)

Добавьте в crontab для ежедневного прогона в 2:00:

```cron
0 2 * * * cd /home/alex/front/trade/scanner_infra/python-worker && \
  python -m analytics.nightly_pipeline \
  --symbols XAUUSD,XAGUSD \
  --strategies aggregated,orderflow \
  --days 7 >> /var/log/nightly_analytics.log 2>&1
```

---

## 📊 Использование модулей

### Dataset Export

**Непартиционированный экспорт:**

```python
from analytics.repository import Repository, RepoConfig
from analytics.dataset_export import export_dataset

repo = Repository(RepoConfig())
orders = list(repo.read_closed_trades(1000))
signals = list(repo.iter_signals(limit=1000))

path = export_dataset(repo, orders, signals, out_name="my_dataset.parquet")
print(f"Датасет: {path}")
```

**Партиционированный экспорт:**

```python
from analytics.dataset_export import export_dataset_partitioned

base_dir = export_dataset_partitioned(
    repo, 
    orders, 
    signals,
    partition_cols=("symbol", "strategy", "year", "month"),
    base_dir="/data/datasets_partitioned"
)

print(f"Датасет: {base_dir}")
# Структура: /data/datasets_partitioned/symbol=XAUUSD/strategy=orderflow/year=2025/month=11/*.parquet
```

### Threshold Tuner

**Автоматический тюнинг порога:**

```python
from analytics.threshold_tuner import ThresholdTuner

tuner = ThresholdTuner(repo)

result = tuner.tune_and_publish(
    strategy="aggregated",
    symbol="XAUUSD",
    signals=signals,
    orders=orders,
    emit_telegram=True
)

if result:
    print(f"Порог: {result['thr']:.2f}")
    print(f"AUC: {result['auc']:.3f}")
    print(f"Youden J: {result['youdenJ']:.3f}")
    print(f"F1-score: {result['f1_at_thr']:.3f}")
```

**Получение текущего порога:**

```python
threshold_data = tuner.get_threshold("aggregated", "XAUUSD")

if threshold_data:
    print(f"Текущий порог: {threshold_data['thr']:.2f}")
```

### ROC Store

**Сохранение ROC точек:**

```python
from analytics.roc_store import ROCStore

roc_store = ROCStore()

points = [
    {"thr": 0.5, "tpr": 0.8, "fpr": 0.2, "prec": 0.75, "rec": 0.8, "f1": 0.77, "support": 100},
    {"thr": 0.7, "tpr": 0.6, "fpr": 0.1, "prec": 0.85, "rec": 0.6, "f1": 0.70, "support": 100},
    # ... больше точек
]

roc_store.save("aggregated", "XAUUSD", points, auc=0.85)
```

**Загрузка ROC данных:**

```python
roc_data = roc_store.load("aggregated", "XAUUSD")

if roc_data:
    print(f"AUC: {roc_data['auc']:.3f}")
    print(f"Точек: {roc_data['num_points']}")
    
    # Построение ROC кривой
    fprs = [p["fpr"] for p in roc_data["points"]]
    tprs = [p["tpr"] for p in roc_data["points"]]
```

### Metrics Publisher

**Публикация метрик:**

```python
from analytics.metrics_publisher import MetricsPublisher

publisher = MetricsPublisher()

publisher.publish(
    strategy="aggregated",
    symbol="XAUUSD",
    metrics={
        "total_trades": 150,
        "wins": 90,
        "losses": 60,
        "winrate": 0.60,
        "total_pnl": 1250.50,
        "avg_pnl_usd": 8.34,
        "auc": 0.72,
        "thr": 0.55
    }
)
```

**Получение последних метрик:**

```python
metrics = publisher.get_latest("aggregated", "XAUUSD")

if metrics:
    print(f"Winrate: {metrics['winrate']:.1%}")
    print(f"Avg P/L: ${metrics['avg_pnl_usd']:.2f}")
```

**Временной ряд для графиков:**

```python
timeseries = publisher.get_timeseries(
    count=100,
    strategy="aggregated",
    symbol="XAUUSD"
)

for entry in timeseries:
    print(f"{entry['ts']}: WR={entry['winrate']}, P/L={entry['avg_pnl']}")
```

### Telegram Reporter Extended

**ROC отчёт с графиком:**

```python
from analytics.telegram_reporter_ext import TelegramReporterExt

reporter = TelegramReporterExt()

reporter.send_roc_report(
    strategy="aggregated",
    symbol="XAUUSD",
    roc_points=roc_data["points"],
    auc=0.85,
    summary={
        "thr": 0.55,
        "youdenJ": 0.42,
        "f1_at_thr": 0.78,
        "support": 150
    }
)
```

**Confusion Matrix отчёт:**

```python
reporter.send_confusion_matrix_report(
    strategy="aggregated",
    symbol="XAUUSD",
    tp=75,
    fp=15,
    tn=45,
    fn=15,
    threshold=0.55
)
```

---

## 📁 Redis схема

### Keys

```
hub:threshold:{strategy}:{symbol}          # Пороги для фильтрации
analytics:roc:{strategy}:{symbol}          # ROC точки и AUC
metrics:last:{strategy}:{symbol}           # Последние метрики
```

### Streams

```
metrics:roc                                # События ROC (для мониторинга)
metrics:strategy_perf                      # Метрики стратегий (для Grafana)
aggregated_hub:control                     # Команды перезагрузки hub
notify:telegram                            # Уведомления в Telegram
```

---

## 🎨 Примеры графиков

### ROC Curve

При вызове `send_roc_report()` генерируется график:

- **X-axis:** False Positive Rate (FPR)
- **Y-axis:** True Positive Rate (TPR)
- **Diagonal:** Random classifier
- **AUC:** Area Under Curve

**Сохранение:**
- `/data/reports/roc_{strategy}_{symbol}_{ts}.png`

### Confusion Matrix

При вызове `send_confusion_matrix_report()` генерируется heatmap:

```
           Predicted Win  |  Predicted Loss
Actual Win      TP        |       FN
Actual Loss     FP        |       TN
```

**Метрики:**
- Precision, Recall, F1-score, Accuracy

---

## ⚙️ Конфигурация

### Переменные окружения

```bash
# Redis
export REDIS_URL="redis://scanner-redis-worker-1:6379/0"

# Датасеты
export DATASET_DIR="/data/datasets"                    # Непартиционированные
export DATASET_DIR="/data/datasets_partitioned"        # Партиционированные

# Графики
export REPORT_IMG_DIR="/data/reports"                  # PNG файлы

# Streams
export ROC_METRICS_STREAM="metrics:roc"
export STRATEGY_METRICS_STREAM="metrics:strategy_perf"
export AGG_HUB_CONTROL_STREAM="aggregated_hub:control"
export NOTIFY_STREAM="notify:telegram"
```

---

## 🧪 Тестирование

### Запуск тестов

```bash
# Тест экспорта датасета
python -c "
from analytics.repository import Repository, RepoConfig
from analytics.dataset_export import export_dataset_partitioned

repo = Repository(RepoConfig())
orders = list(repo.read_closed_trades(100))
signals = list(repo.iter_signals(limit=100))

path = export_dataset_partitioned(repo, orders, signals)
print(f'✅ Датасет: {path}')
"

# Тест тюнинга порога
python -m analytics.multi_publish_best_threshold \
  --symbols XAUUSD \
  --strategies aggregated \
  --days 3 \
  --emit-telegram 0
```

---

## 📈 Интеграция с Grafana

### 1. Redis Data Source

Установите Grafana Redis Data Source:

```bash
grafana-cli plugins install redis-datasource
```

### 2. Настройка панели

**Query для winrate:**

```
TS.RANGE metrics:strategy_perf:{symbol}:{strategy}:winrate - +
```

**Query для avg_pnl:**

```
TS.RANGE metrics:strategy_perf:{symbol}:{strategy}:avg_pnl - +
```

### 3. Переменные

Создайте переменные для фильтрации:

- `$strategy`: aggregated, orderflow, ta
- `$symbol`: XAUUSD, XAGUSD, BTCUSD

---

## 🔄 Интеграция с Signal Performance Tracker

Analytics v2.0 полностью совместим с Signal Performance Tracker:

### Workflow

```
Signal → TradeMonitor → StatsAggregator
                ↓
          Closed Trades
                ↓
        Analytics v2.0 ←→ Repository
                ↓
      ┌─────────┴─────────┐
      ↓                   ↓
 Threshold Tuner    Dataset Export
      ↓                   ↓
 ROC Analysis       Parquet Files
      ↓                   ↓
 Metrics Publisher  ML Analysis
      ↓
 Telegram Reports
      ↓
   Grafana
```

### Автоматическая интеграция

Threshold Tuner автоматически:
1. Публикует пороги в `hub:threshold:{strategy}:{symbol}`
2. Отправляет команду reload в aggregated hub
3. Обновляет фильтрацию сигналов

---

## 🎯 Лучшие практики

### 1. Частота обновления порогов

- **Daily:** Для высоковолатильных рынков
- **Weekly:** Для стабильных стратегий
- **After N trades:** При накоплении новых данных

### 2. Размер выборки

- Минимум: **50-100 сделок** для надёжной статистики
- Оптимум: **200-500 сделок**
- Больше не всегда лучше (старые данные могут быть нерелевантны)

### 3. Мониторинг

Отслеживайте:
- **AUC < 0.6:** Стратегия не работает
- **Youden J < 0.2:** Слабое разделение
- **F1-score < 0.5:** Низкое качество предсказаний

### 4. Telegram отчёты

- Включайте графики для визуального анализа
- Используйте `group_id` для группировки связанных сообщений
- Избегайте спама — настройте разумную частоту

---

## 🐛 Troubleshooting

### Ошибка: "PyArrow недоступен"

```bash
pip install pyarrow
```

### Ошибка: "Matplotlib недоступен"

```bash
pip install matplotlib
```

Графики будут пропущены, но текстовые отчёты работают.

### Ошибка: "Недостаточно данных"

- Уменьшите `--days`
- Проверьте наличие сделок: `redis-cli XLEN trades:closed`
- Проверьте сигналы: `redis-cli XLEN signals:{symbol}:{strategy}`

### Не отправляются Telegram уведомления

Проверьте notify-worker:
- Stream `notify:telegram` содержит сообщения?
- Bot token и chat_id корректны?
- notify-worker запущен?

---

## 📚 Дополнительная документация

- [Signal Performance Tracker](./README_SIGNAL_TRACKER.md)
- [Repository API](./REPOSITORY_API.md)
- [Metrics Documentation](./METRICS_GUIDE.md)
- [Deployment Guide](./DEPLOYMENT.md)

---

## 🎉 Что дальше?

### Потенциальные расширения

1. **Prometheus экспорт** - `/metrics` endpoint для Go-gateway
2. **SVG рендеры** - Лёгкие векторные графики
3. **Auto-AB сравнение** - Статистическое сравнение стратегий
4. **Bootstrap CI** - Доверительные интервалы для метрик
5. **Latency анализ** - Время до срабатывания TP/SL
6. **Drawdown tracking** - Отслеживание просадок

---

**Analytics Package v2.0** - Полная автоматизация аналитики торговых сигналов! 🚀

