# 🚀 START HERE - Analytics v2.0

Добро пожаловать в Analytics Package v2.0!

---

## 🎯 Что это?

**Analytics v2.0** - полный аналитический пакет для Signal Performance Tracker:

✅ **Партиционированный экспорт** датасетов в Parquet  
✅ **Автоматический тюнинг порогов** с ROC/AUC анализом  
✅ **Telegram отчёты** с графиками  
✅ **Метрики для Grafana** в real-time  
✅ **CLI инструменты** для автоматизации  

---

## ⚡ Быстрый старт (5 минут)

### 1. Установите зависимости

```bash
pip install pandas pyarrow matplotlib redis
```

### 2. Запустите мульти-тюнинг

```bash
cd /home/alex/front/trade/scanner_infra/python-worker

python -m analytics.multi_publish_best_threshold \
  --symbols XAUUSD \
  --strategies aggregated \
  --days 7 \
  --emit-telegram 1
```

**Результат:**
- ✅ Пороги обновлены в Redis
- ✅ ROC кривые сохранены
- ✅ Telegram уведомления отправлены

### 3. Запустите полный прогон

```bash
python -m analytics.nightly_pipeline \
  --symbols XAUUSD \
  --strategies aggregated,orderflow \
  --days 7
```

**Результат:**
- ✅ Датасет экспортирован
- ✅ Пороги настроены
- ✅ Метрики опубликованы
- ✅ Отчёты отправлены

---

## 📚 Документация

### Основные файлы

| Файл | Описание |
|------|----------|
| [ANALYTICS_V2_README.md](./ANALYTICS_V2_README.md) | 📖 Полная документация API |
| [QUICKSTART_V2.md](./QUICKSTART_V2.md) | ⚡ Быстрый старт и примеры |
| [INTEGRATION_GUIDE.md](./INTEGRATION_GUIDE.md) | 🔗 Интеграция с инфраструктурой |
| [ANALYTICS_V2_COMPLETE.md](./ANALYTICS_V2_COMPLETE.md) | ✅ Итоговая сводка проекта |

### Читайте в следующем порядке

1. **00_START_HERE_V2.md** ← Вы здесь
2. **QUICKSTART_V2.md** - быстрые примеры
3. **ANALYTICS_V2_README.md** - полная документация
4. **INTEGRATION_GUIDE.md** - интеграция с системой

---

## 🏗️ Архитектура

```
Signals → TradeMonitor → Closed Trades → Analytics v2.0
                              ↓              ↓
                           Redis         ┌───┴────┐
                                         ↓        ↓
                                    Dataset   Metrics
                                    Export    Publisher
                                       ↓        ↓
                                    Parquet  Grafana
```

---

## 🔧 Основные модули

### 1. Dataset Export

**Что делает:** Экспорт сигналов и ордеров в партиционированные Parquet файлы

**Использование:**
```python
from analytics.dataset_export import export_dataset_partitioned
from analytics.repository import Repository, RepoConfig

repo = Repository(RepoConfig())
orders = list(repo.read_closed_trades(1000))
signals = list(repo.iter_signals(limit=1000))

path = export_dataset_partitioned(repo, orders, signals)
# → /data/datasets_partitioned/symbol=XAUUSD/strategy=orderflow/...
```

### 2. Threshold Tuner

**Что делает:** Автоматический подбор оптимального порога для фильтрации сигналов

**Использование:**
```python
from analytics.threshold_tuner import ThresholdTuner

tuner = ThresholdTuner(repo)
result = tuner.tune_and_publish(
    strategy="aggregated",
    symbol="XAUUSD",
    signals=signals,
    orders=orders
)
# → Порог обновлён в Redis, hub перезагружен
```

### 3. ROC Store

**Что делает:** Хранение ROC кривых и метрик качества

**Использование:**
```python
from analytics.roc_store import ROCStore

roc_store = ROCStore()
roc_data = roc_store.load("aggregated", "XAUUSD")
# → {"auc": 0.72, "points": [...], "ts": ...}
```

### 4. Metrics Publisher

**Что делает:** Публикация метрик для Grafana и мониторинга

**Использование:**
```python
from analytics.metrics_publisher import MetricsPublisher

publisher = MetricsPublisher()
publisher.publish(
    strategy="aggregated",
    symbol="XAUUSD",
    metrics={"winrate": 0.62, "auc": 0.72}
)
# → Данные в Grafana
```

### 5. Telegram Reporter Extended

**Что делает:** Расширенные отчёты с ROC графиками

**Использование:**
```python
from analytics.telegram_reporter_ext import TelegramReporterExt

reporter = TelegramReporterExt()
reporter.send_roc_report(
    strategy="aggregated",
    symbol="XAUUSD",
    roc_points=[...],
    auc=0.72,
    summary={...}
)
# → Отчёт с графиком в Telegram
```

---

## 🚀 CLI команды

### Мульти-тюнинг порогов

```bash
python -m analytics.multi_publish_best_threshold \
  --symbols XAUUSD,XAGUSD \
  --strategies aggregated,orderflow \
  --days 7 \
  --emit-telegram 1
```

### Полный ночной прогон

```bash
python -m analytics.nightly_pipeline \
  --symbols XAUUSD \
  --strategies aggregated,orderflow,ta \
  --days 7
```

**Опции:**
- `--skip-dataset` - пропустить экспорт датасета
- `--skip-telegram` - пропустить Telegram отчёты

---

## 🐳 Docker интеграция

Добавьте в `docker-compose.yml`:

```yaml
services:
  analytics-nightly:
    build: ./python-worker
    environment:
      - REDIS_URL=redis://scanner-redis-worker-1:6379/0
    command: >
      bash -c "
        while true; do
          python -m analytics.nightly_pipeline \
            --symbols XAUUSD \
            --strategies aggregated \
            --days 7;
          sleep 86400;
        done
      "
    volumes:
      - ./data/datasets_partitioned:/data/datasets_partitioned
      - ./data/reports:/data/reports
```

**Запуск:**
```bash
docker-compose up -d analytics-nightly
```

---

## 📅 Автоматизация (Cron)

```bash
# Добавьте в crontab
crontab -e
```

```cron
# Ночной прогон в 02:00
0 2 * * * cd /home/alex/front/trade/scanner_infra/python-worker && \
  python -m analytics.nightly_pipeline \
  --symbols XAUUSD --strategies aggregated --days 7

# Тюнинг порогов каждые 12 часов
0 */12 * * * cd /home/alex/front/trade/scanner_infra/python-worker && \
  python -m analytics.multi_publish_best_threshold \
  --symbols XAUUSD --strategies aggregated --days 3
```

---

## 📊 Grafana Dashboard

### 1. Установите Redis Data Source

```bash
grafana-cli plugins install redis-datasource
```

### 2. Создайте панель

**Query для Winrate:**
```
XREVRANGE metrics:strategy_perf + - COUNT 1000
```

**Фильтры:**
- strategy = `$strategy`
- symbol = `$symbol`

---

## 🎯 Типичные сценарии

### Сценарий 1: Ежедневный анализ

```bash
# Каждый день в 02:00
python -m analytics.nightly_pipeline \
  --symbols XAUUSD \
  --strategies aggregated,orderflow \
  --days 7
```

**Что происходит:**
1. Экспорт датасета за 7 дней
2. Тюнинг порогов для всех стратегий
3. Публикация метрик в Grafana
4. Telegram отчёт с ROC графиками

### Сценарий 2: On-demand тюнинг

```bash
# Быстрый тюнинг для одной стратегии
python -m analytics.multi_publish_best_threshold \
  --symbols XAUUSD \
  --strategies aggregated \
  --days 3 \
  --emit-telegram 1
```

### Сценарий 3: Экспорт для ML

```python
from analytics.dataset_export import export_dataset_partitioned
from analytics.repository import Repository, RepoConfig
import time

repo = Repository(RepoConfig())

# Получить все данные за 30 дней
since = time.time() - 30*86400
orders = list(repo.read_closed_trades(50000))
signals = list(repo.iter_signals(since_ts=since))

# Экспорт
path = export_dataset_partitioned(repo, orders, signals)
print(f"Dataset: {path}")

# Теперь можно использовать для ML моделей
import pandas as pd
df = pd.read_parquet(path)
```

---

## 🐛 Troubleshooting

### Проблема: "PyArrow недоступен"

```bash
pip install pyarrow
```

### Проблема: "Недостаточно данных"

**Решение:** Уменьшите `--days` или дождитесь накопления данных

```bash
# Проверьте количество сделок
redis-cli -h scanner-redis-worker-1 XLEN trades:closed

# Проверьте сигналы
redis-cli -h scanner-redis-worker-1 KEYS "signals:*"
```

### Проблема: "Telegram не отправляет"

**Проверьте:**
1. notify-worker запущен?
2. TELEGRAM_BOT_TOKEN установлен?
3. Stream `notify:telegram` содержит сообщения?

```bash
redis-cli -h scanner-redis-worker-1 XLEN notify:telegram
```

---

## ✅ Чек-лист первого запуска

- [ ] Установлены зависимости (`pip install pandas pyarrow matplotlib redis`)
- [ ] Redis доступен и содержит данные
- [ ] Переменные окружения настроены (`REDIS_URL`, etc)
- [ ] Запущен тестовый мульти-тюнинг
- [ ] Проверены результаты в Redis
- [ ] Настроен Telegram (опционально)
- [ ] Настроен Grafana (опционально)
- [ ] Добавлены cron jobs или Docker сервисы

---

## 🎓 Дополнительное обучение

### Рекомендуемый путь

1. **День 1:** Прочитайте QUICKSTART_V2.md и запустите примеры
2. **День 2:** Изучите ANALYTICS_V2_README.md, API референс
3. **День 3:** Настройте автоматизацию (Cron/Docker)
4. **День 4:** Настройте Grafana дашборды
5. **День 5:** Начните экспериментировать с ML на датасетах

---

## 📞 Поддержка

### Если что-то не работает

1. **Проверьте документацию:**
   - [ANALYTICS_V2_README.md](./ANALYTICS_V2_README.md)
   - [INTEGRATION_GUIDE.md](./INTEGRATION_GUIDE.md)

2. **Проверьте логи:**
   ```bash
   # Python скрипты
   python -m analytics.nightly_pipeline --symbols XAUUSD --strategies aggregated --days 1
   
   # Docker
   docker-compose logs -f analytics-nightly
   ```

3. **Проверьте Redis:**
   ```bash
   redis-cli -h scanner-redis-worker-1 INFO
   redis-cli -h scanner-redis-worker-1 KEYS "hub:threshold:*"
   ```

---

## 🎉 Начинайте!

**Вы готовы к работе с Analytics v2.0!**

### Следующий шаг:

```bash
# Запустите тестовый прогон
python -m analytics.nightly_pipeline \
  --symbols XAUUSD \
  --strategies aggregated \
  --days 3
```

**Ожидайте:**
- ✅ Экспорт датасета
- ✅ Настройка порогов
- ✅ Публикация метрик
- ✅ Telegram отчёт

---

**Удачи! 🚀**

**Analytics v2.0** - Production Ready!

