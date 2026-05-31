# 🎉 Analytics Package v3.0 - New Features

Новые возможности Analytics Package v3.0 (Phase 3).

---

## 📦 Что нового?

### 1. ✨ SVG Рендеринг (без PIL/matplotlib)

**Модуль:** `svg_renderer.py`

**Что делает:**

- Генерация ROC кривых в SVG формате
- Генерация Confusion Matrix в SVG
- Без зависимостей от PIL или matplotlib
- Лёгкие векторные файлы (<50KB)

**Преимущества:**

- ✅ Нет heavy зависимостей
- ✅ Быстрая генерация
- ✅ Векторная графика (масштабируемость)
- ✅ Встраивание в HTML
- ✅ Отправка в Telegram

**Использование:**

```python
from analytics.svg_renderer import roc_svg, confusion_svg, save_svg
from analytics.roc_store import ROCStore
import json

# Загрузка ROC данных
roc_store = ROCStore()
roc_data = roc_store.load("aggregated", "XAUUSD")

if roc_data:
    # Генерация SVG
    svg = roc_svg(
        points=roc_data["points"],
        auc=roc_data["auc"],
        width=640,
        height=420,
        color="#2a7"
    )

    # Сохранение
    save_svg("/data/reports/roc_aggregated_XAUUSD.svg", svg)

    # Отправка в Telegram (через notify stream)
    # r.xadd("notify:telegram", {
    #     "file_path": "/data/reports/roc_aggregated_XAUUSD.svg",
    #     "caption": "ROC: aggregated/XAUUSD"
    # })
```

**Confusion Matrix:**

```python
from analytics.svg_renderer import confusion_svg, save_svg

# Генерация
svg = confusion_svg(
    tp=75,
    fp=15,
    tn=45,
    fn=15,
    width=420,
    height=320
)

save_svg("/data/reports/cm_aggregated_XAUUSD.svg", svg)
```

---

### 2. 🧪 A/B Сравнение стратегий

**Модуль:** `ab_compare.py`

**Что делает:**

- Статистическое сравнение стратегий
- Bootstrap доверительные интервалы (95%)
- Вероятность превосходства A над B
- Публикация в Redis
- Telegram отчёты

**Метрики:**

- Winrate с CI
- Average P/L с CI
- Median P/L
- Std P/L
- Sharpe-like ratio
- P(WR(A) > WR(B))
- P(P/L(A) > P/L(B))

**Использование:**

```bash
python -m analytics.ab_compare \
  --symbol XAUUSD \
  --strategies aggregated,orderflow,ta \
  --days 14 \
  --pairs aggregated:orderflow,aggregated:ta \
  --n-boot 2000
```

**Результат:**

```
🧪 A/B Сравнение  XAUUSD

aggregated
  • Trades: 150
  • Winrate: 62.0%
  • Avg P/L: $8.34
  • Median: $7.50
  • Std: $15.20
  • Sharpe: 0.55
  • WR CI(95%): [54.2%, 69.8%]
  • P/L CI(95%): [$6.10, $10.58]

orderflow
  • Trades: 142
  • Winrate: 58.5%
  • Avg P/L: $7.12
  • Median: $6.80
  • Std: $14.50
  • Sharpe: 0.49
  • WR CI(95%): [50.3%, 66.7%]
  • P/L CI(95%): [$5.02, $9.22]

📊 Попарные сравнения:

aggregated vs orderflow
  • P(WR(aggregated) > WR(orderflow)) = 65.2%
  • P(P/L(aggregated) > P/L(orderflow)) = 68.5%
```

**Redis схема:**

```
analytics:ab:last:{symbol} = JSON с результатами
metrics:ab stream = события A/B сравнения
```

**Python API:**

```python
from analytics.ab_compare import (
    load_orders,
    summarize_orders,
    bootstrap_ci,
    prob_A_beats_B,
    winrate,
    avg
)

# Загрузка данных
grouped = load_orders(repo, "XAUUSD", ["aggregated", "orderflow"], since, until)

# Вычисление метрик с CI
for strategy, orders in grouped.items():
    pnls = [(o.pnl_usd or 0.0) for o in orders]

    wr, wr_lo, wr_hi = bootstrap_ci(pnls, winrate, n_boot=2000)
    ap, ap_lo, ap_hi = bootstrap_ci(pnls, avg, n_boot=2000)

    print(f"{strategy}: WR={wr:.1%} CI=[{wr_lo:.1%}, {wr_hi:.1%}]")
    print(f"{strategy}: P/L=${ap:.2f} CI=[${ap_lo:.2f}, ${ap_hi:.2f}]")

# Вероятность превосходства
pnls_a = grouped["aggregated"]
pnls_b = grouped["orderflow"]

p_wr = prob_A_beats_B(pnls_a, pnls_b, winrate, 2000)
p_ap = prob_A_beats_B(pnls_a, pnls_b, avg, 2000)

print(f"P(WR(A) > WR(B)) = {p_wr:.1%}")
print(f"P(P/L(A) > P/L(B)) = {p_ap:.1%}")
```

---

### 3. 📊 Prometheus Metrics для Go Gateway

**Модуль:** `go-gateway/internal/metrics/metrics.go`

**Что делает:**

- Экспорт метрик для Prometheus
- Счётчики (signals, orders, deals)
- Гейджи (threshold, AUC, winrate, avg P/L)
- HTTP endpoint `/metrics`

**Метрики:**

| Метрика                   | Тип     | Описание                 |
| ------------------------- | ------- | ------------------------ |
| `gateway_up`              | Gauge   | Gateway состояние (1=up) |
| `signals_total`           | Counter | Всего сигналов           |
| `orders_enqueued_total`   | Counter | Ордеров в очереди        |
| `orders_pushed_total`     | Counter | Ордеров отправлено       |
| `deals_win_total`         | Counter | Прибыльных сделок        |
| `deals_loss_total`        | Counter | Убыточных сделок         |
| `strategy_last_threshold` | Gauge   | Текущий порог            |
| `strategy_avg_pnl_usd`    | Gauge   | Avg P/L                  |
| `strategy_winrate`        | Gauge   | Winrate (0..1)           |
| `strategy_last_auc`       | Gauge   | AUC                      |

**Использование:**

```go
import "yourproject/go-gateway/internal/metrics"

func main() {
    // Регистрация
    metrics.Register()
    metrics.Heartbeat()

    // HTTP endpoint
    r.Handle("/metrics", metrics.Handler()).Methods("GET")

    // В логике
    metrics.ObserveSignal(strategy, symbol)
    metrics.ObserveOrderEnqueued(strategy, symbol)
    metrics.ObserveOrderPushed(strategy, symbol)
    metrics.ObserveDeal(strategy, symbol, pnlUSD)

    // Синхронизация с Analytics v2.0
    metrics.SetThreshold(strategy, symbol, thr)
    metrics.SetAUC(strategy, symbol, auc)
    metrics.SetWinrate(strategy, symbol, wr)
    metrics.SetAvgPnl(strategy, symbol, avg)
}
```

**Prometheus scraping:**

```yaml
scrape_configs:
  - job_name: 'go-gateway'
    static_configs:
      - targets: ['go-gateway:8090']
```

**Grafana queries:**

```promql
# Signal rate
rate(signals_total[5m])

# Winrate
strategy_winrate{strategy="aggregated"}

# AUC качество
strategy_last_auc
```

---

## 🔗 Интеграция компонентов

### Workflow: SVG Генерация → Telegram

```python
from analytics.roc_store import ROCStore
from analytics.svg_renderer import roc_svg, save_svg
import redis
import os

# 1. Загрузка ROC данных
roc_store = ROCStore()
roc_data = roc_store.load("aggregated", "XAUUSD")

# 2. Генерация SVG
svg = roc_svg(roc_data["points"], roc_data["auc"])

# 3. Сохранение
out_path = "/data/reports/roc_aggregated_XAUUSD.svg"
save_svg(out_path, svg)

# 4. Отправка в Telegram
r = redis.from_url(os.getenv("REDIS_URL"), decode_responses=True)
r.xadd("notify:telegram", {
    "file_path": out_path,
    "caption": f"ROC: aggregated/XAUUSD (AUC={roc_data['auc']:.3f})",
    "parse_mode": "HTML"
}, maxlen=2000)
```

### Workflow: A/B Сравнение → Redis → Grafana

```bash
# 1. Запуск A/B сравнения
python -m analytics.ab_compare \
  --symbol XAUUSD \
  --strategies aggregated,orderflow \
  --days 7 \
  --pairs aggregated:orderflow

# 2. Результаты в Redis
redis-cli GET "analytics:ab:last:XAUUSD"

# 3. Prometheus читает из Redis (через exporter)
# 4. Grafana показывает метрики
```

### Workflow: Go Gateway → Prometheus → Grafana

```
Signals → Go Gateway → metrics.ObserveSignal()
                  ↓
             /metrics endpoint
                  ↓
             Prometheus scraping
                  ↓
             Grafana dashboard
```

---

## 📊 Примеры использования

### Пример 1: Генерация ROC SVG из CLI

```bash
python - <<'PY'
import os, json, time
import redis
from analytics.svg_renderer import roc_svg, save_svg

r = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"), decode_responses=True)
key = "analytics:roc:aggregated:XAUUSD"
d = json.loads(r.get(key) or "{}")
points = d.get("points", [])
auc = d.get("auc", 0.0)

svg = roc_svg(points, auc)
out = f"/data/reports/roc_aggregated_XAUUSD_{int(time.time())}.svg"
save_svg(out, svg)
print(f"✅ Saved: {out}")
PY
```

### Пример 2: A/B сравнение в Python

```python
from analytics.ab_compare import main as ab_main
import sys

# Установка аргументов
sys.argv = [
    "ab_compare.py",
    "--symbol", "XAUUSD",
    "--strategies", "aggregated,orderflow,ta",
    "--days", "14",
    "--pairs", "aggregated:orderflow,aggregated:ta",
    "--n-boot", "2000"
]

# Запуск
ab_main()
```

### Пример 3: Интеграция Prometheus в Go

См. `go-gateway/METRICS_INTEGRATION.md`

---

## 🎯 Roadmap v3.1 (опционально)

### Потенциальные расширения

1. **Interactive SVG**

   - Tooltips при наведении
   - Zoom/Pan
   - Clickable points

2. **Multi-strategy A/B**

   - Сравнение 3+ стратегий одновременно
   - ANOVA анализ
   - Post-hoc тесты

3. **Real-time A/B**

   - Непрерывное обновление метрик
   - Stream обработка
   - Live dashboard

4. **Python Prometheus Exporter**

   - `/metrics` endpoint в Python
   - Экспорт метрик из Analytics
   - Интеграция с Go gateway

5. **Advanced Bootstrap**
   - Stratified bootstrap
   - Bias correction
   - Percentile CI

---

## ✅ Чеклист внедрения

### SVG Рендеринг

- [ ] Проверьте `svg_renderer.py`
- [ ] Сгенерируйте тестовый ROC SVG
- [ ] Отправьте SVG в Telegram
- [ ] Проверьте размер файлов (<50KB)

### A/B Сравнение

- [ ] Запустите `ab_compare.py` для тестовых данных
- [ ] Проверьте результаты в Redis (`analytics:ab:last:*`)
- [ ] Получите Telegram уведомление
- [ ] Проверьте bootstrap CI (разумные интервалы?)

### Prometheus

- [ ] Добавьте `internal/metrics` в Go gateway
- [ ] Зарегистрируйте метрики в `main.go`
- [ ] Проверьте `/metrics` endpoint
- [ ] Настройте Prometheus scraping
- [ ] Создайте Grafana dashboard

---

**Analytics Package v3.0 - Phase 3 Complete!** 🎉

Вы получили:

- ✅ SVG рендеринг без тяжёлых зависимостей
- ✅ Статистическое A/B сравнение
- ✅ Prometheus интеграция для Go Gateway
- ✅ Полная документация и примеры

**Готово к production!** 🚀
