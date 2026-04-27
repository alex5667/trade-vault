# 📊 Go Gateway - Prometheus Metrics Integration

Интеграция Prometheus метрик для Go Gateway с Analytics v2.0.

---

## 🎯 Обзор

Модуль `internal/metrics` предоставляет:

✅ **Счётчики (Counters):**

- Сигналы, ордера, сделки

✅ **Гейджи (Gauges):**

- Текущий порог (threshold)
- AUC от ROC анализа
- Winrate
- Avg P/L

✅ **HTTP endpoint:**

- `/metrics` для Prometheus scraping

---

## 🚀 Быстрая интеграция

### 1. Добавьте модуль в main.go

```go
package main

import (
    "net/http"
    "github.com/gorilla/mux"
    "yourproject/go-gateway/internal/metrics"
)

func main() {
    // Регистрация метрик
    metrics.Register()
    metrics.Heartbeat()

    // Создание роутера
    r := mux.NewRouter()

    // Ваши существующие роуты
    r.HandleFunc("/api/signals", handleSignals).Methods("POST")
    r.HandleFunc("/api/orders", handleOrders).Methods("GET")

    // Prometheus endpoint
    r.Handle("/metrics", metrics.Handler()).Methods("GET")

    // Запуск сервера
    http.ListenAndServe(":8090", r)
}
```

### 2. Интеграция в логику обработки

**При получении сигнала:**

```go
func handleSignal(signal Signal) {
    // Ваша логика
    processSignal(signal)

    // Метрика
    metrics.ObserveSignal(signal.Strategy, signal.Symbol)
}
```

**При создании ордера:**

```go
func enqueueOrder(order Order) {
    // Ваша логика
    queue.Push(order)

    // Метрика
    metrics.ObserveOrderEnqueued(order.Strategy, order.Symbol)
}
```

**При отправке ордера в MT5:**

```go
func pushToMT5(order Order) error {
    // Ваша логика
    err := mt5Client.SendOrder(order)

    if err == nil {
        metrics.ObserveOrderPushed(order.Strategy, order.Symbol)
    }

    return err
}
```

**При закрытии сделки:**

```go
func handleClosedDeal(deal Deal) {
    // Ваша логика
    saveDeal(deal)

    // Метрика
    metrics.ObserveDeal(deal.Strategy, deal.Symbol, deal.PnlUSD)
}
```

---

## 📊 Обновление метрик из Analytics v2.0

### Вариант 1: Периодическое чтение из Redis

```go
package main

import (
    "time"
    "encoding/json"
    "github.com/go-redis/redis/v8"
    "yourproject/go-gateway/internal/metrics"
)

func syncMetricsFromAnalytics(rdb *redis.Client) {
    ticker := time.NewTicker(60 * time.Second)
    defer ticker.Stop()

    for range ticker.C {
        // Читаем все ключи metrics:last:*
        keys, err := rdb.Keys(ctx, "metrics:last:*").Result()
        if err != nil {
            continue
        }

        for _, key := range keys {
            data, err := rdb.Get(ctx, key).Result()
            if err != nil {
                continue
            }

            var m struct {
                Strategy string  `json:"strategy"`
                Symbol   string  `json:"symbol"`
                Winrate  float64 `json:"winrate"`
                AvgPnl   float64 `json:"avg_pnl_usd"`
                AUC      float64 `json:"auc"`
            }

            if err := json.Unmarshal([]byte(data), &m); err != nil {
                continue
            }

            // Обновляем Prometheus метрики
            metrics.SetWinrate(m.Strategy, m.Symbol, m.Winrate)
            metrics.SetAvgPnl(m.Strategy, m.Symbol, m.AvgPnl)
            metrics.SetAUC(m.Strategy, m.Symbol, m.AUC)
        }

        // Читаем пороги
        thresholdKeys, _ := rdb.Keys(ctx, "hub:threshold:*").Result()
        for _, key := range thresholdKeys {
            data, _ := rdb.Get(ctx, key).Result()

            var t struct {
                Threshold float64 `json:"thr"`
            }

            if err := json.Unmarshal([]byte(data), &t); err != nil {
                continue
            }

            // Извлекаем strategy/symbol из ключа
            // hub:threshold:aggregated:XAUUSD -> aggregated, XAUUSD
            parts := strings.Split(key, ":")
            if len(parts) >= 4 {
                strategy := parts[2]
                symbol := parts[3]
                metrics.SetThreshold(strategy, symbol, t.Threshold)
            }
        }
    }
}

func main() {
    metrics.Register()
    metrics.Heartbeat()

    // Redis клиент
    rdb := redis.NewClient(&redis.Options{
        Addr: "scanner-redis-worker-1:6379",
    })

    // Синхронизация метрик в фоне
    go syncMetricsFromAnalytics(rdb)

    // ... остальная логика
}
```

### Вариант 2: Подписка на Redis Stream

```go
func watchMetricsStream(rdb *redis.Client) {
    for {
        streams, err := rdb.XRead(ctx, &redis.XReadArgs{
            Streams: []string{"metrics:strategy_perf", "$"},
            Count:   10,
            Block:   1 * time.Second,
        }).Result()

        if err != nil {
            continue
        }

        for _, stream := range streams {
            for _, msg := range stream.Messages {
                strategy := msg.Values["strategy"].(string)
                symbol := msg.Values["symbol"].(string)
                winrate, _ := strconv.ParseFloat(msg.Values["winrate"].(string), 64)
                avgPnl, _ := strconv.ParseFloat(msg.Values["avg_pnl"].(string), 64)
                auc, _ := strconv.ParseFloat(msg.Values["auc"].(string), 64)

                metrics.SetWinrate(strategy, symbol, winrate)
                metrics.SetAvgPnl(strategy, symbol, avgPnl)
                metrics.SetAUC(strategy, symbol, auc)
            }
        }
    }
}
```

---

## 📈 Prometheus Configuration

### prometheus.yml

```yaml
global:
  scrape_interval: 15s
  evaluation_interval: 15s

scrape_configs:
  - job_name: 'go-gateway'
    static_configs:
      - targets: ['go-gateway:8090']
        labels:
          service: 'gateway'

  - job_name: 'analytics'
    static_configs:
      - targets: ['python-worker:9090'] # если добавите Python /metrics
        labels:
          service: 'analytics'
```

---

## 📊 Grafana Dashboard

### Панели

**1. Signal Rate**

```promql
rate(signals_total[5m])
```

**2. Order Success Rate**

```promql
rate(orders_pushed_total[5m]) / rate(orders_enqueued_total[5m])
```

**3. Winrate by Strategy**

```promql
strategy_winrate{strategy="aggregated"}
```

**4. Average P/L Trend**

```promql
strategy_avg_pnl_usd
```

**5. AUC Quality**

```promql
strategy_last_auc
```

**6. Deal Win/Loss Ratio**

```promql
rate(deals_win_total[1h]) / (rate(deals_win_total[1h]) + rate(deals_loss_total[1h]))
```

### Alerts

```yaml
groups:
  - name: trading_alerts
    rules:
      - alert: LowWinrate
        expr: strategy_winrate < 0.5
        for: 1h
        labels:
          severity: warning
        annotations:
          summary: 'Low winrate for {{ $labels.strategy }}/{{ $labels.symbol }}'

      - alert: LowAUC
        expr: strategy_last_auc < 0.6
        for: 30m
        labels:
          severity: warning
        annotations:
          summary: 'Low AUC for {{ $labels.strategy }}/{{ $labels.symbol }}'
```

---

## 🐳 Docker Compose

```yaml
services:
  go-gateway:
    build: ./go-gateway
    ports:
      - '8090:8090'
    environment:
      - REDIS_URL=redis://scanner-redis-worker-1:6379
    networks:
      - scanner-network

  prometheus:
    image: prom/prometheus:latest
    ports:
      - '9090:9090'
    volumes:
      - ./prometheus.yml:/etc/prometheus/prometheus.yml
      - prometheus_data:/prometheus
    command:
      - '--config.file=/etc/prometheus/prometheus.yml'
      - '--storage.tsdb.path=/prometheus'
    networks:
      - scanner-network

  grafana:
    image: grafana/grafana:latest
    ports:
      - '3000:3000'
    environment:
      - GF_SECURITY_ADMIN_PASSWORD=admin
    volumes:
      - grafana_data:/var/lib/grafana
    networks:
      - scanner-network

volumes:
  prometheus_data:
  grafana_data:

networks:
  scanner-network:
    external: true
```

---

## ✅ Проверка

### 1. Проверьте endpoint

```bash
curl http://localhost:8090/metrics
```

**Вывод:**

```
# HELP gateway_up 1 if gateway is up, 0 otherwise
# TYPE gateway_up gauge
gateway_up{service="go-gateway"} 1

# HELP signals_total Total number of signals received
# TYPE signals_total counter
signals_total{strategy="aggregated",symbol="XAUUSD"} 1523

# HELP strategy_winrate Winrate 0..1 (last window) from Analytics v2.0
# TYPE strategy_winrate gauge
strategy_winrate{strategy="aggregated",symbol="XAUUSD"} 0.62

# HELP strategy_last_auc AUC from ROC tuner (Analytics v2.0)
# TYPE strategy_last_auc gauge
strategy_last_auc{strategy="aggregated",symbol="XAUUSD"} 0.72
```

### 2. Проверьте Prometheus

```bash
# Targets
open http://localhost:9090/targets

# Query
open http://localhost:9090/graph?g0.expr=strategy_winrate
```

### 3. Проверьте Grafana

```bash
# Dashboard
open http://localhost:3000

# Login: admin / admin
# Data Source: Prometheus (http://prometheus:9090)
# Create Dashboard with panels
```

---

## 🎯 Best Practices

1. **Labels:** Используйте `strategy` и `symbol` для всех метрик
2. **Naming:** Следуйте Prometheus naming conventions
3. **Cardinality:** Ограничивайте количество уникальных label combinations
4. **Sync Frequency:** Синхронизация с Redis каждые 60 секунд оптимальна
5. **Error Handling:** Не падайте если Redis недоступен

---

**Prometheus интеграция готова!** 🚀

Теперь у вас есть полный мониторинг:

- Real-time метрики из Go Gateway
- Аналитические метрики из Analytics v2.0
- Визуализация в Grafana
- Алерты в Prometheus
