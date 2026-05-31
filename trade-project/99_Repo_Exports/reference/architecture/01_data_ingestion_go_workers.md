# Этап 1: Источники данных и Ingestion (Go Workers)

## Что это и зачем?
Go Workers — это первая "рука" системы, которая физически подключается к биржам (Binance) и получает поток рыночных данных. Если сравнивать с кухней ресторана — это официант, который принимает заказы от клиентов (биржи) и передает их поварам (Python).

Go был выбран для этой задачи не случайно:
- **Скорость**: Go-программы компилируются в нативный машинный код и обрабатывают сетевые соединения намного быстрее Python.
- **Горутины**: Тысячи "легких" потоков (goroutines) позволяют одновременно слушать 100+ торговых пар через WebSocket без блокировки.
- **Надежность**: Явная обработка ошибок (нет исключений — есть явные `err != nil`) делает код предсказуемым.

---

## Архитектура контейнеров
В `docker-compose-go-workers.yml` определен отдельный контейнер для КАЖДОГО таймфрейма:

```
scanner-go-worker-1m    (таймфрейм: kline_1m — самый нагруженный)
scanner-go-worker-5m    (таймфрейм: kline_5m)
scanner-go-worker-15m   (таймфрейм: kline_15m)
scanner-go-worker-1h    (таймфрейм: kline_1h)
... и так далее вплоть до 3month
```

**Почему не один контейнер на все?**
Потому что 1m-воркер генерирует огромный поток событий (каждую минуту по каждой паре), а 1w-воркер — раз в неделю. Смешивать их — значит "мешать шахматы с шашками" в одной очереди.

---

## Как Go Worker подключается к бирже?

### Шаг 1: WebSocket подписка
Воркер подключается к URLs вроде `wss://fstream.binance.com/stream?streams=btcusdt@trade/btcusdt@depth20@100ms...`.

Ключевые ENV параметры из `docker-compose-go-workers.yml`:
```yaml
- BINANCE_WS_TIMEFRAME=kline_1m       # На какой таймфрейм подписываться
- FUTURES_WS_ENABLED=true             # Торговать фьючерсами
- WS_HANDSHAKE_TIMEOUT=45s            # Тайм-аут рукопожатия
- WS_DIAL_TIMEOUT=30s                 # Тайм-аут подключения
- WS_TCP_KEEPALIVE=15s                # "Живы ли мы?" пинг каждые 15 секунд
- FUTURES_WS_PING_PERIOD=10s          # Как часто посылать пинги
- FUTURES_WS_READ_TIMEOUT=300s        # 5 минут без ответа = ошибка
```

**Что произойдет при обрыве WebSocket?**
Воркер использует экспоненциальный бэкофф:
- 1я попытка: ждем 1 секунду
- 2я попытка: ждем 2 секунды
- 3я попытка: ждем 4 секунды
- ...до максимума `REDIS_RETRY_MAX_BACKOFF=3s`

### Шаг 2: Список торговых пар
```yaml
- REQUIRED_SYMBOLS=BTCUSDT,ETHUSDT,SOLUSDT,BNBUSDT,XRPUSDT,1000PEPEUSDT,DOGEUSDT,1000SHIBUSDT,XAUUSDT
- BINANCE_MIN_TRADES_24H=1000    # Минимум 1000 сделок за сутки (фильтр неликвида)
- BINANCE_MAX_PAIRS_TO_FETCH=300 # Насколько широко смотреть на бирже
```
Сначала берутся `REQUIRED_SYMBOLS` (гарантированные пары). Затем дополняются топом с биржи. Пары с `trades_24h < 1000` отсеиваются — они слишком неликвидны и дадут ложные сигналы.

### Шаг 3: Запасной план — REST API
Если WebSocket умер и данные потеряны — Go Worker делает REST запрос:
```
GET https://fapi.binance.com/fapi/v1/klines?symbol=BTCUSDT&interval=1m&limit=500
```
Это называется **Backfill** (восстановление пропущенного). Таймауты: `REST_CANDLE_TIMEOUT=60s`.

---

## Куда идут данные? (Redis Streams)

После получения данных с биржи, Go Worker публикует их в **Redis Streams** — это высокопроизводительная очередь сообщений. Проще говоря, как почтовый ящик с подписями (каждое сообщение имеет ID).

```
Биржа --WebSocket--> Go Worker --XADD--> Redis Streams
```

Три вида потоков:

| Поток | Redis | Данные |
|-------|-------|--------|
| `stream:tick_BTCUSDT` | redis-ticks | Сырые сделки: цена, объем, сторона |
| `stream:book_BTCUSDT` | redis-ticks | Снимок стакана (Bid/Ask уровни) |
| `candles:data` | redis-worker-1/2 | Финальные и промежуточные свечи |

Пример данных тика в стриме:
```
XADD stream:tick_BTCUSDT * price 64000.5 qty 0.012 side B ts_ms 1700000000000
```
- `price`: Цена сделки
- `qty`: Объем в базовой монете
- `side`: B (Buyer/покупатель инициировал сделку) или S (Seller/продавец)
- `ts_ms`: Время биржи в миллисекундах

---

## Конфигурация Redis Pool (почему так много соединений?)

```yaml
- REDIS_POOL_SIZE=500        # Максимальных соединений в пуле
- REDIS_MIN_IDLE_CONNS=15    # Всегда держим 15 "тёплых" соединений
- REDIS_WRITE_TIMEOUT=30s
- REDIS_MAX_RETRIES=5
- REDIS_RETRY_MIN_BACKOFF=100ms
```

**Почему 500 соединений?** При прихождении нового тика для каждой из 100 торговых пар, Go Worker пишет в Redis одновременно из нескольких горутин. Если пул соединений маленький — горутины встают в очередь и теряется low-latency преимущество.

---

## Мониторинг и Healthcheck
Каждый Go Worker запускает встроенный HTTP-сервер Prometheus:
```yaml
- PROMETHEUS_PORT=2112
healthcheck:
  test: ["CMD", "wget", "--spider", "http://localhost:2112/metrics"]
  interval: 30s   # Проверка каждые 30 сек
  retries: 5      # После 5 неудач — контейнер перезапускается
  start_period: 60s  # Даем 60 сек на старт
```

На `/metrics` вы увидите метрики вроде:
```
go_worker_ticks_published_total{symbol="BTCUSDT"} 14256
go_worker_redis_write_latency_ms_p99 3.2
go_worker_ws_reconnects_total{symbol="BTCUSDT"} 2
```

---

## Ресурсные ограничения
```yaml
deploy:
  resources:
    limits:
      memory: 1.5G      # Потолок памяти
      cpus: '1.5'       # Максимум 1.5 CPU ядра
    reservations:
      memory: 384M      # Гарантированно зарезервировано
ulimits:
  nofile:
    soft: 65536         # Лимит открытых файлов/сокетов
    hard: 65536
```

**Зачем `nofile: 65536`?** В Linux каждый сокет (=WebSocket соединение) — это файловый дескриптор. Дефолтный лимит = 1024. Для 100+ торговых пар с двумя стримами каждая, нам нужно минимум 1024+ соединения. Иначе ядро Linux ответит `Too many open files` и Worker упадет.
