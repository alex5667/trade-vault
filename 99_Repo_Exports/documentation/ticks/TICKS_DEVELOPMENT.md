# 🔧 Tick Ingestion Development Guide (2025-11-26)

> Практическое руководство по разработке и поддержке ingestion-слоя. Обновлено для dedicated `redis-ticks`, Signal Performance Tracker и статистики по источникам.

---

## 🚀 Быстрый старт

```bash
make up-bg                       # поднять базовую инфраструктуру
make ticks-status                # проверить redis-ticks и ingest
make ticks-streams               # убедиться, что stream:tick_* наполняются
make ticks-groups                # посмотреть consumer-группы и лаги
```

### Локальная настройка

1. Скопируйте `config/services/go-worker.env` → `.env.go-worker.local`.
2. Заполните список символов (`SYMBOLS=XAUUSD,BTCUSDT`).
3. Проверьте `REDIS_TICKS_URL` / `REDIS_TICKS_HOST` — по умолчанию `redis://redis-ticks:6379/0`.
4. Для MT5 настройте `TickBridge.mq5` (см. `mt5/README.md`), укажите `TickEndpoint`.
5. Используйте `docker-compose-optimized.yml` для высоконагруженных тестов.

---

## 🛠️ Разработка Go Workers

### Структура

```
go-worker/
  ├── cmd/worker/main.go
  ├── internal/binance/
  ├── internal/redis/
  └── internal/metrics/
```

### Основные приёмы

- Реализация новых подписок → `internal/binance/stream_manager.go`.
- Конвертация payload → `internal/binance/normalizer.go`.
- Публикация в Redis → `internal/redis/publisher.go`.
- Метрики → `internal/metrics/prometheus.go`.
- Запись в `redis-ticks` → `core/ticks_redis_client.DualTicksRedisClient` (primary `redis-ticks`, fallback основной Redis).

### Тестирование

```bash
go test ./internal/... -race
go run cmd/worker/main.go --config ../config/services/go-worker.env
```

Используйте `WS_DEBUG=true` для verbose логов.

---

## 🗄️ Работа с redis-ticks

- **Назначение**: высокочастотные тики/DOM хранятся в отдельном инстансе `redis-ticks`. Основной Redis используется для сигналов, конфигураций и отчётности.
- **Подключение (Python)**:

```python
from core.ticks_redis_client import get_ticks_redis, get_ticks_dual_redis

ticks = get_ticks_redis()              # чтение (decode_responses=True)
dual = get_ticks_dual_redis()          # запись с fallback на основной Redis
dual.xadd("stream:tick_XAUUSD", fields, maxlen=("~", 20000))
```

- **CLI/Make**:
  - `make ticks-status` — здоровье контейнера и порты.
  - `make ticks-streams` — список и длины всех `stream:tick_*`/`stream:book_*`.
  - `make ticks-groups` / `make ticks-consumers` — контроль consumer-групп.
  - `make ticks-test` — smoke-тест (PING/XADD/XREVRANGE).
  - `make ticks-trim` — обрезка стримов (MAXLEN ~ 10000).

> ⚙️ По умолчанию `ticks-*` команды подключаются к `redis-worker-1`. Для альтернативного инстанса задайте `TICKS_REDIS_CONTAINER=scanner-redis-ticks` (или другое имя контейнера).

---

## 🐍 Разработка Python ingest

### Компоненты

- `services/tick_ingest_server.py` — FastAPI (`/tick`, `/book`), DualRedis, MAXLEN trimming.
- `services/book_analytics_service.py` — OBI/DOM аналитика, PNG рендеры, уведомления.
- `services/ohlc_aggregator.py` — строит OHLC ряды.
- `services/tick_latency_monitor.py` — следит за задержками (опционально).
- `core/ticks_redis_client.py` — клиент для `redis-ticks` с fallback.
- `services/signal_performance_tracker.py` — потребитель сигналов/тиков, обновляет статистику.
- `services/stats_aggregator.py` — расчёт статистики по стратегиям/источникам.

### Локальный запуск

```bash
export PYTHONPATH=$PWD/python-worker
export REDIS_TICKS_URL=redis://localhost:6380/0   # при использовании локального инстанса
uvicorn services.tick_ingest_server:app --port 8087 --reload
uvicorn services.book_analytics_service:app --port 8090 --reload
python -m services.signal_performance_tracker     # оркестратор сигналов/статистики
```

**Swagger/OpenAPI**: доступен на `/docs` у `tick_ingest_server` (FastAPI).

### Тестирование

```bash
pytest tests/integration/test_tick_ingest.py
pytest tests/unit/test_tick_normalizer.py
```

---

## 📦 Тестовые данные

- `data/ticks/sample_binance.json` — выборка тиков Binance.
- `data/ticks/sample_mt5.json` — лог MT5.
- `python-worker/tests/fixtures/ticks.py` — генераторы данных.

Запуск эмулятора:

```bash
python -m services.tick_emulator --source binance --symbol XAUUSD --rate 10/s
```

---

## 📊 Мониторинг

| Команда/инструмент     | Назначение                                                                   |
| ---------------------- | ---------------------------------------------------------------------------- |
| `make ticks-status`    | Проверка контейнера redis-ticks и tick_ingest_server                         |
| `make ticks-streams`   | Длины `stream:tick_*`, `stream:book_*`, MAXLEN (по умолчанию redis-worker-1) |
| `make ticks-groups`    | Просмотр consumer групп (`XINFO GROUPS`)                                     |
| `make ticks-consumers` | Активные консьюмеры, idle/pending                                            |
| `make tracker-stats`   | Статистика Signal Performance Tracker по источникам                          |
| Grafana: WebSocket     | Латентность, reconnects                                                      |
| Grafana: Multi Symbol  | Совмещение тиков, трейлинга и сигналов                                       |
| Prometheus `/metrics`  | `ws_messages_total`, `tick_ingest_latency_ms`, `tick_gap_seconds`            |

Рекомендуемые алерты:

- `tick_gap_seconds > 5` (потеря тиков).
- `ws_reconnects_total > 10` в течение часа.
- `mt5_http_failures_total > 0` (проблемы с MT5).

---

## 🧪 QA Checklist

1. Прогон `pytest tests/integration/test_tick_ingest.py`.
2. `go test ./internal/... -race`.
3. Проверка метрик: `make ticks-status`, `make ticks-streams`, Prometheus/Grafana dashboards.
4. `make ticks-test` — записать/прочитать тестовый тик и очистить `test:tick_stream`.
5. `make tracker-stats` — убедиться, что обновились `stats:{strategy}:{symbol}:{tf}` и `stats:*:{source}`.
6. Верификация Redis ключей (`redis-cli --scan --pattern 'stream:tick_*'`, `redis-cli --scan --pattern 'stats:*:{source}'`).
7. Smoke-тест сигнала: убедиться, что Aggregated Hub получает свежий тик и пишет в `signals:*`.
8. Обновление документации (`TICKS_ARCHITECTURE.md`) при изменении схемы.

---

## ❓ FAQ

- **Как добавить новый источник данных?**  
  Создайте адаптер в `adapters/<source>` и подключите его в `go-worker` (см. пример Binance).

- **Как воспроизвести историю тиков?**  
  Используйте `analysis/replay_ticks.py` с параметрами `--source binance --file data/ticks/sample_binance.json`.

- **Можно ли отправлять искусственные тики?**  
  Да, через `python -m services.tick_emulator`. Он пишет напрямую в Redis, либо шлёт HTTP на `tick_ingest_server`.

- **Что делать при задержке > 5 секунд?**  
  Проверить `ws_reconnects_total`, сеть, нагрузку Redis. При необходимости переключиться на `docker-compose-optimized.yml`.

---

## ✅ Контроль качества

- Документ обновлён 2025-11-26. Последняя проверка: 2025-11-13 (`tick_ingest_server`, `aggregated_signal_hub_v2`, `signal_performance_tracker`).
- Проверено: `make ticks-status`, `make ticks-streams`, `make ticks-test`, `make tracker-stats`.
- Ответственные: `@market-data-team`, `@python-team`.
