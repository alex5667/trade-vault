# 🧾 Формирование и исполнение ордеров (2025-11-26)

> Обновлено после внедрения TP1 Trailing, уточнения очереди ордеров и добавления блока аналитики (TradeMonitor → Stats Aggregator → Reporting Service). Описывает путь сигнала от Redis до MT5 и обратно в аналитику.

---

## 📦 Содержание

1. [Сигналы и подготовка данных](#сигналы-и-подготовка-данных)
2. [Очередь ордеров](#очередь-ордеров)
3. [Go Gateway API](#go-gateway-api)
4. [MT5 исполнение](#mt5-исполнение)
5. [Мониторинг и диагностика](#мониторинг-и-диагностика)
6. [FAQ и best practices](#faq-и-best-practices)

---

## 📡 Сигналы и подготовка данных

### Filtered Signal Writer (`python-worker/core/filtered_signal_writer.py`)

- Получает исходные сигналы от Aggregated Hub V2.
- Применяет фильтры режима (`regime-worker`) и риска (`risk/`).
- Записывает результат в Redis:
  - `signals:{sid}` — полный payload (JSON).
  - `trade:state:{sid}` — статус (`ready`, `queued`, `executing`).
  - Публикует событие в `orders:queue`.

### Структура сигнала

```json
{
 "sid": "signal-XAUUSD-1731012450",
 "symbol": "XAUUSD",
 "direction": "LONG",
 "entry": 2765.5,
 "tp": [2770.0, 2774.5, 2781.0],
 "sl": 2758.0,
 "confidence": 0.87,
 "trail_after_tp1": true,
 "trail_profile": "rocket_v1",
 "atr": 2.6,
 "regime": "Momentum"
}
```

---

## 📥 Очередь ордеров

### Redis списки и ключи

| Ключ              | Тип  | Назначение                                       |
| ----------------- | ---- | ------------------------------------------------ |
| `orders:queue`    | List | Очередь команд для Go Gateway (`LPUSH` → `RPOP`) |
| `orders:inflight` | Hash | Команды, отправленные в MT5                      |
| `orders:history`  | List | Лог последних 500 команд                         |
| `trades:closed`   | List | Итоги сделок (формирует TradeMonitor)            |

**Приоритеты:**

- Приоритет определяется продюсерами: трейлинговые команды (`action=trail`) всегда поступают через `LPUSH`.
- Остальные команды (`action=open/modify/cancel`) также вставляются `LPUSH`; Gateway (`RPOP`) обрабатывает их в порядке поступления.
- Отдельная priority queue не используется — порядок обеспечивается дисциплиной продюсеров и idempotency.

**Логика вытягивания:**

- Go Gateway опрашивает `orders:queue` (`RPOP`), декодирует payload, валидирует `sid`.
- Каждая команда сопровождается `idempotency_key` и `timestamp` (TTL задаётся конфигом Go).
- Подтверждение (`/orders/ack`) переводит команду в `orders:history` и убирает из `orders:inflight`.

---

## 🌐 Go Gateway API

### Эндпоинты (все требуют `Authorization: Bearer <API_AUTH_TOKEN>`)

| Метод | Путь           | Описание                                          |
| ----- | -------------- | ------------------------------------------------- |
| POST  | `/orders/push` | Добавить команду (`action=open/modify/trail/...`) |
| POST  | `/orders/ack`  | Подтвердить исполнение (MT5/симулятор)            |
| GET   | `/orders/poll` | Получить следующую команду (MT5 Reader)           |
| POST  | `/events/mt5`  | События от MT5 (`TP1_HIT`, `TRAILING_MOVE`, `SL`) |
| GET   | `/health`      | Healthcheck                                       |

### Пример команды `trail`

```json
{
 "id": "trail-signal-XAUUSD-1731012450",
 "sid": "signal-XAUUSD-1731012450",
 "symbol": "XAUUSD",
 "action": "trail",
 "mode": "ATR",
 "atr_mult": 0.6,
 "trail_points": 15.6,
 "position_id": "1249987",
 "source": "tp1_trailing_orchestrator",
 "timestamp": 1731606000000,
 "metadata": {
  "profile": "rocket_v1",
  "atr": 2.6,
  "trail_request": true
 }
}
```

### Валидация

- `id`, `sid`, `action` обязательны; `action` нормализуется в `go-gateway/main.go`.
- Для `action=trail`/`modify` необходимо указать `mode` (`ATR`, `POINTS`, `STEP`) и соответствующие параметры (`atr_mult`, `trail_points`, `step_points`).
- `position_id` — связка с MT5 позицией (для `trail` и `modify` желательно).
- Idempotency: повторная отправка с тем же `id`/`idempotency_key` игнорируется.

---

## 🖥️ MT5 исполнение

### Компоненты

- **MT5 EA** (`mt5/MT5_TP_EVENTS_INTEGRATION_EXAMPLE.mq5`) — забирает команды, устанавливает SL/Trailing.
- **MT5 Event Executor (Go)** — принимает события от MT5, публикует в Redis.
- **Trade Events Logger** + **MT5TrailingMoveLogger** — записывают `trade:timeline`, `trade:events`.
- **TradeMonitor** (`python-worker/services/trade_monitor.py`) — обновляет виртуальные позиции, публикует `trades:closed`.

### Цикл подтверждения

1. MT5 EA вызывает `/orders/poll` → получает команду `trail`.
2. Выполняет трейлинг, отправляет `/orders/ack` (успех/ошибка).
3. Параллельно шлёт `/events/mt5` с `TRAILING_MOVE` (новый SL), MT5TrailingMoveLogger рассчитывает дистанцию от entry.
4. Gateway обновляет `orders:inflight`, публикует событие в Redis; TradeMonitor фиксирует состояние позиции.
5. Stats Aggregator обновляет `stats:{strategy}:{symbol}:{tf}`, Reporting Service и Signal Performance Tracker берут данные для отчётов.

---

## 📈 Мониторинг и диагностика

### Makefile

```bash
make gateway-status           # здоровье gateway
make gateway-logs             # tail логи
make orders-queue             # показать длину очередей
make trailing-stats           # статистика трейлинга
make trailing-logs            # события TP1 → trailing
```

### Prometheus

- `gateway_requests_total`, `gateway_request_duration_seconds`.
- `orders_queue_length`.
- `trailing_started_total`, `trailing_failures_total`.
- `mt5_events_total{event_type="TRAILING_MOVE"}`.
- `stats:{strategy}:{symbol}:{tf}.trailing_started` (через Stats Aggregator, см. `make tracker-stats`).

### Grafana панели

- **Order Flow Overview** — очередь, задержки, успехи.
- **TP1 Trailing** — latency от события TP1 до trail команд (`trailing_latency_ms`).
- **MT5 Integration** — успешность `/events/mt5` и `/orders/ack`.
- **Signal Tracker Analytics** — `stats:*`, `trades:closed`, winrate после трейлинга.

---

## ❓ FAQ и best practices

- **Как отменить команду?**  
  Используйте `orders:cancel` (см. `go-gateway/internal/orders/cancel.go`). Отправьте `/orders/push` с типом `cancel`.

- **Как гарантировать порядок?**  
  Используйте `priority` для критичных команд, не смешивайте разные `sid` в одном потоке без `idempotency_key`.

- **Что делать, если MT5 offline?**  
  Очередь растёт, `orders:inflight` пуст. Используйте `make gateway-logs`, проверьте состояние MT5. Дополнительно можно включить `fallback=paper_trading`.

- **Можно ли тестировать без MT5?**  
  Да. Используйте `python -m services.tp_event_emulator` и `tests/integration/test_order_trailing.py`. Также есть mock-клиент `mt5/mock_executor.py`.

---

## ✅ Контроль качества

- Документ обновлён 2025-11-26. Последняя проверка: 2025-11-13 (команды Go/Python/Trading/Analytics).
- Примеры payload соответствуют `go-gateway/main.go` (нормализация `action`).
- Мониторинг и Makefile команды верифицированы 2025-11-13 (`make orders-queue`, `make trailing-stats`).
