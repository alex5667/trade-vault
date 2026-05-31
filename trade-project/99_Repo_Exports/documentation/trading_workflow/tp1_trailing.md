# 🎯 TP1 Trailing System (2026-01-27)

> Полный цикл автоматического трейлинга после достижения TP1. Обновлено после интеграции MT5 Event Executor, Signal Performance Tracker и расширенного профилирования.

---

## 📋 Содержание

1. [Зачем нужен трейлинг после TP1](#зачем-нужен-трейлинг-после-tp1)
2. [Компоненты системы](#компоненты-системы)
3. [Поток событий](#поток-событий)
4. [Профили трейлинга](#профили-трейлинга)
5. [Метрики и мониторинг](#метрики-и-мониторинг)
6. [Тестирование](#тестирование)
7. [FAQ](#faq)

---

## ❓ Зачем нужен трейлинг после TP1

Проблема: после фиксации первой цели (TP1) цена часто откатывается и выбивает остаток позиции по исходному SL.  
Решение: автоматизировать перенос стопа на основе ATR/POINTS профилей → фиксировать прибыль и сохранять RR.

---

## 🧩 Компоненты системы

| Компонент                  | Файл/директория                                        | Назначение                                                            |
| -------------------------- | ------------------------------------------------------ | --------------------------------------------------------------------- |
| Trailing Profiles Registry | `python-worker/services/trailing_profiles.py`          | Каталог профилей (ATR/POINTS/STEP, `hard_min_lock`)                   |
| TP Event Listener          | `python-worker/services/tp_event_listener.py`          | Читает `events:trades`, запускает обработку TP1                       |
| TP1 Trailing Orchestrator  | `python-worker/services/tp1_trailing_orchestrator.py`  | Извлекает сигнал, выбирает профиль, строит команду                    |
| Order Trailing Dispatcher  | `python-worker/services/order_trailing_dispatcher.py`  | Отправляет HTTP команду в Go Gateway (`action=trail/modify`)          |
| **Order Management System**|                                                      | **Новая система маршрутизации и исполнения ордеров**                  |
| Orders Router              | `python-worker/services/orders_router.py`              | Маршрутизация ордеров, приоритизация, распределение нагрузки          |
| Orders HTTP Bridge         | `python-worker/services/orders_http_bridge.py`         | HTTP API для ордеров, REST интерфейс к системе исполнения             |
| MT5 Event Executor         | `python-worker/services/mt5_event_executor.py`         | Исполнение ордеров в MT5, обработка результатов                       |
| Signal Dispatcher          | `python-worker/services/signal_dispatcher*.py`         | Диспетчеризация сигналов, multi-target routing                        |
| Signal Target Deliverer    | `python-worker/services/signal_target_deliverer.py`    | Целевая доставка сигналов, selective routing                          |
| MT5 Event Executor (Go)    | `go-gateway/internal/handlers/events_handler.go`       | Принимает события от MT5, публикует в Redis                           |
| MT5TrailingMoveLogger      | `python-worker/services/mt5_trailing_move_logger.py`   | Логирует `TRAILING_MOVE`, вычисляет дистанцию от entry                |
| Trade Events Logger        | `python-worker/services/trade_events_logger.py`        | Записывает таймлайны, TTL                                             |
| TradeMonitor               | `python-worker/services/trade_monitor.py`              | Обновляет виртуальные позиции, фиксирует TP/SL и `trades:closed`      |
| Stats Aggregator           | `python-worker/services/stats_aggregator.py`           | Обновляет `stats:{strategy}:{symbol}:{tf}` и статистику по источникам |
| Trailing Metrics Exporter  | `python-worker/services/trailing_metrics.py`           | Prometheus метрики                                                    |
| Signal Performance Tracker | `python-worker/services/signal_performance_tracker.py` | Оркестратор аналитики: trailing, мониторинг, отчёты                   |
| Makefile.trailing          | `Makefile.trailing`                                    | Интеграционные тесты, вспомогательные команды                         |

---

## 🔄 Поток событий

```
1. MT5 достигает TP1 →
2. MT5 EA отправляет POST /events/mt5 (event_type=TP1_HIT) →
3. Go Gateway публикует запись в Redis stream events:trades →
4. TP Event Listener читает событие, обновляет trade:state →
5. Orchestrator достаёт signals:{sid}, проверяет trail_after_tp1 →
6. Orchestrator выбирает профиль, конвертирует ATR→points →
7. Dispatcher отправляет POST /orders/push (`action=trail`) →
8. MT5 `/orders/poll` получает команду, активирует trailing →
9. MT5 отправляет TRAILING_MOVE события →
10. Trade Events Logger записывает таймлайн →
11. Performance Tracker обновляет метрики (TP1→TP2, winrate).
```

### Временные ограничения

- Цель: < 2.5 секунд от `TP1_HIT` до `trail` команды.
- SLA контролируется метрикой `trailing_latency_ms`.

---

## 🎚️ Профили трейлинга

Хранятся в `config/trailing_config.json` и могут обновляться через API (`TrailingProfilesRegistry().add(...)`).

| Профиль          | Режим    | ATR × | Step pts | Hard lock | Описание                                           |
| ---------------- | -------- | ----- | -------- | --------- | -------------------------------------------------- |
| `rocket_v1`      | `ATR`    | 0.6   | –        | 0.0       | Агрессивное сопровождение тренда                   |
| `lock_and_trail` | `ATR`    | 0.8   | –        | 0.0       | Баланс защита/прибыль                              |
| `wide_swing`     | `ATR`    | 1.2   | –        | 0.0       | Для волатильных рынков                             |
| `crypto_tight`   | `ATR`    | 0.5   | –        | 0.0       | Для высокоскоростных инструментов                  |
| `points_200`     | `POINTS` | –     | –        | –         | Фиксированное значение (fallback)                  |
| `custom_step_*`  | `STEP`   | –     | varies   | varies    | Пользовательские профили со ступенчатым трейлингом |

> Профили `STEP` загружаются из Redis (`trailing:profiles`) и позволяют фиксировать минимальную прибыль (`hard_min_lock`) при каждом шаге.

Пример использования:

```python
from services.trailing_profiles import TrailingProfilesRegistry

registry = TrailingProfilesRegistry()
profile = registry.get("rocket_v1")
print(profile.mode, profile.atr_mult, profile.comment)
```

---

## 📊 Метрики и мониторинг

| Метрика                    | Источник                         | Описание                                               |
| -------------------------- | -------------------------------- | ------------------------------------------------------ |
| `trailing_started_total`   | `trailing_metrics.py`            | Кол-во запущенных трейлингов                           |
| `trailing_latency_ms`      | `trailing_metrics.py`            | Задержка (TP1 → trail команда)                         |
| `trailing_failures_total`  | `trailing_metrics.py`            | Ошибки (нет сигнала, MT5 offline)                      |
| `mt5_events_total{type}`   | Go Gateway                       | Частота событий (`TP1_HIT`, `TRAILING_MOVE`, `SL_HIT`) |
| `orders_queue_length`      | Go Gateway                       | Длина очереди trail-команд                             |
| `stats:*:trailing_started` | `StatsAggregator.update_stats`   | Кол-во сделок, где трейлинг был активирован            |
| `trade:timeline:{sid}`     | `TradeEventsLogger`, Move Logger | Подробная телеметрия SL/TP, используется аналитикой    |
| `trades:closed`            | `TradeMonitor`                   | Финальные итоги сделки (PnL, TP/SL, trailing_stop_hit) |

### Makefile

```bash
make trailing-status
make trailing-logs
make trailing-stats
make trailing-test
make trailing-profiles
make trailing-health
```

`make trailing-test` запускает интеграционный сценарий через `Makefile.trailing`.

### Grafana

- Панель **TP1 Trailing**: latency, количество трейлингов, успехи/ошибки.
- События `TRAILING_MOVE` отображаются рядом с ценой и ATR.

---

## 🧪 Тестирование

### Автоматические тесты

- `pytest tests/integration/test_tp1_trailing.py` — полный цикл, использует mock MT5.
- `pytest tests/unit/test_trailing_profiles.py` — валидация профилей.
- `pytest tests/unit/test_trailing_orchestrator.py` — бизнес-логика конвертации ATR.

### Ручной чек-лист

1. `make up-bg && make trailing-start`.
2. `python -m services.tp_event_emulator --event TP1_HIT --sid signal-demo`.
3. Убедиться в появлении записи `orders:queue` (`trail` команда).
4. Проверить логи gateway (`TRAIL command queued`).
5. Проверить Prometheus (`trailing_started_total` увеличился).
6. Подтвердить `TRAILING_MOVE` событие (эмулятор или реальный MT5).
7. `make trailing-stats` → `trailing_in_flight=0`.

---

## ❓ FAQ

- **Что если сигнал без `trail_after_tp1`?**  
  Трейлинг пропускается, событие логируется как `trailing_skipped_total`.

- **Как изменить профиль на лету?**  
  Добавьте запись в Redis (`profiles:trailing:{name}`) или обновите JSON, перезапустите `tp-event-listener`.

- **Можно ли использовать ATR вместо POINTS?**  
  Да, установите `mode="ATR"` в профиле. Orchestrator конвертирует в points перед отправкой (MT5 совместим с обоими режимами).

- **Как отлаживать без MT5?**  
  Используйте `tp_event_emulator` (эмулирует TP1/Trailing Move), mock-клиент `tests/integration/mt5_mock.py`.

- **Что делать при высоком latency?**  
  Проверить нагрузку Redis, задержку `events:trades` (XINFO), метрику `trailing_latency_ms`. При необходимости масштабировать `tp-event-listener`.

---

## ✅ Контроль качества

- Документ синхронизирован с `ARCHITECTURE.md` (2025-11-26) и `ADR-004-signal-tracker.md`.
- Последняя проверка команд, профилей и телеметрии: 2025-11-13. Обновлено: 2025-11-26.
- Ответственные: `@python-team`, `@trading-analytics`, `@go-team`.

Для дополнительной информации см. `docs/tp1-trailing/TP1_TRAILING_SYSTEM.md` и `Makefile.trailing`.
