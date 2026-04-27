# 📊 Signal Analytics & Trade Lifecycle (2026-01-27)

> Полная документация по аналитике сигналов, отслеживанию сделок, трейлинг стопов, расчету прибыли/убытков и формированию отчетов в Telegram.  
> Команда: Senior Trading Analyst + Senior Python Developer.

---

## 🗂️ Структура раздела

| Документ                                                   | Описание                                                        |
| ---------------------------------------------------------- | --------------------------------------------------------------- |
| **[signal_lifecycle.md](signal_lifecycle.md)**             | Полный цикл от формирования сигнала до отчета в Telegram        |
| **[trailing_stop_tracking.md](trailing_stop_tracking.md)** | Отслеживание трейлинг стопов, профили, метрики                  |
| **[pnl_analysis.md](pnl_analysis.md)**                     | Расчет прибыли/убытков, виртуальные позиции, статистика         |
| **[reporting.md](reporting.md)**                           | Формирование отчетов, отправка в Telegram, периодические сводки |
| **[sl_quantile_analysis.md](sl_quantile_analysis.md)**     | Анализ стоп-лоссов с использованием квантильного подхода         |

---

## 🔀 Рекомендуемый маршрут чтения

1. **`signal_lifecycle.md`** — понять полный цикл от сигнала до отчета
2. **`pnl_analysis.md`** — разобраться в расчете прибыли/убытков
3. **`trailing_stop_tracking.md`** — изучить механизм трейлинг стопов
4. **`reporting.md`** — понять, как формируются и отправляются отчеты

---

## 🎯 Обзор системы аналитики

Система аналитики сигналов состоит из следующих компонентов:

### 🔬 Специализированная аналитика

Система включает специализированные модули для глубокого анализа различных аспектов торговой деятельности:

- **Stop-Loss Analytics**: Анализ эффективности стоп-лоссов с использованием квантильного подхода
- **Trailing Stop Analytics**: Метрики и анализ эффективности трейлинг стоп стратегий
- **Execution Analytics**: Анализ стоимости исполнения, проскальзывания и рыночного воздействия
- **Expected Value Analytics**: Расчет и мониторинг ожидаемой доходности по различным сценариям
- **Risk-Adjusted Analytics**: Метрики с учетом риска и коррекцией волатильности

### Основные сервисы

| Сервис                         | Файл/директория                                        | Назначение                                                  |
| ------------------------------ | ------------------------------------------------------ | ----------------------------------------------------------- |
| **Signal Performance Tracker** | `python-worker/services/signal_performance_tracker.py` | Главный оркестратор: координация всех компонентов аналитики, многопоточная обработка |
| **Trade Monitor**              | `python-worker/services/trade_monitor.py`              | Отслеживание виртуальных позиций, обработка TP/SL событий, частичное закрытие позиций, thread-safe операции и атомарные обновления состояния |
| **P&L Math Module**            | `python-worker/services/pnl_math.py`                   | Корректный расчет P&L с учетом спецификаций символов (тиковая/линейная модель), устранение хардкода |
| **Stats Aggregator**           | `python-worker/services/stats_aggregator.py`           | Агрегация статистики по стратегиям, символам, таймфреймам с атомарными обновлениями и empirical levels buffers |
| **Reporting Service**          | `python-worker/services/reporting_service.py`          | Формирование HTML-отчетов с графиками, отправка в Telegram через notify:telegram stream |
| **Periodic Reporter**          | `python-worker/services/periodic_reporter.py`          | Автоматические отчеты каждые N сделок (по умолчанию 100) с гибкой конфигурацией |
| **TP1 Trailing Orchestrator**  | `python-worker/services/tp1_trailing_orchestrator.py`  | Оркестрация трейлинг стопов после достижения TP1 с поддержкой множественных профилей |
| **Trade Events Logger**        | `python-worker/services/trade_events_logger.py`        | Логирование событий сделок в Redis с метаданными, TTL 7 дней и поддержкой сжатия |
| **Experiment Manager**         | `python-worker/handlers/experiment_manager.py`         | Детерминированное назначение вариантов для A/B-тестирования фильтров сигналов |
| **Experiment Metrics**         | `python-worker/handlers/experiment_metrics.py`         | Расчет метрик качества экспериментов (precision, recall, expectancy, Sharpe ratio) |

### Специализированные сервисы аналитики

| Сервис                         | Файл/директория                                        | Назначение                                                  |
| ------------------------------ | ------------------------------------------------------ | ----------------------------------------------------------- |
| **SL Quantile Aggregator**     | `python-worker/services/sl_quantile_aggregator.py`     | Анализ квантилей стоп-лоссов, оптимизация уровней SL        |
| **SLQ Risk Adjust**            | `python-worker/services/slq_risk_adjust.py`            | Корректировка рисков на основе SL-аналитики                 |
| **SLQ Store**                  | `python-worker/services/slq_store.py`                  | Хранение и кэширование данных SL-квантилей                  |
| **Trailing Metrics**           | `python-worker/services/trailing_metrics.py`           | Метрики эффективности трейлинг стопов                       |
| **Trailing Edge Analyzer**     | `python-worker/services/trailing_edge_analyzer.py`     | Анализ эффективности трейлинг стратегий                     |
| **EV Giveback Stats**          | `python-worker/services/ev_giveback_stats.py`          | Статистика отдачи ожидаемой доходности (EV giveback)       |
| **EV TP1 Stats**               | `python-worker/services/ev_tp1_stats.py`               | Статистика EV для TP1 уровней                               |
| **Execution Cost EMA**         | `python-worker/services/execution_cost_ema.py`         | Экспоненциальное сглаживание стоимости исполнения           |
| **Execution Slippage Stats**   | `python-worker/services/execution_slippage_stats.py`   | Статистика проскальзывания при исполнении                   |
| **Slippage Model**             | `python-worker/services/slippage_model*.py`            | Моделирование и предсказание проскальзывания                |
| **Trade Metrics Service**      | `python-worker/services/trade_metrics_service.py`      | Специализированные метрики по торговым операциям           |
| **Analytics API Service**      | `python-worker/services/analytics_api_service.py`      | REST API для доступа к аналитическим данным                 |
| **Analytics DB**               | `python-worker/services/analytics_db.py`               | Интеграция с PostgreSQL для хранения аналитики              |

### Поток данных

````
1. Сигнал публикуется
   ├─► signals:orderflow:<symbol>
   ├─► signals:audit:<symbol>
   └─► notify:telegram (type=signal)

2. Signal Performance Tracker
   ├─► Читает сигналы из streams
   ├─► Создает виртуальную позицию в TradeMonitor
   └─► Начинает отслеживание тиков

3. Отслеживание позиции
   ├─► Обновление P&L по тикам
   │   ├─► **Signal Performance Tracker** (`signal_performance_tracker.py::_ticks_listener_thread()`)
   │   │   - Читает тики из Redis Streams `stream:tick_{symbol}`
   │   │   - Передает тики в Trade Monitor для обработки
   │   └─► **Trade Monitor** (`trade_monitor.py::process_tick()`)
   │       - Получает тики от Signal Performance Tracker
   │       - Обновляет unrealized P&L для каждой открытой позиции
   │       - Вызывает проверку TP/SL при каждом тике
   ├─► Обработка TP1_HIT, TP2_HIT, TP3_HIT
   │   ├─► **Trade Monitor** (`trade_monitor.py::_handle_take_profit()`)
   │   │   - Обнаруживает достижение TP уровней при обработке тиков
   │   │   - Выполняет частичное закрытие позиции (50%/30%/20%)
   │   │   - Рассчитывает реализованную прибыль
   │   │   - Публикует событие в `events:trades`
   │   ├─► **TP1 Trailing Orchestrator** (`tp1_trailing_orchestrator.py`)
   │   │   - Реагирует на TP1_HIT события
   │   │   - Запускает трейлинг стоп (если `trail_after_tp1=true`)
   │   └─► **Trade Events Logger** (`trade_events_logger.py`)
   │       - Логирует TP события в Redis (`trade:events:{sid}`, `events:trades`)
   ├─► Обработка SL_HIT
   │   ├─► **Trade Monitor** (`trade_monitor.py::_handle_stop_loss()`)
   │   │   - Обнаруживает достижение SL при обработке тиков
   │   │   - Закрывает остаток позиции
   │   │   - Рассчитывает финальный P&L
   │   │   - Определяет причину закрытия (normal_sl / tp1_then_sl / trailing_stop)
   │   └─► **Trade Events Logger** (`trade_events_logger.py`)
   │       - Логирует SL события с метаданными (reason)
   └─► Обновление трейлинг стопа
       ├─► **TP1 Trailing Orchestrator** (`tp1_trailing_orchestrator.py`)
       │   - Рассчитывает новый SL на основе профиля (ATR/points)
       │   - Публикует команду обновления в `events:trades`
       ├─► **Trade Monitor** (`trade_monitor.py::update_trailing_sl()`)
       │   - Принимает команды обновления трейлинг стопа
       │   - Обновляет SL в виртуальной позиции
       │   - Сохраняет в Redis (`order:{position_id}`)
       └─► **MT5** (для реальных позиций)
           - Получает команды через API Gateway
           - Модифицирует SL в MT5 через `ModifyPosition()`

4. Агрегация статистики
   ├─► Stats Aggregator обновляет stats:{strategy}:{symbol}:{tf}
   ├─► Trade Events Logger записывает события
   └─► Trade Monitor фиксирует закрытые сделки

**Детальное описание агрегации статистики:**

#### Stats Aggregator — обновление агрегированной статистики

**Как работает:**
- Вызывается из `TradeMonitor._finalize_position()` после закрытия каждой позиции
- Использует Redis pipeline для атомарного обновления всех метрик за одну транзакцию
- Обновляет два уровня статистики:
  1. **Общая статистика**: `stats:{strategy}:{symbol}:{tf}` (например, `stats:cryptoorderflow:XAUUSD:M1`)
  2. **Статистика по источникам**: `stats:{strategy}:{symbol}:{tf}:{source}` (например, `stats:cryptoorderflow:XAUUSD:M1:OrderFlow`)

**Обновляемые метрики:**
```python
# Базовые счетчики
- total_trades      # Общее количество сделок
- wins              # Количество прибыльных сделок
- losses            # Количество убыточных сделок

# P&L метрики
- total_pnl         # Суммарный P&L (float)
- total_pnl_pct     # Суммарный P&L в процентах

# TP метрики
- tp1_hits          # Количество достижений TP1
- tp2_hits          # Количество достижений TP2
- tp3_hits          # Количество достижений TP3

# Метрики упущенной прибыли
- tp1_then_sl       # TP1 достигнут, но закрылось по SL
- tp2_then_sl       # TP2 достигнут, но закрылось по SL
- tp3_then_sl       # TP3 достигнут, но закрылось по SL

# Трейлинг метрики
- trailing_started  # Количество запущенных трейлинг стопов
- trailing_stop_hits # Количество закрытий по трейлинг стопу
````

**Для чего нужно:**

- Анализ эффективности стратегий по символам и таймфреймам
- Сравнение источников сигналов (OrderFlow vs AggregatedHub vs TechnicalAnalysis)
- Формирование отчетов в Reporting Service
- Мониторинг winrate, P&L, TP hit rates
- Выявление проблемных паттернов (упущенная прибыль)

#### Trade Events Logger — запись событий сделок

**Как работает:**

- Записывает каждое торговое событие в три структуры Redis:
  1. **`events:trades`** (Stream) — глобальный поток всех событий для обработки другими сервисами
  2. **`trade:events:{sid}`** (List) — полная история событий по конкретному сигналу
  3. **`trade:timeline:{sid}`** (Sorted Set) — временная последовательность событий для анализа

**Типы событий:**

- `POSITION_OPENED` — позиция открыта
- `TP1_HIT`, `TP2_HIT`, `TP3_HIT` — достижение целей фиксации прибыли
- `TRAILING_STARTED` — запуск трейлинг стопа
- `TRAILING_MOVE` — перемещение трейлинг стопа (с новым SL)
- `SL_HIT` — срабатывание стоп-лосса
- `POSITION_CLOSED` — закрытие позиции

**Структура события:**

```json
{
 "event_type": "TP1_HIT",
 "sid": "signal-XAUUSD-1731012450",
 "symbol": "XAUUSD",
 "ts": 1731012450000,
 "price": 2770.0,
 "lot": 0.05,
 "pnl": 22.5,
 "position_id": "pos-123",
 "source": "mt5"
}
```

**Для чего нужно:**

- Восстановление полной истории сделки для анализа
- Отслеживание движения трейлинг стопа (как далеко удалось "утащить" прибыль)
- Анализ эффективности профилей трейлинга
- Построение графиков движения SL
- Интеграция с trade_back для расчета winrate/ROC
- Аудит торговых операций

#### Trade Monitor — фиксация закрытых сделок

**Как работает:**

- При закрытии позиции (`_finalize_position()`) сохраняет данные в Redis:
  1. **`trades:closed:{signal_id}`** (Hash) — полные данные закрытой сделки
  2. **`closed:{strategy}:{symbol}:{tf}`** (List) — список ID закрытых позиций (общий)
  3. **`closed:{strategy}:{symbol}:{tf}:{source}`** (List) — список ID по источникам

**Структура закрытой сделки:**

```json
{
 "signal_id": "signal-XAUUSD-1731012450",
 "position_id": "pos-123",
 "strategy": "cryptoorderflow",
 "symbol": "XAUUSD",
 "direction": "LONG",
 "entry_price": 2765.5,
 "close_price": 2770.0,
 "pnl": 22.5,
 "pnl_pct": 0.81,
 "trade_result": "win",
 "tp1_hit": true,
 "tp2_hit": false,
 "tp3_hit": false,
 "tp_before_sl": 1,
 "close_reason": "TRAILING_STOP",
 "entry_time": 1731012450000,
 "close_time": 1731013500000
}
```

**Для чего нужно:**

- Быстрый доступ к данным закрытых сделок
- Фильтрация сделок по стратегии, символу, таймфрейму, источнику
- Анализ паттернов закрытия (TP vs SL vs Trailing Stop)
- Расчет метрик по историческим данным
- Интеграция с Reporting Service для детальных отчетов

1. Формирование отчетов
   ├─► Reporting Service собирает статистику
   ├─► Формирует HTML-отчет
   └─► Публикует в notify:telegram (type=report)

2. Telegram Worker
   ├─► Читает notify:telegram
   └─► Отправляет в Telegram-бот

````

---

## 📌 Ключевые концепции

### Виртуальные позиции

Виртуальные позиции создаются в `TradeMonitor` для каждого сигнала и отслеживаются независимо от реальных позиций в MT5. Это позволяет:

- Анализировать эффективность сигналов до исполнения
- Сравнивать виртуальный и реальный P&L
- Тестировать стратегии без риска

### События сделок

Все события фиксируются в Redis:

- `POSITION_OPENED` — позиция открыта
- `TP1_HIT`, `TP2_HIT`, `TP3_HIT` — достижение целей
- `TRAILING_STARTED` — запуск трейлинг стопа
- `TRAILING_MOVE` — перемещение стопа
- `SL_HIT` — срабатывание стоп-лосса
- `POSITION_CLOSED` — закрытие позиции

### Статистика

Статистика хранится в Redis Hash `stats:{strategy}:{symbol}:{tf}`:

- `total_trades`, `wins`, `losses`, `winrate`
- `tp1_hits`, `tp2_hits`, `tp3_hits`
- `tp1_then_sl`, `tp2_then_sl`, `tp3_then_sl` (упущенная прибыль)
- `total_pnl`, `avg_pnl`
- `trailing_started`, `trailing_stop_hits`

---

## 🚀 Быстрый старт

### Запуск Signal Performance Tracker

```bash
# Через Docker Compose
docker-compose up -d signal-performance-tracker

# Или напрямую
cd python-worker
python -m services.signal_performance_tracker
````

### Проверка статуса

```bash
# Статистика трекера
make tracker-stats

# Метрики трейлинга
make trailing-stats

# Принудительная отправка отчета
make send-real-report
```

### Конфигурация

Основные переменные окружения:

| Переменная              | Назначение                           | По умолчанию               |
| ----------------------- | ------------------------------------ | -------------------------- |
| `TRACKER_SYMBOLS`       | Список отслеживаемых символов        | `XAUUSD,BTCUSDT`           |
| `STRATEGY_WHITELIST`    | Фильтр стратегий                     | (все)                      |
| `REPORT_TRIGGER_COUNT`  | Количество сделок для отправки отчета | `100`                      |
| `DAILY_SUMMARY_HOUR`    | Час отправки ежедневной сводки (UTC) | `0`                        |
| `REPORT_INTERVAL_HOURS` | Интервал периодических отчетов по времени (опционально) | `3` |
| `REDIS_URL`             | URL основного Redis                  | `redis://localhost:6379/0` |
| `REDIS_TICKS_URL`       | URL Redis для тиков (опционально)    | (использует основной)      |

---

## 📊 Метрики и мониторинг

### Prometheus метрики

| Метрика                   | Описание                                |
| ------------------------- | --------------------------------------- |
| `signals_processed_total` | Количество обработанных сигналов        |
| `positions_opened_total`  | Количество открытых виртуальных позиций |
| `positions_closed_total`  | Количество закрытых позиций             |
| `tp1_hits_total`          | Количество достижений TP1               |
| `trailing_started_total`  | Количество запущенных трейлинг стопов   |
| `stats_report_latency_ms` | Задержка формирования отчета            |
| `reports_published_total` | Количество опубликованных отчетов       |

### Grafana Dashboards

- **Signal Performance Tracker** — общая статистика по сигналам
- **Trailing Stop Metrics** — метрики трейлинг стопов
- **P&L Analysis** — анализ прибыли/убытков

---

## 🔗 Связанные документы

- **[trading_workflow/README.md](../trading_workflow/README.md)** — торговый workflow
- **[trading_workflow/tp1_trailing.md](../trading_workflow/tp1_trailing.md)** — система трейлинг стопов
- **[crypto_tick_processing/README.md](../crypto_tick_processing/README.md)** — обработка тиков
- **[ARCHITECTURE.md](../ARCHITECTURE.md)** — общая архитектура системы

---

## ✅ Контроль версий

- **2026-01-21** — обновление дат документации
- **2025-11-26** — обновление документации по аналитике сигналов
- **2025-11-21** — создание документации по аналитике сигналов
- Ответственные: `@trading-analytics`, `@python-team`, `@quant-team`

Добро пожаловать в систему аналитики сигналов! Если обнаружили рассинхрон — создайте issue в `#scanner_analytics_docs`.
