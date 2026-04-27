# Потоки Данных Scanner Infrastructure

Этот документ детально описывает последовательность прохождения данных в платформе: от поступления тиков до генерации отчётов. Здесь приведены участники процессов, форматы сообщений, SLA и точки контроля. Используйте файл как источник правды для онбординга и аудитов.

---

## 1. Обзор потоков

| Поток                        | Назначение                                   | Основные компоненты                                                                                    |
| ---------------------------- | -------------------------------------------- | ------------------------------------------------------------------------------------------------------ |
| Tick → OrderFlow → Signal    | Обработка OrderFlow и генерация сигналов     | Binance WS, MT5 TickBridge, Go gateway, OrderFlow Handlers, Aggregated Signal Hub V2, Calibration     |
| Signal → Order → Execution   | Риск-фильтрация и исполнение ордеров         | Risk filters, Go gateway, MT5 executor, DualRedis, Order queue                                        |
| TP1 Trailing & Events        | Автоматический трейлинг после TP1            | MT5 events, TP Event Listener, Trailing Orchestrator, Dispatcher, Go gateway                           |
| Analytics V2/V3 & Reporting  | Продвинутая аналитика и отчёты               | Analytics system, Signal Performance Tracker, Telegram reporter, GPU compute, Dataset export          |
| Calibration & Optimization   | Автокалибровка порогов и параметров          | Auto calibration service, Threshold tuning, ROC analysis, A/B testing                                 |
| Diagnostics & Monitoring     | Проверка здоровья и метрик                   | Prometheus, Grafana, Alertmanager, Makefile diagnostics                                                |

---

## 2. Поток Tick → G-Gates → Journal → Execution

### 2.1 Последовательность событий (P4.1)

```
1. Ingest (Go): получает пакет из сокета, декодирует, проставляет t0.
2. Redis Stream: tick_<sym>: публикация тика, фиксируется t1.
3. CryptoOrderFlow Pipeline:
   - G0-G5 (Data Health & Filter): начало обработки (t2).
   - G6-G15 (Logic & ML Gating).
   - Формирование SignalDTO (v: 1).
4. Journaling (Redis): запись команды в stream `orders:exec`, фиксируется t3 (publish) и t4 (ack).
5. Execution (BinanceExecutor):
   - Чтение из `orders:exec`.
   - Исполнение на бирже (REST/WS).
   - Фиксация t5 (завершение).
6. Projection (ProjectionWorker): материализация состояния и расчет P4.1 метрик.
```

### 2.2 Схема данных (V1)

- **SignalDTO (v: 1)**:

  ```json
  {
   "sid": "signal-btcusdt-1731145800",
   "v": 1,
   "qty": 0.5,
   "quantity": 0.5,
   "t0": 1731145800100,
   "t1": 1731145800105,
   "t2": 1731145800110,
   "t3": 1731145800115,
   "gates": ["G1", "G6", "G10", "G12"],
   "p_edge": 0.72
  }
  ```

### 2.3 SLA (P4.1 Contract)

| Этап                | Порог (P99) | Описание                                  |
| ------------------- | ----------- | ----------------------------------------- |
| Ingest (t0-t1)      | ≤ 1 мс      | Задержка декодирования и записи в Redis.  |
| Logic (t1-t3)       | ≤ 5 мс      | Время прохождения гейтов (Hot path).      |
| Journal (t3-t4)     | ≤ 1 мс      | Задержка коммита в Redis Stream.          |
| Execution (t4-t5)   | ≤ 200 мс    | Время отклика биржи + сетевой путь.       |

---

## 3. Поток News Intelligence & ML Governance

### 3.1 News Feedback Loop

```
1. News Agent (LLM): мониторинг лент, извлечение событий.
2. Risk Recommendation: публикация NewsTightenRecoDTO в `trade:cache:news_reco_map`.
3. G14 Gate (News Guard): CryptoOrderFlow считывает рекомендацию и блокирует/ужимает входы.
```

### 3.2 ML Governance Loop

```
1. Feature Monitoring: расчет PSI/KS дрейфа признаков.
2. Calibration: ночной расчет ECE/Brier и обновление порогов G10/G12.
```

---

## 3. Поток TP1 Trailing

### 3.1 Последовательность

```
1. MT5 фиксирует TP1 и отправляет событие POST /events/mt5 в Go gateway.
2. Go gateway валидирует токен, кладёт запись в Redis Stream `events:trades`.
3. TP Event Listener слушает `events:trades`, проверяет idempotency по `tp_event_id`.
4. Listener публикует задачу в канал `tp:commands`.
5. TP1 Trailing Orchestrator получает задачу, выбирает профиль трейлинга, формирует команду `TRAIL`.
6. Orchestrator отправляет команду через Order Trailing Dispatcher (`/orders/push`).
7. MT5 executor исполняет трейлинг, подтверждает `/orders/ack`.
8. Событие `TRAILING_MOVE` сохраняется в `trade:timeline:{sid}`.
```

### 3.2 Типы событий

- `TP1_HIT`: содержит `order_id`, `price`, `volume`, `tp_event_id`.
- `TRAILING_MOVE`: содержит `order_id`, `new_sl`, `reason`, `tp_event_id`.
- `SL_HIT`, `ERROR`: негативные исходы, активируют алерты.

### 3.3 Контрольные точки

- `trailing_latency_ms`: P95 ≤ 2500 мс.
- `trailing_started_total`: сравнивается с количеством `TP1_HIT`.
- Очередь `tp:commands` не должна расти > 10 элементов.

---

## 4. Поток Analytics V2/V3 & Reporting

### 4.1 Последовательность

```
1. OrderFlow Handlers публикуют сигналы в `signals:orderflow:<symbol>` с детальными метриками (delta, OBI, spike detection).
2. Aggregated Signal Hub V2 добавляет aggregated данные и calibration info.
3. Analytics V2/V3 system читает все сигналы и трейды, рассчитывает ROC curves, threshold optimization, A/B testing.
4. GPU Compute Service ускоряет тяжёлые вычисления (матричные операции, ML модели).
5. Dataset Export создаёт обучающие датасеты в Parquet формате для ML моделей.
6. Signal Performance Tracker ведёт статистику, обновляет `stats:{strategy}:{symbol}:{tf}`.
7. Telegram Reporter Extended формирует визуальные отчёты с графиками и интерактивными элементами.
8. Auto Calibration Service анализирует результаты и обновляет пороги в реальном времени.
9. Все отчёты публикуются в `notify:telegram` с типами: signal, report, alert, analytics.
```

### 4.2 Форматы

- **Статистика (`stats:`)**:
  - `win_rate`, `avg_rr`, `tp1_hit_ratio`, `tp2_follow_through`, `avg_trailing_distance`.
- **Telegram message** (JSON):

  ```json
  {
   "type": "signal",
   "sid": "crypto:B-123",
   "symbol": "BTCUSDT",
   "side": "LONG",
   "text": "🚀 BTCUSDT LONG @ 95340. Delta spike + OBI 0.82.",
   "timestamp": 1731145805123
  }
  ```

- **Telegram report** (`type=report`):

  ```json
  {
   "type": "report",
   "text": "<b>Ежедневная сводка</b> ...",
   "source": "ReportingService",
   "timestamp": 1731149405123
  }
  ```

- **Отчёт CSV**:
  - Столбцы: `sid`, `symbol`, `entry_ts`, `tp1_ts`, `tp2_ts`, `trail_distance`, `result`.

### 4.3 SLA

| Этап                | Порог            | Комментарий                               |
| ------------------- | ---------------- | ----------------------------------------- |
| Генерация отчёта    | ≤ 5 мин от слота | Контролируется `stats_report_latency_ms`. |
| Доставка Telegram   | ≤ 30 секунд      | Очередь `notify:telegram` < 100.          |
| Обновление `stats:` | ≤ 10 секунд      | Lag consumer групп отслеживается.         |

---

## 5. Поток Market Data Analytics

### 5.1 Компоненты

- `book_analytics_service` снимает DOM (Depth of Market) из `stream:book_*`.
- Анализирует order book imbalance (OBI), выдаёт сигналы ликвидности.
- Публикует результаты в Hash `market_depth:{symbol}` и Stream `market_depth_events`.

### 5.2 Использование

- Риск-модели считывают `market_depth:{symbol}` для корректировки объёмов сделок.
- Grafana `Market Data` отображает OBI и спред.
- Алерты срабатывают при `obi_abs > threshold`.

---

## 6. Диагностика и реплей

### 6.1 Реплей тиков

1. Выберите архив `fixtures/*.json`.
2. Выполните `make replay-ticks FILE=fixtures/...`.
3. Команда публикует события в `stream:tick_*` с сохранением таймингов.
4. Signal Hub и downstream сервисы обрабатывают их как realtime.

### 6.2 Отладка lag

- Используйте `make stream-lag STREAM=stream:tick_btcusdt`.
- Проверьте consumer группу `signal_hub_v2` (`XINFO GROUPS stream:tick_btcusdt`).
- Если lag растёт, увеличьте реплики Hub или оптимизируйте обработку.

---

## 7. Инструменты проверки

| Команда                  | Что проверяет                                 |
| ------------------------ | --------------------------------------------- |
| `make tick-streams`      | Lag, задержки у ingestion                     |
| `make trailing-stats`    | Время реакции трейлинга                       |
| `make tracker-stats`     | Статус отчётов и метрик                       |
| `make stream-sample`     | Выборку последних сообщений из Stream         |
| `make redis-key-inspect` | Содержимое ключа (`signals:{sid}`, `trade:*`) |

---

## 8. Изменения в потоках

При добавлении нового сервиса или шага:

1. Обновите схему в этом документе.
2. Зафиксируйте изменения в `architecture.md`.
3. Добавьте описание сервиса в `services.md`.
4. Обновите `operations.md`, если процесс наблюдения меняется.
5. Добавьте тест реплея (при необходимости) в `fixtures/` и Makefile.

---

## 9. Глоссарий потоков

- **DualRedis** — стратегия записи в два Redis (primary + fallback).
- **Lag** — количество необработанных сообщений в Stream относительно последнего доставленного.
- **TP1/TP2** — уровни фиксации прибыли.
- **Trailing profile** — преднастроенный набор параметров для управления стопом.
- **Signal Hub V2** — основная логика агрегации сигналов.

---

## 10. Связанные документы

- `services.md` — подробности сервисов, API и конфигураций.
- `operations.md` — мониторинг и реагирование на отклонения.
- `troubleshooting.md` — шаги по устранению проблем в каждом потоке.
- `glossary.md` — расширенный словарь терминов.

При обнаружении несовпадений обновите документацию и уведомите команды в `#scanner_docs`.
