# Ответы на вопросы по инфраструктуре и метрикам

Ниже представлены ответы на ваши вопросы, основанные на текущем состоянии кодовой базы. Скопированные файлы-исходники, подтверждающие эти данные, находятся в этой же папке (`reference/qna_docs`).

### 1. Где сейчас источник истины для DQ метрик?
**Источник истины:** `Python TickProcessor` (внутри `python-worker/services/orderflow`).
- Все метрики `tick_gap_p95_ms`, `book_missing_seq_ema` и `tick_missing_seq_ema` инициализируются и обновляются в файле `metrics.py` платформы `python-worker`. 
- В `go-worker` расчет этих EMA-метрик не ведется, он только поставляет сырые данные.

### 2. Какая у вас текущая схема лейблов Prometheus?
Самым главным лейблом (identity) является **`symbol`**. В зависимости от конкретной метрики в `metrics.py` к нему добавляются детализирующие лейблы:
- `where` / `reason` / `kind` (для ошибок, silent exceptions и метрик DQ, например `dq_veto_total_reason`)
- `mode` (например, `shadow` или `enforce` в `trade_gate_ok_total`)
- `bucket` / `window` (для временных окон или бакетов имбаланса)
- `tier` / `session` (для метрик scale/HOW, таких как `of_dn_tier_pass_total`)
- `action` / `decision` (для политик времени, например `tick_time_decision_total`)
Основной подход: низкая кардинальность (только `symbol` по умолчанию), плюс 1-2 лейбла для причины/состояния.

### 3. Список venues в прод-контуре сейчас (что реально включено)
Исходя из конфигураций `.env` и `docker-compose-go-workers.yml`:
- **Binance:** Полностью **ВКЛЮЧЕНО** (работает через `BINANCE_FUTURES_BASE_URL` или демо в зависимости от тестовых настроек, также `FUTURES_WS_ENABLED=true` в go-worker). Для сигналов `BINANCE_VIRTUAL_ORDERS_ENABLED=1`.
- **Bybit:** Исполнение и сбор маркет-даты в ворчерах по умолчанию **ВЫКЛЮЧЕНЫ** (`BYBIT_DATA_COLLECTION_ENABLED=false`, `BYBIT_FUTURES_WS_ENABLED=false`). Однако, метрики качества данных (DQ) включены (`BYBIT_DQ_ENABLED=true` в `.env`).
- **Hyperliquid:** **ВЫКЛЮЧЕНО** (`HYPERLIQUID_WS_ENABLED=false` в `.env`).
- **MT5:** **ВЫКЛЮЧЕНО** (отсутствует активная конфигурация в текущем `.env` для запуска).

### 4. Что является "истиной" для tick_to_trade?
**Истина для tick_to_trade — это `ts_exec_ack_ms` (executor).**
- В HFT и системах реального времени `tick_to_trade` измеряет полное время реакции системы: от момента генерации события на бирже (tick) до момента, когда биржа подтверждает получение нашего ордера (ack).
- Метрика `ts_emit_ms` отражает лишь задержку логики (tick-to-signal: время генерации сигнала в Python TickProcessor), но не учитывает время доставки приказа экзекутором, сетевую задержку до биржи и время обработки биржей. Поэтому для точного контроля latency на всем пути ордера ("tick-to-trade") источником истины выступает таймстемп подтверждения от экзекутора (`ts_exec_ack_ms`).

### 5. Prometheus scrape interval и rule eval interval (реальные значения)
Согласно `monitoring/prometheus/prometheus.yml`:
- `scrape_interval`: **15s** (задано глобально в `global: scrape_interval: 15s`). Некоторые специфичные exporters (например, `edge_stack_shadow_p60` и `of_inputs_dlq_db_p99`) переопределяют его на 30s.
- `evaluation_interval`: **15s** (также глобально `global: evaluation_interval: 15s`). Это строго соответствует вашему правилу: rules оцениваются каждые 15 секунд.
