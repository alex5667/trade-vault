# Аудит: Ответы на вопросы и План Улучшений

## 1. Ответы на вопросы (Architecture & Data Flow)

*   **Рынки / Venues:** Binance, Bybit, Hyperliquid (основные целевые площадки). Необходима стандартизация коннекторов для унифицированного flow.
*   **Decision _price:** Используется **mid price** (базово "mid ok"). Цена формируется из ордербука (BBA - Best Bid/Offer) на момент (tick) принятия решения, с учетом NTP‑синхронизированного epoch-ms.
*   **SID-join (Signal ID / Symbol ID):** Соединение стримов происходит по ключу `sid` (уникальный идентификатор сигнала/символа) `+` `epoch-ms`. Важное требование: `sid` гарантированно уникален (sid uniq).
*   **Maker / Taker:** Вход рыночными ордерами (Taker) для обеспечения заполнения (решающий фактор - fill probability), либо лимитными (Maker), если спред позволяет. Taker-policy требует строгого контроля проскальзывания (slippage & execution penalty).
*   **Цель p99:** Строгий бюджет задержки. Tick-to-trade p99 < 50ms (на уровне Go-воркеров и Redis), p99 < 100ms на полный цикл (Go -> Python -> Execution).
*   **Ретеншн raw (Tick data):** 24–72 часа для сырых тиков в горячем слое (Timescale/Redis) для обеспечения in-memory re-computation и debug. Для агрегатов и TCA - от 90 дней (PQ/CSV). Обоснование: "O 24–72h@tick".
*   **Предпосылки (Assumptions):** Strict `epoch-ms` (UTC), NTP sync across nodes, `sid` globally unique, `mid` price calculation is robust.

---

## 2. Комплексный План Внедрения (Итог)

*Легенда: C(M/H/VH) - Complexity/Сложность, P(L/M/H) - Priority/Приоритет.*

### Этап 1: Configuration & Environment Strictness
**Цель:** Исключить расхождения конфигурации, ввести строгий парсинг Z-правил (Z rules).
*   **План:** Сбор всех ENV в единый валидируемый конфигуратор (CSV/JSON schema), удаление неявных хардкодов. 
*   **Пороги / Тесты:** Failsafe при старте (crash on invalid ENV), unit-тесты парсинга (Type+Ovr).
*   **SLO:** 100% config coverage. Нет `unspecified` дефолтов в prod.
*   **Rollout:** Shadow mode (log diffs) -> Hard enforce. 
*   **Оценка:** C(M), P(H)

### Этап 2: Streams & Context (Raw Data Quality)
**Цель:** Раннее детектирование gaps/OoO (Out of Order) / dupes.
*   **План:** Внедрение sequence IDs для тиков, метрики gap-detection (ts_r - ts_e). JSONL(sid,ts_e,ts_r,ctx) логгер.
*   **Пороги / Тесты:** Alert при gap > 1s, alert при late arrival > 50ms. Unit-test: симуляция OoO.
*   **SLO:** 99.9% тиков без duplicates/gaps (p99 delivery < 50ms).
*   **Rollout:** Parallel processing -> A/B metrics matching.
*   **Оценка:** C(H), P(H)

### Этап 3: Execution & TCA (Transaction Cost Analysis)
**Цель:** Детальный сбор PQ/CSV для ордеров, филлов, котировок (TCA).
*   **План:** Запись всех стейтов: `orders(oid,sid,pid,ts,st)`, `fills(fid,oid,ts,px,fee)`, `quotes(ts,mid@tick)`. Хранение ≥90d.
*   **Пороги / Тесты:** Проверка баланса: Σ(fills) == order_qty. Сверка часов (ts_e vs ts_r).
*   **SLO:** 0% потерь филлов в базе. Latency overhead на сохранение < 2ms (асинхронный writer).
*   **Rollout:** Async pub/sub (zero path delay).
*   **Оценка:** C(VH), P(H)

### Этап 4: ML Artifacts & Data Drift
**Цель:** Контроль версии моделей и деградации (drift).
*   **План:** Версионирование через zip(model, hash, cal) + паркет лог инференса PQ(sid,ts,fold,y,p_raw,p_cal).
*   **Пороги / Тесты:** Alert: abs(p_cal - realized) > threshold, тест на утечку данных (no-leak test) по time-split, валидация hash(model) на старте.
*   **SLO:** < 5% uncalibrated predictions.
*   **Rollout:** Deployment triggers on verified hash only.
*   **Оценка:** C(VH), P(H)

### Этап 5: Observability & Infra Metrics
**Цель:** Унифицированный Prometheus + Grafana stack.
*   **План:** Добавление TSDF metrics (veto_ratio, pipeline_lag, error_rate). Очистка cardinality.
*   **Пороги / Тесты:** Lag p99 > 150ms -> Paging Alert; Error_rate > 1% = SEV2.
*   **SLO:** 99.9% uptime. 14-30 дней retention для high-card metrics. 
*   **Rollout:** Prometheus rules bundle apply (automated via CI).
*   **Оценка:** C(M), P(M)

### Этап 6: DB/Redis Schema and Retention
**Цель:** Гарантия производительности DB (WAL/IO).
*   **План:** pg_dump DDL аудит, EXPLAIN-мониторинг для slow queries. Redis ключи строгой схемы: CSV keys(pat, type, ttl, ex).
*   **Пороги / Тесты:** No full table scans, Query latency p99 < 10ms. 100% покрытие TTL для Redis.
*   **SLO:** Redis memory stable, Postgres CPU < 40%.
*   **Rollout:** Index builds in background (CONCURRENTLY).
*   **Оценка:** C(H), P(H)

### Этап 7: Security & Ops Setup (Kill Switch)
**Цель:** Автоматизированное управление рисками.
*   **План:** Реализация Global Kill-Switch, аудиторские логи, runbook-инструкции для ручного вмешательства.
*   **Пороги / Тесты:** Staging drill: проверка kill-switch сбрасывает лимиты и закрывает/отменяет ордера за < 2сек.
*   **SLO:** 100% audit trail coverage, MTTM (Mean Time to Mitigate) < 1 minute (via kill switch).
*   **Rollout:** Feature flag testing in Staging -> Prod rollout.
*   **Оценка:** C(VH), P(H)
