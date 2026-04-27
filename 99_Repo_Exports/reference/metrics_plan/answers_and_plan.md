# План Метрик и Гейтов (Сводка и Ответы)

В данной директории (`reference/metrics_plan`) собраны примеры конфигураций (`.env`, `docker-compose.yml`), правил алертнига (`prometheus.yml`), а также код экзекьюторов. Ниже представлен запрошенный план и ответы на критические вопросы.

## 1. Ответы на критические вопросы (Предп+W + Вопросы)

**Предпосылки (Assumptions/W):**
*   **Time & Sync:** `epoch-ms` (UTC) строго, синхронизация через NTP на всех узлах.
*   **Join key:** `sid` (Signal ID) однозначно ведет от генерации сигнала до исполнения (`fills`).
*   **Price:** `mid` price окей (считается надежным расчетным ориентиром).

**Вопросы (крит) и Ответы:**
1.  **Рынки / venues?**
    *   Binance, Bybit, Hyperliquid. Архитектура поддерживает расширение через унифицированный интерфейс `venue` в пейлоадах.
2.  **Инструменты?**
    *   Перпетуальные фьючерсы (USDT/USDC-m), спот-пары (редко, зависит от `sid`). Точный инструмент зашит в `sid` (например, `BTCUSDT_PERP`).
3.  **Maker / taker?**
    *   По умолчанию **Taker** (рыночные ордера) для обеспечения 100% fill probability по сгенерированному сигналу (важно минимизировать opportunity cost).
    *   **Maker** (лимитные) используются только там, где есть edge_cost_gate с запасом по спреду и включен режим "пассивного исполнения". 
4.  **decision_price где?**
    *   Находится на стороне Python-генератора сигналов в момент вычисления (Tick/Bar). Это BBA (Best Bid/Offer mid) на момент `tsE` (времени события/тика на бирже).
5.  **p99?**
    *   Tick-to-trade (Go -> Redis -> Python -> Go) p99 < 100ms.
    *   Go Worker pipeline lag p99 < 50ms.
    *   DB queries (Timescale) p99 < 10ms.
6.  **ретеншн raw / agg?**
    *   **Raw (тики/стаканы):** 24–72 часа в горячем слое (Timescale chunks / Redis Streams) для debug/re-computation.
    *   **Agg (1m/5m, TCA, Orders, Fills, Models):** 90+ дней холодное хранилище (PostgreSQL / Parquet в S3/Disk) для бектестов и аналитики.
7.  **ENV / kill кто меняет?**
    *   **ENV:** Инфраструктурная команда (DevOps/Admin) через CI/CD / Terraform. Прямая смена на проде без ревью запрещена.
    *   **Kill Switch:** Активируется **автоматически** риск-сервисом (drift_reader, edge_cost_gate) при выходе метрик за пороги, либо **вручную** Ops-инженером через API / UI (по протоколу из Runbook).

---

## 2. План метрик/гейтов (пороги, тесты, SLO, rollout)

Формат: **Z** (зачем) / **F** (формат) / **W** (окно) / **C** (конф) / **P** (приор) / **V** (чек).

### 2.1. Configuration & ENV
*   **Z (Зачем):** Избежать silent failures из-за кривых конфигов и хардкодов. Строгие Z-правила.
*   **F (Формат):** `env.csv(n,v,scope,svc) + profile.yml`
*   **W (Окно/Хранение):** 90d_hist (история изменений конфигов).
*   **C (Сложность):** M (Medium).
*   **P (Приоритет):** H (High).
*   **SLO:** 100% переменных без `unspecified` дефолтов.
*   **V (Чек):** type validation, override tracking, dead-env detect (используется ли?).
*   **Rollout:** Shadow config generation (dry-run) -> Validation on boot -> Failsafe (crashloop on invalid env).

### 2.2. Streams & Context (ctx) samples
*   **Z (Зачем):** Контроль качества сырых данных (Data Quality, DTO / time sync).
*   **F (Формат):** `ctx.jsonl(sid, tsE, tsR, ctx)`.
*   **W (Окно/Хранение):** 72h@tick (сырые тики).
*   **C (Сложность):** H (High).
*   **P (Приоритет):** H (High).
*   **SLO:** < 0.1% потерянных тиков (gaps). Delivery lag p99 < 50ms.
*   **V (Чек):** epoch precision, Out-of-Order (OoO) detection, gap analysis, unit tests on parsing.
*   **Rollout:** Parallel processing -> Data matching -> Alerting enable.

### 2.3. Historical execution/TCA data
*   **Z (Зачем):** Transaction Cost Analysis, трекинг slippage / execution quality.
*   **F (Формат):** `orders + fills + mid (oid, sid, ts, qty, px, fee, st)`.
*   **W (Окно/Хранение):** 90d.
*   **C (Сложность):** H (High).
*   **P (Приоритет):** H (High).
*   **SLO:** 100% join rate `orders` к `fills`. Разница `decision_price` vs `fill_px` (Slippage) в пределах бюджета.
*   **V (Чек):** Join integrity, sum(fills) == order_qty, ts drift.
*   **Rollout:** Async batch writers (PQ/CSV) по cron 1m, zero latency path.

### 2.4. ML artifacts & model metrics
*   **Z (Зачем):** Model calibration, data drift monitoring, предотвращение leak'ов.
*   **F (Формат):** `model.zip(meta, cal)` + `oof(id, y, p, fold, ts)` + `shadow(brier, ece)`.
*   **W (Окно/Хранение):** В рамках trainwin.
*   **C (Сложность):** H (High).
*   **P (Приоритет):** H (High).
*   **SLO:** Drift detection time < 1h. ECE (Expected Calibration Error) < threshold.
*   **V (Чек):** no-leak tests (time-split), hash matched (model prod/art), feature distribution checks.
*   **Rollout:** Shadow mode inference -> Verification -> Dark-launch alerts -> Production signals.

### 2.5. Observability & infra metrics
*   **Z (Зачем):** Infra SLO, мониторинг здоровья системы.
*   **F (Формат):** `prom_rules + dash + metrics(lat, lag, veto, err)`.
*   **W (Окно/Хранение):** 30d@1m (aggregated Prometheus TSDB).
*   **C (Сложность):** M (Medium).
*   **P (Приоритет):** M (Medium) *(после Data/TCA)*.
*   **SLO:** 99.9% uptime, CPU < 80%, RAM predictable (no OOM).
*   **V (Чек):** Cardinality control (no high-cardinality label explosions), reliable p99 histograms.
*   **Rollout:** Prometheus push config (via GitOps/CI), Grafana provisioning. Установка базовых Warning -> Critical.

### 2.6. DB/Redis schema and retention
*   **Z (Зачем):** Гарантия производительности DB (WAL/IO) и TTL Redis.
*   **F (Формат):** `DDL + tables + redis_keys(pat, type, ttl)` + 3 SQL query plans (veto, fills, midjoin).
*   **W (Окно/Хранение):** curr (актуальное состояние).
*   **C (Сложность):** H (High).
*   **P (Приоритет):** H (High).
*   **SLO:** Redis memory keysize 100% с TTL, Timescale chunk mapping < 1GB per chunk, p99 query < 10ms.
*   **V (Чек):** Indexes on `(sid, ts)`, TTL verifier script, pg_stat_statements monitor.
*   **Rollout:** Migrations via golang-migrate, CONCURRENTLY index builds.

### 2.7. Security/ops/runbook
*   **Z (Зачем):** Управление рисками (Risk limits), Kill-switches.
*   **F (Формат):** `runbook(kill_switch, manual_override) + audit_log + RBAC roles`.
*   **W (Окно/Хранение):** curr.
*   **C (Сложность):** H (High).
*   **P (Приоритет):** H (High).
*   **SLO:** Time to Kill (от обнаружения P0 до halt_trading) < 5s.
*   **V (Чек):** Staging-drill testing (регулярные учения по остановке торгов), Audit log coverage 100%.
*   **Rollout:** Staging -> Validation -> Runbook review -> Prod approval.
