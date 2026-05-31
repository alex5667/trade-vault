# Troubleshooting Scanner Infrastructure

Документ описывает типовые проблемы, способы диагностики и устраняет дублирование знаний. Используйте его как руководство во время инцидентов и при онбординге дежурных инженеров.

---

## 1. Общий алгоритм действий

1. **Зафиксировать симптомы**: какие сервисы недоступны, какие метрики отклоняются, какие алерты пришли.
2. **Проверить здоровье**: `make diagnose`, Grafana дашборды, `/healthz`.
3. **Изолировать домен**: ingestion, сигналы, трейлинг, аналитика, MT5.
4. **Собрать данные**: логи, Redis ключи, состояние очередей.
5. **Сформулировать гипотезы**: инфраструктура, конфигурация, логика.
6. **Применить исправления**: минимально инвазивные, с логированием.
7. **Сделать постмортем**: документировать причину и шаги устранения.

---

## 2. Ingestion (Go worker / Tick ingest)

### 2.1 Симптом: задержка тиков > 0.4с, алерт `tick_gap_seconds`

- **Проверка**:
  - `make tick-streams`
  - `docker logs go-worker`
  - Grafana `Tick Streams` (секция `Gap P95`)
- **Возможные причины**:
  - Сбой WebSocket Binance, превышение rate limit.
  - Перегрузка сети / CPU на хосте.
  - Зависание Redis Ticks.
- **Решения**:
  - Перезапустить `go-worker` (`make go-worker-restart`).
  - Убедиться, что `binance_ws` доступен (curl/ping).
  - Провести failover на резервный источник (см. `documentation/ticks/`).

### 2.2 Симптом: DualRedis failover

- **Проверка**: `make tick-ingest-logs`, `redis-cli -u $REDIS_TICKS_URL ping`.
- **Решения**:
  - Перезапустить недоступный Redis (`docker restart scanner-redis-ticks`).
  - Если ошибка на уровне сети — переключить `DUALREDIS_PRIMARY` на fallback.
  - После восстановления проверить синхронизацию (`make redis-diff`).

### 2.3 Симптом: `/tick` возвращает 5xx

- **Проверка**:
  - Логи `tick_ingest_server`.
  - `make tick-ingest-test`.
- **Решения**:
  - Проверить токен (`INGEST_AUTH_TOKEN`).
  - Убедиться, что JSON payload валиден.
  - Провести `make tick-ingest-restart`.

---

## 3. Signal Hub и Risk Filters

### 3.1 Симптом: сигналы не генерируются (`signals_generated_total` не растёт)

- **Проверка**:
  - `make signals-logs`
  - `redis-cli xlen stream:tick_btcusdt`
  - `redis-cli hlen signals:<sid>`
- **Возможные причины**:
  - Нет данных в `stream:tick_*`.
  - Ошибка в профилях трейлинга / конфиге стратегий.
  - Lag consumer группы.
- **Решения**:
  - Перезапустить Hub (`make signals-restart`).
  - Обновить конфигурацию `SIGNAL_STRATEGIES`.
  - Увеличить `CONCURRENCY`, очистить pending entries (`XCLAIM`).

### 3.2 Симптом: все сигналы блокируются risk-фильтром

- **Проверка**: `redis-cli hgetall trade:state:<sid>`.
- **Решения**:
  - Проверьте лимиты (`MAX_OPEN_POSITIONS_PER_SYMBOL`).
  - Проверить `risk_rules.yaml` на корректные значения.
  - Для теста отключите конкретное правило и наблюдайте.

   - Для теста отключите конкретное правило и заработайте.

### 3.3 Симптом: Сигналы отсеваются Unified ATR Gate (VETO)

- **Проверка**:
  - `docker logs scanner-crypto-orderflow | grep "VETO"`
  - `docker logs scanner-crypto-orderflow | grep "Unified ATR Gate"`
- **Возможные причины**:
  - `atr_bps` ниже порога (Low ATR).
  - Комиссии превышают ожидаемую прибыль (Fees dominant).
  - Включен `GATE_MODE=ENFORCE`.
- **Решения**:
  - Временно включить `DEBUG_VETO=1` в `docker-compose-crypto-orderflow.yml` и сделать `make up`.
  - Проверить значения `ATR_FLOOR_T*` в конфиге.
  - Переключить `ATR_GATE_MODE` в `SHADOW` для анализа без блокировки.

---

## 4. Очередь ордеров и Go gateway

### 4.1 Симптом: `orders_queue_length` > 20

- **Проверка**:
  - `redis-cli llen orders:queue`
  - `make gateway-logs`
- **Причины**:
  - MT5 не забирает команды (`/orders/poll`).
  - Rate limiting на Go gateway.
  - Ошибки в командном payload.
- **Решения**:
  - Проверить MT5 (`make mt5-ping`).
  - Увеличить лимит `RPS_LIMIT_MT5`.
  - Использовать `make orders-drain` (аккуратно в dev/stage).

### 4.2 Симптом: `/orders/push` возвращает 429/401

- **Проверка**:
  - Токен `MT5_ORDER_TOKEN`.
  - Метрика `rate_limit_hits_total`.
- **Решения**:
  - Обновить токен, синхронизировать с MT5.
  - Настроить rate limiting (`RPS_LIMIT_DEFAULT`).
  - Запустить `make gateway-restart`.

### 4.3 Симптом: MT5 не подтверждает команды

- **Проверка**:
  - Логи MT5 (терминал).
  - `/orders/ack` в логах Go gateway.
- **Решения**:
  - Перезапустить EA `TickBridge` и executor.
  - Проверить состояние MT5 счёта (доступность, маржа).
  - Эскалировать TradingOps.

---

## 5. TP1 Trailing

### 5.1 Симптом: `trailing_latency_ms` > 2500 мс

- **Проверка**:
  - `make trailing-stats`.
  - Очередь `tp:commands` (`redis-cli llen tp:commands`).
- **Причины**:
  - Lag consumer групп `tp_event_listener`.
  - Сеть между Go gateway и MT5.
  - Ошибки профиля трейлинга.
- **Решения**:
  - Перезапустить listener (`make trailing-restart`).
  - Увеличить количество воркеров (`TP_EVENT_CONCURRENCY`).
  - Проверить MT5 ping.

### 5.2 Симптом: трейлинг не активируется после TP1

- **Проверка**:
  - `events:trades` stream (наличие `TP1_HIT`).
  - `trade:timeline` (есть ли записи `TRAILING_MOVE`).
- **Решения**:
  - Проверить профили (`profiles:trailing:*`).
  - Убедиться, что событие не в pending (`XPENDING`).
  - Перезапустить `tp1_trailing_orchestrator`.

---

## 6. Signal Performance Tracker & Отчёты

### 6.1 Симптом: нет отчётов (`stats_report_latency_ms` > 5 мин)

- **Проверка**:
  - `make tracker-stats`.
  - Логи tracker (`make tracker-logs`).
- **Решения**:
  - Проверить доступ к Redis Trades (`REDIS_TRADES_URL`).
  - Убедиться, что `REPORT_TRIGGER_COUNT` корректно настроен (по умолчанию 100 сделок).
  - Проверить, что счетчик `report_counter:trades:{source}:{symbol}` увеличивается при закрытии сделок.
  - Перезапустить tracker (`make tracker-restart`).

### 6.2 Симптом: Telegram не получает уведомления

- **Проверка**:
  - Очередь `notify:telegram` (`redis-cli xlen notify:telegram`).
  - Логи `telegram-worker`.
- **Решения**:
  - Проверить токен (`TELEGRAM_BOT_TOKEN`).
  - Включить уведомления (`TELEGRAM_NOTIFICATIONS_ENABLED=true`).
  - Перезапустить воркер (`make telegram-restart`).

---

## 7. Redis

### 7.1 Симптом: рост memory usage > 80%

- **Проверка**: `make redis-stats`.
- **Решения**:
  - Очистить устаревшие ключи (`trade:timeline`, `orders:history`).
  - Увеличить ресурсы хоста или перейти на внешний Redis.
  - Настроить eviction (`maxmemory-policy`).

### 7.2 Симптом: повреждение данных (Corruption)

- **Проверка**:
  - Логи Redis (`redis-server.log`).
  - `redis-cli info persistence`.
- **Процедура восстановления**:
  1. Остановить запись (`make freeze-writes`).
  2. Выполнить `make restore-redis BACKUP=<path>`.
  3. Проверить целостность ключей.
  4. Снять свежий бэкап.
  5. Запустить сервисы.

### 7.3 Симптом: высокая задержка команд (`latency doctor`)

- **Проверка**: `redis-cli --latency`, `redis-cli --latency-history`.
- **Решения**:
  - Проверить сетевые настройки.
  - Убедиться, что `lua` скриптов нет в блокировке.
  - Перераспределить нагрузку между инстансами.

---

## 8. Docker / Инфраструктура

### 8.1 Симптом: контейнеры постоянно перезапускаются

- **Проверка**:
  - `docker ps --format "table {{.Names}}\t{{.Status}}"`.
  - Логи контейнера (`docker logs`).
- **Решения**:
  - Проверить утечки памяти/CPU.
  - Обновить зависимости (`make build-*`).
  - Проверить `ulimit`, swap, дисковое пространство.

### 8.2 Симптом: `make up-bg` зависает

- **Проверка**:
  - Версия Docker/Compose.
  - Логи `docker compose`.
- **Решения**:
  - Очистить dangling volumes (`docker volume prune`).
  - Пересобрать образы (`make build-all`).
  - Проверить, что порты не заняты.

---

## 9. MT5

### 9.1 Симптом: `make mt5-ping` не проходит

- **Проверка**:
  - MT5 терминал онлайн? (сеть, авторизация).
  - `TickBridge` эксплуатируется? (журнал MT5).
- **Решения**:
  - Перезапустить `TickBridge`.
  - Обновить список разрешённых URL в настройках MT5.
  - Проверить firewall/iptables.

### 9.2 Симптом: неверная авторизация `/events/mt5`

- **Решения**:
  - Обновить `MT5_EVENT_TOKEN`.
  - Обновить конфиг `TickBridge` и перезапустить.
  - Убедиться, что MT5 отправляет `Authorization: Bearer`.

---

## 10. Analytics V2/V3 и GPU Compute

### 10.1 Симптом: `analytics_compute_latency_ms` > 30 сек

- **Проверка**:
  - `make analytics-logs`
  - `make gpu-stats` (проверить GPU utilization)
  - `redis-cli hgetall analytics:status`
- **Возможные причины**:
  - GPU memory overflow или driver issues.
  - Большие датасеты без оптимизации.
  - Конфликт CUDA contexts.
- **Решения**:
  - Перезапустить GPU service (`make gpu-service-restart`).
  - Уменьшить `GPU_BATCH_SIZE` или отключить GPU (`GPU_COMPUTE_ENABLED=false`).
  - Проверить CUDA installation (`nvidia-smi`).

### 10.2 Симптом: ROC curves не обновляются, низкий `calibration_score`

- **Проверка**:
  - `make calibration-logs`
  - `redis-cli hgetall calibration:results`
  - Проверить window size (`ANALYTICS_WINDOW_DAYS`)
- **Решения**:
  - Увеличить период анализа или проверить качество данных.
  - Перезапустить calibration service.
  - Проверить threshold tuning parameters.

### 10.3 Симптом: Dataset export зависает или не завершается

- **Проверка**: `make analytics-status`, логи export jobs.
- **Решения**:
  - Проверить дисковое пространство для Parquet files.
  - Уменьшить `EXPORT_WINDOW_DAYS` или использовать compression.
  - Перезапустить с `EXPORT_RETRY_ON_FAILURE=true`.

---

## 11. Общие советы

- Делайте снимки состояния перед вмешательством.
- Используйте `make stream-sample` для проверки содержимого потоков.
- Логируйте всё, что делаете во время инцидента.
- После инцидента обновите соответствующий раздел документации.
- Всегда проверяйте дублирующие изменения в `roadmap.md` и `operations.md`.

---

## 11. Приложения

- **A. Команды Redis**:
  - `XINFO GROUPS <stream>`
  - `XPENDING <stream> <group>`
  - `XCLAIM`
- **B. Справка по Makefile**: `make help` выводит доступные команды.
- **C. Контакты**: см. `overview.md#контакты-и-каналы-взаимодействия`.

Если возникла новая проблема — зафиксируйте её здесь, добавьте сценарий воспроизведения и шаги решения. Так мы сохраняем коллективную память проекта.
