# Audit Data Compilation Report

This document answers the critical audit questions and describes the gathered production data, all collected in the `reference/audit_data_collection/` directory.

### 1. Реальные логи binance-executor за 7–30 дней
- **Файлы:** `binance_executor_1.log` и `binance_executor_2.log`
- **Описание:** Логи были извлечены из контейнеров `scanner-crypto-orderflow` (реплики 1 и 2). Содержат реальные ошибки, ретраи (BINANCE_MAX_RETRY) и таймауты подключения.

### 2. Полный срез Redis execution state
- **Файлы:** `redis_keys.txt`, `redis_queue_binance_sample.txt`, `redis_exec_stream_sample.txt`
- **Описание:** Сделан дамп списка всех ключей. Содержимое очередей `orders:queue:binance` и `orders:exec` извлечено для анализа edge cases.

### 3. История fills / PnL / fees / funding за 90–180 дней
- **Файл:** `trade_history_sample.csv`
- **Описание:** Дамп из таблицы `trades_closed_p0` базы данных PostgreSQL (trade_db). Таблица содержит поля `pnl_gross`, `pnl_net`, `commission`, `funding_fee`, `entry_price` и `exit_price` для полноценной статистики закрытых трейдов. Выгружено 500 последних записей.

### 4. Latency timestamps по пути signal → queue → executor → Binance → ack
- **Описание и где искать:** 
  - Разница между генерацией сигнала и добавлением в очередь отслеживается через timestamp в стриме `orders:exec` и `orders:queue:binance`.
  - Latency внутри самого экзекьютора покрыта в выгруженных логах `binance_executor_X.log` (до отправки на Binance API и после получения ответа).
  - Итоговые таймстемпы исполнения (ack) пишутся в базу PostgreSQL (в таблице `trades_closed_p0` есть `exit_ts` и `exit_ts_ms`). Это позволяет замерить сквозную задержку и выбрать оптимальную архитектуру (polling vs event-driven).

### 5. CI/CD и deployment manifests
- **Папка:** `cicd/`
- **Описание:** Все конфигурации `docker-compose*.yml` были скопированы в папку `cicd`. По ним можно расписать точный rollout/rollback и конфигурации (в т.ч. volumes и networks) по каждому из окружений.

### 6. Политики риска и лимитов в текущем production
- **Файл:** `risk_limits.txt`
- **Описание:** Полные настройки извлечены из текущего `.env`. 
  - **Leverage cap:** `ACCOUNT_LEVERAGE=100`, `BINANCE_DEFAULT_LEVERAGE=100`
  - **Max daily loss / Risk Limits:** `RISK_PERCENT=5.0`, `RISK_MAX_QTY=0.5`
  - **Max exposure (Slippage Cap):** `LCB_SLIPPAGE_BPS_CAP=250`
  - **Symbol caps:** `METRICS_SYMBOLS_MAX=200`
