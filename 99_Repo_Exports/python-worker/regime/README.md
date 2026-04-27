# Signal Family Baseline System

Система расчета baseline-квантилей для контроля качества сигналов на основе исторических данных.

## Обзор

Система анализирует исторические результаты сигналов и рассчитывает статистические характеристики (квантили) для каждого семейства сигналов. Эти характеристики используются для онлайн-контроля качества в `RegimeGuardService`.

## Архитектура

### Таблицы базы данных

#### `signal_exec_summary`
Таблица с результатами выполненных сигналов (исходные данные).

```sql
CREATE TABLE signal_exec_summary (
    signal_id      BIGINT PRIMARY KEY,
    symbol         TEXT NOT NULL,
    family         TEXT NOT NULL,
    opened_at      TIMESTAMPTZ NOT NULL,
    closed_at      TIMESTAMPTZ NOT NULL,
    result_r       DOUBLE PRECISION NOT NULL,  -- R = pnl / risk
    mfe_r          DOUBLE PRECISION,           -- max favorable excursion
    mae_r          DOUBLE PRECISION,           -- max adverse excursion
    ttd_sec        DOUBLE PRECISION,           -- time-to-decay
    extra_json     JSONB                       -- доп. данные
);
```

#### `signal_family_baseline`
Таблица с рассчитанными квантилями baseline.

```sql
CREATE TABLE signal_family_baseline (
    symbol       TEXT NOT NULL,
    family       TEXT NOT NULL,
    metric       TEXT NOT NULL,  -- 'hit_rate', 'expectancy_R'
    window_size  INTEGER NOT NULL,
    horizon_days INTEGER NOT NULL,
    p05          DOUBLE PRECISION,
    p10          DOUBLE PRECISION,
    p25          DOUBLE PRECISION,
    p50          DOUBLE PRECISION,
    p75          DOUBLE PRECISION,
    p90          DOUBLE PRECISION,
    p95          DOUBLE PRECISION,
    sample_size  INTEGER NOT NULL,
    computed_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (symbol, family, metric, window_size, horizon_days)
);
```

## Использование

### 1. Заполнение исторических данных

```sql
-- Пример вставки данных в signal_exec_summary
INSERT INTO signal_exec_summary (
    signal_id, symbol, family, opened_at, closed_at, result_r
) VALUES
(1, 'BTCUSDT', 'volatility_spike', '2024-01-01 10:00:00+00', '2024-01-01 11:00:00+00', 0.5),
(2, 'BTCUSDT', 'volatility_spike', '2024-01-02 10:00:00+00', '2024-01-02 11:00:00+00', -0.3);
```

### 2. Запуск расчета baseline

```bash
# Из директории python-worker
python -m regime.baseline_job
```

Или программно:

```python
from regime.baseline_job import SignalFamilyBaselineJob

job = SignalFamilyBaselineJob(
    dsn="postgresql://user:pass@host:5432/db",
    window_size=50,      # размер окна для rolling-статистики
    horizon_days=180,    # глубина истории в днях
)
job.run()
```

### 3. Использование в RegimeGuardService

```python
from regime.guard import RegimeGuardService

# Инициализация с baseline
guard = RegimeGuardService(
    pg_dsn="postgresql://...",
    redis_dsn="redis://...",
    window_size=50,             # размер rolling-окна
    baseline_horizon_days=180,  # горизонт baseline
)

# При закрытии сигнала
guard.on_signal_closed(
    signal_id="123",
    family="volatility_spike",
    venue="binance",
    symbol="BTCUSDT",
    timeframe="1m",
    r_value=0.5,
    closed_at=datetime.now(),
)
```

## Алгоритм расчета

### 1. Скользящие окна

Для каждого семейства сигналов `(symbol, family)`:
- Берем историю за `horizon_days` дней
- Сортируем по времени открытия
- Строим скользящие окна по `window_size` сигналов

### 2. Расчет метрик по окну

Для каждого окна рассчитываются:
- **Hit Rate**: доля прибыльных сигналов (result_r > 0)
- **Expectancy_R**: среднее арифметическое result_r

### 3. Квантили

По всем окнам рассчитываются квантили:
- p05, p10, p25, p50, p75, p90, p95

### 4. Онлайн-контроль

В `RegimeGuardService`:
- Поддерживается rolling-статистика по последним `window_size` сигналам
- Сравнивается с baseline p10:
  - Если hit_rate < baseline.hit_rate.p10 → degraded mode
  - Если expectancy_R < baseline.expectancy_r.p10 → degraded mode

## Конфигурация

### Переменные окружения

```bash
# Baseline job
BASELINE_WINDOW_SIZE=50      # размер окна
BASELINE_HORIZON_DAYS=180    # глубина истории

# Database
DATABASE_URL=postgresql://user:pass@host:5432/db
```

### Параметры RegimeGuardService

```python
RegimeGuardService(
    window_size=50,             # размер rolling-окна
    baseline_horizon_days=180,  # горизонт baseline
    disable_dd_mult=1.5,        # множитель для отключения при просадке
    degrade_dd_mult=1.0,        # множитель для degraded режима
    wr_safe_margin=0.05,        # гистерезис по winrate
)
```

## Мониторинг

### Просмотр baseline

```sql
SELECT symbol, family, metric, window_size, horizon_days,
       p10, p50, p90, sample_size, computed_at
FROM signal_family_baseline
ORDER BY symbol, family, metric;
```

### Просмотр состояния режимов

```sql
SELECT ts_state, family, venue, symbol, timeframe,
       status, wr_window, exp_r_window, reason
FROM signal_family_regime_state
ORDER BY ts_state DESC
LIMIT 10;
```

## Диагностика

### Логи

Baseline job пишет подробные логи:
```
INFO: Fetching signals since 2024-01-01 (horizon: 180 days)
INFO: Fetched 1234 signals for 5 symbol-family combinations
INFO: Processing BTCUSDT:volatility_spike (567 signals)
INFO:   hit_rate: p10=0.45, p50=0.52, p90=0.58 (samples=15)
```

### Проверка данных

```python
# Запуск примера
python -m regime.example_usage
```
