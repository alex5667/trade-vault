-- Схемы таблиц для baseline-расчета signal family
-- TimescaleDB

-- 1. Таблица с результатами выполненных сигналов (исходные данные для baseline)
CREATE TABLE IF NOT EXISTS signal_exec_summary (
    signal_id      BIGINT PRIMARY KEY,
    symbol         TEXT        NOT NULL,   -- XAUUSD, BTCUSDT, ...
    family         TEXT        NOT NULL,   -- 'volatility_spike', 'reclaim', ...
    opened_at      TIMESTAMPTZ NOT NULL,
    closed_at      TIMESTAMPTZ NOT NULL,

    result_r       DOUBLE PRECISION NOT NULL, -- итог сделки в R
    mfe_r          DOUBLE PRECISION,          -- max favorable excursion (в R)
    mae_r          DOUBLE PRECISION,          -- max adverse excursion (в R)

    -- опционально:
    ttd_sec        DOUBLE PRECISION,  -- time-to-decay/до реализации edge, если считаешь
    extra_json     JSONB              -- любой доп. контекст
);

-- Создание гипертаблицы для оптимизации по времени
SELECT create_hypertable('signal_exec_summary', 'opened_at', if_not_exists => TRUE);

-- Индексы для быстрого поиска
CREATE INDEX IF NOT EXISTS idx_signal_exec_summary_lookup
ON signal_exec_summary (symbol, family, opened_at);

-- 2. Таблица с baseline-квантилями для signal family
CREATE TABLE IF NOT EXISTS signal_family_baseline (
    symbol       TEXT        NOT NULL,
    family       TEXT        NOT NULL,
    metric       TEXT        NOT NULL,  -- 'hit_rate', 'expectancy_R'
    window_size  INTEGER     NOT NULL,  -- N сигналов в окне
    horizon_days INTEGER     NOT NULL,  -- сколько истории учитывали (например, 180)

    p05          DOUBLE PRECISION,
    p10          DOUBLE PRECISION,
    p25          DOUBLE PRECISION,
    p50          DOUBLE PRECISION,
    p75          DOUBLE PRECISION,
    p90          DOUBLE PRECISION,
    p95          DOUBLE PRECISION,

    sample_size  INTEGER     NOT NULL,  -- сколько окон реально посчитали
    computed_at  TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (symbol, family, metric, window_size, horizon_days)
);

-- Индекс для быстрого поиска baseline
CREATE INDEX IF NOT EXISTS idx_signal_family_baseline_lookup
ON signal_family_baseline (symbol, family, metric);
