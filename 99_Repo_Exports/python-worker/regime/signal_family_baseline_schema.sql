-- Схема для baseline-квантилей по signal_family
-- TimescaleDB

-- Таблица с вычисленными baseline-квантилями для каждой группы сигналов
CREATE TABLE IF NOT EXISTS signal_family_baseline (
    -- Hypertable partitioning по времени расчета
    as_of_ts                TIMESTAMPTZ      NOT NULL,

    -- Группа сигналов
    symbol                  TEXT             NOT NULL,
    signal_family           TEXT             NOT NULL,
    direction               SMALLINT         NOT NULL,  -- +1/0/+1

    -- Параметры расчета
    lookback_days           INTEGER          NOT NULL,

    -- Статистика по сигналам
    n_signals               INTEGER          NOT NULL,
    n_trades                INTEGER          NOT NULL,
    hit_rate                DOUBLE PRECISION,
    expectancy_r            DOUBLE PRECISION,
    r_p25                   DOUBLE PRECISION,
    r_p50                   DOUBLE PRECISION,
    r_p75                   DOUBLE PRECISION,

    -- Квантиля по L3-метрикам
    spread_p50              DOUBLE PRECISION,
    spread_p80              DOUBLE PRECISION,
    spread_p95              DOUBLE PRECISION,

    obi_persist_p25         DOUBLE PRECISION,
    obi_persist_p50         DOUBLE PRECISION,
    obi_persist_p75         DOUBLE PRECISION,

    mp_drift_abs_p50        DOUBLE PRECISION,
    mp_drift_abs_p80        DOUBLE PRECISION,

    canc_bid_p50            DOUBLE PRECISION,
    canc_bid_p80            DOUBLE PRECISION,
    canc_ask_p50            DOUBLE PRECISION,
    canc_ask_p80            DOUBLE PRECISION,

    -- Вычисленные thresholds для CryptoConfScorer
    l3_spread_max_ok_bps    DOUBLE PRECISION,
    l3_spread_hard_limit_bps DOUBLE PRECISION,
    l3_cancel_soft          DOUBLE PRECISION,
    l3_cancel_hard          DOUBLE PRECISION,
    l3_obi_good_min         DOUBLE PRECISION,
    l3_obi_bad_max          DOUBLE PRECISION,
    l3_mp_drift_max_bps     DOUBLE PRECISION,

    -- Metadata
    created_at              TIMESTAMPTZ      NOT NULL DEFAULT now(),

    PRIMARY KEY (as_of_ts, symbol, signal_family, direction)
);

-- Создание гипертаблицы (если TimescaleDB доступен)
-- SELECT create_hypertable('signal_family_baseline', 'as_of_ts', if_not_exists => TRUE);

-- Индексы для быстрого поиска
CREATE INDEX IF NOT EXISTS idx_signal_family_baseline_symbol_as_of
ON signal_family_baseline (symbol, as_of_ts DESC);

CREATE INDEX IF NOT EXISTS idx_signal_family_baseline_family_as_of
ON signal_family_baseline (signal_family, as_of_ts DESC);

CREATE INDEX IF NOT EXISTS idx_signal_family_baseline_direction_as_of
ON signal_family_baseline (direction, as_of_ts DESC);
