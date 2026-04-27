-- Схема для таблицы signal_facts с L3-метриками
-- TimescaleDB

-- Основная таблица сигналов с L3-метриками
CREATE TABLE IF NOT EXISTS signal_facts (
    -- Hypertable partitioning
    ts                      TIMESTAMPTZ NOT NULL,

    -- Signal identity
    signal_id               TEXT        NOT NULL,
    symbol                  TEXT        NOT NULL,
    direction               SMALLINT    NOT NULL,  -- +1 long, -1 short, 0 neutral
    signal_family           TEXT        NOT NULL,

    -- Scores
    conf_score              DOUBLE PRECISION NOT NULL,

    -- Basic metrics (examples - extend as needed)
    atr_14                  DOUBLE PRECISION DEFAULT 0.0,
    delta_spike_z           DOUBLE PRECISION DEFAULT 0.0,
    obi_avg_20              DOUBLE PRECISION DEFAULT 0.0,
    weak_progress_ratio     DOUBLE PRECISION DEFAULT 0.0,

    -- L3-Lite metrics
    l3_spread_bps              DOUBLE PRECISION DEFAULT 0.0,
    l3_microprice_shift_bps_20 DOUBLE PRECISION DEFAULT 0.0,
    l3_microprice_velocity_bps DOUBLE PRECISION DEFAULT 0.0,

    l3_obi_5                   DOUBLE PRECISION DEFAULT 0.0,
    l3_obi_20                  DOUBLE PRECISION DEFAULT 0.0,
    l3_obi_50                  DOUBLE PRECISION DEFAULT 0.0,
    l3_obi_persistence_score   DOUBLE PRECISION DEFAULT 0.0,

    l3_cancel_to_trade_bid_5s  DOUBLE PRECISION DEFAULT 0.0,
    l3_cancel_to_trade_ask_5s  DOUBLE PRECISION DEFAULT 0.0,
    l3_cancel_to_trade_bid_20s DOUBLE PRECISION DEFAULT 0.0,
    l3_cancel_to_trade_ask_20s DOUBLE PRECISION DEFAULT 0.0,

    l3_queue_pressure_bid      DOUBLE PRECISION DEFAULT 0.0,
    l3_queue_pressure_ask      DOUBLE PRECISION DEFAULT 0.0,
    l3_market_depth_imbalance  DOUBLE PRECISION DEFAULT 0.0,

    -- Metadata
    created_at              TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (ts, signal_id)
);

-- Создание гипертаблицы (если TimescaleDB доступен)
-- SELECT create_hypertable('signal_facts', 'ts', if_not_exists => TRUE);

-- Индексы для быстрого поиска
CREATE INDEX IF NOT EXISTS idx_signal_facts_symbol_ts
ON signal_facts (symbol, ts DESC);

CREATE INDEX IF NOT EXISTS idx_signal_facts_family_ts
ON signal_facts (signal_family, ts DESC);

CREATE INDEX IF NOT EXISTS idx_signal_facts_direction_ts
ON signal_facts (direction, ts DESC);

-- Индексы для L3-метрик (опционально, для аналитики)
CREATE INDEX IF NOT EXISTS idx_signal_facts_l3_obi_persistence
ON signal_facts (l3_obi_persistence_score, ts DESC)
WHERE l3_obi_persistence_score > 0;

CREATE INDEX IF NOT EXISTS idx_signal_facts_l3_spread
ON signal_facts (l3_spread_bps, ts DESC)
WHERE l3_spread_bps > 0;
