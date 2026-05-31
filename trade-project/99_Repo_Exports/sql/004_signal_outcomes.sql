-- Migration 004: Signal Outcomes Table
-- Purpose: Close the signal → outcome feedback loop for ML training and threshold calibration.
-- Stores outcome records (PnL, R-multiple, execution path, excursions) per closed trade.
-- Retention: 180 days via TimescaleDB retention policy.

CREATE TABLE IF NOT EXISTS signal_outcomes (
    ts               TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- identity
    sid              TEXT NOT NULL,
    order_id         TEXT NOT NULL,
    symbol           TEXT NOT NULL,
    strategy         TEXT DEFAULT '',
    source           TEXT DEFAULT '',
    tf               TEXT DEFAULT '',
    direction        TEXT DEFAULT 'LONG',

    -- signal-time features (snapshot at entry)
    entry_price      DOUBLE PRECISION DEFAULT 0,
    entry_ts_ms      BIGINT DEFAULT 0,
    sl               DOUBLE PRECISION DEFAULT 0,
    tp1_price        DOUBLE PRECISION DEFAULT 0,
    atr              DOUBLE PRECISION DEFAULT 0,
    entry_tag        TEXT DEFAULT '',
    regime           TEXT DEFAULT '',
    scenario         TEXT DEFAULT '',

    -- outcome
    exit_price       DOUBLE PRECISION DEFAULT 0,
    exit_ts_ms       BIGINT DEFAULT 0,
    pnl_net          DOUBLE PRECISION DEFAULT 0,
    pnl_gross        DOUBLE PRECISION DEFAULT 0,
    fees             DOUBLE PRECISION DEFAULT 0,
    r_multiple       DOUBLE PRECISION DEFAULT 0,
    one_r_money      DOUBLE PRECISION DEFAULT 0,
    risk_usd         DOUBLE PRECISION DEFAULT 0,

    -- execution path
    close_reason     TEXT DEFAULT '',
    tp1_hit          BOOLEAN DEFAULT FALSE,
    tp2_hit          BOOLEAN DEFAULT FALSE,
    tp3_hit          BOOLEAN DEFAULT FALSE,
    trailing_started BOOLEAN DEFAULT FALSE,
    trailing_active  BOOLEAN DEFAULT FALSE,
    trailing_moves   INTEGER DEFAULT 0,
    duration_ms      BIGINT DEFAULT 0,

    -- excursions
    mfe_pnl          DOUBLE PRECISION DEFAULT 0,
    mae_pnl          DOUBLE PRECISION DEFAULT 0,
    giveback         DOUBLE PRECISION DEFAULT 0,
    missed_profit    DOUBLE PRECISION DEFAULT 0,

    -- ML label (auto-computed: trade is a "win" if r_multiple >= 1.0)
    is_win           BOOLEAN GENERATED ALWAYS AS (COALESCE(r_multiple, 0) >= 1.0) STORED,

    -- meta
    is_virtual       BOOLEAN DEFAULT FALSE,
    meta_enforce_cov_bucket TEXT DEFAULT '',
    trace_id         TEXT DEFAULT '',
    event_id         TEXT DEFAULT '',

    -- idempotency: one outcome per trade
    UNIQUE (order_id, ts)
);

-- TimescaleDB hypertable (time-partitioned by ts, 1-day chunks)
SELECT create_hypertable('signal_outcomes', 'ts',
    chunk_time_interval => interval '1 day',
    if_not_exists => TRUE
);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS signal_outcomes_sid_idx
    ON signal_outcomes (sid);

CREATE INDEX IF NOT EXISTS signal_outcomes_symbol_ts_idx
    ON signal_outcomes (symbol, ts DESC);

CREATE INDEX IF NOT EXISTS signal_outcomes_is_win_ts_idx
    ON signal_outcomes (is_win, ts DESC);

CREATE INDEX IF NOT EXISTS signal_outcomes_source_ts_idx
    ON signal_outcomes (source, ts DESC);

CREATE INDEX IF NOT EXISTS signal_outcomes_regime_scenario_idx
    ON signal_outcomes (regime, scenario, ts DESC);

-- Retention: 180 days of outcome data
SELECT add_retention_policy('signal_outcomes', INTERVAL '180 days', if_not_exists => TRUE);

-- Comments
COMMENT ON TABLE signal_outcomes IS
    'Signal-to-outcome records for ML feedback loop. One row per closed trade. '
    'Generated is_win column labels trades with r_multiple >= 1.0 as wins.';

COMMENT ON COLUMN signal_outcomes.is_win IS
    'ML label: TRUE when r_multiple >= 1.0 (GENERATED ALWAYS, cannot be set manually)';

COMMENT ON COLUMN signal_outcomes.r_multiple IS
    'Risk-multiple: pnl_net / one_r_money. Key metric for threshold calibration.';
