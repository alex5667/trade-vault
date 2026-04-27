-- scanner_analytics schema (TimescaleDB)
-- Usage:
--   1) psql -U postgres -h <host> -p 5432 -d postgres
--   2) CREATE DATABASE scanner_analytics;
--   3) \c scanner_analytics
--   4) \i docs/scanner_analytics_schema.sql

-- Enable extension (requires TimescaleDB image/extension installed)
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Disable telemetry (optional, harmless if lacks permission)
ALTER DATABASE scanner_analytics SET timescaledb.telemetry_level = 'off';

-- Main closed-trades table
CREATE TABLE IF NOT EXISTS trades_closed (
    id                      BIGSERIAL PRIMARY KEY,
    order_id                TEXT NOT NULL UNIQUE,
    sid                     TEXT,
    strategy                TEXT NOT NULL DEFAULT '',
    source                  TEXT,
    symbol                  TEXT NOT NULL,
    tf                      TEXT,
    direction               TEXT,

    entry_ts_ms             BIGINT NOT NULL,
    exit_ts_ms              BIGINT NOT NULL,
    entry_ts                TIMESTAMPTZ GENERATED ALWAYS AS (to_timestamp(entry_ts_ms / 1000.0)) STORED,
    exit_ts                 TIMESTAMPTZ GENERATED ALWAYS AS (to_timestamp(exit_ts_ms / 1000.0)) STORED,

    entry_price             DOUBLE PRECISION NOT NULL,
    exit_price              DOUBLE PRECISION NOT NULL,
    lot                     DOUBLE PRECISION NOT NULL,
    notional_usd            DOUBLE PRECISION,

    pnl_net                 DOUBLE PRECISION NOT NULL,
    pnl_gross               DOUBLE PRECISION NOT NULL,
    fees                    DOUBLE PRECISION NOT NULL,
    pnl_pct                 DOUBLE PRECISION,

    pnl_if_fixed_exit       DOUBLE PRECISION,
    baseline_exit_reason    TEXT,
    baseline_exit_ts_ms     BIGINT,
    baseline_exit_price     DOUBLE PRECISION,

    tp1_hit                 BOOLEAN,
    tp2_hit                 BOOLEAN,
    tp3_hit                 BOOLEAN,
    tp_hits                 INTEGER,
    tp_before_sl            INTEGER,
    trailing_started        BOOLEAN,
    trailing_active         BOOLEAN,
    trailing_moves          INTEGER,
    trailing_profile        TEXT,

    mfe_pnl                 DOUBLE PRECISION,
    mae_pnl                 DOUBLE PRECISION,
    giveback                DOUBLE PRECISION,
    missed_profit           DOUBLE PRECISION,

    one_r_money             DOUBLE PRECISION,
    r_multiple              DOUBLE PRECISION,

    duration_ms             BIGINT,
    close_reason            TEXT,
    close_reason_raw        TEXT,
    close_reason_detail     TEXT DEFAULT '',

    entry_tag               TEXT,
    max_favorable_price     DOUBLE PRECISION,
    max_favorable_ts        BIGINT,

    is_final_close          BOOLEAN,
    remaining_qty           DOUBLE PRECISION,
    status                  TEXT,

    created_at              TIMESTAMPTZ DEFAULT now()
);

-- Add close_reason_detail column if it doesn't exist (for existing databases)
-- ALTER TABLE trades_closed ADD COLUMN IF NOT EXISTS close_reason_detail TEXT DEFAULT '';

-- Hypertable on close time
SELECT create_hypertable('trades_closed', 'exit_ts', if_not_exists => TRUE);

-- Indexes for analytics
CREATE INDEX IF NOT EXISTS idx_trades_closed_symbol_exit
    ON trades_closed(symbol, exit_ts);
CREATE INDEX IF NOT EXISTS idx_trades_closed_source_symbol_exit
    ON trades_closed(source, symbol, exit_ts);
CREATE INDEX IF NOT EXISTS idx_trades_closed_entry_tag_exit
    ON trades_closed(entry_tag, exit_ts);
CREATE INDEX IF NOT EXISTS idx_trades_closed_sid
    ON trades_closed(sid);

-- Raw ticks (optional)
CREATE TABLE IF NOT EXISTS ticks (
    id          BIGSERIAL PRIMARY KEY,
    source      TEXT,
    symbol      TEXT NOT NULL,
    ts_ms       BIGINT NOT NULL,
    ts          TIMESTAMPTZ GENERATED ALWAYS AS (to_timestamp(ts_ms / 1000.0)) STORED,
    price       DOUBLE PRECISION,
    bid         DOUBLE PRECISION,
    ask         DOUBLE PRECISION,
    volume      DOUBLE PRECISION,
    side        TEXT,
    meta        JSONB
);
SELECT create_hypertable('ticks', 'ts', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_ticks_symbol_ts ON ticks(symbol, ts);

-- Daily aggregates
CREATE TABLE IF NOT EXISTS daily_metrics (
    id                  BIGSERIAL PRIMARY KEY,
    date                DATE NOT NULL,
    source              TEXT,
    symbol              TEXT NOT NULL,

    trades_count        INTEGER,
    wins                INTEGER,
    losses              INTEGER,
    breakeven           INTEGER,

    pnl_net_sum         DOUBLE PRECISION,
    pnl_net_avg         DOUBLE PRECISION,
    pnl_net_std         DOUBLE PRECISION,

    expectancy_r        DOUBLE PRECISION,
    payoff_r            DOUBLE PRECISION,
    payoff_usd          DOUBLE PRECISION,
    kelly_r             DOUBLE PRECISION,

    wr                  DOUBLE PRECISION,
    sharpe              DOUBLE PRECISION,
    sortino             DOUBLE PRECISION,
    mdd_usd             DOUBLE PRECISION,

    wr_fixed            DOUBLE PRECISION,
    expectancy_fixed_r  DOUBLE PRECISION,
    payoff_fixed_r      DOUBLE PRECISION,
    payoff_fixed_usd    DOUBLE PRECISION,
    delta_expectancy_r  DOUBLE PRECISION,

    created_at          TIMESTAMPTZ DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS daily_metrics_uniq
    ON daily_metrics(date, source, symbol);

-- Entry tag aggregates
CREATE TABLE IF NOT EXISTS entry_tag_metrics (
    id                              BIGSERIAL PRIMARY KEY,
    date                            DATE NOT NULL,
    source                          TEXT,
    symbol                          TEXT NOT NULL,
    entry_tag                       TEXT NOT NULL,

    trades_count                    INTEGER,
    wins                            INTEGER,
    losses                          INTEGER,
    breakeven                       INTEGER,

    pnl_net_sum                     DOUBLE PRECISION,
    pnl_net_avg                     DOUBLE PRECISION,
    expectancy_r                    DOUBLE PRECISION,
    payoff_r                        DOUBLE PRECISION,
    payoff_usd                      DOUBLE PRECISION,
    wr                              DOUBLE PRECISION,

    wr_fixed                        DOUBLE PRECISION,
    expectancy_fixed_r              DOUBLE PRECISION,
    payoff_fixed_r                  DOUBLE PRECISION,
    payoff_fixed_usd                DOUBLE PRECISION,
    delta_expectancy_r              DOUBLE PRECISION,

    giveback_avg_usd                DOUBLE PRECISION,
    giveback_avg_r                  DOUBLE PRECISION,
    giveback_avg_ratio              DOUBLE PRECISION,
    giveback_share                  DOUBLE PRECISION,

    missed_avg_usd                  DOUBLE PRECISION,
    missed_avg_r                    DOUBLE PRECISION,
    missed_avg_ratio                DOUBLE PRECISION,
    missed_share                    DOUBLE PRECISION,

    mfe_avg_r                       DOUBLE PRECISION,
    mae_avg_r                       DOUBLE PRECISION,

    trailing_share                  DOUBLE PRECISION,
    trailing_close_share            DOUBLE PRECISION,
    trailing_wr                     DOUBLE PRECISION,
    trailing_expectancy_r           DOUBLE PRECISION,
    trailing_expectancy_fixed_r     DOUBLE PRECISION,
    trailing_delta_expectancy_r     DOUBLE PRECISION,

    created_at                      TIMESTAMPTZ DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS entry_tag_metrics_uniq
    ON entry_tag_metrics(date, source, symbol, entry_tag);

-- Optional policies (enable later as needed)
-- SELECT add_retention_policy('trades_closed', INTERVAL '90 days');
-- SELECT add_compression_policy('trades_closed', INTERVAL '7 days');
-- ALTER TABLE trades_closed SET (timescaledb.compress, timescaledb.compress_segmentby = 'symbol,source');

