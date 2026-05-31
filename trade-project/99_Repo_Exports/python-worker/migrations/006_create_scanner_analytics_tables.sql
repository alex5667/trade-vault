-- Migration: Create scanner_analytics tables
-- Description: Creates tables for trade analytics in scanner_analytics database
-- Date: 2025-12-17
-- NOTE: \c scanner_analytics removed — migration-runner connects via PG_DSN directly

-- Enable TimescaleDB extension if available (optional)
-- CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ============================================================================
-- Table: trades_closed
-- ============================================================================
-- Main table for closed trades with baseline vs managed comparison

CREATE TABLE IF NOT EXISTS trades_closed (
    id                      BIGSERIAL PRIMARY KEY,
    order_id                TEXT NOT NULL UNIQUE,
    sid                     TEXT,
    strategy                TEXT,
    source                  TEXT,
    symbol                  TEXT NOT NULL,
    tf                      TEXT,
    direction               TEXT,           -- 'LONG' / 'SHORT'

    entry_ts_ms             BIGINT NOT NULL,
    exit_ts_ms              BIGINT NOT NULL,
    entry_ts                TIMESTAMPTZ,
    exit_ts                 TIMESTAMPTZ,

    entry_price             DOUBLE PRECISION NOT NULL,
    exit_price              DOUBLE PRECISION NOT NULL,
    lot                     DOUBLE PRECISION NOT NULL,
    notional_usd            DOUBLE PRECISION,

    pnl_net                 DOUBLE PRECISION NOT NULL,
    pnl_gross               DOUBLE PRECISION NOT NULL,
    fees                    DOUBLE PRECISION NOT NULL,
    pnl_pct                 DOUBLE PRECISION,

    -- baseline vs managed
    pnl_if_fixed_exit       DOUBLE PRECISION,
    baseline_exit_reason    TEXT,
    baseline_exit_ts_ms     BIGINT,
    baseline_exit_price     DOUBLE PRECISION,

    -- TP / SL / trailing
    tp1_hit                 BOOLEAN,
    tp2_hit                 BOOLEAN,
    tp3_hit                 BOOLEAN,
    tp_hits                 INTEGER,
    tp_before_sl            INTEGER,
    trailing_started        BOOLEAN,
    trailing_active         BOOLEAN,
    trailing_moves          INTEGER,
    trailing_profile        TEXT,

    -- экскурссии / giveback / missed
    mfe_pnl                 DOUBLE PRECISION,
    mae_pnl                 DOUBLE PRECISION,
    giveback                DOUBLE PRECISION,
    missed_profit           DOUBLE PRECISION,

    -- риск в R
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

    -- Health metrics at trade closure time
    health_l2_stale_ratio_tick    DOUBLE PRECISION,  -- L2 stale ratio (tick-relative)
    health_l2_stale_ratio_now     DOUBLE PRECISION,  -- L2 stale ratio (now-relative)
    health_avg_l2_age_ms          DOUBLE PRECISION,  -- Avg L2 age (ms)
    health_avg_l2_age_tick_ms     DOUBLE PRECISION,  -- Avg L2 age tick (ms)
    health_signal_emit_rate       DOUBLE PRECISION,  -- Signal emit rate (signals/sec)
    health_dlq_rate               DOUBLE PRECISION,  -- DLQ rate (errors/sec)

    created_at              TIMESTAMPTZ DEFAULT now()
);

-- Trigger to populate entry_ts and exit_ts
CREATE OR REPLACE FUNCTION populate_trades_closed_ts() RETURNS TRIGGER AS $$
BEGIN
    NEW.entry_ts := to_timestamp(NEW.entry_ts_ms / 1000.0);
    NEW.exit_ts := to_timestamp(NEW.exit_ts_ms / 1000.0);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_populate_trades_closed_ts ON trades_closed;
CREATE TRIGGER trg_populate_trades_closed_ts
BEFORE INSERT OR UPDATE ON trades_closed
FOR EACH ROW EXECUTE FUNCTION populate_trades_closed_ts();

-- Convert to hypertable if TimescaleDB is available
-- FIX: create_hypertable падает если таблица непустая — используем DO/EXCEPTION
DO $$
BEGIN
    PERFORM create_hypertable('trades_closed', 'exit_ts', if_not_exists => TRUE);
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'create_hypertable(trades_closed): % — пропущено', SQLERRM;
END$$;

-- Indexes for analytics queries
CREATE INDEX IF NOT EXISTS idx_trades_closed_symbol_exit
    ON trades_closed(symbol, exit_ts);

CREATE INDEX IF NOT EXISTS idx_trades_closed_source_symbol_exit
    ON trades_closed(source, symbol, exit_ts);

CREATE INDEX IF NOT EXISTS idx_trades_closed_entry_tag_exit
    ON trades_closed(entry_tag, exit_ts);

CREATE INDEX IF NOT EXISTS idx_trades_closed_sid
    ON trades_closed(sid);

-- ============================================================================
-- Table: ticks (optional)
-- ============================================================================
-- For storing raw tick data for research purposes

CREATE TABLE IF NOT EXISTS ticks (
    id          BIGSERIAL PRIMARY KEY,
    source      TEXT,
    symbol      TEXT NOT NULL,
    ts_ms       BIGINT NOT NULL,
    ts          TIMESTAMPTZ,
    price       DOUBLE PRECISION,
    bid         DOUBLE PRECISION,
    ask         DOUBLE PRECISION,
    volume      DOUBLE PRECISION,
    side        TEXT,              -- 'BUY'/'SELL'/NULL
    meta        JSONB,
    created_at  TIMESTAMPTZ DEFAULT now()
);

-- Trigger to populate ts in ticks
CREATE OR REPLACE FUNCTION populate_ticks_ts() RETURNS TRIGGER AS $$
BEGIN
    NEW.ts := to_timestamp(NEW.ts_ms / 1000.0);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_populate_ticks_ts ON ticks;
CREATE TRIGGER trg_populate_ticks_ts
BEFORE INSERT OR UPDATE ON ticks
FOR EACH ROW EXECUTE FUNCTION populate_ticks_ts();

-- FIX: create_hypertable падает если таблица непустая — используем DO/EXCEPTION
DO $$
BEGIN
    PERFORM create_hypertable('ticks', 'ts', if_not_exists => TRUE);
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'create_hypertable(ticks): % — пропущено', SQLERRM;
END$$;

CREATE INDEX IF NOT EXISTS idx_ticks_symbol_ts
    ON ticks(symbol, ts);

-- ============================================================================
-- Table: daily_metrics
-- ============================================================================
-- Aggregated daily metrics by source/symbol

CREATE TABLE IF NOT EXISTS daily_metrics (
    id                  BIGSERIAL PRIMARY KEY,
    date                DATE NOT NULL,
    source              TEXT,
    symbol              TEXT NOT NULL,

    trades_count        INTEGER DEFAULT 0,
    wins                INTEGER DEFAULT 0,
    losses              INTEGER DEFAULT 0,
    breakeven           INTEGER DEFAULT 0,

    pnl_net_sum         DOUBLE PRECISION DEFAULT 0.0,
    pnl_net_avg         DOUBLE PRECISION DEFAULT 0.0,
    pnl_net_std         DOUBLE PRECISION DEFAULT 0.0,

    expectancy_r        DOUBLE PRECISION DEFAULT 0.0,
    payoff_r            DOUBLE PRECISION DEFAULT 0.0,
    payoff_usd          DOUBLE PRECISION DEFAULT 0.0,
    kelly_r             DOUBLE PRECISION DEFAULT 0.0,

    wr                  DOUBLE PRECISION DEFAULT 0.0,   -- win rate
    sharpe              DOUBLE PRECISION DEFAULT 0.0,
    sortino             DOUBLE PRECISION DEFAULT 0.0,
    mdd_usd             DOUBLE PRECISION DEFAULT 0.0,

    -- baseline vs managed
    wr_fixed            DOUBLE PRECISION DEFAULT 0.0,
    expectancy_fixed_r  DOUBLE PRECISION DEFAULT 0.0,
    payoff_fixed_r      DOUBLE PRECISION DEFAULT 0.0,
    payoff_fixed_usd    DOUBLE PRECISION DEFAULT 0.0,
    delta_expectancy_r  DOUBLE PRECISION DEFAULT 0.0,

    created_at          TIMESTAMPTZ DEFAULT now()
);

-- Unique constraint for daily metrics
CREATE UNIQUE INDEX IF NOT EXISTS daily_metrics_uniq
    ON daily_metrics(date, COALESCE(source, ''), symbol);

-- ============================================================================
-- Table: entry_tag_metrics
-- ============================================================================
-- Aggregated metrics by entry_tag for signal quality analysis

CREATE TABLE IF NOT EXISTS entry_tag_metrics (
    id                      BIGSERIAL PRIMARY KEY,
    date                    DATE NOT NULL,
    source                  TEXT,
    symbol                  TEXT NOT NULL,
    entry_tag               TEXT NOT NULL,

    trades_count            INTEGER DEFAULT 0,
    wins                    INTEGER DEFAULT 0,
    losses                  INTEGER DEFAULT 0,
    breakeven               INTEGER DEFAULT 0,

    pnl_net_sum             DOUBLE PRECISION DEFAULT 0.0,
    pnl_net_avg             DOUBLE PRECISION DEFAULT 0.0,
    expectancy_r            DOUBLE PRECISION DEFAULT 0.0,
    payoff_r                DOUBLE PRECISION DEFAULT 0.0,
    payoff_usd              DOUBLE PRECISION DEFAULT 0.0,
    wr                      DOUBLE PRECISION DEFAULT 0.0,

    -- baseline vs managed
    wr_fixed                DOUBLE PRECISION DEFAULT 0.0,
    expectancy_fixed_r      DOUBLE PRECISION DEFAULT 0.0,
    payoff_fixed_r          DOUBLE PRECISION DEFAULT 0.0,
    payoff_fixed_usd        DOUBLE PRECISION DEFAULT 0.0,
    delta_expectancy_r      DOUBLE PRECISION DEFAULT 0.0,

    -- giveback/missed
    giveback_avg_usd        DOUBLE PRECISION DEFAULT 0.0,
    giveback_avg_r          DOUBLE PRECISION DEFAULT 0.0,
    giveback_avg_ratio      DOUBLE PRECISION DEFAULT 0.0,
    giveback_share          DOUBLE PRECISION DEFAULT 0.0,

    missed_avg_usd          DOUBLE PRECISION DEFAULT 0.0,
    missed_avg_r            DOUBLE PRECISION DEFAULT 0.0,
    missed_avg_ratio        DOUBLE PRECISION DEFAULT 0.0,
    missed_share            DOUBLE PRECISION DEFAULT 0.0,

    -- экскурсии
    mfe_avg_r               DOUBLE PRECISION DEFAULT 0.0,
    mae_avg_r               DOUBLE PRECISION DEFAULT 0.0,

    -- трейлинг
    trailing_share          DOUBLE PRECISION DEFAULT 0.0,
    trailing_close_share    DOUBLE PRECISION DEFAULT 0.0,
    trailing_wr             DOUBLE PRECISION DEFAULT 0.0,
    trailing_expectancy_r   DOUBLE PRECISION DEFAULT 0.0,
    trailing_expectancy_fixed_r DOUBLE PRECISION DEFAULT 0.0,
    trailing_delta_expectancy_r DOUBLE PRECISION DEFAULT 0.0,

    created_at              TIMESTAMPTZ DEFAULT now()
);

-- Unique constraint for entry tag metrics
CREATE UNIQUE INDEX IF NOT EXISTS entry_tag_metrics_uniq
    ON entry_tag_metrics(date, COALESCE(source, ''), symbol, entry_tag);

-- ============================================================================
-- Grant permissions for analytics user (if exists)
-- ============================================================================

-- Grant permissions to trading user if it exists
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'trading') THEN
        GRANT ALL ON SCHEMA public TO trading;
        GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO trading;
        GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO trading;
        ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO trading;
        ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO trading;
    END IF;
END $$;

-- ============================================================================
-- Log completion
-- ============================================================================

SELECT 'Scanner analytics tables created successfully' as status;
