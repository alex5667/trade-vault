-- Signal Execution Tables Migration
-- Creates tables for execution planning and performance tracking

-- Enable TimescaleDB extension if not already enabled (fail-safe)
DO $$ 
BEGIN 
    CREATE EXTENSION IF NOT EXISTS timescaledb; 
EXCEPTION WHEN OTHERS THEN 
    RAISE NOTICE 'TimescaleDB extension not available, skipping...'; 
END $$;

-- 1. Таблица signals (hypertable по ts_signal)
-- Факт появления сигнала
CREATE TABLE IF NOT EXISTS signals (
    signal_id       UUID PRIMARY KEY,
    ts_signal       TIMESTAMPTZ NOT NULL,
    symbol          TEXT        NOT NULL,
    side            TEXT        NOT NULL, -- 'long' / 'short'
    setup_type      TEXT        NOT NULL,
    price_at_signal DOUBLE PRECISION NOT NULL,
    final_score     DOUBLE PRECISION NOT NULL,
    atr_1m          DOUBLE PRECISION,
    atr_5m          DOUBLE PRECISION,
    tick_size       DOUBLE PRECISION,
    contract_size   DOUBLE PRECISION,
    extra_json      JSONB DEFAULT '{}'::jsonb  -- микроструктурные детали, L2, etc.
);

-- Convert to hypertable (chunk by 7 days)
DO $$ 
BEGIN 
    PERFORM create_hypertable('signals', 'ts_signal', chunk_time_interval => INTERVAL '7 days', if_not_exists => TRUE);
EXCEPTION WHEN OTHERS THEN 
    RAISE NOTICE 'Could not create hypertable for signals, skipping...'; 
END $$;

-- Indexes for signals
CREATE INDEX IF NOT EXISTS idx_signals_symbol_ts ON signals (symbol, ts_signal DESC);
CREATE INDEX IF NOT EXISTS idx_signals_setup_ts ON signals (setup_type, ts_signal DESC);
CREATE INDEX IF NOT EXISTS idx_signals_symbol_setup_ts ON signals (symbol, setup_type, ts_signal DESC);

-- 2. Таблица signal_execution_plan
-- Детальный план исполнения
CREATE TABLE IF NOT EXISTS signal_execution_plan (
    signal_id        UUID PRIMARY KEY REFERENCES signals(signal_id) ON DELETE CASCADE,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    entry_zone_low   DOUBLE PRECISION NOT NULL,
    entry_zone_high  DOUBLE PRECISION NOT NULL,
    stop_price       DOUBLE PRECISION NOT NULL,
    tp_levels        DOUBLE PRECISION[] NOT NULL,
    partials         DOUBLE PRECISION[] NOT NULL,
    pos_risk_R       DOUBLE PRECISION NOT NULL,
    risk_usd         DOUBLE PRECISION NOT NULL,
    position_size    DOUBLE PRECISION NOT NULL,
    expiry_bars      INTEGER NOT NULL
);

-- Index for execution plans
CREATE INDEX IF NOT EXISTS idx_execution_plan_risk ON signal_execution_plan (risk_usd);

-- 3. Таблица signal_performance (hypertable по ts_signal)
-- Агрегированные метрики ex-post (TTD, MFE/MAE, realized_R)
CREATE TABLE IF NOT EXISTS signal_performance (
    signal_id        UUID PRIMARY KEY REFERENCES signals(signal_id) ON DELETE CASCADE,
    ts_signal        TIMESTAMPTZ NOT NULL,
    symbol           TEXT        NOT NULL,
    side             TEXT        NOT NULL,
    setup_type       TEXT        NOT NULL,

    ts_entry         TIMESTAMPTZ,
    ts_exit          TIMESTAMPTZ,

    price_at_signal  DOUBLE PRECISION NOT NULL,
    entry_price      DOUBLE PRECISION,
    exit_price       DOUBLE PRECISION,
    stop_price       DOUBLE PRECISION,

    realized_R       DOUBLE PRECISION,
    mfe_R            DOUBLE PRECISION,
    mae_R            DOUBLE PRECISION,

    ttd_bars         INTEGER,
    ttd_seconds      DOUBLE PRECISION,
    outcome          TEXT NOT NULL, -- 'realized', 'stopped', 'expired', 'no_entry', ...

    bars_to_entry    INTEGER,
    bars_to_exit     INTEGER,
    notes            TEXT
);

-- Convert to hypertable (chunk by 30 days)
DO $$ 
BEGIN 
    PERFORM create_hypertable('signal_performance', 'ts_signal', chunk_time_interval => INTERVAL '30 days', if_not_exists => TRUE);
EXCEPTION WHEN OTHERS THEN 
    RAISE NOTICE 'Could not create hypertable for signal_performance, skipping...'; 
END $$;

-- Indexes for signal_performance
CREATE INDEX IF NOT EXISTS idx_performance_symbol_setup_ts ON signal_performance (symbol, setup_type, ts_signal DESC);
CREATE INDEX IF NOT EXISTS idx_performance_setup_ttd ON signal_performance (setup_type, ttd_bars);
CREATE INDEX IF NOT EXISTS idx_performance_symbol_outcome ON signal_performance (symbol, outcome);

-- 4. Таблица signal_ttd_config (квантили и expiry)
-- Сконденсированные TTD-квантили/expiry по инструменту и сетапу
CREATE TABLE IF NOT EXISTS signal_ttd_config (
    symbol         TEXT      NOT NULL,
    setup_type     TEXT      NOT NULL,
    ttd_q50_bars   INTEGER   NOT NULL,
    ttd_q75_bars   INTEGER   NOT NULL,
    ttd_q90_bars   INTEGER   NOT NULL,
    expiry_bars    INTEGER   NOT NULL, -- что вы реально используете (обычно q75 или q80)
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (symbol, setup_type)
);

-- Add comments for documentation
COMMENT ON TABLE signals IS 'Base signal events with context data';
COMMENT ON TABLE signal_execution_plan IS 'Detailed execution plans for signals';
COMMENT ON TABLE signal_performance IS 'Post-execution performance metrics and TTD analysis';
COMMENT ON TABLE signal_ttd_config IS 'TTD quantiles and expiry settings by symbol/setup';

-- Create a view for easy querying of signal chains
CREATE OR REPLACE VIEW signal_execution_summary AS
SELECT
    s.signal_id,
    s.symbol,
    s.side,
    s.setup_type,
    s.ts_signal,
    s.price_at_signal,
    s.final_score,

    ep.entry_zone_low,
    ep.entry_zone_high,
    ep.stop_price,
    ep.tp_levels,
    ep.position_size,
    ep.expiry_bars,

    sp.ts_entry,
    sp.ts_exit,
    sp.entry_price,
    sp.exit_price,
    sp.realized_R,
    sp.mfe_R,
    sp.mae_R,
    sp.ttd_bars,
    sp.ttd_seconds,
    sp.outcome,

    tc.expiry_bars as config_expiry_bars

FROM signals s
LEFT JOIN signal_execution_plan ep ON s.signal_id = ep.signal_id
LEFT JOIN signal_performance sp ON s.signal_id = sp.signal_id
LEFT JOIN signal_ttd_config tc ON s.symbol = tc.symbol AND s.setup_type = tc.setup_type;

COMMENT ON VIEW signal_execution_summary IS 'Unified view of signals, plans, and performance';
