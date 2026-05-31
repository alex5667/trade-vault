-- Signal Execution Database Schema for TimescaleDB
-- Hypertables for signal execution data and performance tracking

-- Enable TimescaleDB extension (run manually if needed)
-- CREATE EXTENSION IF NOT EXISTS timescaledb;

-- ===========================================
-- 1. SIGNALS TABLE (Hypertable by ts_signal)
-- ===========================================
-- Raw signal events with context data

CREATE TABLE IF NOT EXISTS signals (
    signal_id      TEXT        NOT NULL PRIMARY KEY,
    ts_signal      TIMESTAMPTZ NOT NULL,
    symbol         TEXT        NOT NULL,
    setup_type     TEXT        NOT NULL,
    side           TEXT        NOT NULL,

    price_at_signal DOUBLE PRECISION NOT NULL,
    atr_1m          DOUBLE PRECISION NOT NULL,
    final_score     DOUBLE PRECISION NOT NULL,

    -- Experiment layer fields
    experiment_id      TEXT,
    experiment_variant TEXT,

    raw_ctx         JSONB     NOT NULL      -- Full SignalContext in JSON
);

SELECT create_hypertable('signals', 'ts_signal', if_not_exists => TRUE);


-- ===========================================
-- 2. EXECUTION PLAN TABLE
-- ===========================================
-- Detailed execution plans for signals

CREATE TABLE IF NOT EXISTS signal_execution_plan (
    signal_id        TEXT        NOT NULL PRIMARY KEY,
    ts_signal        TIMESTAMPTZ NOT NULL,
    symbol           TEXT        NOT NULL,
    setup_type       TEXT        NOT NULL,
    side             TEXT        NOT NULL,

    entry_zone_low   DOUBLE PRECISION NOT NULL,
    entry_zone_high  DOUBLE PRECISION NOT NULL,
    stop_price       DOUBLE PRECISION NOT NULL,
    tp_levels        DOUBLE PRECISION[] NOT NULL,
    partials         DOUBLE PRECISION[] NOT NULL,

    pos_risk_R       DOUBLE PRECISION NOT NULL,
    risk_usd         DOUBLE PRECISION NOT NULL,
    position_size    DOUBLE PRECISION NOT NULL,

    expiry_bars      INTEGER      NOT NULL,
    created_at       TIMESTAMPTZ  NOT NULL,
    meta             JSONB        NOT NULL DEFAULT '{}'::jsonb
);


-- ===========================================
-- 3. PERFORMANCE TABLE (Hypertable by ts_signal)
-- ===========================================
-- Post-execution performance metrics and TTD analysis

CREATE TABLE IF NOT EXISTS signal_performance (
    signal_id        TEXT        NOT NULL PRIMARY KEY,
    ts_signal        TIMESTAMPTZ NOT NULL,
    symbol           TEXT        NOT NULL,
    setup_type       TEXT        NOT NULL,
    side             TEXT        NOT NULL,

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
    ttd_seconds      INTEGER,

    bars_to_entry    INTEGER,
    bars_to_exit     INTEGER,

    outcome          TEXT        NOT NULL,
    notes            TEXT        NOT NULL DEFAULT '',
    extra            JSONB       NOT NULL DEFAULT '{}'::jsonb
);

SELECT create_hypertable('signal_performance', 'ts_signal', if_not_exists => TRUE);


-- ===========================================
-- 4. TTD CONFIGURATION TABLE
-- ===========================================
-- TTD quantiles and expiry settings by symbol/setup

CREATE TABLE IF NOT EXISTS signal_ttd_config (
    symbol           TEXT        NOT NULL,
    setup_type       TEXT        NOT NULL,
    ttd_target_R     DOUBLE PRECISION NOT NULL DEFAULT 1.0,
    ttd_median_bars  DOUBLE PRECISION NOT NULL,
    ttd_p75_bars     DOUBLE PRECISION NOT NULL,
    recommended_expiry_bars INTEGER NOT NULL,
    updated_at       TIMESTAMPTZ  NOT NULL DEFAULT now(),
    PRIMARY KEY(symbol, setup_type)
);


-- ===========================================
-- INDEXES
-- ===========================================

CREATE INDEX IF NOT EXISTS signals_symbol_ts_idx
    ON signals (symbol, ts_signal DESC);

CREATE INDEX IF NOT EXISTS signals_setup_ts_idx
    ON signals (setup_type, ts_signal DESC);

CREATE INDEX IF NOT EXISTS signal_performance_symbol_setup_signal_ts
    ON signal_performance(symbol, setup_type, ts_signal DESC);


-- ===========================================
-- USEFUL VIEWS
-- ===========================================

-- Unified view of signals, plans, and performance
CREATE OR REPLACE VIEW signal_execution_summary AS
SELECT
    s.signal_id,
    s.ts_signal,
    s.symbol,
    s.setup_type,
    s.side,
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

    tc.recommended_expiry_bars as config_expiry_bars

FROM signals s
LEFT JOIN signal_execution_plan ep ON s.signal_id = ep.signal_id
LEFT JOIN signal_performance sp ON s.signal_id = sp.signal_id
LEFT JOIN signal_ttd_config tc ON s.symbol = tc.symbol AND s.setup_type = tc.setup_type;

COMMENT ON VIEW signal_execution_summary IS 'Unified view of signals, execution plans, and performance metrics';

-- Performance summary by symbol/setup
CREATE OR REPLACE VIEW signal_performance_summary AS
SELECT
    symbol,
    setup_type,
    COUNT(*) as total_signals,
    COUNT(CASE WHEN outcome = 'target_hit' THEN 1 END) as target_hit_count,
    COUNT(CASE WHEN outcome = 'stop_hit' THEN 1 END) as stop_hit_count,
    AVG(realized_R) as avg_realized_r,
    AVG(mfe_R) as avg_mfe_r,
    AVG(mae_R) as avg_mae_r,
    AVG(ttd_bars) as avg_ttd_bars,
    percentile_cont(0.5) WITHIN GROUP (ORDER BY ttd_bars) as median_ttd_bars
FROM signal_performance
WHERE ts_signal >= now() - INTERVAL '30 days'
GROUP BY symbol, setup_type
HAVING COUNT(*) >= 10;

COMMENT ON VIEW signal_performance_summary IS 'Performance summary by symbol and setup type';


-- ===========================================
-- UTILITY FUNCTIONS
-- ===========================================

-- Get TTD config for a symbol/setup
CREATE OR REPLACE FUNCTION get_ttd_config(p_symbol TEXT, p_setup_type TEXT)
RETURNS TABLE (
    ttd_median_bars DOUBLE PRECISION,
    ttd_p75_bars DOUBLE PRECISION,
    recommended_expiry_bars INTEGER
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        tc.ttd_median_bars,
        tc.ttd_p75_bars,
        tc.recommended_expiry_bars
    FROM signal_ttd_config tc
    WHERE tc.symbol = p_symbol AND tc.setup_type = p_setup_type;
END;
$$ LANGUAGE plpgsql;


-- ===========================================
-- SAMPLE TTD UPDATE QUERY
-- ===========================================
-- Run periodically (e.g., daily) to update expiry settings

/*
-- Update TTD configuration from recent performance data
INSERT INTO signal_ttd_config(symbol, setup_type, ttd_target_R, ttd_median_bars, ttd_p75_bars, recommended_expiry_bars)
SELECT
    symbol,
    setup_type,
    1.0 as ttd_target_R,
    percentile_cont(0.5) WITHIN GROUP (ORDER BY ttd_bars) AS ttd_median_bars,
    percentile_cont(0.75) WITHIN GROUP (ORDER BY ttd_bars) AS ttd_p75_bars,
    ceil(percentile_cont(0.75) WITHIN GROUP (ORDER BY ttd_bars))::int AS recommended_expiry_bars
FROM signal_performance
WHERE
    -- Only consider signals that reached 1R target
    mfe_R >= 1.0
    -- Have valid TTD data
    AND ttd_bars IS NOT NULL
    -- Recent data (last 60 days)
    AND ts_signal >= now() - INTERVAL '60 days'
    -- Minimum sample size per group
    AND (symbol, setup_type) IN (
        SELECT symbol, setup_type
        FROM signal_performance
        WHERE ts_signal >= now() - INTERVAL '60 days'
        GROUP BY symbol, setup_type
        HAVING COUNT(*) >= 10  -- At least 10 signals per group
    )
GROUP BY symbol, setup_type
HAVING COUNT(*) >= 10
ON CONFLICT (symbol, setup_type) DO UPDATE SET
    ttd_median_bars = EXCLUDED.ttd_median_bars,
    ttd_p75_bars = EXCLUDED.ttd_p75_bars,
    recommended_expiry_bars = EXCLUDED.recommended_expiry_bars,
    updated_at = now();
*/


-- ===========================================
-- TABLE COMMENTS
-- ===========================================

COMMENT ON TABLE signals IS 'Raw signal events with context data (hypertable by ts_signal)';
COMMENT ON TABLE signal_execution_plan IS 'Detailed execution plans for signals with risk management';
COMMENT ON TABLE signal_performance IS 'Post-execution performance metrics and TTD analysis (hypertable by ts_signal)';
COMMENT ON TABLE signal_ttd_config IS 'TTD quantiles and expiry settings by symbol/setup combination';