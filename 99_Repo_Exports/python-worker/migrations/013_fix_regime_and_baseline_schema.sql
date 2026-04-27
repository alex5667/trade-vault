-- Migration: Restore Regime Guard and Baseline tables
-- Date: 2026-01-08

\c scanner_analytics;

-- 1. Handle signal_family_baseline schema mismatch
-- The current table has L3-metrics schema, but code expects winrate/expectancy quantiles.
DO $$
BEGIN
    -- If hit_rate column exists, it's the old schema
    IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'signal_family_baseline' AND column_name = 'hit_rate') THEN
        ALTER TABLE signal_family_baseline RENAME TO signal_family_baseline_l3;
        RAISE NOTICE 'Renamed old signal_family_baseline to signal_family_baseline_l3';
    END IF;
END $$;

-- 2. Create the correct signal_family_baseline table
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

CREATE INDEX IF NOT EXISTS idx_signal_family_baseline_lookup
ON signal_family_baseline (symbol, family, metric);

-- 3. Create signal_exec_summary (source data for baseline)
CREATE TABLE IF NOT EXISTS signal_exec_summary (
    signal_id      BIGINT PRIMARY KEY,
    symbol         TEXT        NOT NULL,
    family         TEXT        NOT NULL,
    opened_at      TIMESTAMPTZ NOT NULL,
    closed_at      TIMESTAMPTZ NOT NULL,

    result_r       DOUBLE PRECISION NOT NULL,
    mfe_r          DOUBLE PRECISION,
    mae_r          DOUBLE PRECISION,

    ttd_sec        DOUBLE PRECISION,
    extra_json     JSONB
);

CREATE INDEX IF NOT EXISTS idx_signal_exec_summary_lookup
ON signal_exec_summary (symbol, family, opened_at);

-- 4. Create signal_family_regime_state
CREATE TABLE IF NOT EXISTS signal_family_regime_state (
    ts_state       TIMESTAMPTZ NOT NULL,
    family         TEXT        NOT NULL,
    venue          TEXT        NOT NULL,
    symbol         TEXT        NOT NULL,
    timeframe      TEXT        NOT NULL,

    status         TEXT        NOT NULL, -- 'active' | 'degraded' | 'disabled'

    wr_window      DOUBLE PRECISION,
    exp_r_window   DOUBLE PRECISION,
    dd_r_window    DOUBLE PRECISION,
    trades_window  INTEGER,

    reason         TEXT,
    disable_until  TIMESTAMPTZ,
    threshold_mult DOUBLE PRECISION,

    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Convert to hypertable if TimescaleDB is available
DO $$
BEGIN
    PERFORM create_hypertable('signal_family_regime_state', 'ts_state', if_not_exists => TRUE);
EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'TimescaleDB not available for signal_family_regime_state, skipping hypertable';
END $$;

CREATE INDEX IF NOT EXISTS idx_signal_family_regime_state_lookup
ON signal_family_regime_state (family, venue, symbol, timeframe, ts_state DESC);

-- 5. Final grants
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'trading') THEN
        GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO trading;
        GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO trading;
    END IF;
END $$;
