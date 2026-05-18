-- Migration: Create regime_snapshot table
-- Date: 2026-01-08

CREATE TABLE IF NOT EXISTS regime_snapshot (
    symbol           TEXT        NOT NULL,
    timeframe        TEXT        NOT NULL,
    ts               TIMESTAMPTZ NOT NULL,
    
    adx              DOUBLE PRECISION,
    "atrPct"         DOUBLE PRECISION,
    regime           TEXT,
    trend_score      DOUBLE PRECISION DEFAULT 0.0,
    range_score      DOUBLE PRECISION DEFAULT 0.0,
    atr_value        DOUBLE PRECISION,
    atr_quantile     DOUBLE PRECISION,
    volatility_state TEXT,
    is_trending      BOOLEAN DEFAULT FALSE,
    
    created_at       TIMESTAMPTZ DEFAULT now(),
    
    PRIMARY KEY (symbol, timeframe, ts)
);

-- Convert to hypertable if TimescaleDB is available
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'timescaledb') THEN
        PERFORM create_hypertable('regime_snapshot', 'ts', if_not_exists => TRUE);
    END IF;
EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'Failed to create hypertable for regime_snapshot: %', SQLERRM;
END $$;

-- Create index with error handling for permission issues
DO $$
BEGIN
    CREATE INDEX IF NOT EXISTS idx_regime_snapshot_lookup
    ON regime_snapshot (symbol, timeframe, ts DESC);
EXCEPTION
    WHEN insufficient_privilege THEN
        RAISE NOTICE 'Cannot create index idx_regime_snapshot_lookup: insufficient privileges. Table may need ownership transfer.';
    WHEN OTHERS THEN
        RAISE NOTICE 'Error creating index idx_regime_snapshot_lookup: %', SQLERRM;
END $$;

-- Grant access to all service users
GRANT ALL PRIVILEGES ON TABLE regime_snapshot TO trading;
GRANT ALL PRIVILEGES ON TABLE regime_snapshot TO scanner;
GRANT ALL PRIVILEGES ON TABLE regime_snapshot TO trade_user;
GRANT ALL PRIVILEGES ON SEQUENCE regime_snapshot_id_seq TO trading;
GRANT ALL PRIVILEGES ON SEQUENCE regime_snapshot_id_seq TO scanner;
GRANT ALL PRIVILEGES ON SEQUENCE regime_snapshot_id_seq TO trade_user;
