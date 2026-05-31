-- Migration 030: Create Archive Tables for Candles and ATR
-- Purpose: Store historical candles and ATR data from Redis to PostgreSQL
-- Uses TimescaleDB for time-series optimization

-- Enable TimescaleDB extension if not already enabled
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- ============================================================================
-- 1. Candles Archive Table
-- ============================================================================
CREATE TABLE IF NOT EXISTS candles_archive (
    id BIGSERIAL,
    symbol VARCHAR(20) NOT NULL,
    timeframe VARCHAR(10) NOT NULL,
    open_time TIMESTAMPTZ NOT NULL,
    close_time TIMESTAMPTZ NOT NULL,
    ts TIMESTAMPTZ NOT NULL,
    open NUMERIC(20, 8) NOT NULL,
    high NUMERIC(20, 8) NOT NULL,
    low NUMERIC(20, 8) NOT NULL,
    close NUMERIC(20, 8) NOT NULL,
    volume NUMERIC(20, 8),
    quote_volume NUMERIC(20, 8),
    trades INTEGER,
    taker_buy_base NUMERIC(20, 8),
    taker_buy_quote NUMERIC(20, 8),
    archived_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (symbol, timeframe, open_time)
);

-- Convert to hypertable (TimescaleDB)
SELECT create_hypertable('candles_archive', 'open_time', 
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

-- Compression policy for data older than 7 days
ALTER TABLE candles_archive SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol,timeframe'
);

SELECT add_compression_policy('candles_archive', 
    compress_after => INTERVAL '7 days',
    if_not_exists => TRUE
);

-- Retention policy: 30 days for 1m candles
SELECT add_retention_policy('candles_archive', 
    drop_after => INTERVAL '30 days',
    if_not_exists => TRUE
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_candles_symbol_tf_time 
    ON candles_archive (symbol, timeframe, open_time DESC);
CREATE INDEX IF NOT EXISTS idx_candles_archive_ts
    ON candles_archive (ts DESC) WHERE ts IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_candles_archived_at 
    ON candles_archive (archived_at) WHERE archived_at IS NOT NULL;

CREATE OR REPLACE FUNCTION set_candles_archive_ts()
RETURNS TRIGGER AS $$
BEGIN
    NEW.ts := NEW.close_time;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_set_candles_archive_ts ON candles_archive;
CREATE TRIGGER trg_set_candles_archive_ts
BEFORE INSERT OR UPDATE OF close_time, ts ON candles_archive
FOR EACH ROW
EXECUTE FUNCTION set_candles_archive_ts();


-- ============================================================================
-- 2. ATR Archive Table
-- ============================================================================
CREATE TABLE IF NOT EXISTS atr_archive (
    id BIGSERIAL,
    symbol VARCHAR(20) NOT NULL,
    timeframe VARCHAR(10) NOT NULL,
    atr NUMERIC(20, 8) NOT NULL,
    period INTEGER DEFAULT 14,
    close_price NUMERIC(20, 8),
    ts TIMESTAMPTZ NOT NULL,
    count INTEGER,  -- Number of bars used for calculation
    source VARCHAR(20) DEFAULT 'py',  -- py/go/cache
    archived_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (symbol, timeframe, ts)
);

-- Convert to hypertable
SELECT create_hypertable('atr_archive', 'ts', 
    chunk_time_interval => INTERVAL '7 days',
    if_not_exists => TRUE
);

-- Compression for data older than 30 days
ALTER TABLE atr_archive SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol,timeframe'
);

SELECT add_compression_policy('atr_archive', 
    compress_after => INTERVAL '30 days',
    if_not_exists => TRUE
);

-- Retention policy: 365 days for ATR (longer for analysis)
SELECT add_retention_policy('atr_archive', 
    drop_after => INTERVAL '365 days',
    if_not_exists => TRUE
);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_atr_symbol_tf_ts 
    ON atr_archive (symbol, timeframe, ts DESC);

CREATE INDEX IF NOT EXISTS idx_atr_by_symbol 
    ON atr_archive (symbol, ts DESC);

-- ============================================================================
-- 3. Archive Metadata Table (track archiving progress)
-- ============================================================================
CREATE TABLE IF NOT EXISTS archive_metadata (
    stream_name VARCHAR(100) PRIMARY KEY,
    last_archived_id VARCHAR(100),
    last_archived_at TIMESTAMPTZ,
    records_archived BIGINT DEFAULT 0,
    last_error TEXT,
    last_error_at TIMESTAMPTZ
);

-- Insert initial metadata
INSERT INTO archive_metadata (stream_name)
VALUES ('candles:data')
ON CONFLICT (stream_name) DO NOTHING;

-- ============================================================================
-- 4. Comments
-- ============================================================================
COMMENT ON TABLE candles_archive IS 'Historical candles data archived from Redis stream';
COMMENT ON COLUMN candles_archive.ts IS 'Canonical candle event timestamp for freshness checks; mirrors close_time in UTC.';
COMMENT ON TABLE atr_archive IS 'Historical ATR calculations archived from Redis keys';
COMMENT ON TABLE archive_metadata IS 'Track archiving progress and errors';

-- ============================================================================
-- 5. Grants
-- ============================================================================
GRANT SELECT, INSERT ON candles_archive TO trading;
GRANT SELECT, INSERT ON atr_archive TO trading;
GRANT ALL ON archive_metadata TO trading;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO trading;

-- Success message
DO $$
BEGIN
    RAISE NOTICE '✅ Archive tables created successfully with TimescaleDB optimization';
    RAISE NOTICE '   - candles_archive: 30 days retention, 7 days compression';
    RAISE NOTICE '   - atr_archive: 365 days retention, 30 days compression';
END $$;
