-- Migration: Create market_regime_snapshots hypertable for regime analytics
-- Down migration is handled externally if needed

-- 1. Create the table
CREATE TABLE IF NOT EXISTS market_regime_snapshots (
    ts_event_ms BIGINT NOT NULL,
    symbol VARCHAR(32) NOT NULL,
    label VARCHAR(32) NOT NULL,
    direction SMALLINT NOT NULL DEFAULT 0,
    score DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    confidence DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    atr_value DOUBLE PRECISION,
    atr_quantile DOUBLE PRECISION,
    adx_value DOUBLE PRECISION,
    adx_quantile DOUBLE PRECISION,
    delta_ema DOUBLE PRECISION,
    vwap_cross_rate DOUBLE PRECISION,
    hold_side_score DOUBLE PRECISION,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
    PRIMARY KEY (ts_event_ms, symbol)
);

-- 2. Convert to TimescaleDB hypertable
-- Chunk time interval of 1 day (86400000 ms) is appropriate for high-frequency regime data
SELECT create_hypertable('market_regime_snapshots', 'ts_event_ms', chunk_time_interval => 86400000, if_not_exists => TRUE);

-- 3. Add compression policy
ALTER TABLE market_regime_snapshots SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol',
    timescaledb.compress_orderby = 'ts_event_ms DESC'
);

-- Compress data older than 7 days (7 * 24 * 60 * 60 * 1000 = 604800000 ms)
SELECT add_compression_policy('market_regime_snapshots', compress_after => 604800000, if_not_exists => TRUE);

-- 4. Create an index for faster querying by symbol
CREATE INDEX IF NOT EXISTS idx_market_regime_snapshots_symbol_ts 
ON market_regime_snapshots(symbol, ts_event_ms DESC);

-- 5. Create a table to track regime transitions (useful for TCA and audits)
CREATE TABLE IF NOT EXISTS market_regime_transitions (
    ts_event_ms BIGINT NOT NULL,
    symbol VARCHAR(32) NOT NULL,
    prev_label VARCHAR(32) NOT NULL,
    next_label VARCHAR(32) NOT NULL,
    reason VARCHAR(64) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
    PRIMARY KEY (ts_event_ms, symbol)
);

SELECT create_hypertable('market_regime_transitions', 'ts_event_ms', chunk_time_interval => 86400000, if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_market_regime_transitions_symbol_ts 
ON market_regime_transitions(symbol, ts_event_ms DESC);
