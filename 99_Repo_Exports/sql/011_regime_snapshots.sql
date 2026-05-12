-- Migration: 011_regime_snapshots
-- Description: Create TimescaleDB hypertable for storing regime transitions and snapshots for post-trade analysis.

CREATE TABLE IF NOT EXISTS regime_snapshots (
    ts TIMESTAMPTZ NOT NULL,
    symbol VARCHAR(32) NOT NULL,
    label VARCHAR(32) NOT NULL,
    direction SMALLINT NOT NULL,
    score DOUBLE PRECISION NOT NULL,
    confidence DOUBLE PRECISION NOT NULL,
    ts_calc_ms BIGINT DEFAULT 0,        -- epoch_ms when regime was calculated (RegimeSnapshot.ts_calc_ms)
    schema_ver INT DEFAULT 1,
    source VARCHAR(64) DEFAULT 'market_regime_service',
    features JSONB
);

-- Convert to hypertable partitioned by ts and symbol
SELECT create_hypertable('regime_snapshots', 'ts', 'symbol', 4, if_not_exists => TRUE);

-- Create index on symbol and time
CREATE INDEX IF NOT EXISTS regime_snapshots_sym_ts_idx ON regime_snapshots (symbol, ts DESC);

-- Create a continuous aggregate for hourly regime statistics
CREATE MATERIALIZED VIEW IF NOT EXISTS regime_snapshots_1h
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', ts) AS bucket,
    symbol,
    label,
    count(*) AS transition_count,
    avg(score) AS avg_score,
    avg(confidence) AS avg_confidence,
    min(score) AS min_score,
    max(score) AS max_score
FROM regime_snapshots
GROUP BY bucket, symbol, label;

-- Add continuous aggregate policy (refresh every 10 minutes)
SELECT add_continuous_aggregate_policy('regime_snapshots_1h',
    start_offset => INTERVAL '3 hours',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '10 minutes');

-- Add retention policy to raw hypertable (keep raw data for 30 days)
SELECT add_retention_policy('regime_snapshots', INTERVAL '30 days');

-- Add retention policy to continuous aggregate (keep hourly rollups for 1 year)
SELECT add_retention_policy('regime_snapshots_1h', INTERVAL '1 year');
