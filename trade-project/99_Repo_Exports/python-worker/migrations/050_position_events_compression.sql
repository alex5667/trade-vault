-- Migration 050: Enable compression policy for position_events hypertable
-- This was previously commented out in migration 027.
-- Without compression, position_events grows unbounded on disk.

-- Enable compression with segment_by on symbol for efficient queries
ALTER TABLE position_events SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'symbol',
    timescaledb.compress_orderby = 'ts DESC'
);

-- Compress chunks older than 7 days
SELECT add_compression_policy('position_events', INTERVAL '7 days', if_not_exists => TRUE);
