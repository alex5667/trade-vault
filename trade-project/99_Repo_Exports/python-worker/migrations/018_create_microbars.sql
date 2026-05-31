-- Migration: Create microbars table
-- Description: Creates microbars table for historical warmup (Hypertable candidate)
-- Date: 2026-01-17
-- NOTE: \c scanner_analytics removed — migration-runner connects via PG_DSN directly

-- Create microbars table
CREATE TABLE IF NOT EXISTS microbars (
    symbol          TEXT NOT NULL,
    ts_ms           BIGINT NOT NULL,
    o               DOUBLE PRECISION NOT NULL,
    h               DOUBLE PRECISION NOT NULL,
    l               DOUBLE PRECISION NOT NULL,
    c               DOUBLE PRECISION NOT NULL,
    v               DOUBLE PRECISION NOT NULL,
    cvd             DOUBLE PRECISION NOT NULL,
    inserted_at     TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY(symbol, ts_ms)
);

-- Create index for faster lookups
CREATE INDEX IF NOT EXISTS idx_microbars_ts ON microbars (ts_ms DESC);

-- Log completion
SELECT 'Microbars table created successfully' as status;
