-- Migration: Add session/regime fields to signals table and create signal_local_calibration table
-- Run this migration on your PostgreSQL database

-- Add session and regime columns to signals table
ALTER TABLE signals
    ADD COLUMN IF NOT EXISTS session TEXT,
    ADD COLUMN IF NOT EXISTS regime TEXT;

-- Create index for better query performance
CREATE INDEX IF NOT EXISTS idx_signals_session_regime ON signals (symbol, session, regime);
CREATE INDEX IF NOT EXISTS idx_signals_ts_session_regime ON signals (ts_signal, session, regime);

-- Populate session field based on UTC time (Asia: 00:00-08:00, Europe: 08:00-16:00, US: 16:00-24:00)
UPDATE signals
SET session = CASE
    WHEN (EXTRACT(HOUR FROM ts_signal AT TIME ZONE 'UTC') BETWEEN 0 AND 7) THEN 'asia'
    WHEN (EXTRACT(HOUR FROM ts_signal AT TIME ZONE 'UTC') BETWEEN 8 AND 15) THEN 'europe'
    ELSE 'us'
END
WHERE session IS NULL;

-- Set default regime to 'mixed' where NULL
UPDATE signals
SET regime = 'mixed'
WHERE regime IS NULL;

-- Create signal_local_calibration table
CREATE TABLE IF NOT EXISTS signal_local_calibration (
    symbol          TEXT NOT NULL,
    session         TEXT NOT NULL,
    regime          TEXT NOT NULL,
    metric          TEXT NOT NULL,   -- 'delta_spike_z', 'obi', 'weak_progress', ...

    q90             DOUBLE PRECISION,
    q95             DOUBLE PRECISION,
    q98             DOUBLE PRECISION,

    chosen_threshold DOUBLE PRECISION, -- "рабочий" порог по bucket-ам
    count_samples   BIGINT NOT NULL,
    cdf_points      JSONB NOT NULL,   -- [{ "value": ..., "q": ... }, ...]

    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (symbol, session, regime, metric)
);

-- Create indexes for fast lookups
CREATE INDEX IF NOT EXISTS idx_signal_local_calibration_lookup
ON signal_local_calibration (symbol, session, regime, metric);

CREATE INDEX IF NOT EXISTS idx_signal_local_calibration_updated
ON signal_local_calibration (updated_at);

-- Add comment to table
COMMENT ON TABLE signal_local_calibration IS 'Local calibration thresholds and CDFs for signal metrics by symbol/session/regime clusters';
