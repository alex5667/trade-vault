-- Add signal quality tables for offline and online quality assessment

-- Offline quality table - historical quality by feature clusters
CREATE TABLE IF NOT EXISTS signal_quality_offline (
    id               BIGSERIAL PRIMARY KEY,
    symbol           TEXT NOT NULL,
    signal_type      TEXT NOT NULL,
    side             TEXT NOT NULL,
    session          TEXT NOT NULL,
    regime           TEXT NOT NULL,
    feature_bucket   TEXT NOT NULL,

    horizon          TEXT NOT NULL DEFAULT 'R_main',

    n_signals        INTEGER NOT NULL,
    win_rate         DOUBLE PRECISION NOT NULL,
    expectancy_r     DOUBLE PRECISION NOT NULL,
    var_r            DOUBLE PRECISION NOT NULL,
    cvar_r           DOUBLE PRECISION NOT NULL,

    quality_score    DOUBLE PRECISION NOT NULL,
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),

    UNIQUE(symbol, signal_type, side, session, regime, feature_bucket, horizon)
);

-- Online quality table - rolling quality by signal type
CREATE TABLE IF NOT EXISTS signal_quality_online (
    id               BIGSERIAL PRIMARY KEY,
    symbol           TEXT NOT NULL,
    signal_type      TEXT NOT NULL,
    side             TEXT NOT NULL,
    horizon          TEXT NOT NULL DEFAULT 'R_main',

    n_recent         INTEGER NOT NULL,
    win_rate_recent  DOUBLE PRECISION NOT NULL,
    expectancy_r_recent DOUBLE PRECISION NOT NULL,
    var_r_recent     DOUBLE PRECISION NOT NULL,
    cvar_r_recent    DOUBLE PRECISION NOT NULL,

    quality_score_online DOUBLE PRECISION NOT NULL,
    status           TEXT NOT NULL DEFAULT 'ok',

    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),

    UNIQUE(symbol, signal_type, side, horizon)
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_signal_quality_offline_lookup
ON signal_quality_offline (symbol, signal_type, side, session, regime, feature_bucket, horizon);

CREATE INDEX IF NOT EXISTS idx_signal_quality_online_lookup
ON signal_quality_online (symbol, signal_type, side, horizon);

CREATE INDEX IF NOT EXISTS idx_signal_quality_offline_updated
ON signal_quality_offline (updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_signal_quality_online_updated
ON signal_quality_online (updated_at DESC);

-- Add required columns to signals table if they don't exist
ALTER TABLE signals
    ADD COLUMN IF NOT EXISTS symbol          TEXT,
    ADD COLUMN IF NOT EXISTS signal_type     TEXT,
    ADD COLUMN IF NOT EXISTS side            TEXT,
    ADD COLUMN IF NOT EXISTS session         TEXT,
    ADD COLUMN IF NOT EXISTS regime          TEXT,
    ADD COLUMN IF NOT EXISTS pnl_r           DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS delta_spike_z   DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS obi             DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS weak_progress   DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS atr_quantile    DOUBLE PRECISION;

-- Comments
COMMENT ON TABLE signal_quality_offline IS 'Offline signal quality assessment by feature clusters';
COMMENT ON TABLE signal_quality_online IS 'Online rolling signal quality assessment by type';
COMMENT ON COLUMN signal_quality_offline.feature_bucket IS 'Feature cluster key (dz:bin|obi:bin|wp:bin|atr:bin)';
COMMENT ON COLUMN signal_quality_online.status IS 'Quality status: ok/degraded/disabled';
