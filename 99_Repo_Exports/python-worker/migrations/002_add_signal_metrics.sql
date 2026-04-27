-- Add signal metrics columns to signals table for local calibration

ALTER TABLE signals
    ADD COLUMN IF NOT EXISTS delta_spike_z   DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS obi             DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS weak_progress   DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS atr_quantile    DOUBLE PRECISION;

-- Add indexes for better query performance on calibration
CREATE INDEX IF NOT EXISTS idx_signals_metrics ON signals (symbol, session, regime, ts_signal)
    WHERE delta_spike_z IS NOT NULL OR obi IS NOT NULL OR weak_progress IS NOT NULL OR atr_quantile IS NOT NULL;

-- Add comment
COMMENT ON COLUMN signals.delta_spike_z IS 'Delta spike Z-score for signal strength';
COMMENT ON COLUMN signals.obi IS 'Order Book Imbalance metric';
COMMENT ON COLUMN signals.weak_progress IS 'Weak progress indicator (range vs ATR)';
COMMENT ON COLUMN signals.atr_quantile IS 'ATR quantile for volatility assessment';
