CREATE TABLE IF NOT EXISTS calibration_state (
    symbol TEXT NOT NULL,
    regime TEXT NOT NULL,
    kind TEXT NOT NULL,
    ts_ms BIGINT NOT NULL,
    state_json JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (symbol, regime, kind)
);

CREATE INDEX IF NOT EXISTS idx_calibration_state_symbol ON calibration_state (symbol);

COMMENT ON TABLE calibration_state IS 'Stores calibration states (e.g. ATR quantiles, book rate stats) per symbol/regime/kind';
