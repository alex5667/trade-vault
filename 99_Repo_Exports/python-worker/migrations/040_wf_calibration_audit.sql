-- Migration 040: Walk-Forward Calibration Audit Table
-- Stores full OOS results from walk-forward calibration runs.
-- Enables post-hoc analysis of calibration stability across time.

CREATE TABLE IF NOT EXISTS wf_calibration_audit (
    id BIGSERIAL,
    symbol TEXT NOT NULL,
    source TEXT NOT NULL,
    param_name TEXT NOT NULL,          -- 'trailing_tp1_offset' | 'delta_z' | 'obi' etc
    robust_value DOUBLE PRECISION,     -- median of stable OOS folds
    stability_score DOUBLE PRECISION,  -- std(oos_sharpe) across folds (lower = better)
    n_folds INT NOT NULL DEFAULT 0,
    n_stable_folds INT NOT NULL DEFAULT 0,
    mean_oos_sharpe DOUBLE PRECISION,
    overfit_ratio DOUBLE PRECISION,    -- mean(train_score) / mean(oos_score)
    deployed BOOLEAN NOT NULL DEFAULT FALSE,
    fold_details JSONB,                -- full OOS results per fold
    computed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- TimescaleDB hypertable for time-series queries
SELECT create_hypertable('wf_calibration_audit', 'computed_at', if_not_exists => TRUE);

-- Indexes for common access patterns
CREATE INDEX IF NOT EXISTS idx_wf_calib_symbol_ts
    ON wf_calibration_audit (symbol, computed_at DESC);
CREATE INDEX IF NOT EXISTS idx_wf_calib_param_ts
    ON wf_calibration_audit (param_name, computed_at DESC);
CREATE INDEX IF NOT EXISTS idx_wf_calib_deployed
    ON wf_calibration_audit (deployed, computed_at DESC);

COMMENT ON TABLE wf_calibration_audit IS
    'Walk-Forward calibration audit trail. Each row = one calibration run per symbol/param.';
COMMENT ON COLUMN wf_calibration_audit.stability_score IS
    'std(oos_sharpe) across folds. Values < 0.5 indicate stable calibration.';
COMMENT ON COLUMN wf_calibration_audit.overfit_ratio IS
    'mean(train_score)/mean(oos_score). Values > 1.5 indicate significant overfitting.';
