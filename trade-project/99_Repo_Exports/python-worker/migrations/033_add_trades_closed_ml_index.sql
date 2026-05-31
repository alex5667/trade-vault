-- Migration to add a partial index for rapid fetching of ML calibration datasets
CREATE INDEX IF NOT EXISTS idx_trades_closed_tp1_exit_ts
ON trades_closed (exit_ts)
WHERE r_multiple IS NOT NULL AND (tp1_hit = TRUE OR r_multiple > 0);
