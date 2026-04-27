-- Phase 2.4E: live surface A/B analytics columns in trades_closed
-- Safe: ADD COLUMN IF NOT EXISTS, no table lock on live data beyond brief metadata lock.
-- Rollback: DROP COLUMN IF EXISTS (loses analytics data; trading is unaffected).

BEGIN;

ALTER TABLE trades_closed
    ADD COLUMN IF NOT EXISTS live_surface_applied      BOOLEAN,
    ADD COLUMN IF NOT EXISTS live_surface_reason_code  TEXT,
    ADD COLUMN IF NOT EXISTS baseline_sl_price         DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS baseline_tp1_price        DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS selected_sl_price         DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS selected_tp1_price        DOUBLE PRECISION;

-- Index for A/B analytics: applied vs not, by symbol and time
CREATE INDEX IF NOT EXISTS idx_trades_closed_live_surface_applied_exit_ts
    ON trades_closed (symbol, live_surface_applied, exit_ts_ms DESC);

COMMIT;

-- Verify (run manually after migration):
-- SELECT column_name, data_type FROM information_schema.columns
-- WHERE table_name = 'trades_closed'
--   AND column_name LIKE '%live_surface%' OR column_name LIKE '%baseline_%' OR column_name LIKE '%selected_%'
-- ORDER BY column_name;
