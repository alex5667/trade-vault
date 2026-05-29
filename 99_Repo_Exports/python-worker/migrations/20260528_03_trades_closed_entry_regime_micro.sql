-- Migration: add entry_regime_micro to trades_closed
-- Added in Phase regime_micro (dual-regime), migration 20260528_03, 2026-05-28
--
-- entry_regime_micro: fast 5-bar micro-regime label at entry time.
--   Values: trend_micro_up | trend_micro_down | range_micro | shock_micro | squeeze_micro | mixed_micro | NULL
--   NULL = not computed yet (pre-migration rows) or REGIME_MICRO_ENABLED=0.
--   Source: bar_processor._update_regime_micro → runtime.last_regime_micro
--           → _publish_of_inputs indicators.regime_micro_1m → signal_payload → here.
-- entry_regime_micro_age_ms: staleness of micro-regime label at emit time (ms).
--   NULL acceptable for shadow-stage rows where age was not yet computed.

ALTER TABLE trades_closed
    ADD COLUMN IF NOT EXISTS entry_regime_micro     TEXT,
    ADD COLUMN IF NOT EXISTS entry_regime_micro_age_ms  INTEGER;

-- Partial index for efficient per-micro-regime analytics
CREATE INDEX IF NOT EXISTS idx_trades_closed_entry_regime_micro
    ON trades_closed (entry_regime_micro)
    WHERE entry_regime_micro IS NOT NULL;
