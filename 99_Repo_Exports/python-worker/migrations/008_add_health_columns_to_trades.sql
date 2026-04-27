-- Migration: Add health columns to trades_closed
-- Description: Adds missing health metrics columns that are required by the python-worker
-- Date: 2025-12-29

ALTER TABLE trades_closed ADD COLUMN IF NOT EXISTS health_l2_stale_ratio_tick DOUBLE PRECISION;
ALTER TABLE trades_closed ADD COLUMN IF NOT EXISTS health_l2_stale_ratio_now DOUBLE PRECISION;
ALTER TABLE trades_closed ADD COLUMN IF NOT EXISTS health_avg_l2_age_ms DOUBLE PRECISION;
ALTER TABLE trades_closed ADD COLUMN IF NOT EXISTS health_avg_l2_age_tick_ms DOUBLE PRECISION;
ALTER TABLE trades_closed ADD COLUMN IF NOT EXISTS health_signal_emit_rate DOUBLE PRECISION;
ALTER TABLE trades_closed ADD COLUMN IF NOT EXISTS health_dlq_rate DOUBLE PRECISION;
