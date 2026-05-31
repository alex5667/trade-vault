-- Phase 0.3: horizon scalars as first-class columns in trades_closed
-- Migration: 20260416_phase0_3_horizon_scalars_v1.sql
-- Safe to run multiple times (IF NOT EXISTS / ADD COLUMN IF NOT EXISTS).
-- Columns prefixed sc_* to avoid naming conflicts with existing JSONB-derived columns.
-- Idempotent: ADD COLUMN IF NOT EXISTS is safe on re-run.

BEGIN;

ALTER TABLE trades_closed
    ADD COLUMN IF NOT EXISTS sc_contract_ver       SMALLINT,
    ADD COLUMN IF NOT EXISTS sc_risk_horizon_bucket TEXT,
    ADD COLUMN IF NOT EXISTS sc_hold_target_ms      BIGINT,
    ADD COLUMN IF NOT EXISTS sc_alpha_half_life_ms  BIGINT,
    ADD COLUMN IF NOT EXISTS sc_max_signal_age_ms   BIGINT,
    ADD COLUMN IF NOT EXISTS sc_atr_age_ms          BIGINT,
    ADD COLUMN IF NOT EXISTS sc_atr_source          TEXT,
    ADD COLUMN IF NOT EXISTS sc_atr_pct             DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS sc_vol_ratio_fast_slow DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS sc_vol_ratio_z         DOUBLE PRECISION;

-- Composite index for post-trade analytics: symbol × horizon bucket × exit time
CREATE INDEX IF NOT EXISTS idx_trades_closed_symbol_sc_hbucket_exit_ts
    ON trades_closed (symbol, sc_risk_horizon_bucket, exit_ts_ms DESC);

-- Index for ATR timeframe slicing (atr_tf_ms already exists as extracted scalar from JSONB)
-- This index covers the new sc_* path for batch writer (does not replace the existing atr_tf_ms col)
CREATE INDEX IF NOT EXISTS idx_trades_closed_symbol_sc_atr_source_exit_ts
    ON trades_closed (symbol, sc_atr_source, exit_ts_ms DESC);

COMMIT;

-- Verify (run manually after migration):
-- SELECT column_name, data_type
-- FROM information_schema.columns
-- WHERE table_name = 'trades_closed'
--   AND column_name LIKE 'sc_%'
-- ORDER BY column_name;
