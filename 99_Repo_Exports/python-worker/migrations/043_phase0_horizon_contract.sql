-- migrations/043_phase0_horizon_contract.sql
--
-- Phase 0: Horizon-aware contract columns for open_positions and closed_trades.
--
-- Purpose:
--   Phase 1 depends on historical horizon snapshots for calibration/holding analysis.
--   These columns must exist from Phase 0 so that replay history is not gapped.
--
-- Safety guarantees:
--   - All columns are nullable (ADD COLUMN IF NOT EXISTS).
--   - No existing columns are renamed or dropped.
--   - No DEFAULT values that could mask missing data.
--   - Indexes are partial/conditional to minimize write amplification.
--
-- Rollback: simply drop the added columns (no trading logic depends on them).

-- ─── open_positions ──────────────────────────────────────────────────────────

ALTER TABLE open_positions
  ADD COLUMN IF NOT EXISTS contract_ver         SMALLINT,
  ADD COLUMN IF NOT EXISTS hold_target_ms       BIGINT,
  ADD COLUMN IF NOT EXISTS alpha_half_life_ms   BIGINT,
  ADD COLUMN IF NOT EXISTS max_signal_age_ms    BIGINT,
  ADD COLUMN IF NOT EXISTS risk_horizon_bucket  TEXT,
  ADD COLUMN IF NOT EXISTS horizon_profile_source TEXT,
  ADD COLUMN IF NOT EXISTS horizon_profile_conf DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS horizon_reason_code  TEXT,
  ADD COLUMN IF NOT EXISTS atr_mode             TEXT,
  ADD COLUMN IF NOT EXISTS atr_value            DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS atr_tf_ms            BIGINT,
  ADD COLUMN IF NOT EXISTS atr_window_n         INTEGER,
  ADD COLUMN IF NOT EXISTS atr_age_ms           BIGINT,
  ADD COLUMN IF NOT EXISTS atr_source           TEXT,
  ADD COLUMN IF NOT EXISTS atr_regime_value     DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS atr_trail_value      DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS atr_regime_tf_ms     BIGINT,
  ADD COLUMN IF NOT EXISTS atr_trail_tf_ms      BIGINT,
  ADD COLUMN IF NOT EXISTS atr_pct              DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS vol_ratio_fast_slow  DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS vol_ratio_z          DOUBLE PRECISION;

COMMENT ON COLUMN open_positions.contract_ver IS 'Phase 0+: horizon contract version (2 = new)';
COMMENT ON COLUMN open_positions.risk_horizon_bucket IS 'Phase 0+: micro/short/medium/long/unknown';
COMMENT ON COLUMN open_positions.horizon_reason_code IS 'Phase 0+: HZ_* reason code at signal time';
COMMENT ON COLUMN open_positions.atr_mode IS 'Phase 0+: legacy | horizon ATR selection mode';
COMMENT ON COLUMN open_positions.atr_tf_ms IS 'Phase 0+: ATR timeframe used at signal time (ms)';

-- ─── trades_closed ───────────────────────────────────────────────────────────

ALTER TABLE trades_closed
  ADD COLUMN IF NOT EXISTS contract_ver         SMALLINT,
  ADD COLUMN IF NOT EXISTS hold_target_ms       BIGINT,
  ADD COLUMN IF NOT EXISTS alpha_half_life_ms   BIGINT,
  ADD COLUMN IF NOT EXISTS max_signal_age_ms    BIGINT,
  ADD COLUMN IF NOT EXISTS risk_horizon_bucket  TEXT,
  ADD COLUMN IF NOT EXISTS horizon_profile_source TEXT,
  ADD COLUMN IF NOT EXISTS horizon_profile_conf DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS horizon_reason_code  TEXT,
  ADD COLUMN IF NOT EXISTS atr_mode             TEXT,
  ADD COLUMN IF NOT EXISTS atr_value            DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS atr_tf_ms            BIGINT,
  ADD COLUMN IF NOT EXISTS atr_window_n         INTEGER,
  ADD COLUMN IF NOT EXISTS atr_age_ms           BIGINT,
  ADD COLUMN IF NOT EXISTS atr_source           TEXT,
  ADD COLUMN IF NOT EXISTS atr_regime_value     DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS atr_trail_value      DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS atr_regime_tf_ms     BIGINT,
  ADD COLUMN IF NOT EXISTS atr_trail_tf_ms      BIGINT,
  ADD COLUMN IF NOT EXISTS atr_pct              DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS vol_ratio_fast_slow  DOUBLE PRECISION,
  ADD COLUMN IF NOT EXISTS vol_ratio_z          DOUBLE PRECISION;

COMMENT ON COLUMN trades_closed.contract_ver IS 'Phase 0+: horizon contract version (2 = new)';
COMMENT ON COLUMN trades_closed.risk_horizon_bucket IS 'Phase 0+: micro/short/medium/long/unknown';
COMMENT ON COLUMN trades_closed.horizon_reason_code IS 'Phase 0+: HZ_* reason code at signal time';
COMMENT ON COLUMN trades_closed.atr_mode IS 'Phase 0+: legacy | horizon ATR selection mode';
COMMENT ON COLUMN trades_closed.atr_tf_ms IS 'Phase 0+: ATR timeframe used at signal time (ms)';

-- ─── Indexes ──────────────────────────────────────────────────────────────────
-- Partial indexes: only rows where contract_ver IS NOT NULL (Phase 0+ rows).
-- This avoids bloating the index with legacy NULL rows.

CREATE INDEX IF NOT EXISTS idx_trades_closed_horizon_bucket
  ON trades_closed (symbol, kind, risk_horizon_bucket, closed_at DESC)
  WHERE risk_horizon_bucket IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_trades_closed_atr_tf
  ON trades_closed (symbol, kind, atr_tf_ms, closed_at DESC)
  WHERE atr_tf_ms IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_open_positions_horizon_bucket
  ON open_positions (symbol, risk_horizon_bucket)
  WHERE risk_horizon_bucket IS NOT NULL;
