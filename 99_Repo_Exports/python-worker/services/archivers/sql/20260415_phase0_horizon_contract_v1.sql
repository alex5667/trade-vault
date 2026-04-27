-- Migration: Phase 0.1 — horizon-aware contract columns for trades_closed
-- Safe to run multiple times (ADD COLUMN IF NOT EXISTS + CREATE INDEX IF NOT EXISTS)
-- Date: 2026-04-15
-- Author: phase0.1/horizon_contract_v1
-- Impact: additive-only, no trading logic change, backward-compatible

ALTER TABLE trades_closed
  ADD COLUMN IF NOT EXISTS horizon_contract JSONB NOT NULL DEFAULT '{}'::jsonb,
  ADD COLUMN IF NOT EXISTS horizon_bucket   TEXT,
  ADD COLUMN IF NOT EXISTS atr_tf_ms        BIGINT;

-- Analytics index: slice by symbol+bucket for Phase 1 hold calibration
CREATE INDEX IF NOT EXISTS idx_trades_closed_horizon_bucket_exit_ts
  ON trades_closed (symbol, horizon_bucket, exit_ts DESC);

-- Analytics index: slice by symbol+tf for ATR tf selector calibration
CREATE INDEX IF NOT EXISTS idx_trades_closed_atr_tf_ms_exit_ts
  ON trades_closed (symbol, atr_tf_ms, exit_ts DESC);

-- GIN index for ad-hoc JSONB queries on horizon contract fields
CREATE INDEX IF NOT EXISTS idx_trades_closed_horizon_contract_gin
  ON trades_closed
  USING GIN (horizon_contract);
