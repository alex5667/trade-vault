-- Migration: Add missing window_days column to regime_quantiles
-- Description: Ensures window_days column exists (should have been added in migration 010)
-- Date: 2026-01-29

\c scanner_analytics;

-- Add window_days column if it doesn't exist
ALTER TABLE regime_quantiles
ADD COLUMN IF NOT EXISTS window_days INTEGER NOT NULL DEFAULT 14;

-- Add computed_at if missing (from migration 010)
ALTER TABLE regime_quantiles
ADD COLUMN IF NOT EXISTS computed_at TIMESTAMPTZ NOT NULL DEFAULT now();

-- Add src audit fields if missing (from migration 010)
ALTER TABLE regime_quantiles
ADD COLUMN IF NOT EXISTS src_time_min TIMESTAMPTZ,
ADD COLUMN IF NOT EXISTS src_time_max TIMESTAMPTZ;

-- Ensure index exists
CREATE INDEX IF NOT EXISTS idx_regime_quantiles_computed_at
ON regime_quantiles (computed_at DESC);

SELECT 'window_days and related columns added to regime_quantiles' as status;

