-- Migration: Refactor regime_quantiles schema
-- Description: Unifies naming, adds versioning/audit fields
-- Date: 2026-01-08

\c scanner_analytics;

-- 1. Rename sampleSize -> sample_count
DO $$ 
BEGIN 
    IF EXISTS (SELECT 1 FROM information_schema.columns 
               WHERE table_name='regime_quantiles' AND column_name='sampleSize') THEN
        ALTER TABLE regime_quantiles RENAME COLUMN "sampleSize" TO sample_count;
    END IF;
END $$;

-- 2. Add window_days
ALTER TABLE regime_quantiles
ADD COLUMN IF NOT EXISTS window_days INTEGER NOT NULL DEFAULT 14;

-- 3. Add computed_at
ALTER TABLE regime_quantiles
ADD COLUMN IF NOT EXISTS computed_at TIMESTAMPTZ NOT NULL DEFAULT now();

-- 4. Add src audit fields
ALTER TABLE regime_quantiles
ADD COLUMN IF NOT EXISTS src_time_min TIMESTAMPTZ,
ADD COLUMN IF NOT EXISTS src_time_max TIMESTAMPTZ;

SELECT 'regime_quantiles refactored successfully' as status;
