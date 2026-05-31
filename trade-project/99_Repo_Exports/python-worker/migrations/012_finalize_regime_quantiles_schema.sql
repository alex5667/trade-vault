-- Migration: Finalize regime_quantiles schema
-- Description: Drop legacy updatedAt column as we transitioned to computed_at
-- Date: 2026-01-08

\c scanner_analytics;

-- Ensure index on computed_at exists (migration 011 should have done this, but let's be sure)
CREATE INDEX IF NOT EXISTS idx_regime_quantiles_computed_at
ON regime_quantiles (computed_at DESC);

-- Drop legacy column if it exists
DO $$ 
BEGIN 
    IF EXISTS (SELECT 1 FROM information_schema.columns 
               WHERE table_name='regime_quantiles' AND column_name='updatedAt') THEN
        ALTER TABLE regime_quantiles DROP COLUMN "updatedAt";
    END IF;
END $$;

SELECT 'regime_quantiles schema finalized' as status;
