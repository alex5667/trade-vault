-- Migration: Repair atrp_p90 in regime_quantiles
-- Description: Ensures atrp_p90 column exists even if previous migrations were inconsistent
-- Date: 2026-01-11

\c scanner_analytics;

DO $$ 
BEGIN 
    IF NOT EXISTS (
        SELECT 1 
        FROM information_schema.columns 
        WHERE table_name='regime_quantiles' 
        AND column_name='atrp_p90'
    ) THEN
        ALTER TABLE regime_quantiles
        ADD COLUMN atrp_p90 double precision NOT NULL DEFAULT 0.0;
        
        RAISE NOTICE 'Added atrp_p90 column';
    ELSE
        RAISE NOTICE 'Column atrp_p90 already exists';
    END IF;
END $$;

SELECT 'regime_quantiles schema repaired' as status;
