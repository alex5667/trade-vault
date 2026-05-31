-- Migration: Fix missing columns in trades_closed and regime_quantiles
-- Description: Adds strong_gate_ok to trades_closed and atrp_p90 to regime_quantiles if missing
-- Date: 2026-01-29

\c scanner_analytics;

-- Fix trades_closed
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name='trades_closed'
        AND column_name='strong_gate_ok'
    ) THEN
        ALTER TABLE trades_closed
        ADD COLUMN strong_gate_ok BOOLEAN;
        RAISE NOTICE 'Added strong_gate_ok to trades_closed';
    ELSE
        RAISE NOTICE 'strong_gate_ok already exists in trades_closed';
    END IF;
END $$;

-- Fix regime_quantiles
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name='regime_quantiles'
        AND column_name='atrp_p90'
    ) THEN
        ALTER TABLE regime_quantiles
        ADD COLUMN atrp_p90 DOUBLE PRECISION NOT NULL DEFAULT 0.0;
        RAISE NOTICE 'Added atrp_p90 to regime_quantiles';
    ELSE
        RAISE NOTICE 'atrp_p90 already exists in regime_quantiles';
    END IF;
END $$;

SELECT 'Schema fixes applied successfully' as status;

