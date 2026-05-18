-- Migration: Idempotent repair of regime_quantiles schema
-- Ensures atrp_p90, window_days, computed_at, src_time_min, src_time_max
-- exist even when the table was created from an older init-postgres.sql.
-- Safe to re-run: all clauses use ADD COLUMN IF NOT EXISTS.

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'regime_quantiles' AND column_name = 'atrp_p90'
    ) THEN
        ALTER TABLE regime_quantiles ADD COLUMN atrp_p90 DOUBLE PRECISION NOT NULL DEFAULT 0.0;
        RAISE NOTICE 'Added atrp_p90 to regime_quantiles';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'regime_quantiles' AND column_name = 'window_days'
    ) THEN
        ALTER TABLE regime_quantiles ADD COLUMN window_days INTEGER NOT NULL DEFAULT 14;
        RAISE NOTICE 'Added window_days to regime_quantiles';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'regime_quantiles' AND column_name = 'computed_at'
    ) THEN
        ALTER TABLE regime_quantiles ADD COLUMN computed_at TIMESTAMPTZ NOT NULL DEFAULT now();
        RAISE NOTICE 'Added computed_at to regime_quantiles';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'regime_quantiles' AND column_name = 'src_time_min'
    ) THEN
        ALTER TABLE regime_quantiles ADD COLUMN src_time_min TIMESTAMPTZ;
        RAISE NOTICE 'Added src_time_min to regime_quantiles';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_name = 'regime_quantiles' AND column_name = 'src_time_max'
    ) THEN
        ALTER TABLE regime_quantiles ADD COLUMN src_time_max TIMESTAMPTZ;
        RAISE NOTICE 'Added src_time_max to regime_quantiles';
    END IF;
END $$;

