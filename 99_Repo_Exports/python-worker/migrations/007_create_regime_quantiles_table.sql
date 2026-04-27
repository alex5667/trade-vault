-- Migration: Create regime_quantiles table
-- Description: Creates table for storing ADX and ATR% quantiles by symbol/timeframe
-- Date: 2025-12-17

-- Create regime_quantiles table
CREATE TABLE IF NOT EXISTS regime_quantiles (
    id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    symbol       text NOT NULL,
    timeframe    text NOT NULL,

    -- ADX percentiles
    adx_p40      double precision NOT NULL,
    adx_p60      double precision NOT NULL,
    adx_p75      double precision NOT NULL,

    -- ATR% percentiles (note: column name uses camelCase in some contexts)
    atrp_p25     double precision NOT NULL,
    atrp_p50     double precision NOT NULL,
    atrp_p75     double precision NOT NULL,

    -- Metadata
    "sampleSize" integer NOT NULL,                    -- number of samples used
    "updatedAt"  timestamptz NOT NULL DEFAULT now(), -- last update timestamp

    -- Constraints
    UNIQUE(symbol, timeframe)
);

-- Create index for fast lookups (with error handling for permission issues)
DO $$ 
BEGIN 
    CREATE INDEX IF NOT EXISTS idx_regime_quantiles_lookup
    ON regime_quantiles (symbol, timeframe);
EXCEPTION
    WHEN insufficient_privilege THEN
        RAISE NOTICE 'Cannot create index idx_regime_quantiles_lookup: insufficient privileges. Table may need ownership transfer.';
    WHEN OTHERS THEN
        RAISE NOTICE 'Error creating index idx_regime_quantiles_lookup: %', SQLERRM;
END $$;

-- Create index for recent updates (only if updatedAt column exists)
-- Note: This column is renamed to computed_at in migration 010 and dropped in migration 012
DO $$ 
BEGIN 
    IF EXISTS (SELECT 1 FROM information_schema.columns 
               WHERE table_name='regime_quantiles' AND column_name='updatedAt') THEN
        CREATE INDEX IF NOT EXISTS idx_regime_quantiles_updated
        ON regime_quantiles ("updatedAt" DESC);
    END IF;
EXCEPTION
    WHEN insufficient_privilege THEN
        RAISE NOTICE 'Cannot create index idx_regime_quantiles_updated: insufficient privileges. Table may need ownership transfer.';
    WHEN OTHERS THEN
        RAISE NOTICE 'Error creating index idx_regime_quantiles_updated: %', SQLERRM;
END $$;

-- Log completion
SELECT 'Regime quantiles table created successfully' as status;
