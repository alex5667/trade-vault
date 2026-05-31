-- Migration: Update indexing for regime_quantiles
-- Description: Drop index on updatedAt and create index on computed_at
-- Date: 2026-01-08

\c scanner_analytics;

-- Drop old index if exists
DROP INDEX IF EXISTS idx_regime_quantiles_updated;

-- Create new index for computation freshness
CREATE INDEX IF NOT EXISTS idx_regime_quantiles_computed_at
ON regime_quantiles (computed_at DESC);

SELECT 'regime_quantiles indexing updated' as status;
