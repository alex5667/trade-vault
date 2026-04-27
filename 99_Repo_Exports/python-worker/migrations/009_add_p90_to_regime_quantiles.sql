-- Migration: Add p90 to regime_quantiles
-- Description: Adds atrp_p90 column
-- Date: 2026-01-08

\c scanner_analytics;

ALTER TABLE regime_quantiles
ADD COLUMN IF NOT EXISTS atrp_p90 double precision NOT NULL DEFAULT 0.0;

SELECT 'p90 added to regime_quantiles' as status;
