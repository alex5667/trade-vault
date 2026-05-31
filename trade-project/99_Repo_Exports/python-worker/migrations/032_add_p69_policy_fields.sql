-- P69: Add policy hysteresis fields to trades table
-- These fields are enriched by ml_outcome_joiner_worker_v3 from decision records

\c scanner_analytics;

ALTER TABLE trades_closed 
ADD COLUMN IF NOT EXISTS policy_mode TEXT,
ADD COLUMN IF NOT EXISTS policy_raw TEXT;

ALTER TABLE trades_closed_p0
ADD COLUMN IF NOT EXISTS policy_mode TEXT,
ADD COLUMN IF NOT EXISTS policy_raw TEXT;
