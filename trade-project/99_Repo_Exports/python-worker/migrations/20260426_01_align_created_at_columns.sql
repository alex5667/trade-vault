-- Align created_at column naming convention across analytical tables
-- This ensures UNION ALL analytical queries run seamlessly without failing due to schema mismatches.
--
-- Safety: SET LOCAL lock_timeout aborts the ALTER if it cannot acquire ACCESS EXCLUSIVE within 5s
-- rather than waiting indefinitely and blocking downstream queries.

BEGIN;
SET LOCAL lock_timeout = '5s';

-- Add created_at alias to signals
ALTER TABLE signals ADD COLUMN IF NOT EXISTS created_at TIMESTAMP WITH TIME ZONE GENERATED ALWAYS AS (ts_signal) STORED;

-- Add created_at alias to execution_orders
ALTER TABLE execution_orders ADD COLUMN IF NOT EXISTS created_at TIMESTAMP WITH TIME ZONE GENERATED ALWAYS AS (to_timestamp(created_at_ms / 1000.0)) STORED;

COMMIT;
