-- =============================================================================
-- 034_optimize_trades_closed_ml_query.sql
-- Optimize the 2.8s trades_closed scan used by local_calibration / ML pipelines.
--
-- Changes:
--   1. Add 4 STORED generated columns that materialise JSONB indicator values at
--      write time — zero extraction cost at read time.
--   2. Replace migration-033's single-column partial index with a composite,
--      covering partial index (INCLUDE) so the calibration query can be answered
--      from the index alone (no heap fetch for the selected columns).
--
-- Safety:
--   - ALTER TABLE … ADD COLUMN … GENERATED ALWAYS … STORED triggers a one-time
--     table rewrite to back-fill all existing rows. This is a multi-second op on
--     large tables but does NOT lock writes in PG 12+ (short exclusive lock at
--     start/end only). Run during low-traffic window if worried.
--   - CREATE INDEX CONCURRENTLY never takes a full table lock.
--   - DROP INDEX CONCURRENTLY is online too.
-- =============================================================================

\c scanner_analytics;

-- ---------------------------------------------------------------------------
-- Step 1: Stored generated columns for JSONB indicator extraction
-- ---------------------------------------------------------------------------

ALTER TABLE trades_closed
  ADD COLUMN IF NOT EXISTS ind_delta_z       DOUBLE PRECISION
      GENERATED ALWAYS AS (
          (config_json->'indicators'->>'delta_z')::double precision
      ) STORED,
  ADD COLUMN IF NOT EXISTS ind_obi           DOUBLE PRECISION
      GENERATED ALWAYS AS (
          (config_json->'indicators'->>'obi')::double precision
      ) STORED,
  ADD COLUMN IF NOT EXISTS ind_weak_progress BOOLEAN
      GENERATED ALWAYS AS (
          (config_json->'indicators'->>'weak_progress') = 'true'
      ) STORED,
  ADD COLUMN IF NOT EXISTS ind_atr_th_bps    DOUBLE PRECISION
      GENERATED ALWAYS AS (
          (config_json->'indicators'->>'atr_unified_th_bps')::double precision
      ) STORED;

-- ---------------------------------------------------------------------------
-- Step 2: Replace migration-033 index with a composite covering partial index
--
-- The partial predicate (same as the WHERE clause in load_signals) means only
-- qualifying rows are indexed — keeps the index small.
--
-- INCLUDE cols are stored in the leaf pages so the planner can use an
-- Index-Only Scan and never touch the heap for the common ML calibration query.
-- ---------------------------------------------------------------------------

-- Drop old single-column index from migration 033 (non-blocking)
DROP INDEX CONCURRENTLY IF EXISTS idx_trades_closed_tp1_exit_ts;

-- New composite + covering partial index (non-blocking build)
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_trades_closed_ml_v2
    ON trades_closed (exit_ts DESC, symbol, entry_tag)
    INCLUDE (r_multiple, ind_delta_z, ind_obi, ind_weak_progress, ind_atr_th_bps)
    WHERE r_multiple IS NOT NULL
      AND (tp1_hit = TRUE OR r_multiple > 0);

-- ---------------------------------------------------------------------------
-- Step 3: Grant permissions to existing users
-- ---------------------------------------------------------------------------

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'trading') THEN
        GRANT SELECT ON trades_closed TO trading;
    END IF;
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'scanner') THEN
        GRANT SELECT ON trades_closed TO scanner;
    END IF;
END $$;

SELECT '034_optimize_trades_closed_ml_query: done' AS status;
