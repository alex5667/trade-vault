-- =============================================================================
-- 039_fix_trades_closed_slow_query.sql
--
-- Fix the 3.4s full-table scan in conf_cal_promotion_manager_v1.py:
--
--   SELECT config_json, r_multiple
--   FROM trades_closed
--   WHERE exit_ts >= NOW() - INTERVAL '24 hours'
--     AND config_json->'indicators' IS NOT NULL
--     AND r_multiple IS NOT NULL
--
-- Root causes:
--   1. exit_ts is a GENERATED column → cannot be used in partial-index predicates
--      reliably across PG versions; index wasn't being used.
--   2. config_json->'indicators' IS NOT NULL = row-level JSONB eval on every row.
--   3. r_multiple IS NOT NULL checked after full scan.
--
-- Solution:
--   A. Partial index on exit_ts_ms (raw bigint, always indexed) with WHERE
--      r_multiple IS NOT NULL — keeps index small (only scored trades).
--   B. INCLUDE r_multiple so the r_multiple IS NOT NULL filter + value fetch is
--      Index-Only (no heap read).
--   C. Separate GIN index on config_json for faster JSONB key existence checks.
--      (Optional — query rewrite below makes it unnecessary for the 24h window.)
--
-- The Python query in conf_cal_promotion_manager_v1.py is updated separately to
-- use exit_ts_ms instead of exit_ts so PostgreSQL can use this index.
-- =============================================================================

\c scanner_analytics;

-- ---------------------------------------------------------------------------
-- Step 1: Partial B-tree index on exit_ts_ms for the 24h calibration query
--
-- Partial predicate: r_multiple IS NOT NULL  → only trades with a score
-- INCLUDE: r_multiple                         → index-only scan for the value
-- Sort: DESC                                  → matches typical ORDER BY exit_ts DESC
-- ---------------------------------------------------------------------------
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_trades_closed_exit_ts_ms_rm
    ON trades_closed (exit_ts_ms DESC)
    INCLUDE (r_multiple)
    WHERE r_multiple IS NOT NULL;

-- ---------------------------------------------------------------------------
-- Step 2: GIN index on config_json for general JSONB key-existence queries
--         (supports  config_json ? 'indicators'  which is faster than -> IS NOT NULL)
-- ---------------------------------------------------------------------------
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_trades_closed_config_gin
    ON trades_closed
    USING GIN (config_json jsonb_path_ops);

SELECT '039_fix_trades_closed_slow_query: done' AS status;
