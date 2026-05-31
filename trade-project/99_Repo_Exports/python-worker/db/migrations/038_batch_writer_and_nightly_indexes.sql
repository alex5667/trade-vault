-- =============================================================================
-- 038_batch_writer_and_nightly_indexes.sql
--
-- Adds missing composite indexes that enable nightly dataset-export jobs to
-- run as Index Scans instead of full Table Scans.
--
-- Tables targeted:
--   trades_closed            — (symbol, is_virtual, exit_ts_ms)
--   trades_closed_p0         — (symbol, is_virtual, exit_ts_ms)
--   execution_order_events   — plain time-range index
--   execution_orders         — (symbol, updated_at_ms)
--
-- Safety:
--   - CREATE INDEX uses CONCURRENTLY where possible — no table lock at build time.
--   - Hypertable trades_closed_p0 uses plain CREATE INDEX (CONCURRENTLY not supported).
--   - IF NOT EXISTS — safe to re-run on any environment.
--   - Partial predicates (WHERE is_virtual IS NOT NULL) keep index small.
-- =============================================================================

\c scanner_analytics;

-- ---------------------------------------------------------------------------
-- 1. trades_closed: composite index for nightly calibration + dataset export
--
--    Nightly query pattern:
--      WHERE symbol = $1 AND is_virtual = false
--      ORDER BY exit_ts_ms DESC
--      [LIMIT large_number]
--
--    This index eliminates the Seq Scan reported in DIAGNOSTIC_REPORT.md.
-- ---------------------------------------------------------------------------

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_trades_closed_sym_virtual_exit_ts
    ON trades_closed(symbol, is_virtual, exit_ts_ms DESC)
    WHERE is_virtual IS NOT NULL;

-- ---------------------------------------------------------------------------
-- 2. trades_closed_p0: same pattern for shadow/virtual analytics queries
-- ---------------------------------------------------------------------------

CREATE INDEX IF NOT EXISTS idx_trades_closed_p0_sym_virtual_exit_ts
    ON trades_closed_p0(is_virtual, exit_ts_ms DESC)
    WHERE is_virtual IS NOT NULL;

-- ---------------------------------------------------------------------------
-- 3. execution_order_events: plain time-range index
--    Nightly reconciliation: SELECT … WHERE event_ts_ms > (now - N hours)
--    Already specified in migration 037 but using CONCURRENTLY to be safe.
-- ---------------------------------------------------------------------------

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_exec_events_ts
    ON execution_order_events(event_ts_ms DESC);

-- ---------------------------------------------------------------------------
-- 4. execution_orders: symbol + updated_at composite
--    Used by heartbeat/reconciliation workers: latest state per symbol.
-- ---------------------------------------------------------------------------

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_exec_orders_sym_updated
    ON execution_orders(symbol, updated_at_ms DESC);

-- ---------------------------------------------------------------------------
-- 5. trades_closed: additional index for nightly job filter by source/strategy
--    Query: WHERE source = $origin AND exit_ts_ms BETWEEN $start AND $end
-- ---------------------------------------------------------------------------

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_trades_closed_source_exit_ts
    ON trades_closed(source, exit_ts_ms DESC)
    WHERE exit_ts_ms > 0;

-- ---------------------------------------------------------------------------
-- 6. Verify
-- ---------------------------------------------------------------------------
SELECT
    indexname,
    tablename,
    indexdef
FROM pg_indexes
WHERE tablename IN (
    'trades_closed', 'trades_closed_p0',
    'execution_orders', 'execution_order_events'
)
  AND indexname IN (
    'idx_trades_closed_sym_virtual_exit_ts',
    'idx_trades_closed_p0_sym_virtual_exit_ts',
    'idx_exec_events_ts',
    'idx_exec_orders_sym_updated',
    'idx_trades_closed_source_exit_ts'
  )
ORDER BY tablename, indexname;

SELECT '038_batch_writer_and_nightly_indexes: done' AS status;
