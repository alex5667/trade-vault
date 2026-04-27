-- Migration 037: Indexes for execution journal tables + trades_closed performance
-- Applied after table creation in migration 036.
--
-- PROBLEM FOUND: execution journal tables had only 4 indexes, missing:
--   - status/created_at_ms range queries (nightly reconciliation)
--   - GIN for JSONB ad-hoc analysis
--   - is_virtual on trades_closed (nightly dataset export hits table scan)
--
-- All indexes are IF NOT EXISTS and SAFE to re-run.

-- ============================================================
-- execution_orders
-- ============================================================

-- (existing) Fast lookup by symbol+state for reconciliation and monitoring
CREATE INDEX IF NOT EXISTS idx_execution_orders_symbol_state
    ON execution_orders(symbol, fsm_state, updated_at_ms DESC);

-- (NEW) Time-range scan: nightly job queries last N hours of orders
CREATE INDEX IF NOT EXISTS idx_execution_orders_created_ts
    ON execution_orders(created_at_ms DESC);

-- (NEW) Status filter (FILLED / CANCELLED / PARTIAL) + time range
CREATE INDEX IF NOT EXISTS idx_execution_orders_status_ts
    ON execution_orders(status, created_at_ms DESC);

-- (NEW) symbol+status composite — most common nightly query pattern
CREATE INDEX IF NOT EXISTS idx_execution_orders_symbol_status_ts
    ON execution_orders(symbol, status, created_at_ms DESC);

-- (NEW) GIN for JSONB forensics (state_jsonb field)
CREATE INDEX IF NOT EXISTS idx_execution_orders_state_gin
    ON execution_orders USING gin (state_jsonb);

-- ============================================================
-- execution_order_events
-- ============================================================

-- (existing) Event lookup by signal ID and time (primary use case: incident triage)
CREATE INDEX IF NOT EXISTS idx_execution_events_sid_ts
    ON execution_order_events(sid, event_ts_ms DESC);

-- (existing) Event lookup by symbol+type for cross-symbol nightly analysis
CREATE INDEX IF NOT EXISTS idx_execution_events_symbol_type_ts
    ON execution_order_events(symbol, event_type, event_ts_ms DESC);

-- (NEW) Plain time range scan for nightly export (SELECT ... WHERE event_ts_ms > ...)
CREATE INDEX IF NOT EXISTS idx_execution_events_ts
    ON execution_order_events(event_ts_ms DESC);

-- (existing) Full JSONB search (infrequent, ad-hoc analysis)
CREATE INDEX IF NOT EXISTS idx_execution_events_payload_gin
    ON execution_order_events USING GIN (payload_jsonb);

-- ============================================================
-- trades_closed / trades_closed_p0 — is_virtual composite indexes
-- ============================================================
-- is_virtual column was added in migration 025, but WITHOUT an index.
-- Nightly calibration queries: WHERE symbol = $1 AND is_virtual = false
-- ORDER BY exit_ts_ms -> triggers full table scan on every dataset export run.

-- Composite: symbol + is_virtual + exit_ts_ms  (covers all nightly patterns)
CREATE INDEX IF NOT EXISTS idx_trades_closed_sym_virtual_ts
    ON trades_closed(symbol, is_virtual, exit_ts_ms DESC)
    WHERE is_virtual IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_trades_closed_p0_sym_virtual_ts
    ON trades_closed_p0(is_virtual, exit_ts_ms DESC)
    WHERE is_virtual IS NOT NULL;

-- ============================================================
-- TimescaleDB hypertable hints (run manually if TimescaleDB installed)
-- ============================================================
-- Run once to convert execution_order_events to a hypertable:
--
-- SELECT create_hypertable(
--     'execution_order_events',
--     by_range('event_ts_ms', 86400000),  -- 1-day chunks (ms)
--     if_not_exists => TRUE
-- );
--
-- SELECT add_retention_policy(
--     'execution_order_events',
--     INTERVAL '90 days',
--     if_not_exists => TRUE
-- );

