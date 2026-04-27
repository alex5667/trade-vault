-- Migration: 20260416_horizon_profile_bootstrap_indexes_v1.sql
-- Phase 1: indexes to support horizon_profile_bootstrap_service.py queries
--
-- Query 1: trades_closed filtered by source + symbol + exit_ts_ms
--          (row discovery + main load query)
-- Query 2: trades_closed_p0 joined by order_id (+ scenario/regime for grouping)
-- Query 3: trades_closed_p0 holds/mfe stats
--
-- Safe to apply online (CREATE INDEX IF NOT EXISTS = idempotent + concurrent-safe
-- with CONCURRENTLY below if table is large).
-- For large prod tables, add CONCURRENTLY and run outside a transaction.

CREATE INDEX IF NOT EXISTS idx_trades_closed_source_symbol_exit_ts_ms
    ON trades_closed (source, symbol, exit_ts_ms DESC);

CREATE INDEX IF NOT EXISTS idx_trades_closed_exit_ts_ms_source_symbol
    ON trades_closed (exit_ts_ms DESC, source, symbol);

CREATE INDEX IF NOT EXISTS idx_trades_closed_p0_order_id_scenario_regime
    ON trades_closed_p0 (order_id, scenario, regime);

CREATE INDEX IF NOT EXISTS idx_trades_closed_p0_hold_time_mfe
    ON trades_closed_p0 (hold_ms, time_to_mfe_ms)
    WHERE hold_ms IS NOT NULL;
