-- Execution journal for Binance/venue execution facts.
-- Redis stream (orders:exec) remains the online SoT; these tables provide
-- durable audit history for incident analysis and reconciliation.
--
-- Migration: 036_execution_journal.sql

CREATE TABLE IF NOT EXISTS execution_orders (
    sid TEXT PRIMARY KEY,
    symbol TEXT NOT NULL,
    action TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT '',
    fsm_state TEXT NOT NULL DEFAULT '',
    execution_policy TEXT NOT NULL DEFAULT '',
    venue TEXT NOT NULL DEFAULT 'binance',
    position_mode TEXT NOT NULL DEFAULT '',
    position_side TEXT NOT NULL DEFAULT '',
    working_type_policy TEXT NOT NULL DEFAULT '',
    state_jsonb JSONB NOT NULL DEFAULT '{}',
    created_at_ms BIGINT NOT NULL,
    updated_at_ms BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS execution_order_events (
    id BIGSERIAL PRIMARY KEY,
    sid TEXT NOT NULL,
    symbol TEXT NOT NULL DEFAULT '',
    event_type TEXT NOT NULL,
    event_ts_ms BIGINT NOT NULL,
    payload_jsonb JSONB NOT NULL DEFAULT '{}'
);

CREATE TABLE IF NOT EXISTS execution_protection_refs (
    sid TEXT PRIMARY KEY,
    symbol TEXT NOT NULL,
    sl_algo_id BIGINT,
    sl_client_algo_id TEXT,
    tp1_algo_id BIGINT,
    tp2_algo_id BIGINT,
    tp3_algo_id BIGINT,
    trail_algo_id BIGINT,
    trail_client_algo_id TEXT,
    updated_at_ms BIGINT NOT NULL
);
