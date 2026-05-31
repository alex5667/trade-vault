-- P5 journal contract extension: durable signal->execution->closed-trade chain
-- Safe to apply on running DB: all ADD COLUMN IF NOT EXISTS + CREATE TABLE IF NOT EXISTS
-- All new columns are nullable (or have non-blocking DEFAULT) to avoid table rewrite.

ALTER TABLE execution_orders
  ADD COLUMN IF NOT EXISTS entry_policy TEXT NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS exit_policy TEXT NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS signal_id TEXT NULL,
  ADD COLUMN IF NOT EXISTS execution_plan_id TEXT NULL,
  ADD COLUMN IF NOT EXISTS entry_order_ref TEXT NULL,
  ADD COLUMN IF NOT EXISTS exit_order_ref TEXT NULL,
  ADD COLUMN IF NOT EXISTS closed_trade_id TEXT NULL;

ALTER TABLE execution_order_events
  ADD COLUMN IF NOT EXISTS signal_id TEXT NULL,
  ADD COLUMN IF NOT EXISTS execution_plan_id TEXT NULL;

-- Watchdog events: TP-level state machine transitions from the executor watchdog.
-- One row per watchdog state change; allows forensic triage of stuck TP orders.
CREATE TABLE IF NOT EXISTS execution_watchdog_events (
  id               BIGSERIAL PRIMARY KEY,
  sid              TEXT NOT NULL,
  symbol           TEXT NOT NULL DEFAULT '',
  signal_id        TEXT NULL,
  execution_plan_id TEXT NULL,
  tp_level         INTEGER NULL,
  watchdog_state   TEXT NOT NULL DEFAULT '',
  event_type       TEXT NOT NULL,
  event_ts_ms      BIGINT NOT NULL,
  payload_jsonb    JSONB NOT NULL
);
