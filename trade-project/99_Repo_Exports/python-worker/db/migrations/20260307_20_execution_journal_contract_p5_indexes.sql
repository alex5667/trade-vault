-- P5 execution journal contract: indexes for chain join queries
-- All CREATE INDEX IF NOT EXISTS — safe to apply on live DB.
-- Uses CONCURRENTLY where possible to avoid locking (requires single statement, no tx).

CREATE INDEX IF NOT EXISTS idx_execution_orders_signal_plan
  ON execution_orders(signal_id, execution_plan_id, updated_at_ms DESC);

CREATE INDEX IF NOT EXISTS idx_execution_orders_closed_trade
  ON execution_orders(closed_trade_id);

CREATE INDEX IF NOT EXISTS idx_execution_events_signal_plan_ts
  ON execution_order_events(signal_id, execution_plan_id, event_ts_ms DESC);

CREATE INDEX IF NOT EXISTS idx_execution_watchdog_sid_ts
  ON execution_watchdog_events(sid, event_ts_ms DESC);

CREATE INDEX IF NOT EXISTS idx_execution_watchdog_symbol_state_ts
  ON execution_watchdog_events(symbol, watchdog_state, event_ts_ms DESC);
