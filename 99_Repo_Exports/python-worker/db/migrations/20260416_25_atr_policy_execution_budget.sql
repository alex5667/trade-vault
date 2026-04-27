-- 20260416_25_atr_policy_execution_budget.sql
-- Phase 5.4: Hierarchical execution budgets and kill-switch governance for ATR policy

CREATE TABLE IF NOT EXISTS atr_policy_execution_budgets (
  id bigserial PRIMARY KEY,
  scope_kind text NOT NULL,              -- global | venue | cohort | layer | policy_ver
  source text,
  venue text,
  symbol text,
  scenario text,
  regime text,
  risk_horizon_bucket text,
  layer text,                            -- stop_ttl | trailing
  policy_ver integer,
  max_open_risk_pct double precision NOT NULL DEFAULT 0,
  max_open_positions integer NOT NULL DEFAULT 0,
  max_daily_trades integer NOT NULL DEFAULT 0,
  max_daily_loss_usd double precision NOT NULL DEFAULT 0,
  max_daily_loss_bps double precision NOT NULL DEFAULT 0,
  max_slippage_ema_bps double precision NOT NULL DEFAULT 0,
  max_stop_streak integer NOT NULL DEFAULT 0,
  is_enabled boolean NOT NULL DEFAULT true,
  reason_code text NOT NULL DEFAULT '',
  created_at_ms bigint NOT NULL,
  updated_at_ms bigint NOT NULL
);

CREATE TABLE IF NOT EXISTS atr_policy_execution_budget_events (
  id bigserial PRIMARY KEY,
  source text,
  venue text,
  symbol text,
  scenario text,
  regime text,
  risk_horizon_bucket text,
  layer text,
  policy_ver integer,
  action text NOT NULL,                  -- allow | deny | freeze | kill | unfreeze
  reason_code text NOT NULL,
  event_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS atr_policy_kill_switches (
  id bigserial PRIMARY KEY,
  scope_kind text NOT NULL,              -- global | venue | cohort | layer | policy_ver
  source text,
  venue text,
  symbol text,
  scenario text,
  regime text,
  risk_horizon_bucket text,
  layer text,
  policy_ver integer,
  state text NOT NULL,                   -- active | inactive
  reason_code text NOT NULL,
  payload_json jsonb NOT NULL,
  is_current boolean NOT NULL DEFAULT true,
  created_at_ms bigint NOT NULL,
  updated_at_ms bigint NOT NULL
);

-- Basic indexes for fast queries
CREATE INDEX IF NOT EXISTS idx_atr_policy_exec_budgets_scope ON atr_policy_execution_budgets(scope_kind, is_enabled);
CREATE INDEX IF NOT EXISTS idx_atr_policy_exec_events_created ON atr_policy_execution_budget_events(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_atr_policy_kill_switches_current ON atr_policy_kill_switches(scope_kind, is_current, state);
