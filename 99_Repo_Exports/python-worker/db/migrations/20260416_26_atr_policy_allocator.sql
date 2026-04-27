-- Migration: 20260416_26_atr_policy_allocator
-- Description: Creates the tables and views for Phase 5.5 Policy-Aware Capital Allocator

CREATE TABLE IF NOT EXISTS atr_policy_allocator_configs (
  id bigserial PRIMARY KEY,
  scope_kind text NOT NULL,              -- global | venue | symbol | cohort | layer | policy_ver
  source text,
  venue text,
  symbol text,
  scenario text,
  regime text,
  risk_horizon_bucket text,
  layer text,
  policy_ver integer,
  global_open_risk_budget_pct double precision NOT NULL DEFAULT 0,
  global_daily_trades_budget integer NOT NULL DEFAULT 0,
  min_risk_pct_mult double precision NOT NULL DEFAULT 0.25,
  max_risk_pct_mult double precision NOT NULL DEFAULT 1.50,
  max_alloc_share double precision NOT NULL DEFAULT 0.50,
  min_alloc_share double precision NOT NULL DEFAULT 0.00,
  rebalance_interval_sec integer NOT NULL DEFAULT 300,
  is_enabled boolean NOT NULL DEFAULT true,
  created_at_ms bigint NOT NULL,
  updated_at_ms bigint NOT NULL
);

CREATE TABLE IF NOT EXISTS atr_policy_allocator_states (
  id bigserial PRIMARY KEY,
  source text NOT NULL,
  venue text NOT NULL,
  symbol text NOT NULL,
  scenario text NOT NULL,
  regime text NOT NULL,
  risk_horizon_bucket text NOT NULL,
  layer text NOT NULL,
  policy_ver integer NOT NULL,
  rollout_stage text NOT NULL,
  restore_cert_status text NOT NULL,
  alloc_score double precision NOT NULL,
  alloc_weight double precision NOT NULL,
  risk_pct_mult double precision NOT NULL,
  target_max_open_risk_pct double precision NOT NULL,
  target_max_daily_trades integer NOT NULL,
  state_json jsonb NOT NULL,
  is_current boolean NOT NULL DEFAULT true,
  created_at_ms bigint NOT NULL,
  updated_at_ms bigint NOT NULL
);

CREATE TABLE IF NOT EXISTS atr_policy_allocator_events (
  id bigserial PRIMARY KEY,
  source text NOT NULL,
  venue text NOT NULL,
  symbol text NOT NULL,
  scenario text NOT NULL,
  regime text NOT NULL,
  risk_horizon_bucket text NOT NULL,
  layer text NOT NULL,
  policy_ver integer NOT NULL,
  action text NOT NULL,                 -- rebalance | freeze | clip | zero | restore
  reason_code text NOT NULL,
  event_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

-- Note: if `trades_closed` doesn't exist yet, this will fail or warn.
CREATE OR REPLACE VIEW v_atr_policy_allocator_inputs AS
SELECT
    symbol,
    source,
    atr_policy_scenario AS scenario,
    atr_policy_regime AS regime,
    atr_policy_bucket AS risk_horizon_bucket,
    atr_stop_ttl_mode,
    atr_trailing_mode,
    atr_policy_ver,
    atr_restore_cert_status,
    atr_recovery_run_id,
    count(*) AS n_trades,
    avg(pnl_pct * 100.0) AS avg_pnl_bps,
    0.0 AS avg_slippage_bps,
    avg(mae_pnl) AS avg_mae_pct,
    avg(CASE WHEN pnl_net > 0 THEN 1.0 ELSE 0.0 END) AS win_rate,
    avg(CASE WHEN close_reason IN ('stop_loss', 'sl_hit') THEN 1.0 ELSE 0.0 END) AS stop_rate,
    avg(CASE WHEN tp1_hit THEN 1.0 ELSE 0.0 END) AS tp1_rate
FROM trades_closed
WHERE exit_ts >= now() - interval '21 days'
GROUP BY
    symbol, source,
    atr_policy_scenario,
    atr_policy_regime,
    atr_policy_bucket,
    atr_stop_ttl_mode,
    atr_trailing_mode,
    atr_policy_ver,
    atr_restore_cert_status,
    atr_recovery_run_id;
