CREATE TABLE IF NOT EXISTS atr_policy_regime_states (
  id bigserial PRIMARY KEY,
  source text NOT NULL,
  symbol text NOT NULL,
  regime text NOT NULL,                  -- trend_up | trend_down | chop | expansion | unknown
  stress_state text NOT NULL,            -- normal | liquidity_shock | slippage_shock | venue_stress | news_lock | drift_lock | portfolio_stress
  confidence double precision NOT NULL DEFAULT 0,
  state_json jsonb NOT NULL,
  is_current boolean NOT NULL DEFAULT true,
  created_at_ms bigint NOT NULL,
  updated_at_ms bigint NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_atr_policy_regime_states_current
  ON atr_policy_regime_states (source, symbol)
  WHERE is_current = true;

CREATE TABLE IF NOT EXISTS atr_policy_regime_limits (
  id bigserial PRIMARY KEY,
  regime text NOT NULL,
  stress_state text NOT NULL,
  layer text NOT NULL,                   -- stop_ttl | trailing
  rollout_stage text NOT NULL,           -- shadow | canary_5 | canary_25 | live_100 | frozen | rolled_back
  risk_pct_mult double precision NOT NULL DEFAULT 1.0,
  max_open_risk_pct double precision NOT NULL DEFAULT 0,
  max_daily_trades integer NOT NULL DEFAULT 0,
  max_slippage_ema_bps double precision NOT NULL DEFAULT 0,
  max_factor_cluster_risk_pct double precision NOT NULL DEFAULT 0,
  action text NOT NULL DEFAULT 'allow',  -- allow | clip | freeze | deny
  reason_code text NOT NULL DEFAULT '',
  created_at_ms bigint NOT NULL,
  updated_at_ms bigint NOT NULL
);

CREATE TABLE IF NOT EXISTS atr_policy_stress_events (
  id bigserial PRIMARY KEY,
  source text NOT NULL,
  symbol text NOT NULL,
  regime text NOT NULL,
  stress_state text NOT NULL,
  action text NOT NULL,                  -- clip | freeze | deny | recover
  reason_code text NOT NULL,
  event_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);
