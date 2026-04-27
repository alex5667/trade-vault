-- Phase 5.3 ATR Policy Rollout Schema
CREATE TABLE IF NOT EXISTS atr_policy_rollouts (
  id bigserial PRIMARY KEY,
  source text NOT NULL,
  symbol text NOT NULL,
  scenario text NOT NULL,
  regime text NOT NULL,
  risk_horizon_bucket text NOT NULL,
  layer text NOT NULL,                -- stop_ttl | trailing
  policy_ver integer NOT NULL,
  rollout_stage text NOT NULL,        -- shadow | canary_5 | canary_25 | live_100 | frozen | rolled_back
  rollout_share double precision NOT NULL,
  is_current boolean NOT NULL DEFAULT true,
  reason_code text NOT NULL,
  created_at_ms bigint NOT NULL,
  updated_at_ms bigint NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_atr_policy_rollouts_current
  ON atr_policy_rollouts (
    source, symbol, scenario, regime, risk_horizon_bucket, layer
  )
  WHERE is_current = true;

CREATE TABLE IF NOT EXISTS atr_policy_rollout_events (
  id bigserial PRIMARY KEY,
  source text NOT NULL,
  symbol text NOT NULL,
  scenario text NOT NULL,
  regime text NOT NULL,
  risk_horizon_bucket text NOT NULL,
  layer text NOT NULL,
  policy_ver integer NOT NULL,
  old_stage text NOT NULL,
  new_stage text NOT NULL,
  action text NOT NULL,               -- promote | freeze | rollback | hold
  reason_code text NOT NULL,
  event_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE OR REPLACE VIEW v_atr_policy_rollout_health AS
SELECT
    r.source,
    r.symbol,
    r.scenario,
    r.regime,
    r.risk_horizon_bucket,
    r.layer,
    r.policy_ver,
    r.rollout_stage,
    r.rollout_share,
    a.atr_restore_cert_status,
    a.n_trades,
    a.avg_pnl_bps,
    a.avg_slippage_bps,
    a.win_rate,
    a.stop_rate,
    a.tp1_rate
FROM atr_policy_rollouts r
LEFT JOIN v_atr_policy_promotion_inputs a
  ON a.symbol = r.symbol
 AND a.scenario = r.scenario
 AND a.regime = r.regime
 AND a.bucket = r.risk_horizon_bucket
WHERE r.is_current = true;
