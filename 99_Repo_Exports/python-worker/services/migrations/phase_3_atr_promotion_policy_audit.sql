BEGIN;

CREATE TABLE IF NOT EXISTS atr_promotion_policy_audit (
  id bigserial PRIMARY KEY,
  source text NOT NULL,
  symbol text NOT NULL,
  scenario text NOT NULL,
  regime text NOT NULL,
  risk_horizon_bucket text NOT NULL,
  stop_ttl_mode text NOT NULL,
  trailing_mode text NOT NULL,
  reason_code text NOT NULL,
  approved boolean NOT NULL,
  applied boolean NOT NULL,
  suggestion_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_atr_promotion_policy_audit_lookup
  ON atr_promotion_policy_audit (symbol, scenario, regime, risk_horizon_bucket, created_at DESC);

COMMIT;
