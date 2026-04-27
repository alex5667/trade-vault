CREATE TABLE IF NOT EXISTS atr_policy_restore_certifications (
  cert_id text PRIMARY KEY,
  run_id text,
  mode text NOT NULL,                    -- audit_only | bounded_execute
  drill_code text NOT NULL,
  source text NOT NULL,
  symbol text NOT NULL,
  scenario text NOT NULL,
  regime text NOT NULL,
  risk_horizon_bucket text NOT NULL,
  status text NOT NULL,                  -- passed | failed
  checks_json jsonb NOT NULL,
  summary_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_atr_policy_restore_certifications_lookup
  ON atr_policy_restore_certifications (created_at DESC, drill_code, status);
