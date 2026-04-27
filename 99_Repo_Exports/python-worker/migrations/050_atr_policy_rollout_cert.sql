CREATE TABLE IF NOT EXISTS atr_rollout_certifications (
  cert_id text PRIMARY KEY,
  change_id text NOT NULL,
  rollout_stage text NOT NULL,          -- canary_5 | canary_25 | live_100
  scope_kind text NOT NULL,
  source text,
  venue text,
  symbol text,
  scenario text,
  regime text,
  risk_horizon_bucket text,
  layer text,
  policy_ver integer,
  status text NOT NULL,                 -- pending | passed | failed | rolled_back
  monitoring_window_from timestamptz NOT NULL,
  monitoring_window_to timestamptz NOT NULL,
  thresholds_json jsonb NOT NULL,
  checks_json jsonb NOT NULL,
  summary_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  finished_at timestamptz
);

CREATE TABLE IF NOT EXISTS atr_rollout_cert_events (
  id bigserial PRIMARY KEY,
  cert_id text NOT NULL,
  change_id text NOT NULL,
  rollout_stage text NOT NULL,
  action text NOT NULL,                 -- start | pass | fail | pause | rollback
  reason_code text NOT NULL,
  event_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS atr_rollout_closeout_packs (
  closeout_id text PRIMARY KEY,
  change_id text NOT NULL,
  final_status text NOT NULL,           -- completed | rolled_back | failed
  evidence_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

-- Indeces for performance
CREATE INDEX IF NOT EXISTS idx_atr_rollout_certifications_change_id ON atr_rollout_certifications(change_id);
CREATE INDEX IF NOT EXISTS idx_atr_rollout_cert_events_cert_id ON atr_rollout_cert_events(cert_id);
CREATE INDEX IF NOT EXISTS idx_atr_rollout_cert_events_change_id ON atr_rollout_cert_events(change_id);
