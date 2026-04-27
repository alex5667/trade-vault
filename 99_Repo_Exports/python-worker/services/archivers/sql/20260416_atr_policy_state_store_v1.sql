CREATE TABLE IF NOT EXISTS atr_policy_proposals (
  proposal_id text PRIMARY KEY,
  policy_ver integer NOT NULL,
  source text NOT NULL,
  symbol text NOT NULL,
  scenario text NOT NULL,
  regime text NOT NULL,
  risk_horizon_bucket text NOT NULL,
  stop_ttl_mode text NOT NULL,
  trailing_mode text NOT NULL,
  reason_code text NOT NULL,
  status text NOT NULL,
  approved boolean NOT NULL DEFAULT false,
  proposal_json jsonb NOT NULL,
  created_at_ms bigint NOT NULL,
  updated_at_ms bigint NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_atr_policy_proposals_lookup
  ON atr_policy_proposals (source, symbol, scenario, regime, risk_horizon_bucket, updated_at_ms DESC);

CREATE TABLE IF NOT EXISTS atr_policy_decisions (
  id bigserial PRIMARY KEY,
  proposal_id text NOT NULL,
  action text NOT NULL,
  actor text NOT NULL,
  note text NOT NULL DEFAULT '',
  decision_json jsonb NOT NULL,
  ts_ms bigint NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_atr_policy_decisions_proposal
  ON atr_policy_decisions (proposal_id, ts_ms DESC);

CREATE TABLE IF NOT EXISTS atr_policy_snapshots (
  id bigserial PRIMARY KEY,
  snapshot_kind text NOT NULL, -- active | last_good
  source text NOT NULL,
  symbol text NOT NULL,
  scenario text NOT NULL,
  regime text NOT NULL,
  risk_horizon_bucket text NOT NULL,
  policy_ver integer NOT NULL,
  stop_ttl_mode text NOT NULL,
  trailing_mode text NOT NULL,
  snapshot_json jsonb NOT NULL,
  is_current boolean NOT NULL DEFAULT true,
  effective_from_ms bigint NOT NULL,
  effective_to_ms bigint,
  applied_from_proposal_id text,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_atr_policy_snapshots_current
  ON atr_policy_snapshots (snapshot_kind, source, symbol, scenario, regime, risk_horizon_bucket)
  WHERE is_current = true;

CREATE INDEX IF NOT EXISTS idx_atr_policy_snapshots_lookup
  ON atr_policy_snapshots (snapshot_kind, source, symbol, scenario, regime, risk_horizon_bucket, created_at DESC);

CREATE TABLE IF NOT EXISTS atr_policy_recovery_events (
  id bigserial PRIMARY KEY,
  event_type text NOT NULL,
  source text NOT NULL,
  symbol text NOT NULL,
  scenario text NOT NULL,
  regime text NOT NULL,
  risk_horizon_bucket text NOT NULL,
  status text NOT NULL,
  reason_code text NOT NULL,
  payload jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_atr_policy_recovery_events_lookup
  ON atr_policy_recovery_events (source, symbol, scenario, regime, risk_horizon_bucket, created_at DESC);
