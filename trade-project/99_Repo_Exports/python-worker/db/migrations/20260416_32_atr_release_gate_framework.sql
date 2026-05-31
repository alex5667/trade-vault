-- Post-trade execution database - Phase 6.6 Release Gate Framework

CREATE TABLE IF NOT EXISTS atr_release_scorecards (
  scorecard_id text PRIMARY KEY,
  change_id text NOT NULL,
  scope_kind text NOT NULL,
  source text,
  venue text,
  symbol text,
  scenario text,
  regime text,
  risk_horizon_bucket text,
  layer text,
  policy_ver integer,
  readiness_score double precision NOT NULL,
  decision text NOT NULL,                -- allow | allow_with_override | deny
  blockers_json jsonb NOT NULL,
  warnings_json jsonb NOT NULL,
  infos_json jsonb NOT NULL,
  summary_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_atr_release_scorecards_change_id ON atr_release_scorecards(change_id);

CREATE TABLE IF NOT EXISTS atr_release_decisions (
  decision_id text PRIMARY KEY,
  change_id text NOT NULL,
  scorecard_id text NOT NULL,
  actor text NOT NULL,
  action text NOT NULL,                  -- approve_release | deny_release | override_release | pause_release
  reason_code text NOT NULL,
  decision_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_atr_release_decisions_change_id ON atr_release_decisions(change_id);

CREATE TABLE IF NOT EXISTS atr_release_gates (
  gate_id text PRIMARY KEY,
  change_type text NOT NULL,
  risk_level text NOT NULL,              -- low | medium | high | critical
  gate_policy_json jsonb NOT NULL,
  is_current boolean NOT NULL DEFAULT true,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_atr_release_gates_current ON atr_release_gates(change_type, risk_level) WHERE is_current = true;
