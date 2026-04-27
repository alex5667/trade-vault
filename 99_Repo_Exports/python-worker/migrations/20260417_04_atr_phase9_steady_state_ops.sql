CREATE TABLE IF NOT EXISTS atr_operations_ownership (
  ownership_id text PRIMARY KEY,
  domain text NOT NULL,                   -- signal_gates | execution | control_plane | protective | analytics_audit
  owner text NOT NULL,
  secondary_owner text,
  oncall_rotation text,
  escalation_chain_json jsonb NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS atr_operations_cadence (
  cadence_id text PRIMARY KEY,
  cadence_kind text NOT NULL,             -- daily | weekly | monthly | quarterly
  domain text NOT NULL,
  task_name text NOT NULL,
  owner text NOT NULL,
  schedule_json jsonb NOT NULL,
  success_criteria_json jsonb NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS atr_operations_scorecards (
  scorecard_id text PRIMARY KEY,
  period_kind text NOT NULL,              -- daily | weekly | monthly
  period_start timestamptz NOT NULL,
  period_end timestamptz NOT NULL,
  domain text NOT NULL,
  scorecard_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);
