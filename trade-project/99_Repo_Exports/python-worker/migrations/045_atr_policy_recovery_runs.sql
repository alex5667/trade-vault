CREATE TABLE IF NOT EXISTS atr_policy_recovery_runs (
  run_id text PRIMARY KEY,
  mode text NOT NULL,
  status text NOT NULL,              -- started | finished | failed
  steps_json jsonb NOT NULL,
  summary_json jsonb NOT NULL,
  started_at timestamptz NOT NULL DEFAULT now(),
  finished_at timestamptz
);

CREATE INDEX IF NOT EXISTS idx_atr_policy_recovery_runs_started_at
  ON atr_policy_recovery_runs (started_at DESC);
