CREATE TABLE IF NOT EXISTS atr_golden_datasets (
  dataset_id text PRIMARY KEY,
  dataset_class text NOT NULL,
  status text NOT NULL,                  -- CANDIDATE | APPROVED | ACTIVE | DEPRECATED | RETIRED
  scope_json jsonb NOT NULL,
  manifest_json jsonb NOT NULL,
  owner text NOT NULL,
  approver text,
  reason_code text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  activated_at timestamptz,
  retired_at timestamptz
);

CREATE TABLE IF NOT EXISTS atr_replay_cert_runs (
  run_id text PRIMARY KEY,
  change_id text NOT NULL,
  change_class text NOT NULL,
  dataset_id text NOT NULL,
  status text NOT NULL,                  -- running | passed | failed | waived
  baseline_ref text NOT NULL,
  candidate_ref text NOT NULL,
  summary_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  finished_at timestamptz
);

CREATE TABLE IF NOT EXISTS atr_replay_cert_checks (
  check_id text PRIMARY KEY,
  run_id text NOT NULL,
  check_name text NOT NULL,
  status text NOT NULL,                  -- passed | failed | pending
  severity text NOT NULL,                -- warn | error | critical
  details_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS atr_golden_dataset_reviews (
  review_id text PRIMARY KEY,
  dataset_id text NOT NULL,
  reviewer text NOT NULL,
  review_status text NOT NULL,           -- approved | rejected | refresh_required
  review_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);
