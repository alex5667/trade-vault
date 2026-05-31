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
  status text NOT NULL,                  -- running | passed | passed_with_warnings | failed | waived_fail
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

CREATE INDEX IF NOT EXISTS idx_atr_golden_ds_class ON atr_golden_datasets (dataset_class, status);
CREATE INDEX IF NOT EXISTS idx_atr_replay_runs_change ON atr_replay_cert_runs (change_id, change_class);
CREATE INDEX IF NOT EXISTS idx_atr_replay_checks_run ON atr_replay_cert_checks (run_id);

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chk_atr_golden_datasets_status') THEN
    ALTER TABLE atr_golden_datasets
      ADD CONSTRAINT chk_atr_golden_datasets_status
      CHECK (status IN ('CANDIDATE', 'APPROVED', 'ACTIVE', 'DEPRECATED', 'RETIRED'));
  END IF;

  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chk_atr_replay_cert_runs_status') THEN
    ALTER TABLE atr_replay_cert_runs
      ADD CONSTRAINT chk_atr_replay_cert_runs_status
      CHECK (status IN ('running', 'passed', 'passed_with_warnings', 'failed', 'waived_fail'));
  END IF;

  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chk_atr_replay_cert_checks_status') THEN
    ALTER TABLE atr_replay_cert_checks
      ADD CONSTRAINT chk_atr_replay_cert_checks_status
      CHECK (status IN ('passed', 'failed', 'pending'));
  END IF;

  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chk_atr_replay_cert_checks_severity') THEN
    ALTER TABLE atr_replay_cert_checks
      ADD CONSTRAINT chk_atr_replay_cert_checks_severity
      CHECK (severity IN ('warn', 'error', 'critical'));
  END IF;

  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'chk_atr_golden_dataset_reviews_status') THEN
    ALTER TABLE atr_golden_dataset_reviews
      ADD CONSTRAINT chk_atr_golden_dataset_reviews_status
      CHECK (review_status IN ('approved', 'rejected', 'refresh_required'));
  END IF;

  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_atr_replay_runs_dataset') THEN
    ALTER TABLE atr_replay_cert_runs
      ADD CONSTRAINT fk_atr_replay_runs_dataset
      FOREIGN KEY (dataset_id) REFERENCES atr_golden_datasets(dataset_id);
  END IF;

  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_atr_replay_checks_run') THEN
    ALTER TABLE atr_replay_cert_checks
      ADD CONSTRAINT fk_atr_replay_checks_run
      FOREIGN KEY (run_id) REFERENCES atr_replay_cert_runs(run_id) ON DELETE CASCADE;
  END IF;

  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_atr_golden_dataset_reviews_dataset') THEN
    ALTER TABLE atr_golden_dataset_reviews
      ADD CONSTRAINT fk_atr_golden_dataset_reviews_dataset
      FOREIGN KEY (dataset_id) REFERENCES atr_golden_datasets(dataset_id);
  END IF;
END $$;
