-- Migration for Phase 9.7 Backup, Retention and Replay Archive Policy
-- Created at: 2026-04-17

CREATE TABLE IF NOT EXISTS atr_archive_policies (
  policy_id text PRIMARY KEY,
  artifact_class text NOT NULL,           -- signal | dispatch | execution | protective | post_trade | governance
  retention_hot_days integer NOT NULL,
  retention_warm_days integer NOT NULL,
  retention_cold_days integer NOT NULL,
  archive_format text NOT NULL,           -- ndjson | parquet | sql_snapshot | manifest_json
  policy_json jsonb NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS atr_replay_bundles (
  bundle_id text PRIMARY KEY,
  artifact_scope text NOT NULL,
  time_start timestamptz NOT NULL,
  time_end timestamptz NOT NULL,
  status text NOT NULL,                   -- building | ready | failed | restored
  manifest_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  restored_at timestamptz
);

CREATE TABLE IF NOT EXISTS atr_backup_jobs (
  job_id text PRIMARY KEY,
  job_kind text NOT NULL,                 -- redis_export | sql_backup | replay_bundle | archive_compaction | archive_restore_check
  artifact_class text NOT NULL,
  status text NOT NULL,                   -- scheduled | running | passed | failed
  summary_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  finished_at timestamptz
);

CREATE TABLE IF NOT EXISTS atr_archive_integrity_checks (
  check_id text PRIMARY KEY,
  bundle_id text,
  check_kind text NOT NULL,               -- checksum | manifest_complete | restore_replay | restore_sql | restore_projection
  status text NOT NULL,                   -- passed | failed
  details_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

-- Indexes for querying operational logs and bundles
CREATE INDEX IF NOT EXISTS idx_atr_replay_bundles_created ON atr_replay_bundles(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_atr_backup_jobs_created ON atr_backup_jobs(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_atr_archive_checks_created ON atr_archive_integrity_checks(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_atr_replay_bundles_scope ON atr_replay_bundles(artifact_scope);
