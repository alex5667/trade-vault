-- Phase 6.1: Golden Datasets Registry and Replay Manifests

CREATE TABLE IF NOT EXISTS atr_replay_datasets (
  dataset_id text PRIMARY KEY,
  dataset_type text NOT NULL,          -- signal_raw | diagnostics | execution_shadow | closed_trades | mixed_bundle
  symbol text,
  scenario text,
  regime text,
  venue text,
  window_from timestamptz NOT NULL,
  window_to timestamptz NOT NULL,
  source_stream text,
  row_count bigint NOT NULL,
  storage_uri text NOT NULL,           -- file:// / s3:// / minio://
  sha256 text NOT NULL,
  schema_ver text NOT NULL,
  tags_json jsonb NOT NULL,
  is_golden boolean NOT NULL DEFAULT false,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS atr_replay_dataset_versions (
  id bigserial PRIMARY KEY,
  dataset_id text NOT NULL,
  version_tag text NOT NULL,           -- v1, v2, baseline_2026w16
  sha256 text NOT NULL,
  note text NOT NULL DEFAULT '',
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS atr_replay_manifests (
  replay_id text PRIMARY KEY,
  change_id text,
  replay_kind text NOT NULL,           -- gate_replay | execution_replay | portfolio_replay | full_change_replay
  baseline_ref text NOT NULL,
  candidate_ref text NOT NULL,
  datasets_json jsonb NOT NULL,
  config_json jsonb NOT NULL,
  thresholds_json jsonb NOT NULL,
  status text NOT NULL,                -- draft | running | passed | failed
  created_at timestamptz NOT NULL DEFAULT now(),
  finished_at timestamptz
);

-- Note: atr_change_artifacts and atr_change_transitions exist from Phase 6.0
-- (Defined in atr_change_control_service.py implicitly or in previous migrations)

-- Add index on change_id for lookups
CREATE INDEX IF NOT EXISTS idx_atr_replay_manifests_change_id ON atr_replay_manifests(change_id);
