-- Phase 6.3 - Formal Rollback Framework Tables

CREATE TABLE IF NOT EXISTS atr_rollback_requests (
  rollback_id text PRIMARY KEY,
  change_id text,
  rollback_class text NOT NULL,
  scope_kind text NOT NULL,              -- global | venue | symbol | cohort | layer | policy_ver
  source text,
  venue text,
  symbol text,
  scenario text,
  regime text,
  risk_horizon_bucket text,
  layer text,
  policy_ver integer,
  target_policy_ver integer,
  target_stage text,
  use_last_good boolean NOT NULL DEFAULT false,
  status text NOT NULL,
  author text NOT NULL,
  owner text NOT NULL,
  reason_code text NOT NULL,
  request_json jsonb NOT NULL,
  created_at_ms bigint NOT NULL,
  updated_at_ms bigint NOT NULL
);

CREATE TABLE IF NOT EXISTS atr_rollback_artifacts (
  id bigserial PRIMARY KEY,
  rollback_id text NOT NULL,
  artifact_kind text NOT NULL,           -- rollback_manifest | rollback_plan | rollback_report | rollback_cert | rollback_evidence
  artifact_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS atr_rollback_events (
  id bigserial PRIMARY KEY,
  rollback_id text NOT NULL,
  old_status text NOT NULL,
  new_status text NOT NULL,
  reason_code text NOT NULL,
  event_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS atr_post_rollback_certifications (
  cert_id text PRIMARY KEY,
  rollback_id text NOT NULL,
  scope_kind text NOT NULL,
  source text,
  venue text,
  symbol text,
  scenario text,
  regime text,
  risk_horizon_bucket text,
  layer text,
  target_policy_ver integer,
  status text NOT NULL,                  -- pending | passed | failed
  checks_json jsonb NOT NULL,
  summary_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  finished_at timestamptz
);
