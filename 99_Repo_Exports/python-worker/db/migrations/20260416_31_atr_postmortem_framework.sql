-- Phase 6.5: Formal Postmortem Framework & Recurring Failure Prevention

CREATE TABLE IF NOT EXISTS atr_postmortems (
  postmortem_id text PRIMARY KEY,
  incident_id text,
  change_id text,
  rollback_id text,
  severity text NOT NULL,
  status text NOT NULL,                  -- draft | review | approved | corrective_open | verified | closed
  title text NOT NULL,
  owner text NOT NULL,
  facilitator text NOT NULL,
  root_cause_class text NOT NULL,        -- code_bug | config_error | infra_failure | data_quality | venue_failure | governance_gap | model_failure
  reason_code text NOT NULL,
  summary_json jsonb NOT NULL,
  timeline_json jsonb NOT NULL,
  impact_json jsonb NOT NULL,
  contributing_factors_json jsonb NOT NULL,
  created_at_ms bigint NOT NULL,
  updated_at_ms bigint NOT NULL,
  closed_at_ms bigint
);

CREATE TABLE IF NOT EXISTS atr_corrective_actions (
  action_id text PRIMARY KEY,
  postmortem_id text NOT NULL,
  action_type text NOT NULL,             -- code_fix | config_fix | test | replay_pack | dashboard | alert | runbook | process | training
  severity text NOT NULL,
  owner text NOT NULL,
  reviewer text,
  status text NOT NULL,                  -- open | in_progress | blocked | done | verified | dropped
  priority text NOT NULL,                -- p0 | p1 | p2 | p3
  title text NOT NULL,
  reason_code text NOT NULL,
  due_at_ms bigint NOT NULL,
  completed_at_ms bigint,
  verification_required boolean NOT NULL DEFAULT true,
  action_json jsonb NOT NULL,
  verification_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS atr_failure_signatures (
  signature_id text PRIMARY KEY,
  signature_kind text NOT NULL,          -- reason_code_cluster | venue_error_pattern | slippage_shock_pattern | replay_mismatch_pattern
  signature_hash text NOT NULL,
  signature_json jsonb NOT NULL,
  first_seen_at timestamptz NOT NULL DEFAULT now(),
  last_seen_at timestamptz NOT NULL DEFAULT now(),
  hit_count bigint NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS atr_postmortem_regression_packs (
  pack_id text PRIMARY KEY,
  postmortem_id text NOT NULL,
  pack_kind text NOT NULL,               -- smoke_replay | stress_replay | venue_incident_replay | allocator_regression | rollout_regression
  manifest_json jsonb NOT NULL,
  status text NOT NULL,                  -- pending | passed | failed
  created_at timestamptz NOT NULL DEFAULT now(),
  finished_at timestamptz
);

CREATE TABLE IF NOT EXISTS atr_postmortem_verifications (
  verification_id text PRIMARY KEY,
  postmortem_id text NOT NULL,
  action_id text,
  verification_kind text NOT NULL,       -- replay | shadow_monitor | rollout_cert | incident_drill | dashboard_check
  status text NOT NULL,                  -- pending | passed | failed
  summary_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  finished_at timestamptz
);

-- Indexes for efficient lookups
CREATE INDEX IF NOT EXISTS idx_atr_postmortems_status ON atr_postmortems(status, updated_at_ms);
CREATE INDEX IF NOT EXISTS idx_atr_postmortems_incident ON atr_postmortems(incident_id);

CREATE INDEX IF NOT EXISTS idx_atr_corrective_actions_postmortem ON atr_corrective_actions(postmortem_id);
CREATE INDEX IF NOT EXISTS idx_atr_corrective_actions_status_due ON atr_corrective_actions(status, due_at_ms);

CREATE INDEX IF NOT EXISTS idx_atr_failure_signatures_hash ON atr_failure_signatures(signature_kind, signature_hash);
CREATE INDEX IF NOT EXISTS idx_atr_failure_signatures_last_seen ON atr_failure_signatures(last_seen_at);
