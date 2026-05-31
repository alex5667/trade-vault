CREATE TABLE IF NOT EXISTS atr_change_requests (
  change_id text PRIMARY KEY,
  change_type text NOT NULL,
  scope_kind text NOT NULL,                -- global | venue | symbol | cohort | layer | policy_ver
  source text,
  venue text,
  symbol text,
  scenario text,
  regime text,
  risk_horizon_bucket text,
  layer text,
  policy_ver integer,
  status text NOT NULL,
  title text NOT NULL,
  author text NOT NULL,
  owner text NOT NULL,
  risk_level text NOT NULL,                -- low | medium | high | critical
  reason_code text NOT NULL,
  request_json jsonb NOT NULL,
  created_at_ms bigint NOT NULL,
  updated_at_ms bigint NOT NULL
);

CREATE TABLE IF NOT EXISTS atr_change_artifacts (
  id bigserial PRIMARY KEY,
  change_id text NOT NULL,
  artifact_kind text NOT NULL,             -- replay_manifest | replay_report | rollout_manifest | rollback_manifest | evidence_pack | approval_note
  artifact_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS atr_change_approvals (
  id bigserial PRIMARY KEY,
  change_id text NOT NULL,
  actor text NOT NULL,
  action text NOT NULL,                    -- approve | reject | pause | rollback
  note text NOT NULL DEFAULT '',
  action_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS atr_change_transitions (
  id bigserial PRIMARY KEY,
  change_id text NOT NULL,
  old_status text NOT NULL,
  new_status text NOT NULL,
  reason_code text NOT NULL,
  transition_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);
