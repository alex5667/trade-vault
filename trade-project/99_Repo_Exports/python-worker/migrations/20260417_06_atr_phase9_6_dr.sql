-- Phase 9.6: Disaster Recovery and Cold-Start Policy

CREATE TABLE IF NOT EXISTS atr_dr_events (
  dr_id text PRIMARY KEY,
  dr_class text NOT NULL,                 -- WARM_RESTART | PARTIAL_REDIS_LOSS | GRAPH_PROJECTION_LOSS | EXECUTION_BRIDGE_LOSS | PROTECTIVE_STATE_LOSS | FULL_CONTROL_PLANE_COLD_START | FULL_STACK_COLD_START
  scope_kind text NOT NULL,               -- global | venue | symbol | cohort | layer
  scope_value text NOT NULL,
  status text NOT NULL,                   -- opened | restoring | observing | completed | failed
  reason_code text NOT NULL,
  dr_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  completed_at timestamptz
);

CREATE TABLE IF NOT EXISTS atr_restore_steps (
  step_id text PRIMARY KEY,
  dr_id text NOT NULL REFERENCES atr_dr_events(dr_id),
  domain text NOT NULL,                   -- control_plane | signal_runtime | execution | protective | analytics
  step_name text NOT NULL,
  status text NOT NULL,                   -- pending | running | passed | failed
  details_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS atr_restore_certifications (
  cert_id text PRIMARY KEY,
  dr_id text NOT NULL REFERENCES atr_dr_events(dr_id),
  cert_kind text NOT NULL,                -- control_plane_restore | signal_restore | execution_restore | protective_restore | full_restore
  status text NOT NULL,                   -- passed | failed | pending
  checks_json jsonb NOT NULL,
  summary_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  finished_at timestamptz
);

-- Indexes for quick lookup
CREATE INDEX IF NOT EXISTS idx_atr_dr_events_status ON atr_dr_events (status);
CREATE INDEX IF NOT EXISTS idx_atr_restore_steps_dr_id ON atr_restore_steps (dr_id);
CREATE INDEX IF NOT EXISTS idx_atr_restore_certifications_dr_id ON atr_restore_certifications (dr_id);
