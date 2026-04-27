-- Phase 8: ATR Deterministic Control-Plane Graph Migration

-- 1. Control-Plane Event Journal (Append-only source of truth)
CREATE TABLE IF NOT EXISTS atr_control_plane_events (
  event_id text PRIMARY KEY,
  event_type text NOT NULL,                 -- node_created | state_transition | cert_attached | freeze_applied | override_activated | rollback_requested | release_decided
  aggregate_type text NOT NULL,             -- rollout | allocator | freeze | incident | override | release | rollback
  aggregate_id text NOT NULL,
  scope_kind text NOT NULL,
  scope_value text NOT NULL,
  actor text NOT NULL,
  reason_code text NOT NULL,
  event_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_atr_control_plane_events_agg
  ON atr_control_plane_events (aggregate_type, aggregate_id, created_at DESC);

-- 2. Control-Plane Nodes (Current Graph State)
CREATE TABLE IF NOT EXISTS atr_control_plane_nodes (
  node_id text PRIMARY KEY,
  node_type text NOT NULL,
  scope_kind text NOT NULL,
  scope_value text NOT NULL,
  node_state_json jsonb NOT NULL,
  version bigint NOT NULL,
  last_event_id text NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_atr_control_plane_nodes_scope
  ON atr_control_plane_nodes (scope_value, node_type);

-- 3. Control-Plane Edges (Relationships like depends_on, blocks, overrides)
CREATE TABLE IF NOT EXISTS atr_control_plane_edges (
  edge_id text PRIMARY KEY,
  from_node_id text NOT NULL,
  to_node_id text NOT NULL,
  edge_type text NOT NULL,                 -- depends_on | blocks | certifies | overrides | restores | freezes | releases
  edge_state_json jsonb NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_atr_control_plane_edges_from
  ON atr_control_plane_edges (from_node_id, edge_type);
CREATE INDEX IF NOT EXISTS idx_atr_control_plane_edges_to
  ON atr_control_plane_edges (to_node_id, edge_type);

-- 4. Graph Certifications (transition, consistency, projection)
CREATE TABLE IF NOT EXISTS atr_control_plane_certifications (
  cert_id text PRIMARY KEY,
  cert_kind text NOT NULL,                  -- transition_cert | graph_consistency_cert | projection_consistency_cert
  target_node_id text,
  status text NOT NULL,                     -- pending | passed | failed
  checks_json jsonb NOT NULL,
  summary_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  finished_at timestamptz
);

CREATE INDEX IF NOT EXISTS idx_atr_control_plane_cert_target
  ON atr_control_plane_certifications (target_node_id, cert_kind);

-- 5. Illegal Transition Tracking
CREATE TABLE IF NOT EXISTS atr_illegal_transition_attempts (
  attempt_id text PRIMARY KEY,
  aggregate_type text NOT NULL,
  aggregate_id text NOT NULL,
  requested_transition text NOT NULL,
  actor text NOT NULL,
  reason_code text NOT NULL,
  attempt_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

-- 6. Read-Only Auditor Views
CREATE OR REPLACE VIEW v_control_plane_active_nodes AS
SELECT
  node_id,
  node_type,
  scope_kind,
  scope_value,
  version,
  updated_at
FROM atr_control_plane_nodes
ORDER BY updated_at DESC;

CREATE OR REPLACE VIEW v_control_plane_blockers AS
SELECT
  e.from_node_id,
  e.to_node_id,
  e.edge_type,
  e.edge_state_json,
  e.updated_at
FROM atr_control_plane_edges e
WHERE e.edge_type IN ('blocks', 'freezes', 'overrides');
