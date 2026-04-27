-- Phase 8.1: ATR Control-Plane Projection Cert and Shadow Graph Schema

CREATE TABLE IF NOT EXISTS atr_control_plane_events (
  event_id text PRIMARY KEY,
  scope_kind text NOT NULL,
  scope_value text NOT NULL,
  event_type text NOT NULL,
  payload_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS atr_control_plane_nodes (
  node_id text PRIMARY KEY,
  scope_kind text NOT NULL,
  scope_value text NOT NULL,
  node_type text NOT NULL,
  node_state_json jsonb NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS atr_control_plane_edges (
  edge_id text PRIMARY KEY,
  from_node_id text NOT NULL,
  to_node_id text NOT NULL,
  edge_type text NOT NULL,
  status text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS atr_control_plane_projection_checks (
  check_id text PRIMARY KEY,
  scope_kind text NOT NULL,
  scope_value text NOT NULL,
  check_kind text NOT NULL,               -- node_match | edge_match | projection_match | freshness_match
  status text NOT NULL,                   -- passed | failed
  summary_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS atr_control_plane_projection_drifts (
  drift_id text PRIMARY KEY,
  scope_kind text NOT NULL,
  scope_value text NOT NULL,
  drift_kind text NOT NULL,               -- missing_node | stale_node | state_mismatch | projection_mismatch | orphan_edge
  severity text NOT NULL,                 -- warn | error | critical
  status text NOT NULL,                   -- open | resolved
  reason_code text NOT NULL,
  drift_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  resolved_at timestamptz
);

CREATE TABLE IF NOT EXISTS atr_control_plane_cutover_readiness (
  readiness_id text PRIMARY KEY,
  component text NOT NULL,                -- release_gate | freeze_service | override_service | runtime_resolver
  status text NOT NULL,                   -- not_ready | shadow_healthy | ready_for_read | ready_for_enforce
  summary_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

-- Indexes for shadow graph
CREATE INDEX IF NOT EXISTS idx_atr_cp_events_scope ON atr_control_plane_events (scope_kind, scope_value, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_atr_cp_nodes_scope ON atr_control_plane_nodes (scope_kind, scope_value, node_type);
CREATE INDEX IF NOT EXISTS idx_atr_cp_edges_from ON atr_control_plane_edges (from_node_id, status);
CREATE INDEX IF NOT EXISTS idx_atr_cp_drifts_status ON atr_control_plane_projection_drifts (status, severity, created_at DESC);
