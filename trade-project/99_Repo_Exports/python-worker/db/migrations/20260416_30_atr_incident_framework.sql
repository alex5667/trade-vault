-- Phase 6.4: Incident Control Framework

CREATE TABLE IF NOT EXISTS atr_incidents (
  incident_id text PRIMARY KEY,
  incident_class text NOT NULL,
  severity text NOT NULL,
  scope_kind text NOT NULL,          -- global | venue | symbol | cohort | layer | policy_ver
  source text,
  venue text,
  symbol text,
  scenario text,
  regime text,
  risk_horizon_bucket text,
  layer text,
  policy_ver integer,
  status text NOT NULL,
  owner text NOT NULL,
  detected_by text NOT NULL,         -- health_aggregator | gate | operator | replay_runner | cert_service
  reason_code text NOT NULL,
  incident_json jsonb NOT NULL,
  opened_at_ms bigint NOT NULL,
  updated_at_ms bigint NOT NULL,
  closed_at_ms bigint
);

CREATE TABLE IF NOT EXISTS atr_incident_runbooks (
  runbook_id text PRIMARY KEY,
  incident_class text NOT NULL,
  severity text NOT NULL,
  runbook_json jsonb NOT NULL,
  version_tag text NOT NULL,
  is_current boolean NOT NULL DEFAULT true,
  created_at timestamptz NOT NULL DEFAULT now()
);

-- Pre-seed core runbooks
INSERT INTO atr_incident_runbooks (runbook_id, incident_class, severity, runbook_json, version_tag, is_current)
VALUES (
  'rbk_mt5_down_v1', 
  'VENUE_MT5_DOWN', 
  'SEV-1', 
  '{"prechecks": ["mt5_connection_error_rate > threshold", "orders_queue_mt5_backlog rising", "protective exits status check"], "immediate_actions": [{"action": "set_degrade", "target": "venue:mt5", "state": "no_new_risk"}, {"action": "set_kill_switch", "target": "venue:mt5", "state": "active"}], "allowed_during_incident": ["protective_exits_only"], "forbidden_during_incident": ["new_entries_mt5", "rollout_promotion", "allocator_rebalance_on_mt5"], "recovery_checks": ["mt5_connection_ok_5m", "requote_rate_normalized", "backlog_cleared"], "post_actions": [{"action": "request_post_incident_cert"}, {"action": "create_change_record_if_override_persisted"}]}'::jsonb, 
  'v1.0.0', 
  true
) ON CONFLICT (runbook_id) DO NOTHING;

INSERT INTO atr_incident_runbooks (runbook_id, incident_class, severity, runbook_json, version_tag, is_current)
VALUES (
  'rbk_redis_blind_v1', 
  'REDIS_BLIND', 
  'SEV-1', 
  '{"prechecks": ["redis_health_ping_failed"], "immediate_actions": [{"action": "freeze", "target": "all"}], "allowed_during_incident": [], "forbidden_during_incident": ["new_entries_all", "rollback", "operator_approvals"], "recovery_checks": ["redis_health_ok_5m"], "post_actions": []}'::jsonb, 
  'v1.0.0', 
  true
) ON CONFLICT (runbook_id) DO NOTHING;

INSERT INTO atr_incident_runbooks (runbook_id, incident_class, severity, runbook_json, version_tag, is_current)
VALUES (
  'rbk_feature_drift_v1', 
  'FEATURE_DRIFT_FREEZE', 
  'SEV-2', 
  '{"prechecks": ["drift_hard_limit_breach"], "immediate_actions": [{"action": "set_degrade", "target": "affected_scope", "state": "clip"}], "allowed_during_incident": ["protective_exits", "reduced_allocator"], "forbidden_during_incident": ["rollout_promotion"], "recovery_checks": ["drift_metrics_normalized", "manual_operator_review_completed"], "post_actions": [{"action": "create_change_record_if_override_persisted"}]}'::jsonb, 
  'v1.0.0', 
  true
) ON CONFLICT (runbook_id) DO NOTHING;

CREATE TABLE IF NOT EXISTS atr_incident_actions (
  id bigserial PRIMARY KEY,
  incident_id text NOT NULL,
  action text NOT NULL,              -- ack | clip | freeze | reroute | rollback_request | clear_kill_switch | close
  actor text NOT NULL,
  reason_code text NOT NULL,
  action_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS atr_incident_evidence_packs (
  evidence_id text PRIMARY KEY,
  incident_id text NOT NULL,
  evidence_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

-- Index for fast status aggregation and owner queries
CREATE INDEX IF NOT EXISTS idx_atr_incidents_status ON atr_incidents(status, updated_at_ms);
CREATE INDEX IF NOT EXISTS idx_atr_incidents_class ON atr_incidents(incident_class);
