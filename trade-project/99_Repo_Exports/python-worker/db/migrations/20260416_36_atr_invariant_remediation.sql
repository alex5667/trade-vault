-- Phase 7.3: Invariant-driven Auto-Remediation migration

CREATE TABLE IF NOT EXISTS atr_invariant_remediation_policies (
  invariant_id text PRIMARY KEY,
  remediation_kind text NOT NULL,        -- deny_only | runtime_clip | scope_freeze | rollout_pause | rollback_request | last_good_restore | serving_rebuild
  is_auto_enabled boolean NOT NULL DEFAULT true,
  policy_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS atr_invariant_remediation_actions (
  action_id text PRIMARY KEY,
  violation_id text NOT NULL,
  invariant_id text NOT NULL,
  remediation_kind text NOT NULL,
  scope_kind text NOT NULL,
  scope_value text NOT NULL,
  status text NOT NULL,                  -- requested | executed | failed | skipped
  reason_code text NOT NULL,
  action_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  finished_at timestamptz
);

CREATE INDEX IF NOT EXISTS idx_atr_invariant_remediation_open
  ON atr_invariant_remediation_actions (status, created_at DESC);

-- Seed policies
INSERT INTO atr_invariant_remediation_policies (
  invariant_id, remediation_kind, is_auto_enabled, policy_json
) VALUES
('INV_SIGNAL_ID_REQUIRED', 'deny_only', true,
 '{"incident_open":true}'::jsonb),

('INV_PAYLOAD_BUY_ORDERING', 'deny_only', true,
 '{"incident_open":true}'::jsonb),

('INV_PAYLOAD_SELL_ORDERING', 'deny_only', true,
 '{"incident_open":true}'::jsonb),

('INV_NO_ORDER_WITHOUT_RISK_PCT_AND_SL', 'deny_only', true,
 '{"incident_open":true}'::jsonb),

('INV_NO_NEW_RISK_UNDER_DEGRADE', 'scope_freeze', true,
 '{"target_state":"no_new_risk","ttl_sec":900}'::jsonb),

('INV_NO_ALLOCATOR_ON_STALE_STATE_FOR_LIVE_SCOPE', 'scope_freeze', true,
 '{"target_state":"clip","clip_mult":0.25,"escalate_to_freeze_after_hits":3,"window_sec":300}'::jsonb),

('INV_NO_PORTFOLIO_CAP_BYPASS', 'scope_freeze', true,
 '{"target_state":"no_new_risk","ttl_sec":600,"incident_open_after_hits":5}'::jsonb),

('INV_NO_STAGE_ADVANCE_WITHOUT_ROLLOUT_CERT', 'rollout_pause', true,
 '{"pause_target":"change_or_rollout"}'::jsonb),

('INV_NO_LIVE_SCOPE_WITH_OPEN_CRITICAL_INCIDENT', 'rollout_pause', true,
 '{"pause_target":"release_gate"}'::jsonb),

('INV_ROLLBACK_MUST_DOWNGRADE_ROLLOUT_OR_RESTORE_LAST_GOOD', 'last_good_restore', false,
 '{"requires_operator_or_cert_path":true}'::jsonb),

('INV_TRAILING_LAYER_CANNOT_BE_LIVE_IF_STOP_TTL_LAYER_FROZEN', 'rollback_request', true,
 '{"rollback_class":"LAYER_ROLLBACK","target_layer":"trailing"}'::jsonb)

ON CONFLICT DO NOTHING;
