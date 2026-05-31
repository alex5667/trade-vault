CREATE TABLE IF NOT EXISTS atr_freeze_policies (
  policy_id text PRIMARY KEY,
  trigger_kind text NOT NULL,              -- runtime_budget_exhausted | protective_breach | release_budget_exhausted | replay_budget_exhausted | venue_incident | allocator_stale | portfolio_bypass
  scope_kind text NOT NULL,                -- global | venue | symbol | cohort | layer | policy_ver
  severity text NOT NULL,                  -- warn | error | critical
  freeze_state text NOT NULL,              -- clip | no_new_risk | scope_frozen | venue_frozen | promotions_frozen | release_frozen | hard_freeze
  ttl_sec integer NOT NULL,
  policy_json jsonb NOT NULL,
  is_enabled boolean NOT NULL DEFAULT true,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS atr_active_freezes (
  freeze_id text PRIMARY KEY,
  trigger_kind text NOT NULL,
  scope_kind text NOT NULL,
  scope_value text NOT NULL,
  freeze_state text NOT NULL,
  source_reason_code text NOT NULL,
  status text NOT NULL,                    -- active | recovering | released | failed
  started_at timestamptz NOT NULL DEFAULT now(),
  expires_at timestamptz,
  recovery_not_before timestamptz,
  freeze_json jsonb NOT NULL,
  released_at timestamptz
);

CREATE TABLE IF NOT EXISTS atr_freeze_events (
  id bigserial PRIMARY KEY,
  freeze_id text NOT NULL,
  old_status text NOT NULL,
  new_status text NOT NULL,
  reason_code text NOT NULL,
  event_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE OR REPLACE VIEW v_governance_freeze_board AS
SELECT
    freeze_id,
    trigger_kind,
    scope_kind,
    scope_value,
    freeze_state,
    status,
    source_reason_code,
    started_at,
    expires_at,
    recovery_not_before,
    released_at
FROM atr_active_freezes
WHERE status <> 'released'
ORDER BY
    CASE freeze_state
      WHEN 'hard_freeze' THEN 1
      WHEN 'venue_frozen' THEN 2
      WHEN 'scope_frozen' THEN 3
      WHEN 'release_frozen' THEN 4
      WHEN 'promotions_frozen' THEN 5
      ELSE 6
    END,
    started_at DESC;

-- Insert some default policies based on Phase 7.6 guidelines
INSERT INTO atr_freeze_policies (policy_id, trigger_kind, scope_kind, severity, freeze_state, ttl_sec, policy_json)
VALUES
  ('policy_runtime_crit', 'runtime_budget_exhausted', 'symbol', 'critical', 'scope_frozen', 3600, '{"description": "Freeze scope for 1h on runtime budget exhaust"}'),
  ('policy_protective_crit', 'protective_breach', 'global', 'critical', 'hard_freeze', 86400, '{"description": "Hard freeze 24h on protective invariant breach"}'),
  ('policy_release_crit', 'release_budget_exhausted', 'global', 'critical', 'release_frozen', 86400, '{"description": "Release frozen 24h on release budget exhaust"}'),
  ('policy_replay_crit', 'replay_budget_exhausted', 'global', 'critical', 'promotions_frozen', 86400, '{"description": "Promotions frozen 24h on replay budget exhaust"}'),
  ('policy_venue_sev1', 'venue_incident', 'venue', 'critical', 'venue_frozen', 14400, '{"description": "Venue frozen for 4h on SEV-1"}'),
  ('policy_allocator_stale', 'allocator_stale', 'global', 'error', 'scope_frozen', 1800, '{"description": "Scope frozen 30m if allocator stale"}'),
  ('policy_portfolio_bypass', 'portfolio_bypass', 'symbol', 'error', 'scope_frozen', 3600, '{"description": "Scope frozen 1h on portfolio bypass repeated"}')
ON CONFLICT (policy_id) DO NOTHING;
