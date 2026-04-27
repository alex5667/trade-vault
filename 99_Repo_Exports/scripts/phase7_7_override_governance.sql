-- Phase 7.7: Formal Safe-State Ladder + Operator Override Governance

CREATE TABLE IF NOT EXISTS atr_safe_state_policies (
  policy_id text PRIMARY KEY,
  trigger_kind text NOT NULL,
  scope_kind text NOT NULL,
  max_allowed_state text NOT NULL,
  min_required_state text NOT NULL,
  policy_json jsonb NOT NULL,
  is_enabled boolean NOT NULL DEFAULT true,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS atr_override_requests (
  override_id text PRIMARY KEY,
  override_class text NOT NULL,
  scope_kind text NOT NULL,
  source text,
  venue text,
  symbol text,
  scenario text,
  regime text,
  risk_horizon_bucket text,
  layer text,
  policy_ver integer,
  requested_target_state text NOT NULL,
  current_state text NOT NULL,
  status text NOT NULL,                  -- requested | approved | active | expired | rejected | revoked
  requester text NOT NULL,
  approver text,
  reason_code text NOT NULL,
  ttl_sec integer NOT NULL,
  not_after timestamptz NOT NULL,
  request_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  activated_at timestamptz,
  expired_at timestamptz
);

CREATE TABLE IF NOT EXISTS atr_override_events (
  id bigserial PRIMARY KEY,
  override_id text NOT NULL,
  old_status text NOT NULL,
  new_status text NOT NULL,
  reason_code text NOT NULL,
  event_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS atr_post_override_certifications (
  cert_id text PRIMARY KEY,
  override_id text NOT NULL,
  status text NOT NULL,                  -- pending | passed | failed
  checks_json jsonb NOT NULL,
  summary_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  finished_at timestamptz
);

CREATE OR REPLACE VIEW v_governance_override_board AS
SELECT
    override_id,
    override_class,
    scope_kind,
    requested_target_state,
    current_state,
    status,
    requester,
    approver,
    not_after,
    created_at
FROM atr_override_requests
WHERE status IN ('requested','approved','active')
ORDER BY not_after ASC, created_at DESC;

-- Insert some default basic policies to be safe 
INSERT INTO atr_safe_state_policies (policy_id, trigger_kind, scope_kind, max_allowed_state, min_required_state, policy_json) 
VALUES 
('pol_alloc_canary', 'allocator_stale', 'canary', 'clip', 'scope_frozen', '{}'),
('pol_alloc_live', 'allocator_stale', 'live_100', 'no_new_risk', 'hard_freeze', '{}'),
('pol_replay_budget', 'replay_budget_exhausted', 'global', 'promotions_frozen', 'hard_freeze', '{}'),
('pol_release_block', 'release_blockers_exhausted', 'global', 'release_frozen', 'hard_freeze', '{}'),
('pol_venue_down', 'venue_down', 'venue', 'venue_frozen', 'venue_frozen', '{}'),
('pol_protect_breach', 'protective_exit_invariant_breach', 'global', 'hard_freeze', 'hard_freeze', '{}'),
('pol_sev1_live', 'open_sev1_live', 'global', 'release_frozen', 'hard_freeze', '{"also_block_promotions": true}')
ON CONFLICT (policy_id) DO NOTHING;
