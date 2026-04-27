-- Phase 10.2: ATR Policy Enforcement Map
-- Defines where and how charter rules are enforced.

-- 1. Enforcement Map: Rule -> Context -> Layer -> Action
CREATE TABLE IF NOT EXISTS atr_policy_enforcement_map (
  map_id text PRIMARY KEY,
  rule_id text NOT NULL,
  context_kind text NOT NULL,            -- release_context | runtime_context | restore_context | protective_runtime_context | ...
  domain text NOT NULL,                  -- runtime | release | protective | dr | replay | dataset
  target_layer text NOT NULL,            -- L1..L9
  severity text NOT NULL,                -- info | warn | error | critical
  default_action text NOT NULL,          -- WARN | DIAG_ONLY | BLOCK_RELEASE | BLOCK_PROMOTION | DENY_NEW_RISK | CLIP_NEW_RISK | FREEZE_SCOPE | FREEZE_RELEASES | OPEN_QUARANTINE | REQUIRE_ROLLBACK_REVIEW | REQUIRE_DR_RESTORE
  escalation_action text,
  enforcement_mode text NOT NULL,        -- advisory | blocking_high_critical_only | blocking
  owner text NOT NULL,
  map_json jsonb NOT NULL,               -- store extra params like clip_factor, timeout, etc.
  created_at timestamptz NOT NULL DEFAULT now(),
  activated_at timestamptz
);

CREATE INDEX IF NOT EXISTS idx_atr_policy_enforce_rule ON atr_policy_enforcement_map(rule_id);
CREATE INDEX IF NOT EXISTS idx_atr_policy_enforce_ctx ON atr_policy_enforcement_map(context_kind);
CREATE INDEX IF NOT EXISTS idx_atr_policy_enforce_layer ON atr_policy_enforcement_map(target_layer);

-- 2. Enforcement Events: Audit log of individual rule enforcements
CREATE TABLE IF NOT EXISTS atr_policy_enforcement_events (
  event_id text PRIMARY KEY,
  context_kind text NOT NULL,
  context_ref text NOT NULL,
  rule_id text NOT NULL,
  target_layer text NOT NULL,
  action text NOT NULL,
  severity text NOT NULL,
  reason_code text NOT NULL,
  evidence_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_atr_enforce_event_ctx ON atr_policy_enforcement_events(context_kind, context_ref);
CREATE INDEX IF NOT EXISTS idx_atr_enforce_event_rule ON atr_policy_enforcement_events(rule_id);

-- 3. Enforcement Decisions: Aggregated final decision for a context
CREATE TABLE IF NOT EXISTS atr_policy_enforcement_decisions (
  decision_id text PRIMARY KEY,
  context_kind text NOT NULL,
  context_ref text NOT NULL,
  overall_action text NOT NULL,          -- allow | warn | block_release | deny_new_risk | freeze_scope | ...
  summary_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_atr_enforce_decision_ctx ON atr_policy_enforcement_decisions(context_kind, context_ref);

-- 4. Initial Seeding (Examples from user request)

-- CHARTER-R6: Release on quarantined scope
INSERT INTO atr_policy_enforcement_map (map_id, rule_id, context_kind, domain, target_layer, severity, default_action, escalation_action, enforcement_mode, owner, map_json)
VALUES (
    'map_r6_release', 'CHARTER-R6', 'release_context', 'release', 'L3', 'critical', 
    'BLOCK_RELEASE', 'FREEZE_RELEASES', 'blocking', 'platform_owner', 
    '{"reason_codes": {"fail": "CHARTER_RELEASE_ON_QUARANTINE_BLOCKED"}}'
) ON CONFLICT (map_id) DO NOTHING;

-- CHARTER-N4: No new risk without runtime decision
INSERT INTO atr_policy_enforcement_map (map_id, rule_id, context_kind, domain, target_layer, severity, default_action, enforcement_mode, owner, map_json)
VALUES (
    'map_n4_runtime', 'CHARTER-N4', 'runtime_context', 'runtime', 'L2', 'critical', 
    'DENY_NEW_RISK', 'blocking', 'runtime_owner', 
    '{"reason_codes": {"fail": "CHARTER_RUNTIME_DECISION_MISSING"}}'
) ON CONFLICT (map_id) DO NOTHING;

-- CHARTER-P7: Protective path not green
INSERT INTO atr_policy_enforcement_map (map_id, rule_id, context_kind, domain, target_layer, severity, default_action, enforcement_mode, owner, map_json)
VALUES (
    'map_p7_protective', 'CHARTER-P7', 'protective_runtime_context', 'protective', 'L8', 'critical', 
    'REQUIRE_ROLLBACK_REVIEW', 'blocking', 'protective_owner', 
    '{"reason_codes": {"fail": "CHARTER_PROTECTIVE_PATH_INVALID"}}'
) ON CONFLICT (map_id) DO NOTHING;

-- CHARTER-DQ: Data Quality (example from 10.1 gates)
INSERT INTO atr_policy_enforcement_map (map_id, rule_id, context_kind, domain, target_layer, severity, default_action, enforcement_mode, owner, map_json)
VALUES (
    'map_dq_runtime', 'CHARTER-DQ-STALE', 'runtime_context', 'runtime', 'L1', 'error', 
    'DENY_NEW_RISK', 'blocking', 'data_owner', 
    '{"reason_codes": {"fail": "CHARTER_DQ_BLOCK"}}'
) ON CONFLICT (map_id) DO NOTHING;

-- 5. Permissions
DO $$
BEGIN
    EXECUTE 'GRANT ALL ON atr_policy_enforcement_map TO trading';
    EXECUTE 'GRANT ALL ON atr_policy_enforcement_events TO trading';
    EXECUTE 'GRANT ALL ON atr_policy_enforcement_decisions TO trading';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Permission grants skipped: %', SQLERRM;
END
$$;
