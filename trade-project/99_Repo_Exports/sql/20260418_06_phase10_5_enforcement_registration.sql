-- Phase 10.5: Register Go-Live Readiness Rule and Enforcement Mapping

-- 1. Register CHARTER-G1 in the Charter Policy Registry
INSERT INTO atr_charter_policy_registry (
    policy_id, charter_version, rule_id, category, severity,
    enforcement_mode, scope_kind, policy_json, owner, status, activated_at
) VALUES (
    'pol_golive_readiness', '1.0.0', 'CHARTER-G1', 'system_readiness', 'critical',
    'blocking', 'global',
    jsonb_build_object(
        'contexts', jsonb_build_array('release_context', 'runtime_context', 'weekly_review'),
        'reason_codes', jsonb_build_object('fail', 'CHARTER_GO_LIVE_READINESS_FAIL')
    ),
    'technical_owner', 'active', NOW()
) ON CONFLICT (policy_id) DO UPDATE SET
    policy_json = EXCLUDED.policy_json,
    status = 'active';

-- 2. Map Rule to Evaluator (sql_assert against packages table)
-- Status is passed if there is a GO_LIVE or GO_LIVE_WITH_CONSTRAINTS package that is active and not expired
INSERT INTO atr_charter_compliance_mapping (
    mapping_id, rule_id, source_type, source_ref, evaluator_type, evaluator_json, evidence_required
) VALUES (
    'map_golive_check', 'CHARTER-G1', 'sql', 'atr_go_live_readiness_packages', 'sql_assert',
    jsonb_build_object(
        'predicate', 'verdict IN (''GO_LIVE'', ''GO_LIVE_WITH_CONSTRAINTS'') AND package_status = ''signed'' AND (expires_at IS NULL OR expires_at > NOW())'
    ),
    true
) ON CONFLICT (mapping_id) DO UPDATE SET
    evaluator_json = EXCLUDED.evaluator_json;

-- 3. Map Rule to enforcement actions (L3 Release, L2 Risk)
-- This ensures that if CHARTER-G1 fails (no signed package), the enforcement router triggers blocks.

-- Release Context -> BLOCK_RELEASE
INSERT INTO atr_policy_enforcement_map (
    map_id, rule_id, context_kind, domain, target_layer, severity, default_action, enforcement_mode, owner, map_json, activated_at
) VALUES (
    'map_golive_release_block', 'CHARTER-G1', 'release_context', 'release', 'L3', 'critical',
    'BLOCK_RELEASE', 'blocking', 'control_plane_owner',
    jsonb_build_object('reason_codes', jsonb_build_object('fail', 'GO_LIVE_STATE_MISSING_FOR_RELEASE')),
    NOW()
) ON CONFLICT (map_id) DO UPDATE SET
    default_action = EXCLUDED.default_action,
    enforcement_mode = EXCLUDED.enforcement_mode;

-- Runtime Context -> DENY_NEW_RISK
INSERT INTO atr_policy_enforcement_map (
    map_id, rule_id, context_kind, domain, target_layer, severity, default_action, enforcement_mode, owner, map_json, activated_at
) VALUES (
    'map_golive_runtime_deny', 'CHARTER-G1', 'runtime_context', 'runtime', 'L2', 'critical',
    'DENY_NEW_RISK', 'blocking', 'runtime_owner',
    jsonb_build_object('reason_codes', jsonb_build_object('fail', 'GO_LIVE_STATE_MISSING_FOR_RISK')),
    NOW()
) ON CONFLICT (map_id) DO UPDATE SET
    default_action = EXCLUDED.default_action,
    enforcement_mode = EXCLUDED.enforcement_mode;
