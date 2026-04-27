-- Phase 7.2: Invariant Expansion for Rollout / Allocator / Degrade / Portfolio

INSERT INTO atr_invariants (
  invariant_id, invariant_class, scope_kind, severity, enforcement_mode, title, reason_code, invariant_json
) VALUES
('INV_NO_STAGE_ADVANCE_WITHOUT_ROLLOUT_CERT', 'governance', 'layer', 'critical', 'release_block',
 'No stage advance without rollout cert pass', 'INV_NO_STAGE_ADVANCE_WITHOUT_ROLLOUT_CERT',
 '{"must_hold":"required_rollout_cert_status=passed"}'::jsonb),

('INV_NO_ALLOCATOR_ON_STALE_STATE_FOR_LIVE_SCOPE', 'governance', 'policy_ver', 'critical', 'runtime_deny',
 'Live scope cannot trade on stale allocator state', 'INV_NO_ALLOCATOR_ON_STALE_STATE_FOR_LIVE_SCOPE',
 '{"when":"rollout_stage=live_100","must_hold":"allocator_state=fresh"}'::jsonb),

('INV_NO_NEW_RISK_UNDER_DEGRADE', 'execution', 'venue', 'critical', 'runtime_deny',
 'No new risk when degrade state forbids it', 'INV_NO_NEW_RISK_UNDER_DEGRADE',
 '{"must_hold":"degrade_state not in [reduce_only,no_new_risk,hard_freeze] for new entries"}'::jsonb),

('INV_PROTECTIVE_EXITS_ALWAYS_ALLOWED_UNDER_DEGRADE', 'position', 'global', 'critical', 'runtime_deny',
 'Protective exits must remain allowed under degrade', 'INV_PROTECTIVE_EXITS_ALWAYS_ALLOWED_UNDER_DEGRADE',
 '{"must_hold":"protective_exit_actions_allowed=true"}'::jsonb),

('INV_NO_PORTFOLIO_CAP_BYPASS', 'execution', 'cohort', 'critical', 'runtime_deny',
 'No order may bypass portfolio cap', 'INV_NO_PORTFOLIO_CAP_BYPASS',
 '{"must_hold":"portfolio_gate_allow=true"}'::jsonb),

('INV_NO_LIVE_SCOPE_WITH_OPEN_CRITICAL_INCIDENT', 'governance', 'cohort', 'critical', 'release_block',
 'No live scope with open critical incident', 'INV_NO_LIVE_SCOPE_WITH_OPEN_CRITICAL_INCIDENT',
 '{"must_hold":"open_related_sev1_incidents=0"}'::jsonb),

('INV_NO_OVERRIDE_RELEASE_WITH_UNRESOLVED_CRITICAL_POSTMORTEM_ACTION', 'governance', 'cohort', 'critical', 'release_block',
 'No override release with unresolved critical PM actions', 'INV_NO_OVERRIDE_RELEASE_WITH_UNRESOLVED_CRITICAL_POSTMORTEM_ACTION',
 '{"must_hold":"overdue_p0_p1_actions=0"}'::jsonb),

('INV_ROLLBACK_MUST_DOWNGRADE_ROLLOUT_OR_RESTORE_LAST_GOOD', 'governance', 'layer', 'critical', 'replay_fail',
 'Rollback must actually downgrade or restore last_good', 'INV_ROLLBACK_MUST_DOWNGRADE_ROLLOUT_OR_RESTORE_LAST_GOOD',
 '{"must_hold":"stage_downgraded=true OR last_good_restored=true"}'::jsonb),

('INV_TRAILING_LAYER_CANNOT_BE_LIVE_IF_STOP_TTL_LAYER_FROZEN', 'governance', 'layer', 'error', 'release_block',
 'Trailing cannot stay live if stop_ttl frozen', 'INV_TRAILING_LAYER_CANNOT_BE_LIVE_IF_STOP_TTL_LAYER_FROZEN',
 '{"must_hold":"NOT(stop_ttl in [frozen,rolled_back] AND trailing=live_100)"}'::jsonb)
ON CONFLICT DO NOTHING;

-- DROP + CREATE required: cannot rename view columns via CREATE OR REPLACE
DROP VIEW IF EXISTS v_governance_invariant_board;
CREATE VIEW v_governance_invariant_board AS
SELECT
    violation_id,
    invariant_id,
    scope_kind,
    scope_value,
    surface,
    severity,
    status,
    reason_code,
    created_at
FROM atr_invariant_violations
WHERE status NOT IN ('resolved', 'ignored')
ORDER BY
    CASE severity
      WHEN 'critical' THEN 1
      WHEN 'error' THEN 2
      WHEN 'warn' THEN 3
      ELSE 4
    END,
    created_at DESC;

-- DROP + CREATE for consistency (column list change guard)
DROP VIEW IF EXISTS v_governance_release_invariant_blockers;
CREATE VIEW v_governance_release_invariant_blockers AS
SELECT
    scope_value,
    COUNT(*) FILTER (WHERE severity='critical' AND status NOT IN ('resolved','ignored')) AS critical_open,
    COUNT(*) FILTER (WHERE severity='error'    AND status NOT IN ('resolved','ignored')) AS error_open
FROM atr_invariant_violations
WHERE surface IN ('release_gate','runtime','replay')
GROUP BY scope_value;
