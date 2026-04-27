-- Phase 10.1: ATR Charter Compliance Governance
-- Transforms operating charter into machine-readable policy and automated monitoring.

-- 1. Policy Registry: Definitions of charter rules
CREATE TABLE IF NOT EXISTS atr_charter_policy_registry (
  policy_id text PRIMARY KEY,
  charter_version text NOT NULL,
  rule_id text NOT NULL,
  category text NOT NULL,
  severity text NOT NULL,                  -- info | warn | error | critical
  enforcement_mode text NOT NULL,          -- advisory | blocking | blocking_high_critical_only
  scope_kind text NOT NULL,
  policy_json jsonb NOT NULL,
  owner text NOT NULL,
  status text NOT NULL,                    -- draft | approved | active | deprecated
  created_at timestamptz NOT NULL DEFAULT now(),
  activated_at timestamptz
);

CREATE INDEX IF NOT EXISTS idx_atr_charter_policy_rule ON atr_charter_policy_registry(rule_id);
CREATE INDEX IF NOT EXISTS idx_atr_charter_policy_status ON atr_charter_policy_registry(status);

-- 2. Compliance Mapping: Links rules to technical evaluators
CREATE TABLE IF NOT EXISTS atr_charter_compliance_mapping (
  mapping_id text PRIMARY KEY,
  rule_id text NOT NULL,
  source_type text NOT NULL,               -- sql | redis | stream | graph | cert | env | artifact
  source_ref text NOT NULL,
  evaluator_type text NOT NULL,            -- sql_assert | redis_assert | stream_assert | artifact_present | cert_status | env_match
  evaluator_json jsonb NOT NULL,
  evidence_required boolean NOT NULL DEFAULT true,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_atr_charter_mapping_rule ON atr_charter_compliance_mapping(rule_id);

-- 3. Compliance Results: Individual per-rule outcomes
CREATE TABLE IF NOT EXISTS atr_charter_compliance_results (
  result_id text PRIMARY KEY,
  context_kind text NOT NULL,              -- release | runtime | restore | weekly_review | ...
  context_ref text NOT NULL,
  rule_id text NOT NULL,
  status text NOT NULL,                    -- passed | failed | warning | skipped
  severity text NOT NULL,
  reason_code text NOT NULL,
  evidence_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_atr_charter_res_ctx ON atr_charter_compliance_results(context_kind, context_ref);
CREATE INDEX IF NOT EXISTS idx_atr_charter_res_rule ON atr_charter_compliance_results(rule_id);

-- 4. Compliance Bundles: Aggregated outcomes for specific events
CREATE TABLE IF NOT EXISTS atr_charter_compliance_bundles (
  bundle_id text PRIMARY KEY,
  context_kind text NOT NULL,
  context_ref text NOT NULL,
  overall_status text NOT NULL,            -- passed | warning | blocked
  summary_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_atr_charter_bundle_ctx ON atr_charter_compliance_bundles(context_kind, context_ref);

-- 5. Governance Views
CREATE OR REPLACE VIEW v_governance_charter_policy_board AS
SELECT
  rule_id,
  category,
  severity,
  enforcement_mode,
  owner,
  status,
  activated_at
FROM atr_charter_policy_registry
ORDER BY activated_at DESC;

CREATE OR REPLACE VIEW v_governance_charter_compliance_board AS
SELECT
  context_kind,
  context_ref,
  overall_status,
  summary_json,
  created_at
FROM atr_charter_compliance_bundles
ORDER BY created_at DESC;

CREATE OR REPLACE VIEW v_governance_charter_compliance_failures AS
SELECT
  context_kind,
  context_ref,
  rule_id,
  severity,
  reason_code,
  evidence_json,
  created_at
FROM atr_charter_compliance_results
WHERE status = 'failed'
ORDER BY created_at DESC;

-- 6. Permissions
DO $$
BEGIN
    EXECUTE 'GRANT ALL ON atr_charter_policy_registry TO trading';
    EXECUTE 'GRANT ALL ON atr_charter_compliance_mapping TO trading';
    EXECUTE 'GRANT ALL ON atr_charter_compliance_results TO trading';
    EXECUTE 'GRANT ALL ON atr_charter_compliance_bundles TO trading';
    EXECUTE 'GRANT SELECT ON v_governance_charter_policy_board TO trading';
    EXECUTE 'GRANT SELECT ON v_governance_charter_compliance_board TO trading';
    EXECUTE 'GRANT SELECT ON v_governance_charter_compliance_failures TO trading';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Permission grants skipped: %', SQLERRM;
END
$$;
