CREATE TABLE IF NOT EXISTS atr_policy_coverage_inventory (
  surface_id text PRIMARY KEY,
  domain text NOT NULL,                    -- runtime | execution | protective | governance | replay | dr
  surface_json jsonb NOT NULL,
  owner text NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS atr_policy_coverage_results (
  result_id text PRIMARY KEY,
  surface_id text NOT NULL,
  dimension text NOT NULL,                 -- RULE_COVERAGE | ENFORCEMENT_COVERAGE | ...
  status text NOT NULL,                    -- covered | partial | missing | stale
  severity text NOT NULL,                  -- info | warn | error | critical
  reason_code text NOT NULL,
  details_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS atr_policy_gap_closure_matrix (
  row_id text PRIMARY KEY,
  surface_id text NOT NULL,
  gap_type text NOT NULL,
  severity text NOT NULL,
  owner text NOT NULL,
  remediation_status text NOT NULL,        -- open | planned | in_progress | done | verified | waived
  due_at timestamptz,
  remediation_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  closed_at timestamptz
);

CREATE TABLE IF NOT EXISTS atr_policy_coverage_audits (
  audit_id text PRIMARY KEY,
  scope_kind text NOT NULL,
  scope_value text NOT NULL,
  overall_status text NOT NULL,            -- passed | warning | failed
  summary_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE OR REPLACE VIEW v_governance_policy_coverage_board AS
SELECT
  surface_id,
  dimension,
  status,
  severity,
  reason_code,
  created_at
FROM atr_policy_coverage_results
ORDER BY created_at DESC;

CREATE OR REPLACE VIEW v_governance_gap_closure_board AS
SELECT
  surface_id,
  gap_type,
  severity,
  owner,
  remediation_status,
  due_at,
  created_at
FROM atr_policy_gap_closure_matrix
WHERE remediation_status <> 'verified'
ORDER BY
  CASE severity
    WHEN 'critical' THEN 1
    WHEN 'error' THEN 2
    ELSE 3
  END,
  due_at ASC NULLS LAST;

CREATE OR REPLACE VIEW v_governance_policy_coverage_audit_board AS
SELECT
  scope_kind,
  scope_value,
  overall_status,
  created_at
FROM atr_policy_coverage_audits
ORDER BY created_at DESC;
