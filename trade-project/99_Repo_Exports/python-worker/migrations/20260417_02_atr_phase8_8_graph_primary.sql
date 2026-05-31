-- Phase 8.8: Graph Primary as Source of Truth

CREATE TABLE IF NOT EXISTS atr_graph_primary_cutover (
  cutover_id text PRIMARY KEY,
  component text NOT NULL,                 -- release | freeze | override | effective_state
  scope_value text NOT NULL,
  status text NOT NULL,                    -- requested | active | rolled_back
  summary_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  rolled_back_at timestamptz
);

CREATE TABLE IF NOT EXISTS atr_graph_reconciliation_drifts (
  drift_id text PRIMARY KEY,
  scope_value text NOT NULL,
  drift_kind text NOT NULL,
  severity text NOT NULL,                  -- warn | error | critical
  status text NOT NULL,                    -- open | resolved
  reason_code text NOT NULL,
  drift_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  resolved_at timestamptz
);

CREATE TABLE IF NOT EXISTS atr_graph_primary_authority_violations (
  violation_id text PRIMARY KEY,
  component text NOT NULL,
  scope_value text NOT NULL,
  actor text NOT NULL,
  reason_code text NOT NULL,
  violation_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

-- Note: Using CREATE OR REPLACE VIEW ensures idempotency
CREATE OR REPLACE VIEW v_governance_graph_primary_cutover_board AS
SELECT
  component,
  scope_value,
  status,
  created_at,
  rolled_back_at
FROM atr_graph_primary_cutover
ORDER BY created_at DESC;

CREATE OR REPLACE VIEW v_governance_graph_reconciliation_drift_board AS
SELECT
  scope_value,
  drift_kind,
  severity,
  status,
  created_at
FROM atr_graph_reconciliation_drifts
WHERE status = 'open'
ORDER BY
  CASE severity
    WHEN 'critical' THEN 1
    WHEN 'error' THEN 2
    ELSE 3
  END,
  created_at DESC;

CREATE OR REPLACE VIEW v_governance_graph_authority_violation_board AS
SELECT
  component,
  scope_value,
  actor,
  reason_code,
  created_at
FROM atr_graph_primary_authority_violations
ORDER BY created_at DESC;

-- Grant permissions for new tables and views to trading user
GRANT SELECT, INSERT, UPDATE, DELETE ON atr_graph_primary_cutover TO trading;
GRANT SELECT, INSERT, UPDATE, DELETE ON atr_graph_reconciliation_drifts TO trading;
GRANT SELECT, INSERT, UPDATE, DELETE ON atr_graph_primary_authority_violations TO trading;

GRANT SELECT ON v_governance_graph_primary_cutover_board TO trading;
GRANT SELECT ON v_governance_graph_reconciliation_drift_board TO trading;
GRANT SELECT ON v_governance_graph_authority_violation_board TO trading;
