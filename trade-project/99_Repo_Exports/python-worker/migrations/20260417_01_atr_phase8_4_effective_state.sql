CREATE TABLE IF NOT EXISTS atr_effective_state_equivalence_checks (
  check_id text PRIMARY KEY,
  scope_value text NOT NULL,
  legacy_state_json jsonb NOT NULL,
  graph_state_json jsonb NOT NULL,
  status text NOT NULL,                  -- passed | failed
  summary_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS atr_effective_state_drifts (
  drift_id text PRIMARY KEY,
  scope_value text NOT NULL,
  drift_kind text NOT NULL,              -- state_mismatch | precedence_mismatch | constraint_mismatch | projection_version_mismatch
  severity text NOT NULL,                -- warn | error | critical
  status text NOT NULL,                  -- open | resolved
  reason_code text NOT NULL,
  drift_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  resolved_at timestamptz
);

CREATE TABLE IF NOT EXISTS atr_effective_state_cutover_readiness (
  readiness_id text PRIMARY KEY,
  component text NOT NULL,               -- effective_state_resolver
  status text NOT NULL,                  -- not_ready | shadow_healthy | ready_for_read | ready_for_enforce
  summary_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE OR REPLACE VIEW v_governance_effective_state_board AS
SELECT
  scope_value,
  status,
  created_at
FROM atr_effective_state_equivalence_checks
ORDER BY created_at DESC;

CREATE OR REPLACE VIEW v_governance_effective_state_drift_board AS
SELECT
  scope_value,
  drift_kind,
  severity,
  status,
  created_at
FROM atr_effective_state_drifts
WHERE status = 'open'
ORDER BY
  CASE severity
    WHEN 'critical' THEN 1
    WHEN 'error' THEN 2
    ELSE 3
  END,
  created_at DESC;

-- Grant permissions for trading role so workers can use them
GRANT ALL PRIVILEGES ON TABLE atr_effective_state_equivalence_checks TO trading;
GRANT ALL PRIVILEGES ON TABLE atr_effective_state_drifts TO trading;
GRANT ALL PRIVILEGES ON TABLE atr_effective_state_cutover_readiness TO trading;
GRANT SELECT ON v_governance_effective_state_board TO trading;
GRANT SELECT ON v_governance_effective_state_drift_board TO trading;
