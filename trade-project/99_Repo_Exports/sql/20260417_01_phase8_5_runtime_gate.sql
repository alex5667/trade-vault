BEGIN;

CREATE TABLE IF NOT EXISTS atr_runtime_gate_equivalence_checks (
  check_id text PRIMARY KEY,
  scope_value text NOT NULL,
  legacy_decision text NOT NULL,         -- allow | clip | deny
  graph_decision text NOT NULL,          -- allow | clip | deny
  status text NOT NULL,                  -- passed | failed
  summary_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS atr_runtime_gate_drifts (
  drift_id text PRIMARY KEY,
  scope_value text NOT NULL,
  drift_kind text NOT NULL,              -- decision_mismatch | risk_mult_mismatch | blocker_family_mismatch | projection_stale
  severity text NOT NULL,                -- warn | error | critical
  status text NOT NULL,                  -- open | resolved
  reason_code text NOT NULL,
  drift_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  resolved_at timestamptz
);

CREATE TABLE IF NOT EXISTS atr_runtime_gate_cutover_readiness (
  readiness_id text PRIMARY KEY,
  component text NOT NULL,               -- runtime_gate
  status text NOT NULL,                  -- not_ready | shadow_healthy | ready_for_canary | ready_for_live
  summary_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE OR REPLACE VIEW v_governance_runtime_gate_graph_board AS
SELECT
  scope_value,
  legacy_decision,
  graph_decision,
  status,
  created_at
FROM atr_runtime_gate_equivalence_checks
ORDER BY created_at DESC;

CREATE OR REPLACE VIEW v_governance_runtime_gate_drift_board AS
SELECT
  scope_value,
  drift_kind,
  severity,
  status,
  created_at
FROM atr_runtime_gate_drifts
WHERE status = 'open'
ORDER BY
  CASE severity
    WHEN 'critical' THEN 1
    WHEN 'error' THEN 2
    ELSE 3
  END,
  created_at DESC;

COMMIT;
