-- Phase 8.3 — ATR Freeze / Override Graph-Backed Source of Truth Migration (Refined)

-- 1. Equivalence Checks for Shadow Mode
DROP TABLE IF EXISTS atr_freeze_override_equivalence_checks CASCADE;
CREATE TABLE atr_freeze_override_equivalence_checks (
  check_id text PRIMARY KEY,
  scope_kind text NOT NULL,              -- symbol | global
  scope_value text NOT NULL,             -- e.g. BTCUSDT | all
  legacy_state_json jsonb NOT NULL,
  graph_state_json jsonb NOT NULL,
  checks_json jsonb NOT NULL,            -- detailed results of F1-F9
  drift_detected boolean NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX idx_atr_freeze_override_eq_scope
  ON atr_freeze_override_equivalence_checks (scope_kind, scope_value, created_at DESC);

-- 2. Governance Drifts (specific to freeze/override)
DROP TABLE IF EXISTS atr_freeze_override_drifts CASCADE;
CREATE TABLE atr_freeze_override_drifts (
  drift_id text PRIMARY KEY,
  scope_kind text NOT NULL,
  scope_value text NOT NULL,
  drift_type text NOT NULL,              -- equivalence_failure | precedence_violation
  status text NOT NULL DEFAULT 'open',   -- open | resolved
  source_diff_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  resolved_at timestamptz
);

CREATE INDEX idx_atr_freeze_override_drifts_open
  ON atr_freeze_override_drifts (scope_kind, scope_value, drift_type) WHERE status = 'open';

-- 3. Cutover Readiness Ladder (Per-Scope)
DROP TABLE IF EXISTS atr_freeze_override_cutover_readiness CASCADE;
CREATE TABLE atr_freeze_override_cutover_readiness (
  scope_kind text NOT NULL,
  scope_value text NOT NULL,
  readiness_stage text NOT NULL,         -- F1_DUAL_WRITE_ESTABLISHED | F2_SHADOW_CONSISTENT | F3_CERTIFIED | F4_READ_PRIMARY
  status text NOT NULL,                  -- passed | failed | blocked
  last_cert_id text,
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (scope_kind, scope_value)
);

-- 4. Auditor Integration Views
CREATE OR REPLACE VIEW v_governance_freeze_override_drift_board AS
SELECT
  scope_kind,
  scope_value,
  drift_type,
  status,
  created_at,
  source_diff_json
FROM atr_freeze_override_drifts
WHERE status = 'open'
ORDER BY created_at DESC;

CREATE OR REPLACE VIEW v_governance_freeze_override_readiness AS
SELECT
  scope_kind,
  scope_value,
  readiness_stage,
  status,
  updated_at
FROM atr_freeze_override_cutover_readiness
ORDER BY 
  CASE readiness_stage
    WHEN 'F4_READ_PRIMARY' THEN 4
    WHEN 'F3_CERTIFIED' THEN 3
    WHEN 'F2_SHADOW_CONSISTENT' THEN 2
    WHEN 'F1_DUAL_WRITE_ESTABLISHED' THEN 1
    ELSE 0
  END DESC,
  updated_at DESC;
