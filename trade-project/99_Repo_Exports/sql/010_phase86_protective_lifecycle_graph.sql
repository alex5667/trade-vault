-- Phase 8.6: Graph-Backed Protective Lifecycle Shadowing
-- Mirrors post-trade protective lifecycle into the control-plane graph
-- for shadow comparison and certification before any enforcement.
--
-- Tables:
--   atr_protective_equivalence_checks  — C1-C7 certification results
--   atr_protective_drifts              — drift taxonomy with severity
--   atr_protective_cutover_readiness   — readiness ladder
--
-- Views:
--   v_governance_protective_graph_board — auditor board
--   v_governance_protective_drift_board — open drifts sorted by severity

-- ──────────────────────────────────────────────────────────────────────
-- 1. Equivalence checks
-- ──────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS atr_protective_equivalence_checks (
    check_id       text        PRIMARY KEY,
    signal_id      text        NOT NULL,
    legacy_state_json  jsonb   NOT NULL,
    graph_state_json   jsonb   NOT NULL,
    status         text        NOT NULL,   -- passed | failed
    summary_json   jsonb       NOT NULL,
    created_at     timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_atr_prot_eq_signal
    ON atr_protective_equivalence_checks (signal_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_atr_prot_eq_status
    ON atr_protective_equivalence_checks (status, created_at DESC);

-- ──────────────────────────────────────────────────────────────────────
-- 2. Drift records
-- ──────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS atr_protective_drifts (
    drift_id       text        PRIMARY KEY,
    signal_id      text        NOT NULL,
    drift_kind     text        NOT NULL,
    severity       text        NOT NULL,   -- warn | error | critical
    status         text        NOT NULL,   -- open | resolved
    reason_code    text        NOT NULL,
    drift_json     jsonb       NOT NULL,
    created_at     timestamptz NOT NULL DEFAULT now(),
    resolved_at    timestamptz
);

CREATE INDEX IF NOT EXISTS idx_atr_prot_drift_signal
    ON atr_protective_drifts (signal_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_atr_prot_drift_open
    ON atr_protective_drifts (status, severity, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_atr_prot_drift_kind
    ON atr_protective_drifts (drift_kind, severity);

-- ──────────────────────────────────────────────────────────────────────
-- 3. Cutover readiness ladder
-- ──────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS atr_protective_cutover_readiness (
    readiness_id   text        PRIMARY KEY,
    component      text        NOT NULL,   -- protective_lifecycle
    status         text        NOT NULL,   -- not_ready | shadow_healthy | ready_for_read | ready_for_enforce
    summary_json   jsonb       NOT NULL,
    created_at     timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_atr_prot_cutover_comp
    ON atr_protective_cutover_readiness (component, created_at DESC);

-- ──────────────────────────────────────────────────────────────────────
-- 4. Governance views
-- ──────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW v_governance_protective_graph_board AS
SELECT
    check_id,
    signal_id,
    status,
    summary_json,
    created_at
FROM atr_protective_equivalence_checks
ORDER BY created_at DESC;

CREATE OR REPLACE VIEW v_governance_protective_drift_board AS
SELECT
    drift_id,
    signal_id,
    drift_kind,
    severity,
    status,
    reason_code,
    drift_json,
    created_at
FROM atr_protective_drifts
WHERE status = 'open'
ORDER BY
    CASE severity
        WHEN 'critical' THEN 1
        WHEN 'error'    THEN 2
        ELSE 3
    END,
    created_at DESC;

-- ──────────────────────────────────────────────────────────────────────
-- 5. Permissions (match existing pattern)
-- ──────────────────────────────────────────────────────────────────────
DO $$
BEGIN
    EXECUTE 'GRANT ALL ON atr_protective_equivalence_checks TO trading';
    EXECUTE 'GRANT ALL ON atr_protective_drifts TO trading';
    EXECUTE 'GRANT ALL ON atr_protective_cutover_readiness TO trading';
    EXECUTE 'GRANT SELECT ON v_governance_protective_graph_board TO trading';
    EXECUTE 'GRANT SELECT ON v_governance_protective_drift_board TO trading';
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Permission grants skipped (role may not exist): %', SQLERRM;
END
$$;
