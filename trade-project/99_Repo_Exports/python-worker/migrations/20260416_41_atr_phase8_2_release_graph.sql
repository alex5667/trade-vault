-- Phase 8.2: Graph-backed release gate
-- Tables: equivalence checks, release drifts, cutover readiness
-- Views: release readiness, effective release state, governance boards

-- ─── Tables ──────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS atr_release_equivalence_checks (
    check_id        text         PRIMARY KEY,
    change_id       text         NOT NULL,
    scope_value     text         NOT NULL,
    legacy_decision text         NOT NULL,
    graph_decision  text         NOT NULL,
    status          text         NOT NULL,  -- passed | failed
    summary_json    jsonb        NOT NULL DEFAULT '{}',
    created_at      timestamptz  NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_rel_equiv_change
    ON atr_release_equivalence_checks (change_id);
CREATE INDEX IF NOT EXISTS idx_rel_equiv_scope_created
    ON atr_release_equivalence_checks (scope_value, created_at DESC);

-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS atr_release_drifts (
    drift_id     text         PRIMARY KEY,
    change_id    text         NOT NULL,
    scope_value  text         NOT NULL,
    drift_kind   text         NOT NULL,   -- release_decision_mismatch | missing_replay_cert_edge | …
    severity     text         NOT NULL,   -- critical | error | warn
    status       text         NOT NULL,   -- open | resolved
    reason_code  text         NOT NULL,
    drift_json   jsonb        NOT NULL DEFAULT '{}',
    created_at   timestamptz  NOT NULL DEFAULT now(),
    resolved_at  timestamptz
);

CREATE INDEX IF NOT EXISTS idx_rel_drift_open
    ON atr_release_drifts (status, severity, created_at DESC)
    WHERE status = 'open';
CREATE INDEX IF NOT EXISTS idx_rel_drift_change
    ON atr_release_drifts (change_id, created_at DESC);

-- ─────────────────────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS atr_release_cutover_readiness (
    readiness_id  text         PRIMARY KEY,
    component     text         NOT NULL,   -- release_gate
    status        text         NOT NULL,   -- not_ready | shadow_healthy | ready_for_read | ready_for_enforce
    summary_json  jsonb        NOT NULL DEFAULT '{}',
    created_at    timestamptz  NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_rel_cutover_component_created
    ON atr_release_cutover_readiness (component, created_at DESC);

-- ─── Views ────────────────────────────────────────────────────────────────────

-- Raw release-domain node selector
CREATE OR REPLACE VIEW v_control_plane_release_readiness AS
SELECT
    n.node_id,
    n.scope_kind,
    n.scope_value,
    n.node_state_json,
    n.version,
    n.updated_at
FROM atr_control_plane_nodes n
WHERE n.node_type IN ('RolloutState', 'ReleaseScorecard', 'FreezeState', 'OverrideState');

-- Resolved release projection via graph edges
CREATE OR REPLACE VIEW v_control_plane_effective_release_state AS
SELECT DISTINCT ON (n_rollout.scope_value)
    n_rollout.node_id                                         AS rollout_node_id,
    n_rollout.scope_value,
    n_rollout.node_state_json ->> 'rollout_stage'             AS rollout_stage,
    n_release.node_state_json ->> 'decision'                  AS release_decision,
    n_release.node_state_json -> 'blockers'                   AS blockers_json,
    n_release.node_state_json -> 'warnings'                   AS warnings_json,
    n_freeze.node_state_json ->> 'freeze_state'               AS freeze_state,
    n_override.node_state_json ->> 'override_state'           AS override_state,
    n_replay.node_state_json ->> 'status'                     AS replay_cert_status,
    n_cert.node_state_json ->> 'status'                       AS rollout_cert_status
FROM atr_control_plane_nodes n_rollout
LEFT JOIN atr_control_plane_edges e_release
    ON e_release.from_node_id = n_rollout.node_id AND e_release.edge_type = 'releases'
LEFT JOIN atr_control_plane_nodes n_release
    ON n_release.node_id = e_release.to_node_id
LEFT JOIN atr_control_plane_edges e_freeze
    ON e_freeze.to_node_id = n_rollout.node_id AND e_freeze.edge_type = 'freezes'
LEFT JOIN atr_control_plane_nodes n_freeze
    ON n_freeze.node_id = e_freeze.from_node_id
LEFT JOIN atr_control_plane_edges e_override
    ON e_override.to_node_id = n_rollout.node_id AND e_override.edge_type = 'overrides'
LEFT JOIN atr_control_plane_nodes n_override
    ON n_override.node_id = e_override.from_node_id
LEFT JOIN atr_control_plane_edges e_replay
    ON e_replay.to_node_id = n_rollout.node_id AND e_replay.edge_type = 'certifies'
LEFT JOIN atr_control_plane_nodes n_replay
    ON n_replay.node_id = e_replay.from_node_id AND n_replay.node_type = 'ReplayCertification'
LEFT JOIN atr_control_plane_edges e_cert
    ON e_cert.to_node_id = n_rollout.node_id AND e_cert.edge_type = 'certifies'
LEFT JOIN atr_control_plane_nodes n_cert
    ON n_cert.node_id = e_cert.from_node_id AND n_cert.node_type = 'RolloutCertification'
WHERE n_rollout.node_type = 'RolloutState'
ORDER BY n_rollout.scope_value, n_rollout.node_id DESC;


-- Auditor board: equivalence checks (last 200)
CREATE OR REPLACE VIEW v_governance_release_graph_board AS
SELECT
    check_id,
    change_id,
    scope_value,
    legacy_decision,
    graph_decision,
    status,
    created_at
FROM atr_release_equivalence_checks
ORDER BY created_at DESC;

-- Auditor board: open release drifts by severity
CREATE OR REPLACE VIEW v_governance_release_drift_board AS
SELECT
    drift_id,
    change_id,
    scope_value,
    drift_kind,
    severity,
    status,
    created_at
FROM atr_release_drifts
WHERE status = 'open'
ORDER BY
    CASE severity
        WHEN 'critical' THEN 1
        WHEN 'error'    THEN 2
        ELSE 3
    END,
    created_at DESC;
