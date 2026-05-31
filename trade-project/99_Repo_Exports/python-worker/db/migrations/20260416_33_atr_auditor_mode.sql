BEGIN;

CREATE TABLE IF NOT EXISTS atr_policy_health_states (
    id bigserial PRIMARY KEY,
    scope_kind text NOT NULL,
    scope_value text NOT NULL,
    redis_state text NOT NULL,
    sql_state text NOT NULL,
    metrics_state text NOT NULL,
    regime_state text NOT NULL,
    allocator_state text NOT NULL,
    venue_state text NOT NULL,
    degrade_state text NOT NULL,
    reason_code text NOT NULL,
    is_current boolean NOT NULL DEFAULT true,
    updated_at_ms bigint NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_atr_policy_health_states_curr ON atr_policy_health_states(scope_kind, scope_value) WHERE is_current = true;

-- 1. Create Immutable Auditor Snapshots Table
CREATE TABLE IF NOT EXISTS atr_auditor_snapshots (
  snapshot_id text PRIMARY KEY,
  snapshot_kind text NOT NULL,         -- release_board | incident_board | postmortem_board | runtime_health
  snapshot_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_atr_auditor_snapshots_kind_time ON atr_auditor_snapshots(snapshot_kind, created_at DESC);

-- 2. Release Readiness Board View (Unified Current Status)
CREATE OR REPLACE VIEW v_governance_current_state AS
SELECT
    cr.change_id,
    cr.change_type,
    cr.status AS change_status,
    cr.owner,
    cr.risk_level,
    rs.readiness_score,
    rs.decision AS release_decision,
    rs.blockers_json,
    rs.warnings_json,
    COALESCE((
      SELECT count(*) FROM atr_incidents i
      WHERE i.status NOT IN ('CLOSED')
        AND (
          i.symbol IS NULL OR i.symbol = cr.symbol
        )
    ), 0) AS open_related_incidents,
    COALESCE((
      SELECT count(*) FROM atr_corrective_actions a
      JOIN atr_postmortems p ON p.postmortem_id = a.postmortem_id
      WHERE a.status NOT IN ('verified','dropped')
        AND a.due_at_ms < extract(epoch from now()) * 1000
    ), 0) AS overdue_actions
FROM atr_change_requests cr
LEFT JOIN LATERAL (
    SELECT *
    FROM atr_release_scorecards rs
    WHERE rs.change_id = cr.change_id
    ORDER BY rs.created_at DESC
    LIMIT 1
) rs ON true;

-- 3. Incident Board View
CREATE OR REPLACE VIEW v_governance_incident_board AS
SELECT
    i.incident_id,
    i.incident_class,
    i.severity,
    i.scope_kind,
    i.venue,
    i.symbol,
    i.status,
    i.owner,
    i.reason_code,
    i.detected_by,
    i.opened_at_ms,
    CASE
      WHEN i.closed_at_ms IS NULL THEN
        (extract(epoch from now()) * 1000 - i.opened_at_ms) / 1000
      ELSE
        (i.closed_at_ms - i.opened_at_ms) / 1000
    END AS age_sec
FROM atr_incidents i
WHERE i.status NOT IN ('CLOSED')
ORDER BY
    CASE i.severity
      WHEN 'SEV-1' THEN 1
      WHEN 'SEV-2' THEN 2
      WHEN 'SEV-3' THEN 3
      ELSE 4
    END,
    i.opened_at_ms DESC;

-- 4. Postmortem Board View
CREATE OR REPLACE VIEW v_governance_postmortem_board AS
SELECT
    p.postmortem_id,
    p.incident_id,
    p.severity,
    p.status,
    p.owner,
    p.root_cause_class,
    p.reason_code,
    COUNT(a.*) FILTER (WHERE a.status NOT IN ('verified','dropped')) AS open_actions,
    COUNT(a.*) FILTER (WHERE a.due_at_ms < extract(epoch from now()) * 1000
                       AND a.status NOT IN ('verified','dropped')) AS overdue_actions
FROM atr_postmortems p
LEFT JOIN atr_corrective_actions a
  ON a.postmortem_id = p.postmortem_id
GROUP BY
    p.postmortem_id, p.incident_id, p.severity, p.status,
    p.owner, p.root_cause_class, p.reason_code;

-- 5. Runtime Health Board View
CREATE OR REPLACE VIEW v_governance_runtime_health AS
SELECT
    scope_kind,
    scope_value,
    redis_state,
    sql_state,
    metrics_state,
    regime_state,
    allocator_state,
    venue_state,
    degrade_state,
    reason_code,
    updated_at_ms
FROM atr_policy_health_states
WHERE is_current = true;

COMMIT;
