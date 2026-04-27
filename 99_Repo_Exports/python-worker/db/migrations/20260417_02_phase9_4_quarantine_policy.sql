-- Phase 9.4: Incident-to-Release Quarantine Policy
-- Creates the taxonomy for quarantine states and rules explicitly blocking releases.

CREATE TABLE IF NOT EXISTS atr_release_quarantines (
  quarantine_id text PRIMARY KEY,
  incident_id text NOT NULL,
  quarantine_class text NOT NULL,
  scope_kind text NOT NULL,
  scope_value text NOT NULL,
  status text NOT NULL,                  -- NOT_QUARANTINED | QUARANTINED | RECOVERING_IN_QUARANTINE | READY_FOR_REVIEW | RELEASE_ELIGIBLE | WAIVED
  severity text NOT NULL,                -- warn | error | critical
  reason_code text NOT NULL,
  started_at timestamptz NOT NULL DEFAULT now(),
  not_before_release_at timestamptz NOT NULL,
  summary_json jsonb NOT NULL,
  released_at timestamptz
);

CREATE INDEX IF NOT EXISTS idx_atr_release_quarantines_status ON atr_release_quarantines(status);
CREATE INDEX IF NOT EXISTS idx_atr_release_quarantines_scope ON atr_release_quarantines(scope_kind, scope_value);

CREATE TABLE IF NOT EXISTS atr_quarantine_exit_checks (
  check_id text PRIMARY KEY,
  quarantine_id text NOT NULL,
  check_name text NOT NULL,
  status text NOT NULL,                  -- passed | failed | pending
  details_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_atr_quarantine_exit_checks_qid ON atr_quarantine_exit_checks(quarantine_id);

CREATE TABLE IF NOT EXISTS atr_quarantine_waivers (
  waiver_id text PRIMARY KEY,
  quarantine_id text NOT NULL,
  approver text NOT NULL,
  reason_code text NOT NULL,
  ttl_sec integer NOT NULL,
  not_after timestamptz NOT NULL,
  waiver_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  expired_at timestamptz
);

CREATE INDEX IF NOT EXISTS idx_atr_quarantine_waivers_qid ON atr_quarantine_waivers(quarantine_id);

CREATE OR REPLACE VIEW v_ops_release_quarantine_board AS
SELECT
  incident_id,
  quarantine_class,
  scope_value,
  status,
  severity,
  started_at,
  not_before_release_at
FROM atr_release_quarantines
ORDER BY started_at DESC;

CREATE OR REPLACE VIEW v_ops_quarantine_exit_checks_board AS
SELECT
  quarantine_id,
  check_name,
  status,
  created_at
FROM atr_quarantine_exit_checks
ORDER BY created_at DESC;
