-- Phase 10: ATR Operating Charter
-- Formal governance baseline and operating rules

CREATE TABLE IF NOT EXISTS atr_operating_charters (
  charter_id text PRIMARY KEY,
  version text NOT NULL,
  status text NOT NULL,                  -- draft | approved | active | superseded
  charter_json jsonb NOT NULL,
  created_by text NOT NULL,
  approved_by text,
  created_at timestamptz NOT NULL DEFAULT now(),
  activated_at timestamptz,
  superseded_at timestamptz
);

CREATE INDEX IF NOT EXISTS idx_atr_charters_status ON atr_operating_charters(status);

CREATE TABLE IF NOT EXISTS atr_charter_amendments (
  amendment_id text PRIMARY KEY,
  charter_id text NOT NULL,
  amendment_class text NOT NULL,         -- MINOR_EDITORIAL | POLICY_TUNING | AUTHORITY_CHANGE | RISK_BOUNDARY_CHANGE | STATE_MACHINE_CHANGE | NON_NEGOTIABLE_CHANGE
  status text NOT NULL,                  -- requested | approved | rejected | activated
  proposer text NOT NULL,
  approver text,
  amendment_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  activated_at timestamptz
);

CREATE INDEX IF NOT EXISTS idx_atr_amendments_charter ON atr_charter_amendments(charter_id);
CREATE INDEX IF NOT EXISTS idx_atr_amendments_status ON atr_charter_amendments(status);

CREATE TABLE IF NOT EXISTS atr_charter_compliance_checks (
  check_id text PRIMARY KEY,
  charter_version text NOT NULL,
  domain text NOT NULL,                  -- runtime | execution | control_plane | protective | release | dr | archive | replay
  status text NOT NULL,                  -- passed | failed | warning
  details_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_atr_compliance_domain ON atr_charter_compliance_checks(domain);
CREATE INDEX IF NOT EXISTS idx_atr_compliance_status ON atr_charter_compliance_checks(status);
CREATE INDEX IF NOT EXISTS idx_atr_compliance_created ON atr_charter_compliance_checks(created_at);
