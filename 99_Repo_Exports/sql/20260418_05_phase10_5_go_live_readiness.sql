-- Migration for Phase 10.5 — go-live readiness review + sign-off package

CREATE TABLE IF NOT EXISTS atr_go_live_readiness_packages (
  package_id text PRIMARY KEY,
  target_scope text NOT NULL,
  charter_version text NOT NULL,
  package_status text NOT NULL,          -- draft | ready | signed | rejected | expired
  verdict text NOT NULL,                 -- GO_LIVE | GO_LIVE_WITH_CONSTRAINTS | HOLD | NO_GO | ROLLBACK_ONLY
  summary_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  signed_at timestamptz,
  expires_at timestamptz
);

CREATE TABLE IF NOT EXISTS atr_go_live_readiness_checks (
  check_id text PRIMARY KEY,
  package_id text NOT NULL,
  domain text NOT NULL,
  check_name text NOT NULL,
  status text NOT NULL,                  -- passed | failed | warning | pending
  severity text NOT NULL,                -- warn | error | critical
  details_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS atr_go_live_signoffs (
  signoff_id text PRIMARY KEY,
  package_id text NOT NULL,
  signer_role text NOT NULL,             -- runtime_owner | execution_owner | protective_owner | control_plane_owner | oncall | technical_owner
  signer text NOT NULL,
  status text NOT NULL,                  -- approved | rejected
  signoff_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);
