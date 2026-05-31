-- Phase 10.6 Program Closure Artifacts

CREATE TABLE IF NOT EXISTS atr_program_closure_packages (
  package_id text PRIMARY KEY,
  charter_version text NOT NULL,
  target_scope text NOT NULL,
  status text NOT NULL,                  -- draft | ready | signed | active | rejected
  verdict text NOT NULL,                 -- PROGRAM_CLOSED | CLOSED_WITH_RESIDUAL_BACKLOG | HOLD_OPEN | REJECT_CLOSE
  summary_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  signed_at timestamptz,
  activated_at timestamptz
);

CREATE TABLE IF NOT EXISTS atr_program_handoffs (
  handoff_id text PRIMARY KEY,
  package_id text NOT NULL,
  domain text NOT NULL,
  primary_owner text NOT NULL,
  secondary_owner text,
  oncall_route text,
  status text NOT NULL,                  -- pending | accepted | rejected
  handoff_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS atr_program_residual_backlog (
  item_id text PRIMARY KEY,
  package_id text NOT NULL,
  domain text NOT NULL,
  priority text NOT NULL,                -- P0 | P1 | P2 | P3
  status text NOT NULL,                  -- open | accepted | scheduled | done | dropped
  backlog_class text NOT NULL,           -- blocking | non_blocking | hygiene | deferred_experiment
  title text NOT NULL,
  reason_code text NOT NULL,
  backlog_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);
