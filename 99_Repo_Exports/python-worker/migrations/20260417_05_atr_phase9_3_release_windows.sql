-- 20260417_05_atr_phase9_3_release_windows.sql
-- Migration for ATR Phase 9.3: Formal Release Window Policy + Pre-Release Checklist

CREATE TABLE IF NOT EXISTS atr_pre_release_checklists (
  checklist_id text PRIMARY KEY,
  change_id text NOT NULL,
  change_class text NOT NULL,
  target_scope text NOT NULL,
  status text NOT NULL,                  -- draft | ready | approved | blocked | expired
  checks_json jsonb NOT NULL,
  summary_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  approved_at timestamptz
);

CREATE TABLE IF NOT EXISTS atr_release_windows (
  window_id text PRIMARY KEY,
  window_kind text NOT NULL,             -- standard | governance | runtime_critical | execution_critical | protective_isolated
  starts_at timestamptz NOT NULL,
  ends_at timestamptz NOT NULL,
  status text NOT NULL,                  -- planned | open | blocked | closed
  window_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS atr_release_signoffs (
  signoff_id text PRIMARY KEY,
  checklist_id text NOT NULL,
  signer_role text NOT NULL,             -- owner | oncall | execution_owner | control_plane_owner | protective_owner
  signer text NOT NULL,
  status text NOT NULL,                  -- approved | rejected
  signoff_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE OR REPLACE VIEW v_ops_release_window_board AS
SELECT
  window_kind,
  starts_at,
  ends_at,
  status,
  created_at
FROM atr_release_windows
ORDER BY starts_at DESC;

CREATE OR REPLACE VIEW v_ops_pre_release_checklist_board AS
SELECT
  change_id,
  change_class,
  target_scope,
  status,
  created_at,
  approved_at
FROM atr_pre_release_checklists
ORDER BY created_at DESC;
