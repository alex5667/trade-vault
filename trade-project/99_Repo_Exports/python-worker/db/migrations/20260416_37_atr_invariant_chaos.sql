CREATE TABLE IF NOT EXISTS atr_invariant_chaos_runs (
  run_id text PRIMARY KEY,
  drill_code text NOT NULL,
  invariant_id text NOT NULL,
  mode text NOT NULL,                    -- audit_only | bounded_execute
  target_scope text NOT NULL,
  status text NOT NULL,                  -- started | passed | failed
  summary_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  finished_at timestamptz
);

CREATE TABLE IF NOT EXISTS atr_invariant_chaos_results (
  result_id text PRIMARY KEY,
  run_id text NOT NULL,
  check_name text NOT NULL,
  status text NOT NULL,                  -- passed | failed
  details_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS atr_invariant_chaos_packs (
  pack_id text PRIMARY KEY,
  run_id text NOT NULL,
  pack_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE OR REPLACE VIEW v_governance_invariant_chaos_board AS
SELECT
    run_id,
    drill_code,
    invariant_id,
    mode,
    status,
    created_at,
    finished_at
FROM atr_invariant_chaos_runs
ORDER BY created_at DESC;

CREATE OR REPLACE VIEW v_governance_invariant_chaos_results AS
SELECT
    run_id,
    check_name,
    status,
    details_json,
    created_at
FROM atr_invariant_chaos_results
ORDER BY created_at DESC;
