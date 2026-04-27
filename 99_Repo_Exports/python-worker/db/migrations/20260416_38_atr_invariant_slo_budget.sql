CREATE TABLE IF NOT EXISTS atr_invariant_slo_policies (
  policy_id text PRIMARY KEY,
  invariant_class text NOT NULL,          -- payload | gate | execution | position | governance | observability
  surface text NOT NULL,                  -- runtime | replay | release_gate
  severity text NOT NULL,                 -- warn | error | critical
  window_sec integer NOT NULL,
  max_violations integer NOT NULL,
  burn_rate_warn double precision NOT NULL DEFAULT 0.5,
  burn_rate_critical double precision NOT NULL DEFAULT 1.0,
  auto_action text NOT NULL,              -- none | freeze_promotions | deny_live_release | scope_clip | scope_freeze | hard_freeze
  policy_json jsonb NOT NULL,
  is_enabled boolean NOT NULL DEFAULT true,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS atr_invariant_budget_states (
  state_id text PRIMARY KEY,
  invariant_class text NOT NULL,
  surface text NOT NULL,
  severity text NOT NULL,
  scope_kind text NOT NULL,
  scope_value text NOT NULL,
  window_sec integer NOT NULL,
  violations_count integer NOT NULL,
  max_violations integer NOT NULL,
  burn_rate double precision NOT NULL,
  budget_status text NOT NULL,            -- healthy | warning | exhausted
  summary_json jsonb NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS atr_invariant_budget_actions (
  action_id text PRIMARY KEY,
  state_id text NOT NULL,
  auto_action text NOT NULL,              -- freeze_promotions | deny_live_release | scope_clip | scope_freeze | hard_freeze
  status text NOT NULL,                   -- requested | executed | failed
  reason_code text NOT NULL,
  action_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  finished_at timestamptz
);

CREATE OR REPLACE VIEW v_governance_invariant_budget_board AS
SELECT
    invariant_class,
    surface,
    severity,
    scope_kind,
    scope_value,
    window_sec,
    violations_count,
    max_violations,
    burn_rate,
    budget_status,
    updated_at
FROM atr_invariant_budget_states
ORDER BY
    CASE budget_status
      WHEN 'exhausted' THEN 1
      WHEN 'warning' THEN 2
      ELSE 3
    END,
    burn_rate DESC,
    updated_at DESC;

-- Pre-seed some default SLO policies as requested
INSERT INTO atr_invariant_slo_policies (policy_id, invariant_class, surface, severity, window_sec, max_violations, burn_rate_warn, burn_rate_critical, auto_action, policy_json)
VALUES 
  ('runtime_critical', '*', 'runtime', 'critical', 3600, 3, 0.5, 1.0, 'scope_freeze', '{}'),
  ('replay_critical', '*', 'replay', 'critical', 86400, 1, 0.5, 1.0, 'freeze_promotions', '{}'),
  ('release_critical', '*', 'release_gate', 'critical', 86400, 1, 0.5, 1.0, 'deny_live_release', '{}'),
  ('protective_critical', 'position', 'runtime', 'critical', 86400, 1, 0.5, 1.0, 'hard_freeze', '{}')
ON CONFLICT (policy_id) DO NOTHING;

