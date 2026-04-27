BEGIN;

CREATE TABLE IF NOT EXISTS atr_legacy_path_inventory (
  path_id text PRIMARY KEY,
  component text NOT NULL,                 -- release | freeze | override | effective_state | runtime_helper | protective
  owner_service text NOT NULL,
  path_kind text NOT NULL,                 -- read | write
  legacy_target text NOT NULL,             -- table/key/function
  graph_replacement text NOT NULL,
  retirement_class text NOT NULL,          -- RETIRE_NOW | SHADOW_ONLY | FALLBACK_ONLY | KEEP_UNTIL_PROTECTIVE_CUTOVER
  status text NOT NULL,                    -- active | shadow_only | fallback_only | retired
  inventory_json jsonb NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS atr_legacy_decommission_events (
  event_id text PRIMARY KEY,
  path_id text NOT NULL,
  component text NOT NULL,
  old_status text NOT NULL,
  new_status text NOT NULL,
  actor text NOT NULL,
  reason_code text NOT NULL,
  event_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS atr_hidden_dependency_findings (
  finding_id text PRIMARY KEY,
  component text NOT NULL,
  severity text NOT NULL,                  -- warn | error | critical
  status text NOT NULL,                    -- open | resolved
  reason_code text NOT NULL,
  finding_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  resolved_at timestamptz
);

CREATE TABLE IF NOT EXISTS atr_legacy_decommission_readiness (
  readiness_id text PRIMARY KEY,
  component text NOT NULL,                 -- release | freeze | override | effective_state
  status text NOT NULL,                    -- not_ready | ready_to_shadow_only | ready_to_fallback_only | ready_to_retire
  summary_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

-- Auditor Views
CREATE OR REPLACE VIEW v_governance_legacy_inventory_board AS
SELECT
  component,
  owner_service,
  path_kind,
  legacy_target,
  retirement_class,
  status,
  updated_at
FROM atr_legacy_path_inventory
ORDER BY component, updated_at DESC;

CREATE OR REPLACE VIEW v_governance_hidden_dependency_board AS
SELECT
  component,
  severity,
  status,
  reason_code,
  created_at
FROM atr_hidden_dependency_findings
WHERE status = 'open'
ORDER BY
  CASE severity
    WHEN 'critical' THEN 1
    WHEN 'error' THEN 2
    ELSE 3
  END,
  created_at DESC;

CREATE OR REPLACE VIEW v_governance_legacy_decommission_readiness_board AS
SELECT
  component,
  status,
  created_at
FROM atr_legacy_decommission_readiness
ORDER BY created_at DESC;

-- Optional Seeds based on user's examples
INSERT INTO atr_legacy_path_inventory (path_id, component, owner_service, path_kind, legacy_target, graph_replacement, retirement_class, status, inventory_json)
VALUES 
  ('legacy_release_writes', 'release', 'atr_release_gate', 'write', 'redis:atr:release:*', 'graph:edge:release', 'RETIRE_NOW', 'fallback_only', '{"note": "Primary graph active. Can retire."}'),
  ('legacy_freeze_writes', 'freeze', 'atr_freeze_evaluator', 'write', 'redis:atr:freeze:*', 'graph:edge:freeze', 'RETIRE_NOW', 'fallback_only', '{"note": "Primary graph active. Can retire."}'),
  ('legacy_override_writes', 'override', 'atr_override_service', 'write', 'redis:cfg:atr_override:*', 'graph:edge:override', 'RETIRE_NOW', 'fallback_only', '{"note": "Primary graph active. Can retire."}'),
  ('legacy_effective_state_writes', 'effective_state', 'atr_effective_state_resolver', 'write', 'redis:atr:effective_state', 'graph:node:effective', 'RETIRE_NOW', 'fallback_only', '{"note": "Primary graph active. Can retire."}'),
  ('legacy_release_freeze_read_helpers', 'runtime_helper', 'crypto_orderflow_service', 'read', 'redis:atr:release/freeze', 'graph:projection', 'FALLBACK_ONLY', 'active', '{"note": "Needed for compare during cutover."}'),
  ('post_trade_broker_protective', 'protective', 'trade_monitor', 'write', 'mt5/binance protective SL/TP', 'tbd', 'KEEP_UNTIL_PROTECTIVE_CUTOVER', 'active', '{"note": "Protective isolation."}')
ON CONFLICT (path_id) DO NOTHING;

COMMIT;
