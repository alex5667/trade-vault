-- ==============================================================================
-- Migration: 007_atr_invariants
-- Description: Phase 7 Formal Invariants (Registry, Violations, Snapshots, Board)
-- ==============================================================================

BEGIN;

CREATE TABLE IF NOT EXISTS atr_invariants (
  invariant_id text PRIMARY KEY,
  invariant_class text NOT NULL,          -- payload | gate | execution | position | governance | observability
  scope_kind text NOT NULL,               -- global | venue | symbol | cohort | layer | policy_ver
  severity text NOT NULL,                 -- info | warn | error | critical
  enforcement_mode text NOT NULL,         -- advisory | runtime_deny | release_block | replay_fail | incident_open
  title text NOT NULL,
  reason_code text NOT NULL,
  invariant_json jsonb NOT NULL,
  is_enabled boolean NOT NULL DEFAULT true,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS atr_invariant_violations (
  violation_id text PRIMARY KEY,
  invariant_id text NOT NULL,
  scope_kind text NOT NULL,
  scope_value text NOT NULL,
  surface text NOT NULL,                  -- runtime | replay | release_gate | auditor
  severity text NOT NULL,
  status text NOT NULL,                   -- detected | enforced | ignored | resolved
  reason_code text NOT NULL,
  violation_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  resolved_at timestamptz
);

CREATE INDEX IF NOT EXISTS idx_atr_inv_viol_status_severity ON atr_invariant_violations(status, severity);
CREATE INDEX IF NOT EXISTS idx_atr_inv_viol_scope ON atr_invariant_violations(scope_kind, scope_value);
CREATE INDEX IF NOT EXISTS idx_atr_inv_viol_created_at ON atr_invariant_violations(created_at);

CREATE TABLE IF NOT EXISTS atr_invariant_snapshots (
  snapshot_id text PRIMARY KEY,
  snapshot_kind text NOT NULL,            -- runtime_check | replay_check | release_check
  snapshot_json jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_atr_inv_snap_created_at ON atr_invariant_snapshots(created_at);

CREATE OR REPLACE VIEW v_governance_invariant_board AS
SELECT
    violation_id,
    invariant_id,
    scope_kind,
    scope_value,
    surface,
    severity,
    status,
    reason_code,
    created_at
FROM atr_invariant_violations
WHERE status NOT IN ('resolved', 'ignored')
ORDER BY
    CASE severity
      WHEN 'critical' THEN 1
      WHEN 'error' THEN 2
      WHEN 'warn' THEN 3
      ELSE 4
    END,
    created_at DESC;

-- Seed Critical Runtime Invariants
INSERT INTO atr_invariants (invariant_id, invariant_class, scope_kind, severity, enforcement_mode, title, reason_code, invariant_json, is_enabled)
VALUES
('INV_PAYLOAD_BUY_ORDERING', 'payload', 'global', 'critical', 'runtime_deny', 'BUY Ordering', 'INV_PAYLOAD_BUY_ORDERING', '{"when": "side == BUY", "must_hold": "sl_price < entry_price && entry_price < tp1_price"}', true),
('INV_PAYLOAD_SELL_ORDERING', 'payload', 'global', 'critical', 'runtime_deny', 'SELL Ordering', 'INV_PAYLOAD_SELL_ORDERING', '{"when": "side == SELL", "must_hold": "sl_price > entry_price && entry_price > tp1_price"}', true),
('INV_SIGNAL_ID_REQUIRED', 'payload', 'global', 'critical', 'runtime_deny', 'Signal ID Required', 'INV_SIGNAL_ID_REQUIRED', '{"when": "always", "must_hold": "signal_id != null && signal_id != \'\'"}', true),
('INV_TRADEABLE_REQUIRES_NO_HARD_VETO', 'gate', 'global', 'critical', 'runtime_deny', 'Tradeable Requires No Hard Veto', 'INV_TRADEABLE_REQUIRES_NO_HARD_VETO', '{"when": "tradeable == true", "must_hold": "veto_reason == null"}', true),
('INV_NO_ORDER_WITHOUT_RISK_PCT', 'execution', 'global', 'critical', 'runtime_deny', 'No Order Without Risk Pct', 'INV_NO_ORDER_WITHOUT_RISK_PCT', '{"when": "always", "must_hold": "risk_pct > 0 || effective_risk_pct > 0"}', true),
('INV_NO_ORDER_WITHOUT_SL', 'execution', 'global', 'critical', 'runtime_deny', 'No Order Without SL', 'INV_NO_ORDER_WITHOUT_SL', '{"when": "always", "must_hold": "sl_price > 0"}', true)
ON CONFLICT (invariant_id) DO NOTHING;

COMMIT;
