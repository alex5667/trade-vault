-- Phase 5 Provenance Expansion for partitions
-- Adds missing ATR-related columns to trades_closed_p0 for parity with main trades_closed table.

ALTER TABLE trades_closed_p0 
ADD COLUMN IF NOT EXISTS atr_policy_ver integer DEFAULT 0,
ADD COLUMN IF NOT EXISTS atr_policy_tag character varying(16) DEFAULT '',
ADD COLUMN IF NOT EXISTS atr_policy_source character varying(32) DEFAULT '',
ADD COLUMN IF NOT EXISTS atr_policy_scenario character varying(32) DEFAULT '',
ADD COLUMN IF NOT EXISTS atr_policy_regime character varying(32) DEFAULT '',
ADD COLUMN IF NOT EXISTS atr_policy_bucket character varying(32) DEFAULT '',
ADD COLUMN IF NOT EXISTS atr_stop_ttl_mode character varying(16) DEFAULT '',
ADD COLUMN IF NOT EXISTS atr_trailing_mode character varying(16) DEFAULT '',
ADD COLUMN IF NOT EXISTS atr_recovery_run_id character varying(64) DEFAULT '',
ADD COLUMN IF NOT EXISTS atr_restore_cert_id character varying(64) DEFAULT '',
ADD COLUMN IF NOT EXISTS atr_restore_cert_status character varying(16) DEFAULT '',
ADD COLUMN IF NOT EXISTS atr_policy_snapshot_json jsonb DEFAULT '{}';

-- Optional: Synchronize existing 'scenario' and 'regime' if they were used
-- UPDATE trades_closed_p0 SET atr_policy_scenario = scenario WHERE atr_policy_scenario = '' AND scenario IS NOT NULL;
-- UPDATE trades_closed_p0 SET atr_policy_regime = regime WHERE atr_policy_regime = '' AND regime IS NOT NULL;

-- Ensure indexes exist for budget monitoring performance
CREATE INDEX IF NOT EXISTS idx_trades_closed_p0_atr_scenario ON trades_closed_p0 (atr_policy_scenario);
CREATE INDEX IF NOT EXISTS idx_trades_closed_p0_atr_source ON trades_closed_p0 (atr_policy_source);
