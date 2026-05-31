-- Phase 2.8 (2026-05-18): finish trades_closed schema for batch_trade_writer INSERT
-- 25 columns referenced in services/batch_trade_writer.py:319-401 but missing in scanner_analytics.trades_closed.
-- Types inferred from mirror `sc_*` columns and ATR governance contract.

ALTER TABLE trades_closed
  ADD COLUMN IF NOT EXISTS policy_mode               text,
  ADD COLUMN IF NOT EXISTS policy_raw                jsonb,
  ADD COLUMN IF NOT EXISTS atr_policy_ver            integer,
  ADD COLUMN IF NOT EXISTS atr_policy_tag            text,
  ADD COLUMN IF NOT EXISTS atr_policy_source         text,
  ADD COLUMN IF NOT EXISTS atr_recovery_run_id       text,
  ADD COLUMN IF NOT EXISTS atr_restore_cert_id       text,
  ADD COLUMN IF NOT EXISTS atr_policy_snapshot_json  jsonb,
  ADD COLUMN IF NOT EXISTS atr_sel_tf                text,
  ADD COLUMN IF NOT EXISTS atr_sel_src               text,
  ADD COLUMN IF NOT EXISTS atr_sel_age_ms            bigint,
  ADD COLUMN IF NOT EXISTS contract_ver              integer,
  ADD COLUMN IF NOT EXISTS hold_target_ms            bigint,
  ADD COLUMN IF NOT EXISTS alpha_half_life_ms        bigint,
  ADD COLUMN IF NOT EXISTS max_signal_age_ms         bigint,
  ADD COLUMN IF NOT EXISTS risk_horizon_bucket       text,
  ADD COLUMN IF NOT EXISTS horizon_profile_source    text,
  ADD COLUMN IF NOT EXISTS horizon_profile_conf      double precision,
  ADD COLUMN IF NOT EXISTS horizon_reason_code       text,
  ADD COLUMN IF NOT EXISTS atr_mode                  text,
  ADD COLUMN IF NOT EXISTS atr_value                 double precision,
  ADD COLUMN IF NOT EXISTS atr_window_n              integer,
  ADD COLUMN IF NOT EXISTS atr_age_ms                bigint,
  ADD COLUMN IF NOT EXISTS atr_source                text,
  ADD COLUMN IF NOT EXISTS atr_regime_value          double precision,
  ADD COLUMN IF NOT EXISTS atr_trail_value           double precision,
  ADD COLUMN IF NOT EXISTS atr_regime_tf_ms          bigint,
  ADD COLUMN IF NOT EXISTS atr_trail_tf_ms           bigint,
  ADD COLUMN IF NOT EXISTS atr_pct                   double precision,
  ADD COLUMN IF NOT EXISTS vol_ratio_fast_slow       double precision,
  ADD COLUMN IF NOT EXISTS vol_ratio_z               double precision;

-- The pre-existing INSERT references `atr_stop_ttl_mode`, `atr_trailing_mode`,
-- `atr_restore_cert_status` — verify presence and add if missing:
ALTER TABLE trades_closed
  ADD COLUMN IF NOT EXISTS atr_stop_ttl_mode         text,
  ADD COLUMN IF NOT EXISTS atr_trailing_mode         text,
  ADD COLUMN IF NOT EXISTS atr_restore_cert_status   text,
  ADD COLUMN IF NOT EXISTS atr_policy_scenario       text,
  ADD COLUMN IF NOT EXISTS atr_policy_regime         text,
  ADD COLUMN IF NOT EXISTS atr_policy_bucket         text;
