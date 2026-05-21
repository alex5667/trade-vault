-- Phase 0 pre-Phase 1 — extend signal_outcome_join VIEW with computed timeout_class.
-- Pure analytics: VIEW change only, no data mutation.
-- timeout_class is a fine-grained classification of TIMEOUT closes based on MFE/MAE/session.

BEGIN;

CREATE OR REPLACE VIEW signal_outcome_join AS
SELECT
  tc.sid,
  tc.order_id,
  tc.symbol,
  COALESCE(NULLIF(tc.strategy, ''), 'unknown')              AS kind,
  LOWER(COALESCE(NULLIF(tc.direction, ''), 'unknown'))      AS side,
  COALESCE(NULLIF(p0.session, ''), 'unknown')               AS session,
  COALESCE(NULLIF(p0.regime, ''), 'unknown')                AS regime,
  COALESCE(NULLIF(p0.scenario, ''), 'unknown')              AS scenario,
  COALESCE(NULLIF(p0.entry_reason, ''), '')                 AS entry_reason,
  tc.entry_ts_ms,
  tc.exit_ts_ms,
  tc.entry_price,
  tc.exit_price,
  tc.pnl_net,
  tc.pnl_pct,
  tc.r_multiple,
  COALESCE(NULLIF(tc.close_reason, ''), 'unknown')          AS close_reason,
  tc.close_reason_raw,
  tc.close_reason_detail,
  tc.tp1_hit,
  tc.tp_hits,
  tc.trailing_started,
  tc.trailing_active,
  tc.mfe_pnl,
  tc.mae_pnl,
  tc.giveback,
  tc.one_r_money,
  COALESCE(p0.hold_ms, tc.duration_ms)                      AS hold_ms,
  p0.mae_bps,
  p0.mfe_bps,
  p0.time_to_mfe_ms,
  p0.spread_bps_at_entry,
  p0.slippage_bps_est,
  p0.book_age_ms,
  p0.features_json,
  tc.is_virtual,
  tc.is_final_close,
  tc.status,
  tc.atr_policy_tag,
  tc.atr_policy_bucket,
  tc.created_at,
  -- Appended (CREATE OR REPLACE VIEW requires new columns at end):
  -- Fine-grained timeout classification (only meaningful when close_reason='TIMEOUT')
  CASE
    WHEN tc.close_reason <> 'TIMEOUT'                              THEN NULL
    WHEN tc.one_r_money IS NULL OR tc.one_r_money <= 0             THEN 'timeout_unknown'
    WHEN (tc.mfe_pnl / NULLIF(tc.one_r_money, 0)) >= 0.5
     AND COALESCE(tc.r_multiple, 0) < 0.1                          THEN 'timeout_after_mfe_giveback'
    WHEN (tc.mfe_pnl / NULLIF(tc.one_r_money, 0)) < 0.25
     AND COALESCE(tc.pnl_net, 0) <= 0                              THEN 'timeout_no_followthrough'
    WHEN COALESCE(p0.session, '') IN ('asian', 'overnight')
     AND (COALESCE(p0.spread_bps_at_entry, 0) +
          COALESCE(p0.slippage_bps_est, 0)) > 5.0                  THEN 'timeout_low_liq_session'
    WHEN COALESCE(tc.pnl_net, 0) > 0                               THEN 'timeout_positive'
    ELSE                                                                'timeout_generic'
  END                                                              AS timeout_class
FROM trades_closed tc
LEFT JOIN trades_closed_p0 p0 USING (order_id);

COMMENT ON VIEW signal_outcome_join IS
  'Phase 0 baseline: canonical join trades_closed × trades_closed_p0 with computed timeout_class. Read-only.';

INSERT INTO _migrations (filename, applied_at)
VALUES ('20260519_03_signal_outcome_join_timeout_class.sql', NOW())
ON CONFLICT (filename) DO NOTHING;

COMMIT;
