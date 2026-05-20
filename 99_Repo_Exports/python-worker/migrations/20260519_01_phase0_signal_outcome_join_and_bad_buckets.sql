-- Phase 0 pre-Phase 1 analytics layer.
-- Read-only join over trades_closed × trades_closed_p0 + bad-bucket materialized view.
-- No trading impact: pure analytics objects.

BEGIN;

-- Canonical join view (no data duplication; reflects live trades_closed/p0).
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
  tc.created_at
FROM trades_closed tc
LEFT JOIN trades_closed_p0 p0 USING (order_id);

COMMENT ON VIEW signal_outcome_join IS
  'Phase 0 baseline: canonical join trades_closed × trades_closed_p0 for bad-bucket / reliability analysis. Read-only.';

-- Bad-bucket materialized view (refresh via timer; see scanner-bad-buckets-refresh-timer).
DROP MATERIALIZED VIEW IF EXISTS bad_signal_buckets;
CREATE MATERIALIZED VIEW bad_signal_buckets AS
SELECT
  symbol,
  kind,
  side,
  session,
  regime,
  close_reason,
  COUNT(*)                                                    AS n,
  ROUND(AVG(CASE WHEN pnl_net > 0 THEN 1 ELSE 0 END)::numeric, 4) AS winrate,
  ROUND(AVG(r_multiple)::numeric, 4)                          AS avg_r,
  ROUND(SUM(pnl_net)::numeric, 4)                             AS pnl_net,
  ROUND(AVG(hold_ms)::numeric, 0)                             AS avg_hold_ms,
  ROUND(AVG(mfe_bps)::numeric, 2)                             AS avg_mfe_bps,
  ROUND(AVG(mae_bps)::numeric, 2)                             AS avg_mae_bps,
  ROUND(AVG(spread_bps_at_entry)::numeric, 2)                 AS avg_spread_bps,
  ROUND(AVG(slippage_bps_est)::numeric, 2)                    AS avg_slip_bps,
  NOW()                                                       AS refreshed_at
FROM signal_outcome_join
WHERE is_final_close IS TRUE
  AND exit_ts_ms IS NOT NULL
  AND exit_ts_ms > (EXTRACT(EPOCH FROM NOW() - INTERVAL '14 days')::bigint * 1000)
GROUP BY symbol, kind, side, session, regime, close_reason
HAVING COUNT(*) >= 30
   AND (
     AVG(r_multiple) < -0.10
     OR AVG(CASE WHEN pnl_net > 0 THEN 1 ELSE 0 END) < 0.40
   );

CREATE INDEX IF NOT EXISTS idx_bad_signal_buckets_pnl
  ON bad_signal_buckets (pnl_net ASC);
CREATE INDEX IF NOT EXISTS idx_bad_signal_buckets_sym_kind
  ON bad_signal_buckets (symbol, kind);

COMMENT ON MATERIALIZED VIEW bad_signal_buckets IS
  'Phase 0 bad-bucket inventory. Refreshed by scanner-bad-buckets-refresh-timer. Threshold: n>=30, avg_r<-0.10 OR winrate<0.40.';

-- Register migration.
INSERT INTO _migrations (filename, applied_at)
VALUES ('20260519_01_phase0_signal_outcome_join_and_bad_buckets.sql', NOW())
ON CONFLICT (filename) DO NOTHING;

COMMIT;
