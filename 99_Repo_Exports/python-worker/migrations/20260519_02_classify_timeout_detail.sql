-- Phase 0 pre-Phase 1 — classify TIMEOUT closes into reason buckets via SQL.
-- Pure analytics: only writes close_reason_detail for rows where close_reason='TIMEOUT'.
-- Never overwrites a non-empty detail. No trading impact.

BEGIN;

CREATE OR REPLACE FUNCTION classify_timeout_detail(
    pnl_net               DOUBLE PRECISION,
    r_multiple            DOUBLE PRECISION,
    mfe_pnl               DOUBLE PRECISION,
    mae_pnl               DOUBLE PRECISION,
    one_r_money           DOUBLE PRECISION,
    giveback              DOUBLE PRECISION,
    session               TEXT,
    spread_bps_at_entry   DOUBLE PRECISION,
    slippage_bps_est      DOUBLE PRECISION
) RETURNS TEXT
LANGUAGE SQL IMMUTABLE AS $$
    SELECT CASE
        WHEN one_r_money IS NULL OR one_r_money <= 0
            THEN 'timeout_unknown'
        WHEN (mfe_pnl / NULLIF(one_r_money, 0)) >= 0.5
         AND COALESCE(r_multiple, 0) < 0.1
            THEN 'timeout_after_mfe_giveback'
        WHEN (mfe_pnl / NULLIF(one_r_money, 0)) < 0.25
         AND COALESCE(pnl_net, 0) <= 0
            THEN 'timeout_no_followthrough'
        WHEN session IN ('asian','overnight')
         AND (COALESCE(spread_bps_at_entry, 0) + COALESCE(slippage_bps_est, 0)) > 5.0
            THEN 'timeout_low_liq_session'
        WHEN COALESCE(pnl_net, 0) > 0
            THEN 'timeout_positive'
        ELSE 'timeout_generic'
    END;
$$;

COMMENT ON FUNCTION classify_timeout_detail IS
  'Phase 0 pre-Phase 1: classify a TIMEOUT close into reason buckets. Pure function, no side effects.';

-- Backfill close_reason_detail for TIMEOUT closes in the last 30 days.
-- Only touches rows where detail is NULL or empty.
WITH cand AS (
    SELECT tc.order_id,
           classify_timeout_detail(
               tc.pnl_net,
               tc.r_multiple,
               tc.mfe_pnl,
               tc.mae_pnl,
               tc.one_r_money,
               tc.giveback,
               p0.session,
               p0.spread_bps_at_entry,
               p0.slippage_bps_est
           ) AS new_detail
    FROM trades_closed tc
    LEFT JOIN trades_closed_p0 p0 USING (order_id)
    WHERE tc.close_reason = 'TIMEOUT'
      AND (tc.close_reason_detail IS NULL OR tc.close_reason_detail = '')
      AND tc.exit_ts > NOW() - INTERVAL '30 days'
)
UPDATE trades_closed tc
SET close_reason_detail = cand.new_detail
FROM cand
WHERE tc.order_id = cand.order_id
  AND cand.new_detail IS NOT NULL;

INSERT INTO _migrations (filename, applied_at)
VALUES ('20260519_02_classify_timeout_detail.sql', NOW())
ON CONFLICT (filename) DO NOTHING;

COMMIT;
