-- Backfill missing/na regime in trades_closed_p0 from regime_snapshot (1m TF, ±60s window).
-- Idempotent: only touches rows where regime IS NULL/''/'na'.
-- Scope: last 24h (recent inserts) to keep the UPDATE cheap.

WITH cand AS (
  SELECT p0.order_id,
         (SELECT rs.regime FROM regime_snapshot rs
          WHERE rs.symbol = tc.symbol
            AND rs.timeframe = '1m'
            AND rs.ts BETWEEN tc.entry_ts - INTERVAL '60 seconds'
                          AND tc.entry_ts + INTERVAL '60 seconds'
          ORDER BY ABS(EXTRACT(EPOCH FROM (rs.ts - tc.entry_ts))) ASC
          LIMIT 1) AS new_regime
  FROM trades_closed_p0 p0
  JOIN trades_closed   tc USING (order_id)
  WHERE (p0.regime IS NULL OR p0.regime IN ('','na'))
    AND p0.exit_ts > NOW() - INTERVAL '24 hours'
),
upd AS (
  UPDATE trades_closed_p0 p0
  SET regime = cand.new_regime,
      updated_at = NOW()
  FROM cand
  WHERE p0.order_id = cand.order_id
    AND cand.new_regime IS NOT NULL
  RETURNING 1
)
SELECT COUNT(*) AS updated_rows FROM upd;
