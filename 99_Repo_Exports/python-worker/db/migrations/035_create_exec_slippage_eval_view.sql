-- FIX: CREATE OR REPLACE VIEW не может менять список колонок уже существующего view.
-- Используем DROP (IF EXISTS) + CREATE для безопасного пересоздания.
DROP VIEW IF EXISTS v_exec_slippage_eval CASCADE;
CREATE VIEW v_exec_slippage_eval AS
SELECT
  ts_ms,
  to_timestamp(ts_ms / 1000.0) AS ts,
  order_id,
  sym,
  side,
  exec_regime_bucket,
  COALESCE(size_usd, 0.0)::double precision                          AS size_usd,
  spread_bps_submit::double precision                                 AS spread_bps,
  impact_proxy::double precision                                      AS impact_proxy,
  expected_slippage_bps::double precision                             AS expected_slip_bps,
  expected_slippage_decomp_bps::double precision                      AS expected_slip_decomp_bps,
  edge_bps::double precision                                          AS edge_bps,
  mid_px_submit::double precision                                     AS mid_px,
  fill_px::double precision                                           AS fill_px,
  GREATEST(
    0.0,
    CASE
      WHEN side IN ('BUY','LONG')   THEN (fill_px - mid_px_submit) / NULLIF(mid_px_submit, 0) * 10000.0
      WHEN side IN ('SELL','SHORT') THEN (mid_px_submit - fill_px) / NULLIF(mid_px_submit, 0) * 10000.0
      ELSE NULL
    END
  )                                                                    AS realized_slip_worse_bps,
  (edge_bps - expected_slippage_bps)                                   AS edge_minus_expected_bps,
  (
    GREATEST(
      0.0,
      CASE
        WHEN side IN ('BUY','LONG')   THEN (fill_px - mid_px_submit) / NULLIF(mid_px_submit, 0) * 10000.0
        WHEN side IN ('SELL','SHORT') THEN (mid_px_submit - fill_px) / NULLIF(mid_px_submit, 0) * 10000.0
        ELSE NULL
      END
    ) - expected_slippage_bps
  )                                                                    AS slippage_residual_bps
FROM (
    SELECT
        exit_ts_ms                                                          AS ts_ms,
        order_id,
        split_part(order_id, '_', 1)                                        AS sym,
        CASE WHEN split_part(order_id, '_', 3) = 'LONG' THEN 'LONG'
             ELSE 'SHORT' END                                               AS side,
        COALESCE(features_json->>'exec_regime_bucket', regime)              AS exec_regime_bucket,
        (features_json->>'size_usd')::numeric                               AS size_usd,
        (features_json->>'spread_bps_submit')::numeric                      AS spread_bps_submit,
        (features_json->>'impact_proxy')::numeric                           AS impact_proxy,
        (features_json->>'expected_slippage_bps')::numeric                  AS expected_slippage_bps,
        (features_json->>'expected_slippage_decomp_bps')::numeric           AS expected_slippage_decomp_bps,
        (features_json->>'edge_bps')::numeric                               AS edge_bps,
        (features_json->>'mid_px_submit')::numeric                          AS mid_px_submit,
        (features_json->>'fill_px')::numeric                                AS fill_px
    FROM trades_closed_p0
) t
WHERE expected_slippage_bps IS NOT NULL
  AND edge_bps              IS NOT NULL
  AND mid_px_submit         IS NOT NULL
  AND fill_px               IS NOT NULL;
