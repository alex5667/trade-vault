CREATE OR REPLACE VIEW v_atr_policy_promotion_inputs AS
SELECT
    t.symbol,
    t.atr_policy_scenario AS scenario,
    t.atr_policy_regime AS regime,
    t.atr_policy_bucket AS bucket,
    t.atr_stop_ttl_mode,
    t.atr_trailing_mode,
    t.atr_policy_ver,
    t.atr_restore_cert_status,
    count(*) AS n_trades,
    avg(t.pnl_pct * 10000) AS avg_pnl_bps,
    avg(p0.slippage_bps_est) AS avg_slippage_bps,
    avg(p0.mae_bps) AS avg_mae_pct,
    avg(CASE WHEN t.pnl_net > 0 THEN 1.0 ELSE 0.0 END) AS win_rate,
    avg(CASE WHEN t.close_reason = 'stop_loss' THEN 1.0 ELSE 0.0 END) AS stop_rate,
    avg(CASE WHEN t.close_reason = 'tp1_hit' THEN 1.0 ELSE 0.0 END) AS tp1_rate
FROM trades_closed t
LEFT JOIN trades_closed_p0 p0 ON t.order_id = p0.order_id
WHERE t.exit_ts >= now() - interval '30 days'
GROUP BY
    t.symbol,
    t.atr_policy_scenario,
    t.atr_policy_regime,
    t.atr_policy_bucket,
    t.atr_stop_ttl_mode,
    t.atr_trailing_mode,
    t.atr_policy_ver,
    t.atr_restore_cert_status;

CREATE TABLE IF NOT EXISTS atr_policy_promotion_daily (
  day date NOT NULL,
  symbol text NOT NULL,
  scenario text NOT NULL,
  regime text NOT NULL,
  bucket text NOT NULL,
  layer text NOT NULL,                 -- stop_ttl | trailing
  candidate_mode text NOT NULL,        -- live | canary
  baseline_mode text NOT NULL,         -- canary | shadow
  atr_policy_ver integer NOT NULL,
  restore_cert_status text NOT NULL,
  n_trades integer NOT NULL,
  avg_pnl_bps double precision,
  avg_slippage_bps double precision,
  avg_mae_pct double precision,
  win_rate double precision,
  stop_rate double precision,
  tp1_rate double precision,
  promotion_score double precision,
  PRIMARY KEY (
    day, symbol, scenario, regime, bucket,
    layer, candidate_mode, baseline_mode,
    atr_policy_ver, restore_cert_status
  )
);
