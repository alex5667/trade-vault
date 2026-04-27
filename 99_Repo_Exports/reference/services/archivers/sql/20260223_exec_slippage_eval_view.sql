-- P86/P90: Exec slippage evaluation view (validation of expected slippage models)

CREATE OR REPLACE VIEW v_exec_slippage_eval AS
WITH base AS (
  SELECT
    p0.exit_ts               AS ts,
    t.symbol                 AS sym,
    COALESCE(NULLIF(p0.features_json->>'exec_regime_bucket',''), 'NORMAL') AS exec_regime_bucket,

    -- Spread at submit (bps): prefer features_json snapshot, fall back to column
    COALESCE(
      NULLIF((p0.features_json->>'spread_bps_submit')::double precision, 0),
      NULLIF(p0.spread_bps_at_entry, 0),
      NULLIF((p0.features_json->>'spread_bps')::double precision, 0),
      0.0
    ) AS spread_bps,

    -- Market impact proxy: |dn_usd| / depth_min_5_usd
    COALESCE(NULLIF((p0.features_json->>'impact_proxy')::double precision, 0), 0.0) AS impact_proxy,

    -- Mid price at submit (for realized slip calc)
    COALESCE(
      NULLIF((p0.features_json->>'mid_px_submit')::double precision, 0),
      NULLIF(t.entry_price, 0),
      0.0
    ) AS mid_px_submit,

    -- Fill price: prefer features_json (true fill reported by exchange), fall back to entry_price
    COALESCE(
      NULLIF((p0.features_json->>'fill_px')::double precision, 0),
      NULLIF(t.entry_price, 0),
      0.0
    ) AS fill_px,

    -- Size proxy (USD)
    COALESCE(NULLIF(t.notional_usd, 0), NULLIF((p0.features_json->>'size_usd')::double precision, 0), 0.0) AS size_usd,

    -- Expected slippage (legacy model)
    COALESCE(NULLIF((p0.features_json->>'expected_slippage_bps')::double precision, 0), 0.0)   AS expected_slip_model_bps,
    -- Expected slippage (decomp model)
    COALESCE(NULLIF((p0.features_json->>'expected_slippage_decomp_bps')::double precision, 0), 0.0) AS expected_slip_decomp_bps,
    -- k coefficient used at decision time (for retrospective audit)
    COALESCE(NULLIF((p0.features_json->>'slip_decomp_coeff_bps')::double precision, 0), 0.0)   AS slip_decomp_coeff_bps,

    -- Spread component logged at decision time
    COALESCE(NULLIF((p0.features_json->>'slip_decomp_spread_bps')::double precision, 0), 0.0)  AS slip_decomp_spread_bps,

    -- Impact component logged at decision time
    COALESCE(NULLIF((p0.features_json->>'slip_decomp_impact_bps')::double precision, 0), 0.0)  AS slip_decomp_impact_bps,

    -- Optional edge proxy (if present)
    NULLIF((p0.features_json->>'edge_bps')::double precision, 0) AS edge_bps,

    -- Direction normalised for CASE expressions below
    UPPER(COALESCE(NULLIF(t.direction,''), '')) AS dir,

    -- Taker flow imbalance z-score at submit
    COALESCE(NULLIF((p0.features_json->>'taker_flow_imb_z')::double precision, 0), 0.0) AS taker_flow_imb_z,

    -- Liquidity / volume regime labels for diagnostics
    COALESCE(NULLIF(p0.features_json->>'liq_regime_label', ''), 'na') AS liq_regime_label,
    COALESCE(NULLIF(p0.features_json->>'vol_regime_label', ''), 'na') AS vol_regime_label,

    p0.features_json AS features_json

  FROM trades_closed_p0 p0
  JOIN trades_closed t
    ON t.order_id = p0.order_id
  WHERE p0.exit_ts > now() - interval '60 days'
),
inner_calc AS (
  SELECT
    base.*,

    -- Realized slippage (worse direction): fill vs mid at submit.
    CASE
      WHEN mid_px_submit <= 0 OR fill_px <= 0 THEN 0.0
      WHEN dir = 'LONG'  THEN GREATEST(0.0, (fill_px - mid_px_submit) / mid_px_submit * 10000.0)
      WHEN dir = 'SHORT' THEN GREATEST(0.0, (mid_px_submit - fill_px) / mid_px_submit * 10000.0)
      ELSE 0.0
    END AS realized_slip_worse_bps,

    -- Edge minus expected (decomp) (negative => edge was not enough to cover expected slippage)
    CASE
      WHEN base.edge_bps IS NULL THEN NULL
      ELSE base.edge_bps - base.expected_slip_decomp_bps
    END AS edge_minus_expected_bps,

    -- Edge minus expected (model)
    CASE
      WHEN base.edge_bps IS NULL THEN NULL
      ELSE base.edge_bps - base.expected_slip_model_bps
    END AS edge_minus_expected_model_bps

  FROM base
),
calc AS (
  SELECT
    x.*,
    -- Residuals
    (x.realized_slip_worse_bps - x.expected_slip_decomp_bps) AS slippage_residual_bps,
    (x.realized_slip_worse_bps - x.expected_slip_model_bps)  AS slippage_residual_model_bps
  FROM inner_calc x
)
SELECT
  ts,
  sym,
  exec_regime_bucket,
  spread_bps,
  impact_proxy,
  mid_px_submit,
  fill_px,
  size_usd,
  expected_slip_model_bps,
  expected_slip_decomp_bps,
  slip_decomp_coeff_bps,
  slip_decomp_spread_bps,
  slip_decomp_impact_bps,
  edge_bps,
  realized_slip_worse_bps,
  edge_minus_expected_bps AS edge_minus_expected_slip_decomp_bps,
  edge_minus_expected_bps,
  edge_minus_expected_model_bps,
  taker_flow_imb_z,
  liq_regime_label,
  vol_regime_label,
  features_json,
  slippage_residual_bps,
  slippage_residual_model_bps
FROM calc;
