from __future__ import annotations

"""
v10_of — Feature schema v10 (OrderFlow), pinned snapshot + stream-proven extensions.

Generated: 2026-03-15 (manual bump from v9_of).

v10_of = v9_of (128 keys) + 37 additional indicators:
  Group 1 (16)  — already published in signals:of:inputs, not in v9_of
  Group 2A (4)  — Adverse Selection / VPIN extension
  Group 2B (4)  — Order Book microstructure (restored from v4_of + new)
  Group 2C (5)  — Momentum / Technical Analysis
  Group 2D (4)  — Execution Quality (post-trade rolling averages)
  Group 2E (4)  — Context / External (go-worker → Redis → runtime, fail-open)

Coverage: 165 numeric indicators (no separate bool block — all bool as float 0/1)
          + direction/bucket/hour/dow/session one-hots → ~225 total feature_cols.

Design notes
------------
- Fail-open: missing runtime keys vectorize as 0.0 (safe for all groups).
- Group 2D (mae_r, mfe_r): rolling averages from last-N closed trades in runtime.
- Group 2E: populated by go-worker → Redis consumer in runtime state;
  values are 0.0 until go-worker pipeline is deployed.
- Append-only: new schema versions always add keys, never remove.
"""



SCHEMA_HASH = "bc489cb596eb"



# ---------------------------------------------------------------------------
# V9_OF base keys (128 — copied verbatim for auditability)
# ---------------------------------------------------------------------------

_V9_OF_BASE: list[str] = [
    "abs_lvl_calib_n",
    "abs_lvl_eff_quote_th",
    "abs_lvl_min_quote_delta",
    "abs_lvl_ready",
    "atr",
    "atr_age_ms",
    "atr_bad",
    "atr_bps",
    "atr_bps_exec",
    "atr_candidates_n",
    "atr_cons_ok",
    "atr_consistency",
    "atr_fees_rocket_mult",
    "atr_fees_th_bps",
    "atr_fees_tp1_share",
    "atr_floor_t0_bps",
    "atr_floor_t1_bps",
    "atr_floor_t2_bps",
    "atr_floor_tier",
    "atr_jump_count_window",
    "atr_sanity_ok",
    "atr_ts_ms",
    "atr_unified_th_bps",
    "atr_used_last_good",
    "book_evidence_allowed",
    "book_health_ok",
    "book_health_veto_book_evidence",
    "book_rate_hz",
    "book_ts_gap_ms",
    "burst_churn",
    "burst_cr_ema",
    "burst_ctr",
    "burst_exc",
    "burst_ha_lam",
    "burst_pen",
    "burst_tr_ema",
    "burst_veto",
    "burst_z",
    "conf_rsi_agree",
    "confidence",
    "cooldown_hit_rate",
    "cooldown_hit_rate_ema",
    "cvd_ema",
    "cvd_jump_events_total",
    "cvd_median_abs_delta_usd",
    "cvd_quarantine_active",
    "cvd_quarantine_until_ms",
    "cvd_reset_skipped_bad_time",
    "cvd_resets",
    "cvd_slope",
    "cvd_tick",
    "data_health",
    "data_health_shadow_only",
    "data_health_veto_book_evidence",
    "delta",
    "delta_mad",
    "delta_med",
    "delta_n",
    "delta_notional_usd",
    "delta_robust_z",
    "delta_tick",
    "div_match",
    "div_match_fallback",
    "dn_t1_usd",
    "dn_tier",
    "dn_tier_active",
    "dn_tier_threshold",
    "dn_usd",
    "dq_veto_suppressed",
    "ema_delta",
    "eta_fill_sec",
    "exec_fill_pen",
    "exec_pen",
    "exec_risk_bps",
    "exec_risk_norm",
    "exec_risk_ref_bps",
    "expected_slippage_bps",
    "fill_prob_p_base",
    "fill_prob_p_wait",
    "fill_prob_proxy",
    "fp_edge_absorb",
    "fp_edge_age_ms",
    "hour_of_week",
    "iceberg_avg_qty",
    "iceberg_refresh",
    "iceberg_strict",
    "liq_depth_usd_min_5",
    "liq_depth_warn",
    "liq_ofi_align",
    "liq_pressure_boost",
    "liq_pressure_pen",
    "liq_pressure_veto",
    "liq_q_align",
    "liq_rate_warn",
    "liq_score",
    "liq_spread_crit",
    "liq_spread_warn",
    "liqmap_gate_adverse_peak_usd",
    "liqmap_gate_favorable_peak_usd",
    "liqmap_gate_reward_bps",
    "liqmap_gate_risk_bps",
    "liqmap_gate_rr",
    "liqmap_gate_shadow_veto",
    "liqmap_gate_soft",
    "liqmap_gate_veto",
    "liqmap_ok",
    "liqmap_sl_base_bps",
    "liqmap_sl_reco_bps",
    "liqmap_sl_widen_needed",
    "liqmap_sl_widen_ratio",
    "liquidity_scale",
    "now_ts_ms_used",
    "obi",
    "obi_stable",
    "obi_z",
    "of_base_score",
    "of_confirm_have_need_ratio",
    "of_confirm_ok",
    "of_confirm_ok_soft",
    "of_confirm_score",
    "of_score_final",
    "of_score_final_raw",
    "ofi",
    "ofi_age_ms",
    "ofi_dir_ok",
    "ofi_stability_score",
    "ofi_stable",
    "ofi_stable_secs",
]

# ---------------------------------------------------------------------------
# Group 1 — already published in signals:of:inputs, absent from v9_of (16 keys)
# ---------------------------------------------------------------------------

_GROUP1_STREAM_PROVEN: list[str] = [
    # RSI momentum
    "rsi_price",           # RSI on price — momentum/divergence with CVD
    "rsi_cvd",             # RSI on cumulative delta flow
    # Spread / execution cost
    "spread_bps",          # Actual bid-ask spread in bps — real execution cost
    # Pressure signals
    "pressure",            # Aggressive order flow intensity in window
    "pressure_per_min",    # Signal pressure rate per minute (per_min_ema)
    "pressure_hi",         # Flag: extreme pressure (binary 0/1)
    "pressure_per_min_ema", # Smoothed pressure rate EMA
    # Volatility regime
    "vol_fast_bps",        # ATR short-window volatility in bps
    "vol_slow_bps",        # ATR long-window volatility in bps
    "vol_ratio",           # vol_fast / vol_slow — regime indicator
    "vol_ratio_z",         # Z-score of vol_ratio
    # Source consistency
    "source_consistency_ok",  # Data-source price consistency flag (0/1)
    "source_jump_usd",        # USD magnitude of source price jump
    # Sweep divergence
    "sweep_div_match",     # Sweep + divergence co-occurrence flag
    # LiqMap 1h window keys (in stream, confirmed by tests)
    "liqmap_1h_levels_n",  # Number of active liquidity levels in 1h window
    "liqmap_1h_age_ms",    # Age of 1h liqmap snapshot in ms
]

# ---------------------------------------------------------------------------
# Group 2A — Adverse Selection / VPIN extension (4 keys)
# ---------------------------------------------------------------------------

_GROUP2A_ADVERSE_SELECTION: list[str] = [
    "vpin_rolling",        # |buy_vol_N - sell_vol_N| / total_vol_N — flow toxicity [0,1]
    "taker_lambda",        # Hawkes λ: rate of aggressive taker orders
    "maker_cancel_ratio",  # cancels / (cancels + fills) — MM behavior proxy
    "adverse_drift_ms",    # Rolling avg post-trade price drift (adverse selection realised)
]

# ---------------------------------------------------------------------------
# Group 2B — Order Book microstructure (4 keys, restored from v4_of + new)
# ---------------------------------------------------------------------------

_GROUP2B_ORDERBOOK: list[str] = [
    "book_slope_bid",       # Linear slope of bid-side depth curve (5 levels)
    "book_slope_ask",       # Linear slope of ask-side depth curve (5 levels)
    "book_imbalance_5lvl",  # (depth_bid_5 - depth_ask_5) / (depth_bid_5 + depth_ask_5)
    "bid_ask_depth_ratio",  # depth_bid_5 / depth_ask_5 — one-sided imbalance ratio
    # Note: book_churn_score ≈ burst_churn (already in v9_of base)
]

# ---------------------------------------------------------------------------
# Group 2C — Momentum / Technical Analysis (5 keys)
# ---------------------------------------------------------------------------

_GROUP2C_MOMENTUM: list[str] = [
    "microbar_range_bps",    # Micro-bar price range in bps (was in v4_of)
    "microbar_body_bps",     # Micro-bar body size vs range
    "microbar_vwap_mid_bps", # VWAP deviation from mid-price in bps
    "price_to_ema_bps",      # Current price deviation from slow EMA in bps
    "momentum_10s",          # Price change over last 10 seconds in bps
]

# ---------------------------------------------------------------------------
# Group 2D — Execution Quality / post-trade rolling averages (4 keys)
# ---------------------------------------------------------------------------

_GROUP2D_EXEC_QUALITY: list[str] = [
    "mae_r",                  # Max Adverse Excursion ratio — rolling avg (last-N trades)
    "mfe_r",                  # Max Favorable Excursion ratio — rolling avg (last-N trades)
    "slippage_realized_bps",  # Actual slippage vs expected_slippage_bps (rolling avg)
    "fill_time_p90_ms",       # P90 fill latency in ms (rolling window)
]

# ---------------------------------------------------------------------------
# Group 2E — Context / External market data (4 keys, fail-open until go-worker)
# ---------------------------------------------------------------------------

_GROUP2E_CONTEXT: list[str] = [
    "btc_corr_5m",          # Rolling 5-min correlation of symbol with BTC returns
    "funding_rate_bps",     # Perpetual funding rate in bps (carry risk)
    "open_interest_delta",  # Change in open interest (normalised USD)
    "liquidation_usd_1m",   # Liquidation volume in USD over last 1 minute
]

# ---------------------------------------------------------------------------
# Final composite key list — V10_OF_NUMERIC_KEYS (165 keys, sorted for determinism)
# ---------------------------------------------------------------------------

V10_OF_NUMERIC_KEYS: list[str] = sorted(set(
    _V9_OF_BASE
    + _GROUP1_STREAM_PROVEN
    + _GROUP2A_ADVERSE_SELECTION
    + _GROUP2B_ORDERBOOK
    + _GROUP2C_MOMENTUM
    + _GROUP2D_EXEC_QUALITY
    + _GROUP2E_CONTEXT
))

# Sanity guard (will be caught immediately at import in tests)
_EXPECTED_MIN = 160
_EXPECTED_MAX = 180
assert _EXPECTED_MIN <= len(V10_OF_NUMERIC_KEYS) <= _EXPECTED_MAX, (
    f"v10_of key count {len(V10_OF_NUMERIC_KEYS)} out of expected range "
    f"[{_EXPECTED_MIN}, {_EXPECTED_MAX}] — check for duplicates or deletions"
)


def get_v10_of_numeric_keys() -> list[str]:
    """Return sorted list of numeric indicator keys for v10_of."""
    return list(V10_OF_NUMERIC_KEYS)


def v10_of_info() -> dict:
    """Summary dict for logging / audit."""
    return {
        "ver": "v10_of",
        "n_numeric_keys": len(V10_OF_NUMERIC_KEYS),
        "groups": {
            "v9_of_base": len(_V9_OF_BASE),
            "group1_stream_proven": len(_GROUP1_STREAM_PROVEN),
            "group2a_adverse_selection": len(_GROUP2A_ADVERSE_SELECTION),
            "group2b_orderbook": len(_GROUP2B_ORDERBOOK),
            "group2c_momentum": len(_GROUP2C_MOMENTUM),
            "group2d_exec_quality": len(_GROUP2D_EXEC_QUALITY),
            "group2e_context": len(_GROUP2E_CONTEXT),
        },
    }
