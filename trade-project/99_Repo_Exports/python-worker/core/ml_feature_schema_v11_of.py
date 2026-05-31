from __future__ import annotations

"""
v11_of — Feature schema v11 (OrderFlow), pinned snapshot + regime/microstructure/interaction extensions.

Generated: 2026-03-15 (manual bump from v10_of).

v11_of = v10_of (165 keys) + 28 additional indicators:
  Group A (5)  — Regime / Structural Context
  Group B (5)  — Trade History / Session Context (from runtime_state)
  Group C (4)  — Cross-Asset / Correlation (fail-open until go-worker / external feed)
  Group D (5)  — Order Flow Microstructure Extensions
  Group E (4)  — Signal Self-Awareness (rolling signal stats)
  Group F (5)  — Derived / Interaction Features (pure deterministic products)

Coverage: 193 numeric indicators (no separate bool block — all bool as float 0/1)
          + direction/bucket/hour/dow/session one-hots → ~253 total feature_cols.

Design notes
------------
- Fail-open: missing runtime keys vectorize as 0.0 (safe for all groups).
- Group C (cross-asset): 0.0 until go-worker / external price feed deployed.
- Group E (self-awareness): 0.0 until signal history ring-buffer is populated (first ~100 signals).
- Group F (interactions): pure element-wise products; always available if source keys present.
- Append-only: new schema versions always add keys, never remove.
- vol_regime_code: 0=low, 1=normal, 2=high, 3=extreme (ordinal, not one-hot — tree-friendly).
"""



SCHEMA_HASH = "de592c070cb6"


# Import base v10 keys to avoid duplication drift
try:
    from core.ml_feature_schema_v10_of import V10_OF_NUMERIC_KEYS as _V10_OF_BASE
except ImportError:
    # Fallback only if running in a weird strict environment
    _V10_OF_BASE = []


# ---------------------------------------------------------------------------
# Group A — Regime / Structural Context (5 keys)
# ---------------------------------------------------------------------------

_GROUP_A_REGIME: list[str] = [
    "hurst_exp_50",         # Hurst exponent on last 50 ticks: <0.5 mean-revert, >0.5 trend
    "vol_regime_code",      # Ordinal: 0=low, 1=normal, 2=high, 3=extreme (tree-friendly)
    "tick_autocorr_lag1",   # Autocorrelation of tick signs at lag-1 (persistence of flow)
    "kyle_lambda",          # Kyle's Lambda: price impact per unit volume (adverse selection slope)
    "roll_spread_est",      # Roll's spread: 2*sqrt(-Cov(ΔP_t, ΔP_{t-1})) — effective spread
]

# ---------------------------------------------------------------------------
# Group B — Trade History / Session Context (5 keys)
# ---------------------------------------------------------------------------

_GROUP_B_TRADE_HISTORY: list[str] = [
    # Note: win_rate_session, consecutive_losses, mfe_mae_ratio_roll,
    # time_in_trade_p50_ms, calmar_roll_20 were already merged in v10_of
    "kelly_fraction_roll",  # Kelly criterion fraction: win_rate*(edge/odds) — position sizing signal
    "profit_factor_roll20", # Gross profit / gross loss over last 20 trades (>1 = edge)
    "expectancy_bps",       # (win_rate * avg_win - (1-win_rate) * avg_loss) in bps
    "recovery_factor_roll", # Net profit / max drawdown (last 20 trades)
    "trade_freq_per_hr",    # Trade frequency per hour in current session — regime activity
]

# ---------------------------------------------------------------------------
# Group C — Cross-Asset / Correlation (4 keys)
# ---------------------------------------------------------------------------

_GROUP_C_CROSS_ASSET: list[str] = [
    "market_breadth_score",  # Ratio of assets trending in same direction as signal (0-1)
    "crypto_fear_greed",     # Fear/Greed index proxy (liquidation_usd vs open_interest_delta)
    "alt_season_index",      # Alts vs BTC dominance momentum (alt_btc_beta_1h derivative)
    "cross_asset_vol_ratio", # Implied vol of BTC perp / symbol ATR — relative vol regime
]

# ---------------------------------------------------------------------------
# Group D — Order Flow Microstructure Extensions (5 keys)
# ---------------------------------------------------------------------------

_GROUP_D_MICROSTRUCTURE: list[str] = [
    "trade_size_skew",           # Skewness of trade sizes in window — whale activity
    "sweep_velocity_bps_s",      # Sweep speed in bps/sec — aggression intensity
    "book_refresh_rate_hz",      # Order book update frequency — MM liquidity depth
    "cancel_to_fill_ratio",      # Order cancels / fills — spoofing / MM behavior proxy
    "depth_pull_ratio",          # (depth_before - depth_after) / depth_before at best bid/ask
]

# ---------------------------------------------------------------------------
# Group E — Signal Self-Awareness (4 keys)
# ---------------------------------------------------------------------------

_GROUP_E_SELF_AWARENESS: list[str] = [
    "conf_ma_ratio",            # confidence / moving_avg_conf_24h — relative signal strength
    "signal_cluster_flag",      # 1.0 if 3+ signals within 60s (cluster / noise flag)
    "gate_hardness_score",      # Ratio of hard-veto gates fired vs total gates checked (0-1)
    "model_calibration_err",    # |predicted_confidence - realized_win_rate| rolling 50 trades
]

# ---------------------------------------------------------------------------
# Group F — Derived / Interaction Features (5 keys)
# ---------------------------------------------------------------------------

_GROUP_F_INTERACTIONS: list[str] = [
    "kyle_x_vpin",              # Kyle lambda × VPIN — toxic flow with price impact
    "momentum_x_vol_ratio",     # momentum_10s × vol_ratio — momentum quality in regime
    "pressure_x_obi",           # pressure × OBI — aggressive flow + book structure alignment
    "liq_score_x_spread",       # liq_score × spread_bps — risk-adjusted liquidity gate
    "confidence_x_of_score",    # confidence × of_score_final — double-gate signal strength
]


# ---------------------------------------------------------------------------
# Final composite key list — V11_OF_NUMERIC_KEYS (sorted for determinism)
# ---------------------------------------------------------------------------

V11_OF_NUMERIC_KEYS: list[str] = sorted(set(
    _V10_OF_BASE
    + _GROUP_A_REGIME
    + _GROUP_B_TRADE_HISTORY
    + _GROUP_C_CROSS_ASSET
    + _GROUP_D_MICROSTRUCTURE
    + _GROUP_E_SELF_AWARENESS
    + _GROUP_F_INTERACTIONS
))

# Sanity guard (caught immediately at import in tests)
_EXPECTED_MIN = 185
_EXPECTED_MAX = 220
if _V10_OF_BASE:
    assert _EXPECTED_MIN <= len(V11_OF_NUMERIC_KEYS) <= _EXPECTED_MAX, (
        f"v11_of key count {len(V11_OF_NUMERIC_KEYS)} out of expected range "
        f"[{_EXPECTED_MIN}, {_EXPECTED_MAX}] — check for duplicates or deletions"
    )


def get_v11_of_numeric_keys() -> list[str]:
    """Return sorted list of numeric indicator keys for v11_of."""
    return list(V11_OF_NUMERIC_KEYS)


def v11_of_info() -> dict:
    """Summary dict for logging / audit."""
    n_v10 = len(_V10_OF_BASE)
    n_new = len(V11_OF_NUMERIC_KEYS) - n_v10
    return {
        "ver": "v11_of",
        "n_numeric_keys": len(V11_OF_NUMERIC_KEYS),
        "n_v10_of_base": n_v10,
        "n_new_keys": n_new,
        "groups": {
            "group_a_regime": len(_GROUP_A_REGIME),
            "group_b_trade_history": len(_GROUP_B_TRADE_HISTORY),
            "group_c_cross_asset": len(_GROUP_C_CROSS_ASSET),
            "group_d_microstructure": len(_GROUP_D_MICROSTRUCTURE),
            "group_e_self_awareness": len(_GROUP_E_SELF_AWARENESS),
            "group_f_interactions": len(_GROUP_F_INTERACTIONS),
        },
    }
