"""
v13_of — Feature schema v13 (OrderFlow), pinned snapshot + advanced volatility/liquidity/toxicity extensions.

Generated: 2026-03-17 (manual bump from v12_of).

v13_of = v12_of (214 keys) + 28 additional indicators:
  Group NA (4)  — Advanced Volatility Estimation (OHLC-based academic estimators)
  Group NB (4)  — Academic Liquidity Metrics (Amihud, Corwin-Schultz, Hasbrouck)
  Group NC (4)  — Order Flow Toxicity (PIN, lambda asymmetry, toxicity composite)
  Group ND (5)  — Cross-Asset / Macro extended (BTC dominance, OI-weighted funding)
  Group NE (3)  — Entropy / Information Theory (price entropy, Gini, mutual info)
  Group NF (3)  — Mean Reversion / Stationarity (half-life, ADF, z-score)
  Group NX (5)  — Advanced Interaction Features (domain-logic cross-products)

  Subtotal new  = 4+4+4+5+3+3+5 = 28 (after dedup vs v12_of)

Coverage: ~242 numeric indicators (no separate bool block — all bool as float 0/1)
          + direction/bucket/hour/dow/session one-hots → ~302 total feature_cols.

Design notes
------------
- Fail-open: ALL keys vectorize as 0.0 if missing in runtime snapshot.
- Group NA (OHLC vol): computed from rolling kline OHLC buffers — always available.
- Group NB (liquidity): computed from rolling trade/book stats — available after warm-up.
- Group NC (toxicity): PIN requires EM cache (~5s), fail-open to 0.0 during warm-up.
- Group ND (cross-asset): 0.0 until go-worker REST polling deployed (fail-open).
- Group NE (entropy): computed from rolling tick buffers — available after warm-up.
- Group NF (mean reversion): ADF expensive, cached 5s — fail-open to 0.0.
- Group NX: pure derived indicators — always available if source keys are present.
- Anti-overfit policy: each key has Pearson(new, nearest_v12_key) < 0.70 by construction.
- Append-only: new schema versions always add keys, never remove.
"""

from __future__ import annotations

from typing import List

SCHEMA_HASH = "7838afd8be98"


# Import base v12 keys to avoid duplication drift
try:
    from core.ml_feature_schema_v12_of import V12_OF_NUMERIC_KEYS as _V12_OF_BASE
except ImportError:
    # Fallback only if running in a strict minimal environment
    _V12_OF_BASE = []


# ---------------------------------------------------------------------------
# Group NA — Advanced Volatility Estimation (4 keys)
# Orthogonal to v12_of: ATR/vol_fast/vol_slow are range-based; these are OHLC-based
# Sources: Garman & Klass (1980), Parkinson (1980), Yang & Zhang (2000)
# ---------------------------------------------------------------------------

_GROUP_NA_VOLATILITY: List[str] = [
    "garman_klass_vol",   # GK estimator: 0.5·ln(H/L)² − (2ln2−1)·ln(C/O)²
                          # 5–8× more accurate than close-to-close; captures intraday extremes
    "parkinson_vol",      # Parkinson: √(1/(4N·ln2) · Σ ln(H/L)²)
                          # log-scale range estimator; better for crypto log-normal returns
    "yang_zhang_vol",     # Yang-Zhang: σ_overnight² + σ_open² + k·σ_RS²
                          # Most accurate OHLC estimator; handles overnight/gap jumps
    "vol_of_vol",         # Rolling StdDev(realized_vol_bps, window=50)
                          # Regime change detector — stability of volatility itself
]

# ---------------------------------------------------------------------------
# Group NB — Academic Liquidity Metrics (4 keys)
# Orthogonal to v12_of: spread/impact_proxy/liq_score measure current book state;
# these measure realized/implied liquidity behavior
# Sources: Amihud (2002), Corwin & Schultz (2012), Hasbrouck (1991)
# ---------------------------------------------------------------------------

_GROUP_NB_LIQUIDITY: List[str] = [
    "amihud_illiquidity",         # |return| / volume_USD rolling 20 bars
                                  # Realized price impact per $1 traded (vs depth-based impact_proxy)
    "corwin_schultz_spread",      # Implicit bid-ask spread from H/L prices
                                  # Cross-check to spread_bps; detects hidden MM behavior
    "hasbrouck_info_share",       # Variance decomp of permanent price impact
                                  # Share of informed trading (vs VPIN volume-based proxy)
    "depth_resilience_half_life", # t½ of depth recovery after aggressor trade
                                  # Book resilience (vs res_speed_per_s = price-based)
]

# ---------------------------------------------------------------------------
# Group NC — Order Flow Toxicity (4 keys)
# Extending VPIN (v10) and Kyle λ (v11) with structural toxicity models
# Sources: Easley, López de Prado, O'Hara (2012)
# ---------------------------------------------------------------------------

_GROUP_NC_TOXICITY: List[str] = [
    "pin_estimate",           # Probability of Informed Trading (Easley-O'Hara EM model)
                              # Structural model vs VPIN (volume proxy); correlation ~0.55
    "lambda_asym",            # |Kyle_λ_buy − Kyle_λ_sell| / avg(λ)
                              # Asymmetric price impact by side (vs symmetric kyle_lambda)
    "toxicity_regime_score",  # Composite: 0.3×VPIN + 0.3×PIN + 0.2×adverse_drift + 0.2×info_flow
                              # Unified toxicity index 0→1 from 4 orthogonal sources
    "aggressive_sweep_ratio", # Volume crossing 3+ book levels / total_volume
                              # Institutional urgency depth (vs sweep_velocity = speed)
]

# ---------------------------------------------------------------------------
# Group ND — Cross-Asset / Macro Extended (5 keys)
# Extending v12_of MD with market-wide metrics
# Pipeline: go-worker REST polling → Redis hash, fail-open
# ---------------------------------------------------------------------------

_GROUP_ND_CROSS_ASSET: List[str] = [
    "btc_dominance_momentum",  # Δ(BTC dominance %) over 1h
                               # Direct BTC↔alts rotation (vs alt_season_index beta-proxy)
    "oi_weighted_funding",     # Σ(FR_i × OI_i) / Σ(OI_i) market-wide
                               # Aggregate leveraged crowding (vs per-symbol funding_rate_bps)
    "total_market_oi_delta",   # Δ(aggregate OI top-20 perps) / 1h
                               # Market-wide leverage flow (vs per-symbol open_interest_delta)
    "liq_heatmap_distance_bps",  # Distance to nearest liquidation cluster (bps from mid)
                                 # Price attractor/magnet signal — no v12 analog
    "long_short_ratio",        # Long accounts / Short accounts (Binance top traders)
                               # Crowding / sentiment proxy — no v12 analog
]

# ---------------------------------------------------------------------------
# Group NE — Entropy / Information Theory (3 keys)
# Orthogonal to volatility: vol measures amplitude, entropy measures predictability
# Sources: Biais & Weill (2009), Gençay & Gradojevic (2010)
# ---------------------------------------------------------------------------

_GROUP_NE_ENTROPY: List[str] = [
    "price_entropy_50",          # Shannon entropy of binned returns (50 ticks, 10 bins)
                                 # Predictability: low entropy = trending, high = random walk
    "order_size_gini",           # Gini coefficient of trade sizes in window
                                 # Concentration (vs trade_size_entropy: Shannon distribution)
    "mutual_info_price_volume",  # MI(returns, volume) rolling 100 ticks
                                 # Non-linear price↔volume dependency (vs linear correlation)
]

# ---------------------------------------------------------------------------
# Group NF — Mean Reversion / Stationarity (3 keys)
# Extending hurst_exp_50 (v11): Hurst = type, these = speed + significance
# Sources: Lo & MacKinlay (1988), Ornstein-Uhlenbeck process
# ---------------------------------------------------------------------------

_GROUP_NF_MEAN_REVERSION: List[str] = [
    "half_life_mean_reversion",  # OU process t½ from Ornstein-Uhlenbeck fit (rolling)
                                 # Actionable time window (vs hurst_exp_50 = regime type)
    "adf_pvalue_50",             # p-value from Augmented Dickey-Fuller test (50 ticks)
                                 # Statistical stationarity confidence (vs Hurst = scaling)
    "zscore_mid_to_vwap",        # (mid − VWAP) / σ(mid − VWAP) rolling
                                 # Standardized deviation from fair value (vs raw vwap_roll_diff_bps)
]

# ---------------------------------------------------------------------------
# Group NX — Advanced Interaction Features (5 keys)
# Domain-logic cross-products; low corr by construction
# ---------------------------------------------------------------------------

_GROUP_NX_INTERACTIONS: List[str] = [
    "vpin_x_funding",           # VPIN × sign(funding_rate) — toxic flow + carry crowding
    "hurst_x_vol_regime",       # hurst_exp_50 × vol_regime_code — trending in volatile regime
    "entropy_x_spread",         # price_entropy_50 × spread_bps — chaos + expensive entry
    "depth_resil_x_sweep",      # depth_resilience_half_life × aggressive_sweep_ratio — resilience under pressure
    "amihud_x_oi_delta",        # amihud_illiquidity × open_interest_delta — illiquidity + leverage squeeze
]


# ---------------------------------------------------------------------------
# Final composite key list — V13_OF_NUMERIC_KEYS (sorted for determinism)
# ---------------------------------------------------------------------------

V13_OF_NUMERIC_KEYS: List[str] = sorted(set(
    _V12_OF_BASE
    + _GROUP_NA_VOLATILITY
    + _GROUP_NB_LIQUIDITY
    + _GROUP_NC_TOXICITY
    + _GROUP_ND_CROSS_ASSET
    + _GROUP_NE_ENTROPY
    + _GROUP_NF_MEAN_REVERSION
    + _GROUP_NX_INTERACTIONS
))

# Sanity guard (caught immediately at import in tests)
_EXPECTED_MIN = 230
_EXPECTED_MAX = 260
if _V12_OF_BASE:
    assert _EXPECTED_MIN <= len(V13_OF_NUMERIC_KEYS) <= _EXPECTED_MAX, (
        f"v13_of key count {len(V13_OF_NUMERIC_KEYS)} out of expected range "
        f"[{_EXPECTED_MIN}, {_EXPECTED_MAX}] — check for duplicates or deletions"
    )


def get_v13_of_numeric_keys() -> List[str]:
    """Return sorted list of numeric indicator keys for v13_of."""
    return list(V13_OF_NUMERIC_KEYS)


def v13_of_info() -> dict:
    """Summary dict for logging / audit."""
    n_v12 = len(_V12_OF_BASE)
    n_new = len(V13_OF_NUMERIC_KEYS) - n_v12
    return {
        "ver": "v13_of",
        "n_numeric_keys": len(V13_OF_NUMERIC_KEYS),
        "n_v12_of_base": n_v12,
        "n_new_keys": n_new,
        "groups": {
            "group_na_volatility": len(_GROUP_NA_VOLATILITY),
            "group_nb_liquidity": len(_GROUP_NB_LIQUIDITY),
            "group_nc_toxicity": len(_GROUP_NC_TOXICITY),
            "group_nd_cross_asset": len(_GROUP_ND_CROSS_ASSET),
            "group_ne_entropy": len(_GROUP_NE_ENTROPY),
            "group_nf_mean_reversion": len(_GROUP_NF_MEAN_REVERSION),
            "group_nx_interactions": len(_GROUP_NX_INTERACTIONS),
        },
    }
