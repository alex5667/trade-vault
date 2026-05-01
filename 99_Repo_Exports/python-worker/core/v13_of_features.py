from __future__ import annotations
"""
core/v13_of_features.py
=======================
Compute functions for the 28 new v13_of indicator keys (Groups NA–NX).

Design principles:
  - Every function is FAIL-OPEN: exceptions return 0.0.
  - Train == Serve: same code runs in tick_processor.py and offline dataset builder.
  - All keys match ml_feature_schema_v13_of.py exactly.
  - No heavy imports; only stdlib math + existing runtime attributes.
"""


import math
from typing import Any, Dict


# ---------------------------------------------------------------------------
# Group NA — Advanced Volatility Estimation (OHLC-based)
# Keys: garman_klass_vol, parkinson_vol, yang_zhang_vol, vol_of_vol
#
# Sources: Garman & Klass (1980), Parkinson (1980), Yang & Zhang (2000)
# Runtime attributes populated by BarProcessor rolling kline buffers.
# ---------------------------------------------------------------------------

_LN2 = math.log(2.0)


def compute_group_na(runtime: Any, now_ms: int, indicators: Dict[str, Any]) -> Dict[str, float]:
    """
    Group NA: OHLC-based volatility estimators from rolling kline stats.

    Runtime attributes consumed (all fail-open to 0.0 if absent):
      - garman_klass_vol    : float — GK volatility estimator (pre-computed in bar_processor)
      - parkinson_vol       : float — Parkinson high-low range estimator
      - yang_zhang_vol      : float — Yang-Zhang comprehensive OHLC estimator
      - vol_of_vol          : float — StdDev(realized_vol_bps) over rolling window
    """
    out: Dict[str, float] = {}

    for key in (
        "garman_klass_vol",
        "parkinson_vol",
        "yang_zhang_vol",
        "vol_of_vol",
    ):
        try:
            out[key] = float(getattr(runtime, key, 0.0) or 0.0)
        except Exception:
            out[key] = 0.0

    return out


# ---------------------------------------------------------------------------
# Group NB — Academic Liquidity Metrics
# Keys: amihud_illiquidity, corwin_schultz_spread, hasbrouck_info_share,
#       depth_resilience_half_life
# ---------------------------------------------------------------------------

def compute_group_nb(runtime: Any, now_ms: int, indicators: Dict[str, Any]) -> Dict[str, float]:
    """
    Group NB: academic liquidity metrics from rolling trade/book statistics.

    Runtime attributes:
      - amihud_illiquidity         : float — |return| / volume_USD (rolling 20 bars)
      - corwin_schultz_spread      : float — implied spread from H/L prices
      - hasbrouck_info_share       : float — permanent price impact share [0,1]
      - depth_resilience_half_life : float — t½ of depth recovery after aggressive trade (ms)
    """
    out: Dict[str, float] = {}

    for key in (
        "amihud_illiquidity",
        "corwin_schultz_spread",
        "hasbrouck_info_share",
        "depth_resilience_half_life",
    ):
        try:
            out[key] = float(getattr(runtime, key, 0.0) or 0.0)
        except Exception:
            out[key] = 0.0

    return out


# ---------------------------------------------------------------------------
# Group NC — Order Flow Toxicity
# Keys: pin_estimate, lambda_asym, toxicity_regime_score, aggressive_sweep_ratio
# ---------------------------------------------------------------------------

def compute_group_nc(runtime: Any, now_ms: int, indicators: Dict[str, Any]) -> Dict[str, float]:
    """
    Group NC: flow toxicity metrics.

    pin_estimate           — from runtime PIN EM cache (updated every ~5s)
    lambda_asym            — computed from runtime Kyle λ buy/sell components
    toxicity_regime_score  — composite from existing indicators + PIN
    aggressive_sweep_ratio — from runtime sweep tracker (trades crossing 3+ levels)
    """
    out: Dict[str, float] = {}

    # pin_estimate (from EM cache in runtime)
    try:
        out["pin_estimate"] = float(getattr(runtime, "pin_estimate", 0.0) or 0.0)
    except Exception:
        out["pin_estimate"] = 0.0

    # lambda_asym: |λ_buy − λ_sell| / avg(λ)
    try:
        lam_buy = float(getattr(runtime, "kyle_lambda_buy", 0.0) or 0.0)
        lam_sell = float(getattr(runtime, "kyle_lambda_sell", 0.0) or 0.0)
        avg_lam = (lam_buy + lam_sell) / 2.0
        if avg_lam > 1e-12:
            out["lambda_asym"] = abs(lam_buy - lam_sell) / avg_lam
        else:
            out["lambda_asym"] = 0.0
    except Exception:
        out["lambda_asym"] = 0.0

    # toxicity_regime_score: composite 0→1
    # 0.3×VPIN + 0.3×PIN + 0.2×adverse_drift_norm + 0.2×info_flow
    try:
        vpin = float(indicators.get("vpin_rolling", 0.0) or 0.0)
        pin = float(out.get("pin_estimate", 0.0) or 0.0)
        # Normalise adverse_drift_ms to [0,1] with sigmoid-like clamping
        adv_raw = float(indicators.get("adverse_drift_ms", 0.0) or 0.0)
        adv_norm = min(1.0, max(0.0, adv_raw / 50.0))  # 50ms → 1.0
        info = float(indicators.get("info_flow", 0.0) or 0.0)
        score = 0.3 * min(1.0, vpin) + 0.3 * min(1.0, pin) + 0.2 * adv_norm + 0.2 * min(1.0, info)
        out["toxicity_regime_score"] = float(min(1.0, max(0.0, score)))
    except Exception:
        out["toxicity_regime_score"] = 0.0

    # aggressive_sweep_ratio (from runtime sweep tracker)
    try:
        out["aggressive_sweep_ratio"] = float(
            getattr(runtime, "aggressive_sweep_ratio", 0.0) or 0.0
        )
    except Exception:
        out["aggressive_sweep_ratio"] = 0.0

    return out


# ---------------------------------------------------------------------------
# Group ND — Cross-Asset / Macro Extended
# Keys: btc_dominance_momentum, oi_weighted_funding, total_market_oi_delta,
#       liq_heatmap_distance_bps, long_short_ratio
#
# All sourced from runtime:crossasset Redis Hash via maybe_load_crossasset().
# Fail-open to 0.0 until go-worker REST polling deployed.
# ---------------------------------------------------------------------------

def compute_group_nd(runtime: Any, now_ms: int, indicators: Dict[str, Any]) -> Dict[str, float]:
    """
    Group ND: extended cross-asset macro features from go-worker → Redis hash.
    """
    out: Dict[str, float] = {}

    for key in (
        "btc_dominance_momentum",
        "oi_weighted_funding",
        "total_market_oi_delta",
        "liq_heatmap_distance_bps",
        "long_short_ratio",
    ):
        try:
            out[key] = float(getattr(runtime, key, 0.0) or 0.0)
        except Exception:
            out[key] = 0.0

    return out


# ---------------------------------------------------------------------------
# Group NE — Entropy / Information Theory
# Keys: price_entropy_50, order_size_gini, mutual_info_price_volume
# ---------------------------------------------------------------------------

def compute_group_ne(runtime: Any, now_ms: int, indicators: Dict[str, Any]) -> Dict[str, float]:
    """
    Group NE: entropy and information-theory features from rolling tick buffers.

    Runtime attributes:
      - price_entropy_50         : float — Shannon entropy of binned returns (50 ticks)
      - order_size_gini          : float — Gini coefficient of trade sizes [0,1]
      - mutual_info_price_volume : float — MI(returns, volume) rolling 100 ticks
    """
    out: Dict[str, float] = {}

    for key in (
        "price_entropy_50",
        "order_size_gini",
        "mutual_info_price_volume",
    ):
        try:
            out[key] = float(getattr(runtime, key, 0.0) or 0.0)
        except Exception:
            out[key] = 0.0

    return out


# ---------------------------------------------------------------------------
# Group NF — Mean Reversion / Stationarity
# Keys: half_life_mean_reversion, adf_pvalue_50, zscore_mid_to_vwap
# ---------------------------------------------------------------------------

def compute_group_nf(runtime: Any, now_ms: int, indicators: Dict[str, Any]) -> Dict[str, float]:
    """
    Group NF: mean reversion and stationarity features.

    half_life_mean_reversion — from runtime OU process fit (cached 5s)
    adf_pvalue_50            — from runtime ADF test cache (cached 5s)
    zscore_mid_to_vwap       — computed from indicators (mid, VWAP, rolling σ)
    """
    out: Dict[str, float] = {}

    # half_life_mean_reversion (from runtime cache)
    try:
        out["half_life_mean_reversion"] = float(
            getattr(runtime, "half_life_mean_reversion", 0.0) or 0.0
        )
    except Exception:
        out["half_life_mean_reversion"] = 0.0

    # adf_pvalue_50 (from runtime ADF cache)
    try:
        out["adf_pvalue_50"] = float(
            getattr(runtime, "adf_pvalue_50", 0.0) or 0.0
        )
    except Exception:
        out["adf_pvalue_50"] = 0.0

    # zscore_mid_to_vwap: (mid − VWAP) / σ(mid − VWAP)
    try:
        mid = float(getattr(runtime, "last_book_mid", 0.0) or 0.0)
        vwap = float(indicators.get("roll_vwap_px", 0.0) or 0.0)
        sigma = float(getattr(runtime, "mid_vwap_diff_std", 0.0) or 0.0)
        if mid > 0 and vwap > 0 and sigma > 1e-12:
            out["zscore_mid_to_vwap"] = (mid - vwap) / sigma
        else:
            out["zscore_mid_to_vwap"] = 0.0
    except Exception:
        out["zscore_mid_to_vwap"] = 0.0

    return out


# ---------------------------------------------------------------------------
# Group NX — Advanced Interaction Features (derived from existing indicators)
# Keys: vpin_x_funding, hurst_x_vol_regime, entropy_x_spread,
#       depth_resil_x_sweep, amihud_x_oi_delta
# All domain-logic cross-products, not data-mined.
# ---------------------------------------------------------------------------

def compute_group_nx(runtime: Any, now_ms: int, indicators: Dict[str, Any]) -> Dict[str, float]:
    """
    Group NX: advanced domain-logic interaction features.

    All computed from existing v12 indicators + new v13 groups.
    Low pipeline cost; always available if source keys present.
    """
    out: Dict[str, float] = {}

    # vpin_x_funding: VPIN × sign(funding_rate)
    try:
        vpin = float(indicators.get("vpin_rolling", 0.0) or 0.0)
        fr = float(indicators.get("funding_rate_bps", 0.0) or 0.0)
        sign_fr = 1.0 if fr > 0 else (-1.0 if fr < 0 else 0.0)
        out["vpin_x_funding"] = vpin * sign_fr
    except Exception:
        out["vpin_x_funding"] = 0.0

    # hurst_x_vol_regime: hurst_exp_50 × vol_regime_code
    try:
        hurst = float(indicators.get("hurst_exp_50",
                       getattr(runtime, "hurst_exp_50", 0.0)) or 0.0)
        vol_code = float(indicators.get("vol_regime_code",
                          getattr(runtime, "vol_regime_code", 0.0)) or 0.0)
        out["hurst_x_vol_regime"] = hurst * vol_code
    except Exception:
        out["hurst_x_vol_regime"] = 0.0

    # entropy_x_spread: price_entropy_50 × spread_bps
    try:
        entropy = float(indicators.get("price_entropy_50", 0.0) or 0.0)
        spread = float(indicators.get("spread_bps", 0.0) or 0.0)
        out["entropy_x_spread"] = entropy * spread
    except Exception:
        out["entropy_x_spread"] = 0.0

    # depth_resil_x_sweep: depth_resilience_half_life × aggressive_sweep_ratio
    try:
        resil = float(indicators.get("depth_resilience_half_life", 0.0) or 0.0)
        sweep = float(indicators.get("aggressive_sweep_ratio", 0.0) or 0.0)
        out["depth_resil_x_sweep"] = resil * sweep
    except Exception:
        out["depth_resil_x_sweep"] = 0.0

    # amihud_x_oi_delta: amihud_illiquidity × open_interest_delta
    try:
        amihud = float(indicators.get("amihud_illiquidity", 0.0) or 0.0)
        oi_d = float(indicators.get("open_interest_delta", 0.0) or 0.0)
        out["amihud_x_oi_delta"] = amihud * oi_d
    except Exception:
        out["amihud_x_oi_delta"] = 0.0

    return out


# ---------------------------------------------------------------------------
# Master injection entry point
# ---------------------------------------------------------------------------

# All 28 new keys with their 0.0 defaults for fail-open guarantee
_DEFAULTS: Dict[str, float] = {
    # NA — Advanced Volatility
    "garman_klass_vol": 0.0,
    "parkinson_vol": 0.0,
    "yang_zhang_vol": 0.0,
    "vol_of_vol": 0.0,
    # NB — Academic Liquidity
    "amihud_illiquidity": 0.0,
    "corwin_schultz_spread": 0.0,
    "hasbrouck_info_share": 0.0,
    "depth_resilience_half_life": 0.0,
    # NC — Flow Toxicity
    "pin_estimate": 0.0,
    "lambda_asym": 0.0,
    "toxicity_regime_score": 0.0,
    "aggressive_sweep_ratio": 0.0,
    # ND — Cross-Asset Macro Extended
    "btc_dominance_momentum": 0.0,
    "oi_weighted_funding": 0.0,
    "total_market_oi_delta": 0.0,
    "liq_heatmap_distance_bps": 0.0,
    "long_short_ratio": 0.0,
    # NE — Entropy / Info Theory
    "price_entropy_50": 0.0,
    "order_size_gini": 0.0,
    "mutual_info_price_volume": 0.0,
    # NF — Mean Reversion
    "half_life_mean_reversion": 0.0,
    "adf_pvalue_50": 0.0,
    "zscore_mid_to_vwap": 0.0,
    # NX — Advanced Interactions
    "vpin_x_funding": 0.0,
    "hurst_x_vol_regime": 0.0,
    "entropy_x_spread": 0.0,
    "depth_resil_x_sweep": 0.0,
    "amihud_x_oi_delta": 0.0,
}

_V13_OF_NEW_KEY_SET = frozenset(_DEFAULTS.keys())


def inject_v13_of_features(
    *,
    runtime: Any,
    now_ms: int,
    indicators: Dict[str, Any],
) -> None:
    """
    Compute and inject all 28 v13_of new indicator keys into `indicators`.

    Called from TickProcessor after the v12_of injection block.
    Fail-open: any exception in a group is silently swallowed; keys default to 0.0.

    Groups:
      NA — Advanced Volatility (OHLC)          (4 keys)
      NB — Academic Liquidity                  (4 keys)
      NC — Order Flow Toxicity                 (4 keys)
      ND — Cross-Asset Macro Extended          (5 keys)
      NE — Entropy / Information Theory        (3 keys)
      NF — Mean Reversion / Stationarity       (3 keys)
      NX — Advanced Interactions               (5 keys)
    """
    # Pre-set defaults so keys always exist even if a group raises
    for k, v in _DEFAULTS.items():
        indicators.setdefault(k, v)

    _groups = [
        compute_group_na,
        compute_group_nb,
        compute_group_nc,
        compute_group_nd,
        compute_group_ne,
        compute_group_nf,
        compute_group_nx,
    ]
    for fn in _groups:
        try:
            result = fn(runtime, now_ms, indicators)
            indicators.update(result)
        except Exception:
            pass  # fail-open: defaults already set above
