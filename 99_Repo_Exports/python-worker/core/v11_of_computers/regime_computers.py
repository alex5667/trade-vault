import math

try:
    import numpy as np
except ImportError:
    np = None


def compute_hurst_exp_50(prices: list[float]) -> float:
    """Compute Hurst exponent on last 50 ticks.
    <0.5 mean-revert, >0.5 trending.
    Fail-open: return 0.5 (random walk) if not enough data.
    """
    if len(prices) < 20:
        return 0.5

    if np is None:
        return 0.5

    p = np.array(prices[-50:])
    # Simple R/S estimation
    # 1. log returns
    diffs = np.diff(p)
    if len(diffs) < 19 or np.all(diffs == 0):
        return 0.5

    m = np.mean(diffs)
    # 2. adjusted series
    y = diffs - m
    # 3. cumulative deviate
    Z = np.cumsum(y)
    # 4. range
    R = np.max(Z) - np.min(Z)
    # 5. standard dev
    S = np.std(diffs)

    if S == 0 or R == 0:
        return 0.5

    try:
        # Hurst ~ log(R/S) / log(N)
        h = math.log(R / S) / math.log(len(p))
        return float(np.clip(h, 0.0, 1.0))
    except Exception:
        return 0.5


def compute_vol_regime_code(vol_fast_bps: float, vol_slow_bps: float) -> float:
    """Ordinal vol regime: 0=low, 1=normal, 2=high, 3=extreme.
    Uses typical crypto bps thresholds if vol_slow is unavailable,
    or ratio if available (tree-friendly ordinal).
    """
    if vol_fast_bps <= 0:
        return 0.0

    v = vol_fast_bps
    # Simple absolute thresholds for crypto 1m-5m ticks (in bps)
    # if slow vol is also known, we adjust the thresholds
    th_low = 3.0
    th_hi = 15.0
    th_ext = 40.0

    if vol_slow_bps > 0:
        th_hi = max(10.0, vol_slow_bps * 1.5)
        th_ext = max(25.0, vol_slow_bps * 3.0)

    if v < th_low:
        return 0.0
    elif v < th_hi:
        return 1.0
    elif v < th_ext:
        return 2.0
    else:
        return 3.0


def compute_tick_autocorr_lag1(tick_signs: list[float]) -> float:
    """Autocorrelation of tick signs (-1, 0, 1) at lag-1.
    Measures persistence of flow direction.
    """
    if len(tick_signs) < 10:
        return 0.0

    if np is None:
        return 0.0

    x = np.array(tick_signs[-50:])
    m = np.mean(x)
    y = x - m

    # lag 1
    y1 = y[:-1]
    y2 = y[1:]

    c0 = np.sum(y * y)
    if c0 == 0:
        return 0.0

    c1 = np.sum(y1 * y2)
    return float(np.clip(c1 / c0, -1.0, 1.0))


def compute_kyle_lambda(prices: list[float], volumes: list[float]) -> float:
    """Kyle's Lambda: price impact per unit volume (adverse selection slope).
    OLS of price_change vs sign(volume) * sqrt(|volume|).
    """
    if len(prices) < 10 or len(volumes) < 10 or len(prices) != len(volumes):
        return 0.0

    if np is None:
        return 0.0

    # Use last N typical for micro
    p = np.array(prices[-50:])
    v = np.array(volumes[-50:])

    dp = np.diff(p)
    v_t = v[1:]

    # Avoid div by zero, use order flow proxy
    v_flow = np.sign(dp) * np.sqrt(np.abs(v_t))

    var_flow = np.var(v_flow)
    if var_flow == 0:
        return 0.0

    cov = np.cov(dp, v_flow)[0, 1]
    lam = cov / var_flow

    # ensure non-negative lambda theoretically
    return float(np.clip(lam, 0.0, 100.0))


def compute_roll_spread_est(prices: list[float]) -> float:
    """Roll's spread estimator = 2*sqrt(-Cov(ΔP_t, ΔP_{t-1})).
    Returns effective spread in bps if base price is > 0.
    """
    if len(prices) < 20:
        return 0.0

    if np is None:
        return 0.0

    p = np.array(prices[-50:])
    dp = np.diff(p)

    if len(dp) < 3:
        return 0.0

    dp1 = dp[:-1]
    dp2 = dp[1:]

    cov = np.cov(dp1, dp2)[0, 1]

    # Roll's model assumes neg cov. If positive, roll spread is technically undefined,
    if cov >= 0:
        return 0.0

    s_abs = 2.0 * math.sqrt(-cov)

    # Convert to bps
    p_last = p[-1]
    if p_last > 0:
        s_bps = (s_abs / p_last) * 10000.0
        return float(np.clip(s_bps, 0.0, 500.0))
    return 0.0
