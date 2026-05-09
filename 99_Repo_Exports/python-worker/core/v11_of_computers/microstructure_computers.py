import math

try:
    import numpy as np
    from scipy.stats import skew
except ImportError:
    np = None
    skew = None

def compute_trade_size_skew(sizes: list[float]) -> float:
    """Skewness of trade sizes in window — whale activity."""
    if len(sizes) < 10:
        return 0.0

    if skew is None or np is None:
        return 0.0

    v = np.array(sizes[-50:])
    try:
        s = skew(v)
        if math.isnan(s):
            return 0.0
        return float(s)
    except Exception:
        return 0.0

def compute_large_trade_ratio(sizes: list[float], threshold_mult: float = 3.0) -> float:
    """Ratio of trades > threshold_mult * median_size."""
    if len(sizes) < 10:
        return 0.0

    if np is None:
        return 0.0

    v = np.array(sizes[-50:])
    med = np.median(v)

    if med == 0:
        return 0.0

    threshold = med * threshold_mult
    large_count = np.sum(v > threshold)

    return float(large_count / len(v))

def compute_ofi_slope_sec30(ofi_history_t: list[float], ofi_history_v: list[float]) -> float:
    """Rolling OLS slope of OFI over last 30 seconds."""
    if len(ofi_history_t) < 5 or len(ofi_history_t) != len(ofi_history_v):
        return 0.0

    if np is None:
        return 0.0

    t = np.array(ofi_history_t[-50:])
    v = np.array(ofi_history_v[-50:])

    # recent 30s only
    now = t[-1]
    mask = t >= (now - 30_000)

    t_w = t[mask]
    v_w = v[mask]

    if len(t_w) < 5:
        return 0.0

    # center time
    t_w = (t_w - t_w[0]) / 1000.0  # seconds

    var_t = np.var(t_w)
    if var_t == 0:
        return 0.0

    cov = np.cov(t_w, v_w)[0, 1]
    return float(cov / var_t)

def compute_book_refresh_rate_hz(update_ts_ms: list[float]) -> float:
    """Order book update frequency — MM liquidity depth."""
    if len(update_ts_ms) < 5:
        return 0.0

    ts = update_ts_ms[-50:]
    dt = ts[-1] - ts[0]

    if dt <= 0:
        return 0.0

    hz = len(ts) / (dt / 1000.0)
    return float(np.clip(hz, 0.0, 1000.0))

def compute_sweep_velocity_bps_s(sweep_start_ms: float, sweep_end_ms: float, price_delta_bps: float) -> float:
    """Sweep speed in bps/sec — aggression intensity."""
    dt_ms = sweep_end_ms - sweep_start_ms

    if dt_ms <= 0:
        return 0.0

    dt_s = dt_ms / 1000.0
    vel = price_delta_bps / dt_s

    return float(vel)

def compute_cancel_to_fill_ratio(cancels: int, fills: int) -> float:
    """Order cancels / fills — spoofing / MM behavior proxy."""
    if fills == 0:
        return 999.0 if cancels > 0 else 0.0

    return float(cancels / fills)

def compute_depth_pull_ratio(depth_before: float, depth_after: float) -> float:
    """(depth_before - depth_after) / depth_before at best bid/ask."""
    if depth_before <= 0:
        return 0.0

    pulled = depth_before - depth_after

    # Can be negative if depth added
    ratio = pulled / depth_before
    return float(np.clip(ratio, -1.0, 1.0))
