from __future__ import annotations

"""A5: flags + sessions helpers.

Design goals:
- deterministic by timestamps (ts_ms), not by tick count
- bounded outputs for ML (bool flags as 0/1)
- minimal dependencies: can run without rolling trackers (A3)

This module is used by tick_processor to:
- maintain time-decayed EMA baselines (trade qty, depth_total_10)
- compute A5 flags and session one-hot
"""


import math
from typing import Any

from core.feature_engineering import derive_session_label


def _alpha_time_decay(dt_ms: int, tau_ms: int) -> float:
    """Time-decayed EMA alpha.

    alpha = 1 - exp(-dt/tau)
    - dt_ms <= 0 -> 0
    - tau_ms <= 0 -> 0
    """

    if dt_ms <= 0 or tau_ms <= 0:
        return 0.0
    x = -float(dt_ms) / float(tau_ms)
    # avoid underflow / exp(-large)
    if x < -50.0:
        return 1.0
    return 1.0 - math.exp(x)


def update_time_ema(
    *,
    prev_ema: float,
    x: float,
    prev_ts_ms: int,
    ts_ms: int,
    tau_ms: int,
) -> tuple[float, int, bool]:
    """Update EMA using timestamps.

    Returns: (new_ema, new_ts_ms, bad_time)

    bad_time=True if ts_ms <= prev_ts_ms when prev_ts_ms>0.
    """

    if ts_ms <= 0:
        return prev_ema, prev_ts_ms, False

    if prev_ts_ms <= 0:
        return float(x), int(ts_ms), False

    dt_ms = int(ts_ms) - int(prev_ts_ms)
    if dt_ms <= 0:
        # out-of-order or duplicate; keep previous EMA and timestamp
        return prev_ema, prev_ts_ms, True

    a = _alpha_time_decay(dt_ms, tau_ms)
    if prev_ema <= 0.0:
        return float(x), int(ts_ms), False

    new_ema = (1.0 - a) * float(prev_ema) + a * float(x)
    return float(new_ema), int(ts_ms), False


def session_onehot(ts_ms: int, cfg: dict[str, Any] | None = None) -> dict[str, int]:
    """Return session one-hot: session_asia/eu/us/off (sum==1)."""

    label = derive_session_label(ts_ms, cfg=cfg)
    if label not in ("asia", "eu", "us", "off"):
        label = "off"
    return {
        "session_asia": 1 if label == "asia" else 0,
        "session_eu": 1 if label == "eu" else 0,
        "session_us": 1 if label == "us" else 0,
        "session_off": 1 if label == "off" else 0,
    }


def session_open_close_flags(
    ts_ms: int,
    *,
    edge_window_ms: int = 300_000,
    cfg: dict[str, Any] | None = None,
) -> dict[str, int]:
    """Flags for session edges.

    Uses the same session boundaries as derive_session_label (UTC):
    - asia: [00:00, 07:00)
    - eu:   [07:00, 13:00)
    - us:   [13:00, 21:00)
    - off:  [21:00, 24:00)

    flag_session_open: within +/- edge_window_ms of any session start.
    flag_session_close: within +/- edge_window_ms of any session end.

    Note: on exact boundary, open and close can both be 1 (acceptable).
    """

    if ts_ms <= 0:
        return {"flag_session_open": 0, "flag_session_close": 0}

    win_s = max(int(edge_window_ms) // 1000, 0)
    sec_of_day = (int(ts_ms) // 1000) % 86_400

    def _near(target_s: int) -> bool:
        d = abs(sec_of_day - target_s)
        d = min(d, 86_400 - d)
        return d <= win_s

    # boundaries consistent with derive_session_label defaults
    start_bounds = [0, 7 * 3600, 13 * 3600, 21 * 3600]
    end_bounds = [7 * 3600, 13 * 3600, 21 * 3600, 0]  # 24:00 -> 0

    open_flag = 1 if any(_near(t) for t in start_bounds) else 0
    close_flag = 1 if any(_near(t) for t in end_bounds) else 0
    return {"flag_session_open": open_flag, "flag_session_close": close_flag}


def compute_a5_flags(
    *,
    ts_ms: int,
    qty: float,
    indicators: dict[str, Any],
    trade_qty_ema: float,
    depth_total10: float,
    depth_total10_ema: float,
    cfg: dict[str, Any] | None = None,
) -> dict[str, int]:
    """Compute A5 flags as 0/1.

    Inputs:
    - indicators: must include vol_ratio_z (preferred) for high-vol and microbar_* for mean-reversion MVP.

    Returns dict of flags (0/1), plus reserved flag_macro_event=0.
    """

    cfg = cfg or {}
    out: dict[str, int] = {}

    # high volatility
    high_vol_z_th = float(cfg.get("a5_high_vol_z_th", 2.0) or 2.0)
    try:
        vol_z = float(indicators.get("vol_ratio_z", float("nan")))
    except Exception:
        vol_z = float("nan")
    out["flag_high_vol"] = 1 if (vol_z == vol_z and vol_z >= high_vol_z_th) else 0

    # low liquidity: compare current depth_total_10 vs EMA baseline
    low_liq_ratio_th = float(cfg.get("a5_low_liq_ratio_th", 0.35) or 0.35)
    eps = 1e-12
    ratio = float(depth_total10) / max(float(depth_total10_ema), eps) if depth_total10_ema > 0 else 1.0
    out["flag_low_liquidity"] = 1 if (depth_total10_ema > 0 and ratio <= low_liq_ratio_th) else 0

    # large trade: qty >= mult * EMA(qty)
    large_trade_mult = float(cfg.get("a5_large_trade_mult", 6.0) or 6.0)
    out["flag_large_trade"] = 1 if (trade_qty_ema > 0 and qty > 0 and qty >= large_trade_mult * trade_qty_ema) else 0

    # mean reversion MVP: small body vs range (wicky bar) + non-trivial range
    mr_body_ratio_th = float(cfg.get("a5_mr_body_ratio_th", 0.35) or 0.35)
    mr_range_th_bps = float(cfg.get("a5_mr_range_th_bps", 5.0) or 5.0)
    try:
        rng = abs(float(indicators.get("microbar_range_bps", 0.0) or 0.0))
        body = abs(float(indicators.get("microbar_body_bps", 0.0) or 0.0))
    except Exception:
        rng, body = 0.0, 0.0
    out["flag_mean_reversion_mode"] = 1 if (rng >= mr_range_th_bps and rng > 0 and (body / rng) <= mr_body_ratio_th) else 0

    # session edges
    edge_window_ms = int(cfg.get("a5_session_edge_window_ms", 300_000) or 300_000)
    out.update(session_open_close_flags(ts_ms, edge_window_ms=edge_window_ms, cfg=cfg))

    # reserved for future wiring
    out["flag_macro_event"] = 0
    return out
