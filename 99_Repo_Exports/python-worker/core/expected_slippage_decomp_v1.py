from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class SlippageDecompResult:
    """Deterministic pre-trade slippage decomposition (bps).

    Fields:
        spread_bps  – half-spread component (bps)
        impact_bps  – market-impact component (bps)
        total_bps   – spread_bps + impact_bps, capped at slippage_decomp_cap_bps
    """
    spread_bps: float
    impact_bps: float
    total_bps: float


def _f(x: Any, d: float = 0.0) -> float:
    """Safe float conversion; returns d on any error or non-finite value."""
    try:
        v = float(x)
        return v if math.isfinite(v) else d
    except Exception:
        return d


def expected_slippage_decomp_bps(
    *,
    spread_bps: float,
    impact_proxy: float,
    cfg: dict[str, Any],
    order_size_usd: float | None = None,
) -> SlippageDecompResult:
    """Compute expected slippage (bps) as: half_spread + k * |impact_proxy| * size_scale.

    Design goals:
      - deterministic, low-latency, no I/O
      - safe (fail-open) on bad inputs
      - metric observation is done by the CALLER (tick_processor), not here

    Parameters
    ----------
    spread_bps    : current spread in bps (at submit time)
    impact_proxy  : dimensionless proxy, e.g. |dn_usd| / depth_min_5_usd
    cfg           : runtime config dict; reads slippage_decomp_* keys
    order_size_usd: optional size for size scaling (default: size_ref_usd)

    Returns
    -------
    SlippageDecompResult(spread_bps, impact_bps, total_bps)
    """
    # Guard: disabled unless explicitly enabled
    enable = int(cfg.get("slippage_decomp_enable", 0) or 0)
    if enable != 1:
        return SlippageDecompResult(spread_bps=0.0, impact_bps=0.0, total_bps=0.0)

    half_spread_mult  = _f(cfg.get("slippage_decomp_half_spread_mult", 0.5), 0.5)
    impact_coeff_bps  = _f(cfg.get("slippage_decomp_impact_coeff_bps", 8.0), 8.0)
    size_ref_usd      = _f(cfg.get("slippage_decomp_size_ref_usd", 10_000.0), 10_000.0)
    size_power        = _f(cfg.get("slippage_decomp_size_power", 1.0), 1.0)
    cap_bps           = _f(cfg.get("slippage_decomp_cap_bps", 250.0), 250.0)

    sp = max(0.0, _f(spread_bps, 0.0))
    ip = abs(_f(impact_proxy, 0.0))

    # Spread component: half_spread_mult * spread_bps
    spread_comp = max(0.0, half_spread_mult) * sp

    # Size scaling
    if order_size_usd is None:
        order_size = size_ref_usd
    else:
        order_size = _f(order_size_usd, size_ref_usd)
        if order_size <= 0:
            order_size = size_ref_usd

    size_ratio = order_size / max(size_ref_usd, 1e-9)
    if size_ratio <= 0:
        size_ratio = 1.0

    try:
        size_scale = size_ratio ** max(0.0, size_power)
    except Exception:
        size_scale = 1.0

    # Impact component: k * |impact_proxy| * size_scale
    impact_comp = max(0.0, impact_coeff_bps) * ip * size_scale

    total = spread_comp + impact_comp
    if cap_bps > 0:
        total = min(total, cap_bps)

    return SlippageDecompResult(
        spread_bps=float(spread_comp),
        impact_bps=float(impact_comp),
        total_bps=float(total),
    )
