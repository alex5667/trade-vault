from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional
import math


@dataclass
class WeakProgressSnapshot:
    """
    Weak progress metrics computed on bar_close (microbar).

    Definitions:
      range_atr = (high-low) / ATR
      body_atr  = abs(close-open) / ATR

    Alternative:
      eff = move_ticks / abs(delta_sum)
      where move_ticks = abs(close-open)/tick_size_px
    """
    atr: float
    range_atr: float
    body_atr: float
    move_ticks: float
    eff: float
    weak_range: bool
    weak_body: bool
    weak_eff: bool
    weak_any: bool


def _f(x: Any, d: float = 0.0) -> float:
    try:
        v = float(x)
        return v if math.isfinite(v) else d
    except Exception:
        return d


def _pick_tick_size_px(bar: Any, cfg: Dict[str, Any]) -> float:
    """
    tick_size_px is needed for delta efficiency eff.
    Priority:
      1) cfg["tick_size_px"]
      2) bar.fp_bucket_px (если footprint включен)
      3) cfg["tick_size_bp"] * price
      4) fallback small eps
    """
    px = _f(getattr(bar, "close", 0.0), 0.0)
    v = _f(cfg.get("tick_size_px", 0.0), 0.0)
    if v > 0:
        return v
    v = _f(getattr(bar, "fp_bucket_px", 0.0), 0.0)
    if v > 0:
        return v
    bp = _f(cfg.get("tick_size_bp", 0.0), 0.0)
    if bp > 0 and px > 0:
        return max(1e-9, px * (bp / 10000.0))
    return 1e-9


def compute_weak_progress(bar: Any, atr: Optional[float], cfg: Dict[str, Any]) -> WeakProgressSnapshot:
    """
    Compute weak progress metrics on bar_close.
    Fail-open: if ATR missing/invalid => weak flags only via eff if possible.
    """
    o = _f(getattr(bar, "open", 0.0), 0.0)
    h = _f(getattr(bar, "high", o), o)
    l = _f(getattr(bar, "low", o), o)
    c = _f(getattr(bar, "close", o), o)
    dsum = _f(getattr(bar, "delta_sum", 0.0), 0.0)

    atr_v = _f(atr, 0.0)
    has_atr = atr_v > 0
    rng = max(0.0, h - l)
    body = abs(c - o)

    range_atr = (rng / atr_v) if has_atr else 0.0
    body_atr = (body / atr_v) if has_atr else 0.0

    # thresholds (start defaults)
    # thresholds (start defaults)
    # Prefer new explicit keys if available, fallback to legacy _max if present in raw dict, else defaults
    th_range = _f(cfg.get("weak_progress_range_atr", cfg.get("weak_progress_range_atr_max", 0.35)), 0.35)
    th_body  = _f(cfg.get("weak_progress_body_atr", cfg.get("weak_progress_body_atr_max", 0.25)), 0.25)

    weak_range = bool(has_atr and range_atr <= th_range)
    weak_body = bool(has_atr and body_atr <= th_body)

    # delta efficiency
    tick_px = _pick_tick_size_px(bar, cfg)
    move_ticks = (body / tick_px) if tick_px > 0 else 0.0
    eff = (move_ticks / max(1e-9, abs(dsum))) if abs(dsum) > 0 else 0.0

    th_eff = _f(cfg.get("weak_progress_eff_max", 0.02), 0.02)
    min_abs_delta = _f(cfg.get("weak_progress_min_abs_delta", 0.0), 0.0)
    weak_eff = bool(abs(dsum) > min_abs_delta and eff <= th_eff)

    weak_any = bool(weak_range or weak_body or weak_eff)
    return WeakProgressSnapshot(
        atr=float(atr_v),
        range_atr=float(range_atr),
        body_atr=float(body_atr),
        move_ticks=float(move_ticks),
        eff=float(eff),
        weak_range=weak_range,
        weak_body=weak_body,
        weak_eff=weak_eff,
        weak_any=weak_any,
    )
