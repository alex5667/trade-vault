from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass
class RegimeFeatures:
    # raw metrics
    vwap_dev_bps: Optional[float] = None
    daily_open_dev_bps: Optional[float] = None
    daily_open_cross_freq: Optional[float] = None
    htf_level_dist_bps: Optional[float] = None

    # biases [-1..+1] (or None if not available)
    atr_bias: Optional[float] = None
    delta_dir_bias: Optional[float] = None
    vwap_dev_bias: Optional[float] = None
    daily_open_dev_bias: Optional[float] = None
    daily_open_cross_bias: Optional[float] = None
    htf_prox_bias: Optional[float] = None
    weak_progress_bias: Optional[float] = None
    session_bias: Optional[float] = None


@dataclass
class RegimeSample:
    ts: float
    price: float
    vwap_side: int
    daily_open_side: int
    bar_index: Optional[int] = None
