from __future__ import annotations

from dataclasses import dataclass


@dataclass
class RegimeFeatures:
    # raw metrics
    vwap_dev_bps: float | None = None
    daily_open_dev_bps: float | None = None
    daily_open_cross_freq: float | None = None
    htf_level_dist_bps: float | None = None

    # biases [-1..+1] (or None if not available)
    atr_bias: float | None = None
    delta_dir_bias: float | None = None
    vwap_dev_bias: float | None = None
    daily_open_dev_bias: float | None = None
    daily_open_cross_bias: float | None = None
    htf_prox_bias: float | None = None
    weak_progress_bias: float | None = None
    session_bias: float | None = None


@dataclass
class RegimeSample:
    ts: float
    price: float
    vwap_side: int
    daily_open_side: int
    bar_index: int | None = None
