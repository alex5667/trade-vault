# crypto_orderflow_handler_types.py
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto
from typing import List, Optional


class ZoneType(str, Enum):
    PDH = "PDH"
    PDL = "PDL"
    PDM = "PDM"
    WH = "WH"
    WL = "WL"
    SESSION_OPEN = "SESSION_OPEN"
    HTF_OB = "HTF_OB"  # FVG / order-block зона и т.п.


@dataclass
class HTFLevel:
    price: float
    zone_type: ZoneType
    strength: float  # [0..1]


@dataclass
class GeoZoneHit:
    nearest_zone: Optional[ZoneType]
    distance_bps: float
    in_zone: bool
    zone_strength: float


@dataclass
class LiquidityContext:
    best_bid_notional: float
    best_ask_notional: float
    dense_cluster_share: float
    liquidity_context_score: float


@dataclass
class BarSample:
    ts: float
    open: float
    high: float
    low: float
    close: float
    volume_usd: float


@dataclass
class L2Level:
    price: float
    size: float
    notional: float


@dataclass
class L2Snapshot:
    bids: List[L2Level]
    asks: List[L2Level]


@dataclass
class SimpleL2Snapshot:
    """Simplified L2 Snapshot used in logic."""
    bids: List[L2Level]
    asks: List[L2Level]


@dataclass(frozen=True)
class ClusterVol:
    price: float
    vol_buy: float
    vol_sell: float


@dataclass(frozen=True)
class RegimeFeatures:
    """
    Raw metrics (bps / freq) are optional and used for audit/debug.
    Biases are expected in [-1..+1] (or close), optional when data is missing.
    """
    # raw metrics
    vwap_dev_bps: Optional[float] = None
    daily_open_dev_bps: Optional[float] = None
    daily_open_cross_freq: Optional[float] = None
    htf_level_dist_bps: Optional[float] = None

    # biases
    atr_bias: Optional[float] = None
    delta_dir_bias: Optional[float] = None
    vwap_dev_bias: Optional[float] = None
    daily_open_dev_bias: Optional[float] = None
    daily_open_cross_bias: Optional[float] = None
    htf_prox_bias: Optional[float] = None
    weak_progress_bias: Optional[float] = None
    session_bias: Optional[float] = None


@dataclass(frozen=True)
class RegimeSample:
    ts: float
    price: float
    vwap_side: int
    daily_open_side: int
    vol_total: float
    notional: float
    bar_index: Optional[int] = None
