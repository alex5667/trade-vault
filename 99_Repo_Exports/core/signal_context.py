"""
Unified Signal Context for the entire trading pipeline.

This module provides a single SignalContext class used across all components:
- OrderFlow Handlers
- Signal Scoring Engine
- Execution Planning
- Performance Tracking
"""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, Any, Literal, Optional
from datetime import datetime

Side = Literal["long", "short", "flat"]


@dataclass
class SignalContext:
    """
    Unified Signal Context for the entire trading pipeline.

    Contains all data needed for signal generation, scoring, and execution.
    """

    # Core identification
    symbol: str
    side: Side
    ts_ms: int

    # Human-readable timestamp
    ts: Optional[datetime] = None

    # Regime and session
    regime_label: Optional[str] = None  # "trend_up", "range", "chop", ...
    trend_score: float = 0.0
    range_score: float = 0.0
    session: Optional[str] = None  # "asia", "eu", "us"

    # Raw metrics (before calibration)
    metrics: Dict[str, float] = field(default_factory=dict)
    # Examples:
    #   "deltaSpike_z": 2.5,
    #   "obi": 0.75,
    #   "absorption_score": 1.2,
    #   "liquidity_score": 0.8,
    #   "weak_progress": 0.3,
    #   ...

    # Local calibration results
    calibrated: Dict[str, Any] = field(default_factory=dict)
    # Examples:
    #   "deltaSpike_z": {
    #       "value": 2.5,
    #       "is_extreme": True,
    #       "threshold": 2.1,
    #       "quantile": 0.95,
    #       "p50": 0.5, "p75": 1.1, "p90": 1.8
    #   },
    #   ...

    # Geometry (HTF levels analysis)
    geometry_score: float = 0.0
    geo_zone_hits: list[Any] = field(default_factory=list)
    is_new_local_extreme: bool = False

    # Liquidity analysis
    liquidity_score: float = 0.0
    liquidity_ctx: Dict[str, Any] = field(default_factory=dict)

    # Signal quality and scoring
    confidence: float = 0.0
    confidence_breakdown: Dict[str, float] = field(default_factory=dict)
    min_confidence_used: Optional[int] = None
    final_score: Optional[float] = None

    # Pattern analysis
    pattern_name: Optional[str] = None
    pattern_family: Optional[str] = None  # 'continuation' / 'fade' / 'other'
    is_golden_pattern: bool = False
    golden_pattern_label: Optional[str] = None

    # Quality assessment
    quality_offline: Optional[float] = None
    quality_online: Optional[float] = None
    quality_combined: Optional[float] = None
    quality_status: Optional[str] = None
    is_disabled_by_quality: bool = False

    # Additional tags and metadata
    tags: Dict[str, Any] = field(default_factory=dict)

    # Legacy compatibility fields (may be removed in future)
    delta_spike_z: Optional[float] = None
    obi: Optional[float] = None
    weak_progress: Optional[float] = None
    atr_quantile: Optional[float] = None
    delta_spike_z_local_q: Optional[float] = None
    obi_local_q: Optional[float] = None
    weak_progress_local_q: Optional[float] = None
    atr_local_q: Optional[float] = None
    reverse_delta_spike_z: Optional[float] = None
    volume_z: Optional[float] = None
    progress_score_component: Optional[int] = None
