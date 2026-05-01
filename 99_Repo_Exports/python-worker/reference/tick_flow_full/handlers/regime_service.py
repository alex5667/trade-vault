# regime_service.py
from __future__ import annotations
"""
Market regime service for orderflow handler.
"""


from typing import Optional, Dict, Any, List
from dataclasses import dataclass, field
import time
import math


@dataclass
class RegimeConfig:
    """Configuration for market regime detection."""
    # window sizes
    window_bars: int = 20

    # score thresholds for label
    score_hi: float = 0.35      # >= -> trend
    score_lo: float = -0.35     # <= -> range

    # feature normalization scales
    atr_q_hi: float = 0.70      # high ATR quantile => supports trend
    atr_q_lo: float = 0.35      # low ATR quantile => supports range
    
    # ADX quantile thresholds (trend strength)
    adx_q_hi: float = 0.75      # high ADX => trending
    adx_q_lo: float = 0.40      # low ADX => chop/range
    
    ping_scale: float = 0.20    # vwap_cross_rate normalization
    delta_scale: float = 1.0    # if your delta_ema is already normalized, keep 1.0

    # weights (sum can be >1, we clamp)
    w_atr: float = 0.35
    w_adx: float = 0.20         # ADX weight for trend strength
    w_delta: float = 0.25
    w_hold: float = 0.25
    w_ping: float = 0.15
    
    # trend direction decision
    trend_dir_hold_min: float = 0.10  # min |hold_side_score| to use for direction


@dataclass
class RegimeState:
    """Current state of market regime."""
    regime: str = "unknown"
    confidence: float = 0.0
    last_update: float = 0.0
    score: float = 0.0   # [-1..+1]


@dataclass
class RegimeFeatures:
    """Features used for regime classification."""
    # 0..1: ATR quantile proxy (or other vol-quantile)
    atr_q: float = 0.5
    # 0..1: ADX quantile proxy (trend strength). Fail-open default 0.5.
    # Filled from Redis adx:{symbol} + regime:q:{symbol}:{tf} percentiles.
    adx_q: float = 0.5
    # signed delta flow EMA (normalized if possible)
    delta_ema: float = 0.0
    # [-1..+1] persistence: EMA(sign(price-vwap))
    hold_side_score: float = 0.0
    # 0..1 frequency of VWAP crossings ("ping-pong")
    vwap_cross_rate: float = 0.0

    # optional extras
    vwap: float = 0.0
    open_day: float = 0.0
    volume_profile: Dict[str, float] = field(default_factory=dict)


@dataclass
class RegimeUpdatePayload:
    """Payload for regime updates."""
    symbol: str
    regime: str
    confidence: float
    features: Dict[str, Any]
    timestamp: float


class MarketRegimeService:
    """Service for market regime detection and management."""

    def __init__(self, config: RegimeConfig = None):
        self.config = config or RegimeConfig()
        self.state = RegimeState()
        self.features = RegimeFeatures()

    @staticmethod
    def _clamp(x: float, lo: float, hi: float) -> float:
        return lo if x < lo else hi if x > hi else x

    def _score_from_features(self, f: RegimeFeatures) -> float:
        """
        score in [-1..+1]:
          +1 => trend day bias
          -1 => range / mean-reversion bias
        """
        cfg = self.config

        # ATR quantile -> s_atr in [-1..+1]
        if f.atr_q >= cfg.atr_q_hi:
            s_atr = +1.0
        elif f.atr_q <= cfg.atr_q_lo:
            s_atr = -1.0
        else:
            s_atr = self._clamp((f.atr_q - 0.5) / 0.20, -1.0, +1.0)

        # ADX quantile -> s_adx in [-1..+1]
        # High ADX => trending; low ADX => chop/range.
        try:
            adx_q = float(getattr(f, "adx_q", 0.5) or 0.5)
        except Exception:
            adx_q = 0.5
        if adx_q >= getattr(cfg, "adx_q_hi", 0.75):
            s_adx = +1.0
        elif adx_q <= getattr(cfg, "adx_q_lo", 0.40):
            s_adx = -1.0
        else:
            # scale around 0.5 similarly to ATR
            s_adx = self._clamp((adx_q - 0.5) / 0.20, -1.0, +1.0)

        # delta flow: tanh for robustness
        d = float(f.delta_ema) / float(cfg.delta_scale if cfg.delta_scale > 0 else 1.0)
        s_delta = math.tanh(d)

        # persistence vs VWAP already in [-1..+1]
        s_hold = self._clamp(float(f.hold_side_score), -1.0, +1.0)

        # ping-pong crossings penalize trend, push to range
        s_ping = -self._clamp(float(f.vwap_cross_rate) / float(cfg.ping_scale), 0.0, 1.0)

        w_adx = float(getattr(cfg, "w_adx", 0.20))
        score = (
            cfg.w_atr * s_atr
            + w_adx * s_adx
            + cfg.w_delta * s_delta
            + cfg.w_hold * s_hold
            + cfg.w_ping * s_ping
        )
        return self._clamp(float(score), -1.0, +1.0)

    def update_regime(self, features: RegimeFeatures) -> str:
        """Update market regime based on features (single source of truth)."""
        self.features = features

        score = self._score_from_features(features)
        cfg = self.config

        if score >= cfg.score_hi:
            # Directional label for downstream (SMT/OF policy):
            # prefer hold_side_score sign (price vs vwap persistence),
            # fallback to delta_ema sign if hold is too small.
            hs = float(getattr(features, "hold_side_score", 0.0) or 0.0)
            de = float(getattr(features, "delta_ema", 0.0) or 0.0)
            trend_dir_min = float(getattr(cfg, "trend_dir_hold_min", 0.10))
            if abs(hs) >= trend_dir_min:
                regime = "trending_bull" if hs >= 0 else "trending_bear"
            else:
                regime = "trending_bull" if de >= 0 else "trending_bear"
        elif score <= cfg.score_lo:
            regime = "range"
        else:
            regime = "mixed"

        self.state.regime = regime
        self.state.score = float(score)
        # confidence: |score| (можно усложнить позже)
        self.state.confidence = self._clamp(abs(score), 0.0, 1.0)
        self.state.last_update = time.time()
        return regime

    def get_current_regime(self) -> RegimeState:
        """Get current regime state."""
        return self.state

    def get_regime_features(self) -> RegimeFeatures:
        """Get current regime features."""
        return self.features


def regime_label_to_enum(label: str) -> str:
    """Convert regime label to enum value."""
    s = (label or "").strip().lower()
    if s in ("trend", "trending"):
        return "TREND"
    if s in ("range", "ranging", "mean_reversion", "mr"):
        return "RANGE"
    if s in ("mixed", "unknown", ""):
        return "MIXED" if s == "mixed" else "UNKNOWN"
    return s.upper()
