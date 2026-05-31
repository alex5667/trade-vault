# regime_service.py
from __future__ import annotations

"""
Market regime service for orderflow handler.
"""


import math
from dataclasses import dataclass, field
from typing import Any

from common.regime_contract import RegimeSnapshot, RegimeSwitchPolicy, should_switch, RegimeLabel


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
    
    switch_policy: RegimeSwitchPolicy = field(default_factory=RegimeSwitchPolicy)


@dataclass
class RegimeState:
    """Current state of market regime."""
    regime: str = "unknown"
    confidence: float = 0.0
    # epoch_ms of the last call to update_regime() — deterministic for replay.
    last_update_ms: int = 0
    score: float = 0.0   # [-1..+1]
    snapshot: RegimeSnapshot | None = None


@dataclass
class RegimeFeatures:
    """Features used for regime classification."""
    # 0..1: ATR quantile proxy (or other vol-quantile)
    atr_q: float = 0.5
    # 0..1: ADX quantile proxy (trend strength). Fail-open default 0.5.
    # Filled from Redis adx:{symbol}:{tf} + percentiles.
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
    volume_profile: dict[str, float] = field(default_factory=dict)


@dataclass
class RegimeUpdatePayload:
    """Payload for regime updates."""
    symbol: str
    regime: str
    confidence: float
    features: dict[str, Any]
    timestamp: float


class MarketRegimeService:
    """Service for market regime detection and management."""

    def __init__(self, config: RegimeConfig = None):  # type: ignore
        self.config = config or RegimeConfig()
        self.state = RegimeState()
        self.features = RegimeFeatures()
        self.switch_policy = self.config.switch_policy
        self._candidate_label = "unknown"
        self._candidate_count = 0
        self._last_switch_ms = 0

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
        try:
            adx_q = float(getattr(f, "adx_q", 0.5) or 0.5)
        except Exception:
            adx_q = 0.5
        if adx_q >= getattr(cfg, "adx_q_hi", 0.75):
            s_adx = +1.0
        elif adx_q <= getattr(cfg, "adx_q_lo", 0.40):
            s_adx = -1.0
        else:
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

    def update_regime(
        self,
        features: RegimeFeatures,
        *,
        symbol: str = "UNKNOWN",
        ts_event_ms: int = 0,
        ts_calc_ms: int | None = None,
    ) -> str:
        """Update market regime based on features.

        Args:
            features:      Input features for regime scoring.
            symbol:        Instrument symbol (UPPER). Must NOT be empty in prod.
            ts_event_ms:   Exchange/event timestamp in epoch ms (canonical time).
            ts_calc_ms:    Calculation timestamp in epoch ms.  Defaults to
                           ts_event_ms when not provided (deterministic replay).

        Returns:
            Current regime label string.
        """
        self.features = features
        self.last_transition = None

        # Deterministic time: use event ts, not wall clock, for decision logic.
        now_ms: int = int(ts_calc_ms) if ts_calc_ms is not None else int(ts_event_ms)

        score = self._score_from_features(features)
        cfg = self.config

        if score >= cfg.score_hi:
            hs = float(getattr(features, "hold_side_score", 0.0) or 0.0)
            de = float(getattr(features, "delta_ema", 0.0) or 0.0)
            trend_dir_min = float(getattr(cfg, "trend_dir_hold_min", 0.10))
            if abs(hs) >= trend_dir_min:
                raw_label = "trending_bull" if hs >= 0 else "trending_bear"
            else:
                raw_label = "trending_bull" if de >= 0 else "trending_bear"
        elif score <= cfg.score_lo:
            raw_label = "range"
        else:
            raw_label = "mixed"

        # Hysteresis Logic
        if raw_label == self._candidate_label:
            self._candidate_count += 1
        else:
            self._candidate_label = raw_label
            self._candidate_count = 1

        allow, reason = should_switch(
            prev_label=self.state.regime,
            next_label=raw_label,
            score=score,
            confirm_count=self._candidate_count,
            now_ms=now_ms,
            last_switch_ms=self._last_switch_ms,
            policy=self.switch_policy,
        )

        if allow:
            old_regime = self.state.regime
            self.state.regime = raw_label
            self._last_switch_ms = now_ms
            self._candidate_count = 0
            
            if old_regime != "unknown" and old_regime != raw_label:
                self.last_transition = {
                    "symbol": symbol.upper(),
                    "old_regime": old_regime,
                    "new_regime": raw_label,
                    "reason": reason,
                    "score": float(score),
                    "ts_ms": now_ms
                }

        self.state.score = float(score)
        self.state.confidence = self._clamp(abs(score), 0.0, 1.0)
        # epoch_ms — deterministic, no wall-clock dependency in decision path.
        self.state.last_update_ms = now_ms

        # Populate snapshot — symbol is always the real instrument, never "unknown".
        direction = 0
        if "bull" in self.state.regime:
            direction = 1
        elif "bear" in self.state.regime:
            direction = -1

        try:
            regime_enum = RegimeLabel(self.state.regime)
        except ValueError:
            regime_enum = RegimeLabel.UNKNOWN

        self.state.snapshot = RegimeSnapshot(
            symbol=symbol.upper(),
            label=regime_enum,
            direction=direction,
            score=self.state.score,
            confidence=self.state.confidence,
            features={
                "score": score,
                "atr_q": features.atr_q,
                "adx_q": features.adx_q,
                "delta_ema": features.delta_ema,
                "hold_side_score": features.hold_side_score,
                "vwap_cross_rate": features.vwap_cross_rate,
            },
            ts_calc_ms=now_ms,
            ts_event_ms=int(ts_event_ms),
            source="market_regime_service",
        )
        return self.state.regime

    def get_current_regime(self) -> RegimeState:
        """Get current regime state."""
        return self.state

    def get_regime_features(self) -> RegimeFeatures:
        """Get current regime features."""
        return self.features


def regime_label_to_enum(label: str) -> str:
    """Convert regime label to enum value."""
    from common.market_mode import is_range_regime, is_trend_regime
    s = (label or "").strip().lower()
    if is_trend_regime(s):
        return "TREND"
    if is_range_regime(s):
        return "RANGE"
    if s == "mixed":
        return "MIXED"
    if s in ("unknown", ""):
        return "UNKNOWN"
    return s.upper()
