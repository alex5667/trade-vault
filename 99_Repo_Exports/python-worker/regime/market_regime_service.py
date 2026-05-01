# regime/market_regime_service.py

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Optional, Dict, Any, Tuple, List
from collections import deque
from collections import defaultdict
import time

from common.log import setup_logger


class RegimeType(Enum):
    RANGE = "range"
    TREND_UP = "trend_up"
    TREND_DOWN = "trend_down"
    SQUEEZE = "squeeze"
    EXPANSION = "expansion"


@dataclass
class RegimeSnapshot:
    symbol: str
    ts_event_ms: int
    regime: RegimeType
    atr_value: float
    atr_quantile: float
    volatility_state: str  # например, "low/normal/high"
    is_trending: bool
    # любые доп. поля, которыми вы пользуетесь в guards / scoring
    trend_score: float = 0.0
    range_score: float = 0.0


@dataclass
class BarSample:
    """Bar sample for regime calculation"""
    symbol: str
    ts_event_ms: int
    open: float
    high: float
    low: float
    close: Optional[float]
    volume: float


@dataclass
class RegimeSample:
    ts: float
    price: float
    vwap_side: int        # -1 / 0 / +1 (ниже / на / выше VWAP)
    daily_open_side: int  # -1 / 0 / + 1 (ниже / на / выше open)
    bar_index: int | None = None


@dataclass
class RegimeFeatures:
    # raw метрики (по желанию для логов)
    vwap_dev_bps: float | None = None
    daily_open_dev_bps: float | None = None
    daily_open_cross_freq: float | None = None
    htf_level_dist_bps: float | None = None

    # bias в диапазоне [-1; +1]
    atr_bias: float | None = None
    delta_dir_bias: float | None = None
    vwap_dev_bias: float | None = None
    daily_open_dev_bias: float | None = None
    daily_open_cross_bias: float | None = None
    htf_prox_bias: float | None = None
    weak_progress_bias: float | None = None
    session_bias: float | None = None


@dataclass
class RegimeState:
    label: str
    trend_score: float
    range_score: float
    session_bias: float = 0.0
    daily_open_cross_freq: float = 0.0
    ts: float = 0.0
    symbol=""


@dataclass
class RegimeConfig:
    # ATR thresholds for regime classification
    atr_quantile_trend_thr: float = 0.7
    atr_quantile_range_thr: float = 0.3

    # Weak progress thresholds
    weak_progress_trend_min: float = 0.3
    weak_progress_range_max: float = 0.7

    # Daily open metrics
    daily_open_range_bps_min_for_trend: float = 50.0
    daily_open_range_bps_max_for_range: float = 25.0
    daily_open_cross_freq_trend_max: float = 0.3
    daily_open_cross_freq_range_min: float = 0.7

    # Weights for regime scoring
    atr_weight: float = 1.0
    delta_weight: float = 0.8
    vwap_dev_weight: float = 0.6
    daily_open_dev_weight: float = 0.7
    daily_open_cross_weight: float = 0.5
    htf_level_weight: float = 0.4
    weak_progress_weight: float = 0.9
    session_weight: float = 0.3

    # Regime score thresholds
    regime_trend_threshold: float = 0.6
    regime_range_threshold: float = -0.6

    # Window sizes
    regime_window_size: int = 60

    # Session biases (example values)
    session_bias_default: Dict[str, float] = None

    def __post_init__(self):
        if self.session_bias_default is None:
            self.session_bias_default = {
                "asia": 0.1,
                "europe": 0.0,
                "us": -0.1,
            }

    @classmethod
    def from_env(cls) -> "RegimeConfig":
        """Create config from environment variables"""
        import os
        return cls(
            atr_quantile_trend_thr=float(os.getenv("REGIME_ATR_QUANTILE_TREND_THR", "0.7")),
            atr_quantile_range_thr=float(os.getenv("REGIME_ATR_QUANTILE_RANGE_THR", "0.3")),
        )


class MarketRegimeService:
    """
    Service responsible for market regime detection and classification.

    Handles ATR calculation, volatility quantiles, regime classification (RANGE/TREND/SQUEEZE/EXPANSION),
    and regime guards for signal emission.
    """

    def __init__(self, atr_window: int = 14, regime_config: Optional[RegimeConfig] = None):
        self._atr_window = atr_window
        self._cfg = regime_config or RegimeConfig.from_env()
        self._state_by_symbol: Dict[str, RegimeSnapshot] = {}

        # ATR and bar history
        self._atr_history: Dict[str, deque[float]] = defaultdict(lambda: deque(maxlen=atr_window))
        self._bar_history: Dict[str, deque[BarSample]] = defaultdict(lambda: deque(maxlen=240))  # ~4 hours at 1m

        # Legacy regime state for compatibility
        self._last_state: Dict[str, RegimeState] = {}

        # Regime history for cross-bias calculations
        self._regime_history: Dict[str, deque[RegimeSample]] = defaultdict(
            lambda: deque(maxlen=self._cfg.regime_window_size)
        )

        self.logger = setup_logger("MarketRegimeService")

    def on_bar(self, bar: BarSample) -> None:
        """
        Update internal regime state by new bar:
        - update ATR
        - calculate quantile/volatility regime
        - determine RegimeType
        - save RegimeSnapshot
        """
        symbol = bar.symbol

        # Update ATR history
        self._update_atr_history(symbol, bar)

        # Store bar for regime calculation
        self._bar_history[symbol].append(bar)

        # Calculate current ATR
        atr_value = self._calculate_atr(symbol)
        if atr_value <= 0:
            return  # Not enough data

        # Calculate ATR quantile
        atr_quantile = self._calculate_atr_quantile(symbol, atr_value)

        # Determine volatility state
        volatility_state = self._determine_volatility_state(atr_quantile)

        # Classify regime
        regime, trend_score, range_score = self._classify_regime(symbol, atr_quantile)

        # Determine if trending
        is_trending = regime in [RegimeType.TREND_UP, RegimeType.TREND_DOWN]

        # Create snapshot
        snapshot = RegimeSnapshot(
            symbol=symbol,
            ts_event_ms=bar.ts_event_ms,
            regime=regime,
            atr_value=atr_value,
            atr_quantile=atr_quantile,
            volatility_state=volatility_state,
            is_trending=is_trending,
            trend_score=trend_score,
            range_score=range_score,
        )

        self._state_by_symbol[symbol] = snapshot

    def _update_atr_history(self, symbol: str, bar: BarSample) -> None:
        """Update ATR calculation for symbol"""
        if len(self._bar_history[symbol]) < 2:
            return

        # Calculate true range (max of |high-low|, |high-prev_close|, |low-prev_close|)
        prev_bar = self._bar_history[symbol][-1]
        if prev_bar.close is not None:
            tr1 = bar.high - bar.low
            tr2 = abs(bar.high - prev_bar.close)
            tr3 = abs(bar.low - prev_bar.close)
            atr_value = max(tr1, tr2, tr3)
        else:
            # Fallback to high-low range
            atr_value = bar.high - bar.low

        self._atr_history[symbol].append(atr_value)

    def _calculate_atr(self, symbol: str) -> float:
        """Calculate current ATR value"""
        history = self._atr_history[symbol]
        if not history:
            return 0.0
        return sum(history) / len(history)

    def _calculate_atr_quantile(self, symbol: str, current_atr: float) -> float:
        """Calculate ATR quantile relative to historical values"""
        history = list(self._atr_history[symbol])
        if not history:
            return 0.5

        # Sort for quantile calculation
        sorted_history = sorted(history)
        n = len(sorted_history)

        # Find position
        for i, val in enumerate(sorted_history):
            if current_atr <= val:
                return i / max(n - 1, 1)

        return 1.0

    def _determine_volatility_state(self, atr_quantile: float) -> str:
        """Determine volatility state based on ATR quantile"""
        if atr_quantile < self._cfg.atr_quantile_range_thr:
            return "low"
        elif atr_quantile > self._cfg.atr_quantile_trend_thr:
            return "high"
        else:
            return "normal"

    def _classify_regime(self, symbol: str, atr_quantile: float) -> Tuple[RegimeType, float, float]:
        """
        Classify market regime based on ATR and other factors.
        Returns (regime, trend_score, range_score)
        """
        bars = list(self._bar_history[symbol])
        if len(bars) < 10:
            return RegimeType.RANGE, 0.0, 0.0

        # First check ATR-based classification
        if atr_quantile > self._cfg.atr_quantile_trend_thr:
            # High volatility - expansion
            return RegimeType.EXPANSION, 0.8, 0.2
        elif atr_quantile < self._cfg.atr_quantile_range_thr:
            # Low volatility - squeeze
            return RegimeType.SQUEEZE, 0.2, 0.8

        # Medium volatility - analyze price movement direction
        return self._analyze_trend_vs_range(bars)

    def _analyze_trend_vs_range(self, bars: List[BarSample]) -> Tuple[RegimeType, float, float]:
        """Analyze recent bars to determine trend vs range"""
        if len(bars) < 10:
            return RegimeType.RANGE, 0.5, 0.5

        recent_bars = bars[-10:]

        # Calculate price movement
        if len(recent_bars) >= 2:
            first_price = recent_bars[0].close
            last_price = recent_bars[-1].close

            if first_price is not None and last_price is not None:
                price_change_pct = (last_price - first_price) / first_price

                if abs(price_change_pct) > 0.02:  # 2% threshold for trend
                    direction = RegimeType.TREND_UP if price_change_pct > 0 else RegimeType.TREND_DOWN
                    return direction, 0.8, 0.2

        return RegimeType.RANGE, 0.4, 0.6

    def get_regime(self, symbol: str) -> Optional[RegimeSnapshot]:
        """Get current regime snapshot for symbol"""
        return self._state_by_symbol.get(symbol)

    def allow_emit(self, symbol: str, ts_event_ms: int, ctx: Any) -> bool:
        """
        Regime guard: can we emit signal in current regime.
        Here goes logic like:
        - don't trade in SQUEEZE
        - don't short in TREND_UP if score < threshold
        - etc.
        """
        snap = self.get_regime(symbol)
        if not snap:
            return True  # or strict guard -> False

        # Example regime guards
        if snap.regime == RegimeType.SQUEEZE:
            return False  # Don't trade in squeeze

        if snap.regime == RegimeType.EXPANSION and snap.atr_quantile > 0.9:
            return False  # Too volatile

        # Add more sophisticated guards based on context
        # For now, allow all other regimes
        return True

    # Legacy methods for compatibility with existing BaseOrderFlowHandler

    def last_state(self, symbol: str) -> Optional[RegimeState]:
        """Legacy method for compatibility"""
        return self._last_state.get(symbol)

    def _update_regime_history(self, ctx: Any, bar_index: int | None = None) -> None:
        """Update regime history for cross-bias calculations"""
        if not hasattr(ctx, 'symbol') or not hasattr(ctx, 'last_price'):
            return

        symbol = ctx.symbol
        now = getattr(ctx, 'ts_utc', None) or time.time()

        # VWAP side
        vwap_side = 0
        if hasattr(ctx, 'vwap') and ctx.vwap is not None and ctx.last_price is not None:
            diff_v = ctx.last_price - ctx.vwap
            if diff_v > 0.0:
                vwap_side = 1
            elif diff_v < 0.0:
                vwap_side = -1

        # Daily open side
        daily_open_side = 0
        if hasattr(ctx, 'daily_open') and ctx.daily_open is not None and ctx.last_price is not None:
            diff_o = ctx.last_price - ctx.daily_open
            if diff_o > 0.0:
                daily_open_side = 1
            elif diff_o < 0.0:
                daily_open_side = -1

        hist = self._regime_history[symbol]
        hist.append(
            RegimeSample(
                ts=now,
                price=ctx.last_price,
                vwap_side=vwap_side,
                daily_open_side=daily_open_side,
                bar_index=bar_index,
            )
        )

    def _compute_cross_bias(self, symbol: str) -> float | None:
        """Compute cross bias from regime history"""
        hist = self._regime_history.get(symbol)
        if not hist or len(hist) < 3:
            return None

        vwap_crosses = 0
        open_crosses = 0
        pairs = 0

        prev = hist[0]
        for cur in list(hist)[1:]:
            # VWAP crosses
            if prev.vwap_side != 0 and cur.vwap_side != 0 and prev.vwap_side != cur.vwap_side:
                vwap_crosses += 1
            # Daily open crosses
            if prev.daily_open_side != 0 and cur.daily_open_side != 0 and prev.daily_open_side != cur.daily_open_side:
                open_crosses += 1

            pairs += 1
            prev = cur

        if pairs == 0:
            return None

        cross_rate_vwap = vwap_crosses / pairs
        cross_rate_open = open_crosses / pairs
        cross_rate = 0.5 * (cross_rate_vwap + cross_rate_open)

        # cross_rate ≈ 0 → rarely cross → trend → bias ~ +1
        # cross_rate ≈ 1 → constantly cross → range → bias ~ -1
        bias = 1.0 - 2.0 * max(0.0, min(1.0, cross_rate))  # [0..1] → [+1..-1]
        return bias

    def _compute_regime_features(self, ctx: Any) -> RegimeFeatures:
        """Compute regime features from context"""

        return RegimeFeatures(
            # Raw metrics
            vwap_dev_bps=getattr(ctx, 'vwap_dev_bps', None),
            daily_open_dev_bps=getattr(ctx, 'daily_open_dev_bps', None),
            daily_open_cross_freq=getattr(ctx, 'daily_open_cross_freq', None),
            htf_level_dist_bps=getattr(ctx, 'htf_level_dist_bps', None),

            # Bias metrics [-1, +1]
            atr_bias=getattr(ctx, 'atr_bias', None),
            delta_dir_bias=getattr(ctx, 'delta_dir_bias', None),
            vwap_dev_bias=getattr(ctx, 'vwap_dev_bias', None),
            daily_open_dev_bias=getattr(ctx, 'daily_open_dev_bias', None),
            daily_open_cross_bias=self._compute_cross_bias(getattr(ctx, 'symbol', None)),
            htf_prox_bias=getattr(ctx, 'htf_prox_bias', None),
            weak_progress_bias=getattr(ctx, 'weak_progress_bias', None),
            session_bias=getattr(ctx, 'session_bias', None),
        )
