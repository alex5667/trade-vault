"""python-worker/contexts.py

Shared data structures and types for the trading pipeline.
Contains all Signal/Liquidity contexts, Enums, and Config classes.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Optional, List, Literal, Tuple
from enum import Enum, auto

# ---- Enums ----

class TradeSide(str, Enum):
    """Trading side enumeration."""
    BUY = "BUY"
    SELL = "SELL"
    LONG = "LONG"
    SHORT = "SHORT"

class LiquidityPattern(str, Enum):
    """Liquidity pattern types."""
    NONE = "none"
    BREAK = "break"
    ABSORPTION = "absorption"
    WALL_BUY = "wall_buy"
    WALL_SELL = "wall_sell"
    CLUSTER_BUY = "cluster_buy"
    CLUSTER_SELL = "cluster_sell"

class MarketRegime(str, Enum):
    """Market regime types."""
    TREND = "TREND"
    RANGE = "RANGE"
    MIXED = "MIXED"
    UNKNOWN = "UNKNOWN"

class ZoneType(str, Enum):
    """Geographic or Structural Zone Types."""
    PDH = "PDH"
    PDL = "PDL"
    PDM = "PDM"
    WH = "WH"
    WL = "WL"
    SESSION_OPEN = "SESSION_OPEN"
    HTF_OB = "HTF_OB"
    HTF_FVG = "HTF_FVG"

# ---- Simple Types ----

SignalKind = Literal["breakout", "sweep", "reclaim", "absorption"]

RegimeLabel = Literal[
    "squeeze",
    "squeeze_bullish",
    "squeeze_bearish",
    "range",
    "range_bullish",
    "range_bearish",
    "trending_bull",
    "trending_bear",
    "expansion_bull",
    "expansion_bear",
    "mixed",
    "unknown",
    "trending",
]

# ---- Dataclasses ----

@dataclass
class RegimeDecision:
    regime: RegimeLabel
    cross_bias: float

@dataclass
class GoldenThresholds:
    regime_min: float
    geometry_min: float
    liquidity_min: float


@dataclass(slots=True)
class Tick:
    """Minimal tick model."""
    ts: int                 # timestamp in ms
    bid: float
    ask: float
    last: float
    volume: float
    flags: int
    is_buyer_maker: Optional[bool] = None
    raw: Optional[dict[str, Any]] = None

@dataclass
class BarSample:
    """Minimal bar sample."""
    ts: float
    open: float = 0.0
    high: float = 0.0
    low: float = 0.0
    close: float = 0.0
    volume: float = 0.0
    volume_usd: float = 0.0
    delta: float = 0.0
    ts_open: int = 0
    ts_close: int = 0

@dataclass
class RegimeFeatures:
    """Features for market regime classification."""
    atr_intraday_bps: float = 0.0
    atr_quantile_1d: float = 0.0
    weak_progress: float = 0.0
    vwap_distance_bps: float = 0.0
    vwap_trend_bps: float = 0.0
    daily_open_range_bps: float = 0.0
    daily_open_cross_freq: float = 0.0
    atr_q: float = 0.5
    delta_ema: float = 0.0
    hold_side_score: float = 0.0
    vwap_cross_rate: float = 0.0
    vwap: float = 0.0
    open_day: float = 0.0

@dataclass
class RegimeState:
    """State of the market regime."""
    label: str = "mixed"
    trend_score: float = 0.0
    range_score: float = 0.0
    session_bias: float = 0.0
    daily_open_cross_freq: float = 0.0
    ts: float = 0.0
    ts_ms: int = 0
    symbol: str = ""
    score: float = 0.0

@dataclass
class RegimeConfig:
    """Configuration for regime classifier."""
    score_hi: float = 0.35
    score_lo: float = -0.35
    atr_q_hi: float = 0.70
    atr_q_lo: float = 0.35
    ping_scale: float = 0.20
    delta_scale: float = 1.0
    w_atr: float = 0.35
    w_delta: float = 0.30
    w_hold: float = 0.25
    w_ping: float = 0.20
    window_bars: int = 20
    trend_threshold: float = 0.7
    range_threshold: float = 0.3
    mixed_threshold: float = 0.5
    session_bias_default: Dict[str, float] = field(default_factory=dict)

@dataclass(slots=True)
class L2Level:
    """L2 Orderbook Level."""
    price: float
    size: float
    notional: float = 0.0

@dataclass(slots=True)
class SimpleL2Snapshot:
    """Simplified L2 Snapshot used in logic."""
    bids: list[L2Level]
    asks: list[L2Level]
    symbol: str = ""
    ts_ms: int = 0
    mid: float = 0.0
    best_bid: float = 0.0
    best_ask: float = 0.0
    depth_bid_5: float = 0.0
    depth_ask_5: float = 0.0
    depth_bid_20: float = 0.0
    depth_ask_20: float = 0.0

@dataclass(frozen=True, slots=True)
class ClusterVol:
    """Cluster volume around a price."""
    price: float
    vol_buy: float
    vol_sell: float
    buy_vol_by_price: dict[float, float] = field(default_factory=dict)
    sell_vol_by_price: dict[float, float] = field(default_factory=dict)

@dataclass
class LiquidityContext:
    """Liquidity analysis context."""
    # Aggregated metrics
    aggr_buy_at_wall: float = 0.0
    aggr_sell_at_wall: float = 0.0
    aggr_to_rest_ratio: float = 0.0
    pattern: LiquidityPattern = LiquidityPattern.NONE
    
    # Optional detailed metrics
    near_wall_side: Optional[Literal["bid", "ask"]] = None
    near_wall_price: Optional[float] = None
    near_wall_size: Optional[float] = None
    near_wall_size_z: Optional[float] = None
    
    depth_5_vol: Optional[float] = None
    depth_5_z: Optional[float] = None
    
    liquidity_context_score: Optional[float] = None
    
    # Reference to heavier objects if needed
    cluster: Optional[ClusterVol] = None

    # L2 snapshot compatibility variables if needed
    best_bid_notional: float = 0.0
    best_ask_notional: float = 0.0
    dense_cluster_share: float = 0.0


@dataclass
class GeoZoneHit:
    """Geometry zone hit details."""
    zone_type: ZoneType
    zone_price: float
    dist_bps: float
    atr_htf_bps: float
    dist_rel_atr: float
    strength: float
    nearest_zone: Optional[ZoneType] = None
    in_zone: bool = False
    zone_strength: float = 0.0

@dataclass
class BucketState:
    """
    Cumulative L2/Orderflow state for a bucket (time window).
    Acts as the single source of truth for L2 stats.
    """
    l2_ts: int = 0
    best_bid: float = 0.0
    best_ask: float = 0.0
    mid: float = 0.0
    spread: float = 0.0
    spread_bps_mean: float = 0.0
    spread_bps_z: float = 0.0
    
    # Prices / Timestamps
    price: float = 0.0
    ts: int = 0
    
    # ------------------------------------------------------------------
    # Bar range tracking (tick-driven, tf = BAR_RANGE_TF_MS / config.timeframe_s)
    # Current (forming) bar
    # ------------------------------------------------------------------
    bar_id: int = 0
    bar_ts_open: int = 0
    bar_ts_open_ms: int = 0
    bar_open: float = 0.0
    bar_high: float = 0.0
    bar_low: float = 0.0
    bar_close: float = 0.0
    bar_range: float = 0.0
    bar_range_bps: float = 0.0
    bar_range_bps_ema: float = 0.0
    bar_range_bps_ratio_to_ema: float = 0.0
    bar_range_z: float = 0.0
    bar_range_last_closed_z: float = 0.0
    
    # Bar Range Diagnostics
    prev_bar_open: float = 0.0
    prev_bar_high: float = 0.0
    prev_bar_low: float = 0.0
    prev_bar_close: float = 0.0
    prev_bar_range: float = 0.0
    prev_bar_range_bps: float = 0.0
    prev_bar_range_bps_z: float = 0.0
    
    bar_time_backwards_cnt: int = 0
    bar_time_backwards_flag: bool = False
    bar_time_backwards_ms: int = 0
    bar_gap_bars: int = 0
    bar_gap_flag: bool = False
    bar_late_tick_ignored: int = 0
    
    # OBI / Depth
    depth_bid_5: float = 0.0
    depth_ask_5: float = 0.0
    depth_bid_20: float = 0.0
    depth_ask_20: float = 0.0
    obi_valid: bool = False
    obi_20_valid: bool = False
    obi: float = 0.0
    obi_avg: float = 0.0
    obi_sustained: bool = False
    obi_20: float = 0.0
    obi_avg_20: float = 0.0
    obi_sustained_20: bool = False
    
    # Slopes / Microprice
    microprice: float = 0.0
    microprice_shift_bps_20: float = 0.0
    slope_bid_20: float = 0.0
    slope_ask_20: float = 0.0
    
    # Walls
    wall_bid: bool = False
    wall_ask: bool = False
    wall_bid_dist_bps: float = 0.0
    wall_ask_dist_bps: float = 0.0
    wall_bid_persist_ratio: float = 0.0
    wall_ask_persist_ratio: float = 0.0
    wall_bid_drop_ratio: float = 0.0
    wall_ask_drop_ratio: float = 0.0
    wall_bid_suspicious: bool = False
    wall_ask_suspicious: bool = False
    wall_bid_price: float = 0.0
    wall_bid_size: float = 0.0
    wall_ask_price: float = 0.0
    wall_ask_size: float = 0.0

    # ------------------------------------------------------------------
    # Pivots meta (optional; propagated into OrderflowSignalContext)
    # Single source of truth: BucketState
    # ------------------------------------------------------------------
    pivots_ts_ms: int = 0
    pivots_date: str = ""
    nearest_pivot_key: str = ""
    nearest_pivot_price: float = 0.0

    # Orderflow / Delta
    z_delta: float = 0.0
    current_delta: float = 0.0
    delta_bucket: float = 0.0
    
    # CVD (Cumulative Volume Delta)
    cvd_5m: float = 0.0
    cvd_divergence: float = 0.0
    taker_buy_qty_bucket: float = 0.0
    taker_sell_qty_bucket: float = 0.0
    taker_buy_rate_ema: float = 0.0
    taker_sell_rate_ema: float = 0.0
    
    # L2 Hygiene / Latency
    l2_age_ms: int = 0
    l2_age_ms_tick: int = 0
    l2_is_stale: bool = True
    l2_is_stale_now: bool = True
    l2_skew_tick_flag: bool = False
    
    # ATR / Volatility
    atr: float = 0.0
    atr_14_raw: float = 0.0
    atr_14_bps: float = 0.0
    atr_14_q: float = 0.0
    daily_atr_bps: Optional[float] = None
    
    # Regime
    regime_score: float = 0.0
    regime_label: str = "mixed"
    
    # L3 / Execution factors
    cancel_to_trade_bid: float = 0.0
    cancel_to_trade_ask: float = 0.0
    cancel_bid_rate_ema: float = 0.0
    cancel_ask_rate_ema: float = 0.0
    eta_fill_bid_sec: float = 0.0
    eta_fill_ask_sec: float = 0.0
    pull_ask_qty_proxy: float = 0.0
    pull_bid_qty_proxy: float = 0.0

    # P1: OFI / Churn
    ofi_val: float = 0.0
    ofi_z: float = 0.0
    book_churn_hz: float = 0.0
    book_churn_z: float = 0.0

    # P1: Event Recency (State)
    last_iceberg_ts: int = 0
    last_sweep_ts: int = 0
    last_reclaim_ts: int = 0
    last_microprice_shift_ts: int = 0
    last_obi_spike_ts: int = 0
    
    # Touch Level
    touch_bid_tag: str = "none"
    touch_ask_tag: str = "none"
    touch_bid_rho: float = 0.0
    touch_ask_rho: float = 0.0
    touch_bid_traded_w: float = 0.0
    touch_ask_traded_w: float = 0.0
    touch_bid_drop_w: float = 0.0
    touch_ask_drop_w: float = 0.0
    touch_is_stale: bool = True
    
    # Burstiness
    burst_trade_count_bucket: int = 0
    burst_rate_short: float = 0.0
    burst_rate_long: float = 0.0
    burst_ratio: float = 0.0
    burst_cv_dt: float = 0.0
    burst_fano_counts: float = 0.0
    burst_flip_ratio: float = 0.0
    
    # Pivots
    pivots_ts_ms: int = 0
    pivots_date: str = ""
    notional_usd: float = 0.0

    @classmethod
    def empty(cls) -> "BucketState":
        """Returns a fresh empty state."""
        return cls()

    def update_from_tick_inplace(self, tick: Any, ts_ms: int, delta_classifier=None) -> None:
        """
        Updates basic price/timing features from a tick.
        Usually called before heavy microstructure analysis.
        """
        self.price = float(tick.last)
        self.ts = int(ts_ms)
        
        if self.l2_ts > 0:
            age = int(ts_ms - self.l2_ts)
            self.l2_age_ms_tick = max(0, age)
            self.l2_is_stale_now = (age > 2000)
            self.l2_skew_tick_flag = (age > 3000)
        
        if delta_classifier:
            delta = delta_classifier(tick)
            self.current_delta += float(delta)

# ---- Config Dataclasses ----

@dataclass
class GeometryConfig:
    near_mult: float = 0.25
    far_mult: float = 1.0
    new_extreme_bonus: float = 0.3
    max_score: float = 1.0
    min_score: float = 0.0

@dataclass
class LiquidityConfig:
    min_notional_for_high_liq: float = 250_000.0
    dense_cluster_bps: float = 5.0
    dense_cluster_min_levels: int = 3
    dense_cluster_min_share: float = 0.25

@dataclass
class ConfScoreConfig:
    regime_weight: float = 0.4
    geometry_weight: float = 0.3
    liquidity_weight: float = 0.3
    min_geometry_for_signal: float = 0.25
    min_liquidity_for_signal: float = 0.2

@dataclass
class SignalTypeConf:
    name: SignalKind
    regime_weight: float
    geometry_weight: float
    liquidity_weight: float
    min_conf_factor: float
    min_final_score: float
    min_raw_score: float
    allowed_regimes: Tuple[MarketRegime, ...]
    prefer_trend: bool = False
    prefer_range: bool = False
    forbid_strong_trend: bool = False
    forbid_strong_range: bool = False
    golden_regime_min: float = 0.7
    golden_geometry_min: float = 0.7
    golden_liquidity_min: float = 0.7

@dataclass
class OrderflowSignalThresholds:
    """Thresholds for signal generation."""
    main_z: float = 0.0
    breakout_z: float = 0.0
    obi: float = 0.25
    min_conf: float = 30.0
    min_bucket_trades: int = 0
    min_bucket_notional_usd: float = 0.0
    min_delta_z: float = 0.0
    min_obi_z: float = 0.0

@dataclass
class ExecutionContext:
    """Context for signal execution planning."""
    symbol: str
    price: float
    side: TradeSide
    ts: int
    signal_id: str
    strategy: str = "orderflow"

@dataclass
class PublishResult:
    """Result of a signal publish attempt."""
    success: bool
    message_id: Optional[str] = None
    error: Optional[str] = None

@dataclass(slots=True, frozen=True)
class NewsFeatures:
    """Compact news+calendar features attached to OrderflowSignalContext.

    Keep this object small: primitives only.

    ref:
        Reference to the heavy JSON payload in Redis.
        Recommended format: "news:analysis:<uid>".

    asof_ts_ms:
        When these features were last computed / fetched.
    """

    ref: str = ""
    news_risk: float = 0.0
    surprise_score: float = 0.0
    news_grade_id: int = 0

    # Calendar: time until next event (>=0 means future event), else -1
    event_tminus_sec: int = -1
    event_grade_id: int = 0

    tags_mask: int = 0
    primary_tag_id: int = 0
    confidence: float = 0.0

    # Optional: grade horizon for which the grade was computed (sec)
    horizon_sec: int = 0

    # "as of" timestamp in ms (ingested_ts_ms / now)
    asof_ts_ms: int = 0


@dataclass(slots=True)
class OrderflowSignalContext:
    """Main ctx object used across the orderflow pipeline."""

    # IDs / required
    symbol: str

    # Core routing
    asset_class: str = "crypto"  # crypto|forex|metals ("fx" treated as forex)

    # Timestamps / price
    ts: int = 0
    price: float = 0.0
    ts_utc: float = 0.0 # for compatibility
    
    # Optional: compact news+calendar features
    news: Optional[NewsFeatures] = None

    # config metadata
    family: str = "crypto_orderflow"
    venue: str = "binance_futures"
    timeframe_s: int = 60

    # core
    z_delta: float = 0.0

    # OBI
    obi: float = 0.0
    obi_avg: float = 0.0
    obi_sustained: bool = False
    obi_20: float = 0.0
    obi_avg_20: float = 0.0
    obi_sustained_20: bool = False
    obi_20_valid: bool = False
    
    # OBI local quantiles (added for scoring engine)
    obi_local_q: float = 0.0
    delta_spike_z: float = 0.0
    delta_spike_z_local_q: float = 0.0
    atr_local_q: float = 0.0
    atr_quantile: float = 0.0 # alias

    # depths/slopes/microprice
    depth_bid_5: float = 0.0
    depth_ask_5: float = 0.0
    depth_bid_20: float = 0.0
    depth_ask_20: float = 0.0
    slope_bid_20: float = 0.0
    slope_ask_20: float = 0.0
    microprice_shift_bps_20: float = 0.0
    microprice: float = 0.0

    # spread
    spread_bps: float = 0.0
    spread_bps_mean: float = 0.0
    spread_bps_z: float = 0.0

    # walls
    wall_bid: bool = False
    wall_ask: bool = False
    wall_bid_dist_bps: float = 0.0
    wall_ask_dist_bps: float = 0.0
    wall_bid_persist_ratio: float = 0.0
    wall_ask_persist_ratio: float = 0.0
    wall_bid_suspicious: bool = False
    wall_ask_suspicious: bool = False

    # L2 freshness
    l2_ts: int = 0
    l2_age_ms: int = 0
    l2_is_stale: bool = True

    # ATR / vol
    atr: float = 0.0
    atr_14_raw: float = 0.0
    atr_14_bps: float = 0.0
    atr_14_q: float = 0.0
    daily_atr_bps: Optional[float] = None

    # delta/bucket
    current_delta: float = 0.0
    delta_bucket: float = 0.0
    
    # CVD
    cvd_5m: float = 0.0
    cvd_divergence: float = 0.0

    # L3-lite
    taker_buy_qty_bucket: float = 0.0
    taker_sell_qty_bucket: float = 0.0
    taker_buy_rate_ema: float = 0.0
    taker_sell_rate_ema: float = 0.0
    cancel_to_trade_bid: float = 0.0
    cancel_to_trade_ask: float = 0.0
    cancel_bid_rate_ema: float = 0.0
    cancel_ask_rate_ema: float = 0.0
    eta_fill_bid_sec: float = 0.0
    eta_fill_ask_sec: float = 0.0
    pull_ask_qty_proxy: float = 0.0
    pull_bid_qty_proxy: float = 0.0

    # P1: OFI / Churn
    ofi_val: float = 0.0
    ofi_z: float = 0.0
    book_churn_hz: float = 0.0
    book_churn_z: float = 0.0

    # P1: Event Recency (Features)
    iceberg_age_ms: int = -1
    sweep_age_ms: int = -1
    reclaim_age_ms: int = -1
    microprice_shift_age_ms: int = -1
    obi_event_age_ms: int = -1
    
    # Optional L3 fields
    eta_fill_ms: Optional[float] = None
    imbalance_min: Optional[float] = None

    # touch-level
    touch_bid_tag: str = "none"
    touch_ask_tag: str = "none"
    touch_bid_rho: float = 0.0
    touch_ask_rho: float = 0.0
    touch_bid_traded_w: float = 0.0
    touch_ask_traded_w: float = 0.0
    touch_bid_drop_w: float = 0.0
    touch_ask_drop_w: float = 0.0
    touch_is_stale: bool = True

    # burstiness
    burst_trade_count_bucket: int = 0
    burst_rate_short: float = 0.0
    burst_rate_long: float = 0.0
    burst_ratio: float = 0.0
    burst_cv_dt: float = 0.0
    burst_fano_counts: float = 0.0
    burst_flip_ratio: float = 0.0

    # regime
    regime_score: float = 0.0
    regime_label: str = "mixed"
    market_regime: Optional[MarketRegime] = None
    market_regime_score: float = 0.0
    regime_trend_score: float = 0.0
    regime_range_score: float = 0.0
    cross_bias: float = 0.0
    
    # Compatibility fields for new RegimeService
    last_price: Optional[float] = None
    vwap: Optional[float] = None
    daily_open: Optional[float] = None
    weak_progress_raw: Optional[float] = None
    daily_open_dist_bps: Optional[float] = None
    
    # pivots
    pivots: Dict[str, float] = field(default_factory=dict)

    # Levels
    entry_price: Optional[float] = None
    tp1_price: Optional[float] = None
    sl_price: Optional[float] = None
    side_int: Optional[int] = None
    side: Optional[str] = None # LONG/SHORT
    
    # Geometry / Liquidity Contexts
    liquidity_context: Optional[LiquidityContext] = None
    geometry_hits: List[GeoZoneHit] = field(default_factory=list)

    # Fail-open telemetry flags
    data_quality_flags: list[str] = field(default_factory=list)

    # A "payload" bucket is sometimes convenient when you want to attach
    # non-hot, rarely used data without expanding the schema.
    extra: Dict[str, Any] = field(default_factory=dict)
    
    # Scoring results
    base_confidence: int = 0
    confidence: int = 0
    pattern_name: Optional[str] = None
    pattern_family: Optional[str] = None
    min_confidence_used: float = 0.0
    is_golden_pattern: bool = False
    golden_pattern_label: Optional[str] = None
    weak_progress: float = 0.0 # alias for weak_progress_raw?
    
    # Quality / Final Score
    quality_offline: float = 0.0
    quality_online: float = 50.0
    quality_combined: float = 0.0
    quality_status: str = "unknown"
    final_score: float = 0.0
    is_disabled_by_quality: bool = False
    
    # Compatibility legacy fields
    score_raw: float = 0.0
    score_final: float = 0.0
    quality_reasons: List[str] = field(default_factory=list)
    calibrated: Dict[str, Any] = field(default_factory=dict)
    metrics: Dict[str, Any] = field(default_factory=dict)
    session: str = "unknown" # e.g. "london", "ny"
    regime: str = "unknown" # alias for regime_label

    # ------------------------------------------------------------------
    # Bar Range snapshot (propagated from BucketState)
    # ------------------------------------------------------------------
    bar_id: Optional[int] = None
    bar_ts_open_ms: int = 0
    bar_open: float = 0.0
    bar_high: float = 0.0
    bar_low: float = 0.0
    bar_close: float = 0.0
    bar_range: float = 0.0
    bar_range_bps: float = 0.0
    bar_range_bps_ema: float = 0.0
    bar_range_bps_ratio_to_ema: float = 0.0
    bar_range_z: float = 0.0
    bar_range_last_closed_z: float = 0.0

    # Diagnostics
    prev_bar_open: float = 0.0
    prev_bar_high: float = 0.0
    prev_bar_low: float = 0.0
    prev_bar_close: float = 0.0
    prev_bar_range: float = 0.0
    prev_bar_range_bps: float = 0.0
    prev_bar_range_bps_z: float = 0.0
    bar_time_backwards_cnt: int = 0
    bar_time_backwards_flag: bool = False
    bar_time_backwards_ms: int = 0
    bar_gap_bars: int = 0
    bar_gap_flag: bool = False
    bar_late_tick_ignored: int = 0

    # ------------------------------------------------------------------
    # Pivots meta (optional, but useful for debugging / UI)
    # ------------------------------------------------------------------
    pivots_ts_ms: int = 0
    pivots_date: str = ""
    nearest_pivot_key: str = ""
    nearest_pivot_price: float = 0.0

    def __getattr__(self, name: str) -> Any:
        """Fallback to extra dict for unknown attributes."""
        try:
            extra = object.__getattribute__(self, 'extra')
            if isinstance(extra, dict):
                return extra.get(name)
        except AttributeError:
            pass
        raise AttributeError(f"'{self.__class__.__name__}' object has no attribute '{name}'")

    def __setattr__(self, name: str, value: Any) -> None:
        """Fallback to extra dict for unknown attributes."""
        # Check if we're still initializing the dataclass
        if not hasattr(self, '__dataclass_fields__'):
            # During __init__, just set normally
            object.__setattr__(self, name, value)
            return

        if name != 'extra' and hasattr(self.__class__, 'extra'):
            try:
                extra = object.__getattribute__(self, 'extra')
                if isinstance(extra, dict) and not hasattr(self.__class__, name):
                    extra[name] = value
                    return
            except AttributeError:
                pass
        object.__setattr__(self, name, value)
