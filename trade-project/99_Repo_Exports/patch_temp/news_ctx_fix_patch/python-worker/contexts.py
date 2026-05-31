"""python-worker/contexts.py

OrderflowSignalContext is created inside the tick processing pipeline and then
passed through detectors/extractors/emitters.

IMPORTANT PERFORMANCE NOTE
--------------------------
The ctx object is created very frequently. We therefore use:
- dataclass(slots=True) for compact layout and fast attribute access
- primitive types (int/float/bool/str) and a small number of dicts

WHY THIS FILE EXISTS
--------------------
In your repo `build_signal_ctx()` produces a large `ctx_kwargs` dict
(symbol/ts/price/obi/atr/... etc.).
If OrderflowSignalContext only declares a few fields, then `_filter_dataclass_kwargs`
will silently drop ~all fields, and downstream components will see defaults.

This file declares the fields that are already being produced in ctx_kwargs.
Add more fields here as you add them to ctx_kwargs.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Optional


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

    # pivots
    pivots: Dict[str, float] = field(default_factory=dict)

    # Fail-open telemetry flags
    data_quality_flags: list[str] = field(default_factory=list)

    # A "payload" bucket is sometimes convenient when you want to attach
    # non-hot, rarely used data without expanding the schema.
    extra: Dict[str, Any] = field(default_factory=dict)
