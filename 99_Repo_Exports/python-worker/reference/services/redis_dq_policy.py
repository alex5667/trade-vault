from __future__ import annotations

"""Deterministic Redis/data-quality veto policy for pre-trade publishing.

Levels:
  0 = OK          — all checks green, trade allowed
  1 = soft veto   — tightened mode; trade allowed but notional may be reduced
  2 = hard veto   — trade publication blocked

ENV thresholds (all int, milliseconds or event counts):
  DQ_QUEUE_LAG_SOFT_MS          (default 2000)
  DQ_QUEUE_LAG_HARD_MS          (default 10000)
  DQ_TICK_STALENESS_SOFT_MS     (default 1500)
  DQ_TICK_STALENESS_HARD_MS     (default 5000)
  DQ_BOOK_STALENESS_SOFT_MS     (default 1500)
  DQ_BOOK_STALENESS_HARD_MS     (default 5000)
  DQ_REDIS_TIMEOUT_SOFT_EVENTS  (default 2)
  DQ_REDIS_TIMEOUT_HARD_EVENTS  (default 5)
  DQ_NEGATIVE_AGE_HARD_EVENTS   (default 1)
  DQ_XACK_FAIL_SOFT_EVENTS      (default 2)
  DQ_XACK_FAIL_HARD_EVENTS      (default 5)
  DQ_OUTBOX_BACKLOG_SOFT        (default 100)
  DQ_OUTBOX_BACKLOG_HARD        (default 500)
  DQ_STREAM_TIMEOUT_BURST_SOFT  (default 2)
  DQ_STREAM_TIMEOUT_BURST_HARD  (default 5)
"""

from dataclasses import asdict, dataclass
from typing import Any, Dict, List, Optional
import os

try:
    from prometheus_client import Counter, Gauge, REGISTRY
except Exception:  # pragma: no cover
    Counter = Gauge = None  # type: ignore
    REGISTRY = None  # type: ignore


def _metric(factory, name: str, *args, **kwargs):
    """Idempotent Prometheus metric factory — returns existing metric if already registered."""
    if factory is None:
        return None
    try:
        return factory(name, *args, **kwargs)
    except ValueError:
        return getattr(REGISTRY, "_names_to_collectors", {}).get(name) if REGISTRY is not None else None


TRADE_DQ_LEVEL = _metric(
    Gauge
    "trade_dq_level"
    "Current hard/soft data-quality level for trade publication (0=OK,1=soft,2=hard)."
    ["symbol"]
)
TRADE_DQ_HARD_VETO_TOTAL = _metric(
    Counter
    "trade_dq_hard_veto_total"
    "Number of times trade publication was blocked by Redis/data-quality hard veto."
    ["symbol", "reason"]
)
TRADE_DQ_SOFT_VETO_TOTAL = _metric(
    Counter
    "trade_dq_soft_veto_total"
    "Number of times trade publication entered soft-veto/tightened mode due to data quality."
    ["symbol", "reason"]
)


@dataclass(frozen=True)
class RedisDQThresholds:
    """Immutable threshold config. Built from ENV via from_env() or set per-test."""
    queue_lag_soft_ms: int = 2_000
    queue_lag_hard_ms: int = 10_000
    tick_staleness_soft_ms: int = 1_500
    tick_staleness_hard_ms: int = 5_000
    book_staleness_soft_ms: int = 1_500
    book_staleness_hard_ms: int = 5_000
    redis_timeout_soft_events: int = 2
    redis_timeout_hard_events: int = 5
    negative_age_hard_events: int = 1
    xack_fail_soft_events: int = 2
    xack_fail_hard_events: int = 5
    outbox_backlog_soft: int = 100
    outbox_backlog_hard: int = 500
    stream_timeout_burst_soft: int = 2
    stream_timeout_burst_hard: int = 5

    @classmethod
    def from_env(cls) -> "RedisDQThresholds":
        """Construct thresholds from environment variables with safe fallbacks."""
        def _i(name: str, default: int) -> int:
            try:
                return int(os.getenv(name, str(default)))
            except Exception:
                return int(default)
        return cls(
            queue_lag_soft_ms=_i("DQ_QUEUE_LAG_SOFT_MS", cls.queue_lag_soft_ms)
            queue_lag_hard_ms=_i("DQ_QUEUE_LAG_HARD_MS", cls.queue_lag_hard_ms)
            tick_staleness_soft_ms=_i("DQ_TICK_STALENESS_SOFT_MS", cls.tick_staleness_soft_ms)
            tick_staleness_hard_ms=_i("DQ_TICK_STALENESS_HARD_MS", cls.tick_staleness_hard_ms)
            book_staleness_soft_ms=_i("DQ_BOOK_STALENESS_SOFT_MS", cls.book_staleness_soft_ms)
            book_staleness_hard_ms=_i("DQ_BOOK_STALENESS_HARD_MS", cls.book_staleness_hard_ms)
            redis_timeout_soft_events=_i("DQ_REDIS_TIMEOUT_SOFT_EVENTS", cls.redis_timeout_soft_events)
            redis_timeout_hard_events=_i("DQ_REDIS_TIMEOUT_HARD_EVENTS", cls.redis_timeout_hard_events)
            negative_age_hard_events=_i("DQ_NEGATIVE_AGE_HARD_EVENTS", cls.negative_age_hard_events)
            xack_fail_soft_events=_i("DQ_XACK_FAIL_SOFT_EVENTS", cls.xack_fail_soft_events)
            xack_fail_hard_events=_i("DQ_XACK_FAIL_HARD_EVENTS", cls.xack_fail_hard_events)
            outbox_backlog_soft=_i("DQ_OUTBOX_BACKLOG_SOFT", cls.outbox_backlog_soft)
            outbox_backlog_hard=_i("DQ_OUTBOX_BACKLOG_HARD", cls.outbox_backlog_hard)
            stream_timeout_burst_soft=_i("DQ_STREAM_TIMEOUT_BURST_SOFT", cls.stream_timeout_burst_soft)
            stream_timeout_burst_hard=_i("DQ_STREAM_TIMEOUT_BURST_HARD", cls.stream_timeout_burst_hard)
        )


@dataclass(frozen=True)
class RedisDQSnapshot:
    """Point-in-time DQ health snapshot for a single symbol."""
    symbol: str
    queue_lag_ms: int = 0
    tick_staleness_ms: int = 0
    book_staleness_ms: int = 0
    redis_timeout_events: int = 0
    negative_age_events: int = 0
    xack_fail_events: int = 0
    outbox_backlog: int = 0
    stream_timeout_burst: int = 0
    force_hard_veto: bool = False


@dataclass(frozen=True)
class RedisDQDecision:
    """Result of evaluate_redis_dq(). Immutable; serializable via to_dict()."""
    level: int                    # 0=OK, 1=soft, 2=hard
    allow_trade_publish: bool
    reasons: List[str]
    tightened_mode: bool
    snapshot: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "level": int(self.level)
            "allow_trade_publish": bool(self.allow_trade_publish)
            "reasons": list(self.reasons)
            "tightened_mode": bool(self.tightened_mode)
            "snapshot": dict(self.snapshot)
        }


def _positive_int(v: Any) -> int:
    """Sanitize to non-negative int; malformed values treated as 0."""
    try:
        return max(0, int(v))
    except Exception:
        return 0


def evaluate_redis_dq(snapshot: RedisDQSnapshot, thresholds: Optional[RedisDQThresholds] = None) -> RedisDQDecision:
    """Evaluate DQ health and return a publish decision.

    Pure function — no side effects except Prometheus metric updates.
    The snapshot is sanitized before evaluation; no exceptions propagate.
    """
    t = thresholds or RedisDQThresholds()
    # Sanitize snapshot: non-negative ints, valid symbol string
    s = RedisDQSnapshot(
        symbol=str(snapshot.symbol or "")
        queue_lag_ms=_positive_int(snapshot.queue_lag_ms)
        tick_staleness_ms=_positive_int(snapshot.tick_staleness_ms)
        book_staleness_ms=_positive_int(snapshot.book_staleness_ms)
        redis_timeout_events=_positive_int(snapshot.redis_timeout_events)
        negative_age_events=_positive_int(snapshot.negative_age_events)
        xack_fail_events=_positive_int(snapshot.xack_fail_events)
        outbox_backlog=_positive_int(snapshot.outbox_backlog)
        stream_timeout_burst=_positive_int(snapshot.stream_timeout_burst)
        force_hard_veto=bool(snapshot.force_hard_veto)
    )

    hard: List[str] = []
    soft: List[str] = []

    # Operator-forced hard veto (emergency kill-switch)
    if s.force_hard_veto:
        hard.append("force_hard_veto")

    # Queue lag (Redis consumer lag between producer and worker)
    if s.queue_lag_ms >= t.queue_lag_hard_ms:
        hard.append("queue_lag")
    elif s.queue_lag_ms >= t.queue_lag_soft_ms:
        soft.append("queue_lag")

    # Tick freshness (time since last tick update)
    if s.tick_staleness_ms >= t.tick_staleness_hard_ms:
        hard.append("tick_staleness")
    elif s.tick_staleness_ms >= t.tick_staleness_soft_ms:
        soft.append("tick_staleness")

    # Book freshness (time since last book update)
    if s.book_staleness_ms >= t.book_staleness_hard_ms:
        hard.append("book_staleness")
    elif s.book_staleness_ms >= t.book_staleness_soft_ms:
        soft.append("book_staleness")

    # Redis timeout event counter (transient or persistent connectivity issues)
    if s.redis_timeout_events >= t.redis_timeout_hard_events:
        hard.append("redis_timeout_events")
    elif s.redis_timeout_events >= t.redis_timeout_soft_events:
        soft.append("redis_timeout_events")

    # Negative-age events: timestamps from the future — always hard (data corruption)
    if s.negative_age_events >= t.negative_age_hard_events:
        hard.append("negative_age_events")

    # XACK failure events (message acknowledgement failures in Redis Streams)
    if s.xack_fail_events >= t.xack_fail_hard_events:
        hard.append("xack_fail_events")
    elif s.xack_fail_events >= t.xack_fail_soft_events:
        soft.append("xack_fail_events")

    # Outbox backlog (publisher internal queue depth)
    if s.outbox_backlog >= t.outbox_backlog_hard:
        hard.append("outbox_backlog")
    elif s.outbox_backlog >= t.outbox_backlog_soft:
        soft.append("outbox_backlog")

    # Stream timeout burst (number of xreadgroup timeouts in a short window)
    if s.stream_timeout_burst >= t.stream_timeout_burst_hard:
        hard.append("stream_timeout_burst")
    elif s.stream_timeout_burst >= t.stream_timeout_burst_soft:
        soft.append("stream_timeout_burst")

    # Determine final level
    if hard:
        level = 2
        allow = False
        tightened = False
        reasons = sorted(set(hard))
    elif soft:
        level = 1
        allow = True
        tightened = True
        reasons = sorted(set(soft))
    else:
        level = 0
        allow = True
        tightened = False
        reasons = []

    # Update Prometheus metrics (fail-open: any exception is swallowed)
    if TRADE_DQ_LEVEL:
        TRADE_DQ_LEVEL.labels(symbol=s.symbol or "UNKNOWN").set(level)
    if hard and TRADE_DQ_HARD_VETO_TOTAL:
        for reason in sorted(set(hard)):
            TRADE_DQ_HARD_VETO_TOTAL.labels(symbol=s.symbol or "UNKNOWN", reason=reason).inc()
    if soft and TRADE_DQ_SOFT_VETO_TOTAL:
        for reason in sorted(set(soft)):
            TRADE_DQ_SOFT_VETO_TOTAL.labels(symbol=s.symbol or "UNKNOWN", reason=reason).inc()

    return RedisDQDecision(
        level=level
        allow_trade_publish=allow
        reasons=reasons
        tightened_mode=tightened
        snapshot=asdict(s)
    )
