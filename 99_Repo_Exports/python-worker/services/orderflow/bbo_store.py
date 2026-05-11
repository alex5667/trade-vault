from __future__ import annotations

"""BBO time-series publisher (Phase B1).

Why this module exists
----------------------
TCA metrics (effective/realized spread, price impact, implementation shortfall)
require a reliable mid/bid/ask at:

  - fill time  (t)
  - fill+Δ     (t+Δ), typically 1s and 5s

In a streaming system you must NOT rely on "whatever the book is now" when
post-trade workers run; you need an auditable time-series store.

Design goals
------------
* Hot-path safe: bounded CPU/RAM; never blocks book/tick processing.
* Low-cardinality storage: only BBO + derived mid.
* Deterministic timestamps: uses book event-time (epoch ms, UTC).
* Fail-open: any publish error is ignored (telemetry only).

Implementation choice
---------------------
We publish compact snapshots into Redis Stream `events:bbo_ts`.
Warm-path writer (services/posttrade/bbo_ts_writer.py) persists to Timescale.

This pattern keeps the trading hot-path independent from DB latency.
"""

import json
import math
import os
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any
from core.redis_keys import RedisStreams as RS

# Import is optional to keep unit tests runnable in minimal environments.
if TYPE_CHECKING:  # pragma: no cover
    from services.async_signal_publisher import (
        AsyncSignalPublisher,  # noqa: F401
        StreamSink,  # noqa: F401
    )


_bbo_semaphore = None


def _safe_float(v: Any) -> float | None:
    try:
        f = float(v)
    except Exception:
        return None
    if not math.isfinite(f):
        return None
    return float(f)


def _safe_int(v: Any) -> int | None:
    try:
        i = int(float(v))
    except Exception:
        return None
    return int(i)


def _calc_mid(bid: float | None, ask: float | None) -> float | None:
    if bid is None or ask is None:
        return None
    if bid <= 0 or ask <= 0:
        return None
    return (bid + ask) / 2.0


@dataclass
class BBOStoreCfg:
    enabled: bool
    stream: str
    stream_maxlen: int
    schema_version: int
    min_interval_ms: int
    # Optional allowlist (upper-case symbols). Empty => allow all.
    symbols_allow: set[str]
    venue_default: str

    @staticmethod
    def from_env() -> BBOStoreCfg:
        enabled = bool(int(os.getenv("BBO_TS_PUBLISH_ENABLED", "1") or 1))
        stream = os.getenv("BBO_TS_STREAM", RS.EVENTS_BBO_TS)
        stream_maxlen = int(os.getenv("BBO_TS_STREAM_MAXLEN", "5000") or 5000)
        schema_version = int(os.getenv("BBO_TS_SCHEMA_VERSION", "1") or 1)
        # 100ms => 10Hz upper bound per symbol (sane default for TCA joins).
        min_interval_ms = int(os.getenv("BBO_TS_MIN_INTERVAL_MS", "100") or 100)
        raw_allow = os.getenv("BBO_TS_SYMBOLS_ALLOW", "").strip()
        allow = {s.strip().upper() for s in raw_allow.split(",") if s.strip()} if raw_allow else set()
        venue_default = os.getenv("BBO_TS_VENUE_DEFAULT", "binance")
        return BBOStoreCfg(
            enabled=enabled,
            stream=stream,
            stream_maxlen=stream_maxlen,
            schema_version=schema_version,
            min_interval_ms=min_interval_ms,
            symbols_allow=allow,
            venue_default=venue_default,
        )


async def maybe_publish_bbo(
    *,
    publisher: Any,
    cfg: BBOStoreCfg,
    runtime: Any,
    book_ts_ms: int,
) -> None:
    """Publish a compact BBO snapshot into `events:bbo_ts`.

    Throttling:
      - per symbol, do not publish more frequently than cfg.min_interval_ms
      - optional allowlist by symbol

    Event payload is JSON under field `payload`.
    """

    if not cfg.enabled:
        return

    symbol = str(getattr(runtime, "symbol", "") or "").upper()
    if not symbol:
        return
    if cfg.symbols_allow and symbol not in cfg.symbols_allow:
        return

    ts_ms = _safe_int(book_ts_ms)
    if ts_ms is None or ts_ms <= 0:
        return

    # per-symbol throttle state lives on runtime (hot-path, no globals)
    last_ts = int(getattr(runtime, "bbo_ts_last_publish_ms", 0) or 0)
    if last_ts and (ts_ms - last_ts) < int(cfg.min_interval_ms):
        return

    # Extract BBO from runtime.book_state or runtime.last_book.
    bid = ask = None
    try:
        bs = getattr(runtime, "book_state", None)
        snap = getattr(bs, "snap", None) if bs is not None else None
        if snap is not None:
            bid = _safe_float(getattr(snap, "best_bid_px", None))
            ask = _safe_float(getattr(snap, "best_ask_px", None))
        if bid is None or ask is None:
            snap2 = getattr(runtime, "last_book", None)
            if snap2 is not None:
                bid = bid if bid is not None else _safe_float(getattr(snap2, "best_bid_px", None))
                ask = ask if ask is not None else _safe_float(getattr(snap2, "best_ask_px", None))
                if (bid is None or ask is None) and isinstance(snap2, dict):
                    bids = snap2.get("bids") or []
                    asks = snap2.get("asks") or []
                    if bid is None and bids:
                        bid = _safe_float(bids[0][0])
                    if ask is None and asks:
                        ask = _safe_float(asks[0][0])
    except Exception:
        return

    mid = _calc_mid(bid, ask)
    if bid is None or ask is None or mid is None:
        return

    venue = ""
    try:
        venue = str(getattr(runtime, "venue", None) or (runtime.config.get("venue") if hasattr(runtime, "config") else "") or "")
    except Exception:
        venue = ""
    venue = (venue or cfg.venue_default or "binance").strip().lower()

    payload: dict[str, Any] = {
        "schema_version": int(cfg.schema_version),
        "producer": os.getenv("SERVICE_NAME", "python-worker"),
        "ts_ms": int(ts_ms),
        "symbol": symbol,
        "venue": venue,
        "bid": float(bid),
        "ask": float(ask),
        "mid": float(mid),
    }

    # Persist throttle state only after payload is built.
    runtime.bbo_ts_last_publish_ms = int(ts_ms)

    # Keep stream contract consistent with other services: JSON in field `payload`.
    try:
        from services.async_signal_publisher import StreamSink  # type: ignore

        sink = StreamSink(name=str(cfg.stream), maxlen=int(cfg.stream_maxlen))
    except Exception:
        # Unit-test / minimal env fallback.
        class _Sink:
            def __init__(self, name: str, maxlen: int):
                self.name = name
                self.maxlen = maxlen

    try:
        if hasattr(runtime, "metrics_batcher") and runtime.metrics_batcher is not None:
            # Serialize manually since we bypass xadd_json
            payload_str = json.dumps(payload)
            field_name = str(sink.field or "payload")
            # MetricsBatcher natively handles xadd queuing
            runtime.metrics_batcher.put("xadd", sink.name, {field_name: payload_str}, maxlen=int(cfg.stream_maxlen))
            return
    except Exception:
        pass

    global _bbo_semaphore
    if _bbo_semaphore is None:
        import asyncio
        _bbo_semaphore = asyncio.Semaphore(int(os.getenv("BBO_PUBLISH_CONCURRENCY", "30")))

    if _bbo_semaphore.locked():
        # strict backpressure: if publishes are already in-flight, drop this one
        return

    async def _do_publish():
        async with _bbo_semaphore:  # type: ignore
            try:  # type: ignore
                await publisher.xadd_json(
                    sink=sink,
                    payload=payload,
                    symbol=symbol,
                    approximate=True,
                    no_retry=True,
                    timeout_sec=None,  # Rely on ASYNC_PUB_TIMEOUT_SEC
                )
            except Exception:
                pass

    try:
        from utils.task_manager import safe_create_task
        safe_create_task(_do_publish())
    except Exception:
        # Fail-open: BBO storage must never block trading.
        pass
