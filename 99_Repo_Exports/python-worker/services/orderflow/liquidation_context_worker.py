from __future__ import annotations

"""liquidation_context_worker.py — Rolling liquidation stress aggregator.

Reads ``stream:liq_evt`` (Redis Stream, XREADGROUP), aggregates rolling 60-second
windows per symbol, and writes ``ctx:liq:{SYMBOL}`` JSON to Redis.

Redis contract
--------------
Key  : ``ctx:liq:{SYMBOL}``
Value: JSON object (schema_version=1)

Example::

    {
      "schema_version": 1,
      "symbol": "BTCUSDT",
      "ts_ms": 1760000000000,
      "window_ms": 60000,
      "liq_buy_notional_1m":  800000.0,   -- short-side forced close (BUY order)
      "liq_sell_notional_1m": 1200000.0,  -- long-side forced close (SELL order)
      "liq_imbalance_z": 2.4,             -- MAD z-score of sell-buy imbalance
      "liq_event_count_1m": 12,
      "largest_liq_notional_1m": 350000.0,
      "liq_stress_flag": 0,               -- 1 if |imbalance_z| >= LIQ_STRESS_Z_THR
      "quality_status": "OK"
    }

Side mapping (explicit contract)
---------------------------------
- ``order_side=SELL`` → long liquidation pressure (longs being closed)
- ``order_side=BUY``  → short liquidation pressure (shorts being closed)

This matches Binance forceOrder stream semantics:
  ``o.S`` is the side of the closing order, not the position direction.

ENV variables
-------------
LIQ_CONTEXT_WORKER_ENABLED    : "1" to activate (default off)
LIQ_CONTEXT_STREAM_KEY        : Redis stream key (default: stream:liq_evt)
LIQ_CONTEXT_CONSUMER_GROUP    : consumer group name (default: liq_ctx_worker)
LIQ_CONTEXT_CONSUMER_NAME     : consumer name (default: liq_ctx_worker_1)
LIQ_CONTEXT_WINDOW_MS         : rolling window in ms (default: 60000)
LIQ_CONTEXT_REDIS_TTL_S       : ctx:liq key TTL in seconds (default: 120)
LIQ_CONTEXT_BATCH_SIZE        : XREADGROUP count per call (default: 100)
LIQ_CONTEXT_POLL_MS           : polling interval in ms (default: 500)
LIQ_CONTEXT_STRESS_Z_THR      : |imbalance_z| threshold for liq_stress_flag (default: 3.0)
LIQ_CONTEXT_HISTORY_MAX       : max imbalance history for z-score (default: 200)
"""


import asyncio
import json
import logging
import math
import os
import time
from collections import deque
from dataclasses import asdict, dataclass
from statistics import median
from typing import Any
from core.redis_keys import RedisStreams as RS

logger = logging.getLogger(__name__)

# ─── Constants ────────────────────────────────────────────────────────────────

SCHEMA_VERSION = 1
CTX_LIQ_PREFIX = "ctx:liq:"

_DEFAULT_STREAM_KEY = RS.LIQ_EVT
_DEFAULT_CONSUMER_GROUP = "liq_ctx_worker"
_DEFAULT_CONSUMER_NAME = "liq_ctx_worker_1"
_DEFAULT_WINDOW_MS = 60_000
_DEFAULT_REDIS_TTL_S = 120
_DEFAULT_BATCH_SIZE = 100
_DEFAULT_POLL_MS = 500
_DEFAULT_STRESS_Z_THR = 3.0
_DEFAULT_HISTORY_MAX = 200


# ─── Data types ───────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class LiqEvent:
    ts_ms: int
    symbol: str
    order_side: str   # "BUY" or "SELL"
    notional_usd: float


@dataclass
class LiqContextSnapshot:
    schema_version: int
    symbol: str
    ts_ms: int
    window_ms: int
    liq_buy_notional_1m: float    # short liquidation pressure
    liq_sell_notional_1m: float   # long liquidation pressure
    liq_imbalance_z: float        # MAD z-score of (sell - buy) / total
    liq_event_count_1m: int
    largest_liq_notional_1m: float
    liq_stress_flag: int
    quality_status: str

    def to_json(self) -> str:
        return json.dumps(asdict(self), separators=(",", ":"))


# ─── Math helpers ─────────────────────────────────────────────────────────────

def _robust_z(x: float, history: list[float]) -> float:
    """MAD-based robust z-score, capped ±50. Returns 0.0 if history < 10."""
    if len(history) < 10:
        return 0.0
    med = float(median(history))
    mad = float(median([abs(v - med) for v in history]))
    if mad <= 1e-12:
        return 0.0
    z = (x - med) / (1.4826 * mad)
    if not math.isfinite(z):
        return 0.0
    return float(max(-50.0, min(50.0, z)))


def _parse_float(v: Any, default: float = 0.0) -> float:
    try:
        x = float(v)
        return x if math.isfinite(x) else default
    except Exception:
        return default


def _parse_str(v: Any) -> str:
    return (v or "").strip()


def _now_ms() -> int:
    return int(time.time() * 1000)


# ─── Per-symbol rolling window ─────────────────────────────────────────────────

class _SymbolWindow:
    """Rolling 60-second liquidation events for one symbol."""

    def __init__(self, window_ms: int, history_max: int, stress_z_thr: float) -> None:
        self._window_ms = window_ms
        self._stress_z_thr = stress_z_thr
        # (ts_ms, side, notional)
        self._events: deque[tuple[int, str, float]] = deque()
        # rolling imbalance history for z-score
        self._imbalance_hist: list[float] = []
        self._history_max = history_max

    def push(self, evt: LiqEvent) -> None:
        self._events.append((evt.ts_ms, evt.order_side.upper(), evt.notional_usd))

    def _evict(self, now_ms: int) -> None:
        cutoff = now_ms - self._window_ms
        while self._events and self._events[0][0] < cutoff:
            self._events.popleft()

    def build_snapshot(self, symbol: str, now_ms: int) -> LiqContextSnapshot:
        self._evict(now_ms)

        buy_notional = 0.0   # short liq pressure
        sell_notional = 0.0  # long liq pressure
        largest = 0.0
        count = 0

        for _, side, notional in self._events:
            count += 1
            if notional > largest:
                largest = notional
            if side == "BUY":
                buy_notional += notional
            else:
                sell_notional += notional

        total = buy_notional + sell_notional
        if total > 0:
            imbalance = (sell_notional - buy_notional) / total  # [-1, 1]
        else:
            imbalance = 0.0

        # Update rolling imbalance history for z-score
        if count > 0:
            self._imbalance_hist.append(imbalance)
            if len(self._imbalance_hist) > self._history_max:
                self._imbalance_hist = self._imbalance_hist[-self._history_max:]

        imbalance_z = _robust_z(imbalance, self._imbalance_hist)
        stress_flag = 1 if abs(imbalance_z) >= self._stress_z_thr else 0

        return LiqContextSnapshot(
            schema_version=SCHEMA_VERSION,
            symbol=symbol.upper(),
            ts_ms=now_ms,
            window_ms=self._window_ms,
            liq_buy_notional_1m=round(buy_notional, 2),
            liq_sell_notional_1m=round(sell_notional, 2),
            liq_imbalance_z=round(imbalance_z, 4),
            liq_event_count_1m=count,
            largest_liq_notional_1m=round(largest, 2),
            liq_stress_flag=stress_flag,
            quality_status="OK",
        )


# ─── Event parser ─────────────────────────────────────────────────────────────

def _parse_liq_event(raw_fields: dict[bytes, bytes]) -> LiqEvent | None:
    """Parse a Redis Stream message into a LiqEvent.

    Supports both direct field dict and nested JSON payload.
    """
    try:
        def _b(k: str) -> str | None:
            v = raw_fields.get(k.encode()) or raw_fields.get(k)  # type: ignore
            return v.decode("utf-8", errors="replace") if isinstance(v, bytes) else str(v) if v is not None else None  # type: ignore

        # Try nested JSON payload first (from Go controller.publishNormalized)
        payload_raw = _b("payload") or _b("data")
        if payload_raw:
            try:
                payload = json.loads(payload_raw)
            except Exception:
                payload = {}
        else:
            payload = {}

        # Resolve fields: prefer flat fields, fallback to payload dict
        def _get(key: str) -> str | None:
            v = _b(key)
            if v is not None:
                return v
            v2 = payload.get(key)
            return str(v2) if v2 is not None else None

        symbol = _parse_str(_get("symbol") or _get("s") or "").upper()
        if not symbol:
            return None

        side = _parse_str(_get("order_side") or _get("S") or "").upper()
        if side not in {"BUY", "SELL"}:
            return None

        # notional_usd: use pre-computed if available, else quantity * price
        notional_raw = _get("notional_usd") or _get("q_notional_usd")
        if notional_raw is not None:
            notional = _parse_float(notional_raw)
        else:
            qty = _parse_float(_get("quantity") or _get("q") or "0")
            price = _parse_float(_get("price") or _get("p") or "0")
            notional = qty * price

        if notional <= 0:
            return None

        ts_raw = _get("ts_ms") or _get("T") or _get("timestamp")
        ts_ms = int(_parse_float(ts_raw)) if ts_raw else _now_ms()

        return LiqEvent(ts_ms=ts_ms, symbol=symbol, order_side=side, notional_usd=notional)

    except Exception as exc:
        logger.debug("liq_ctx: parse error: %s", exc)
        return None


# ─── Worker ───────────────────────────────────────────────────────────────────

class LiquidationContextWorker:
    """Async worker that aggregates liq events into rolling ctx:liq snapshots.

    Lifecycle
    ---------
    1. ``await worker.start()`` — creates consumer group (MKSTREAM), runs loop.
    2. ``await worker.stop()`` — signals shutdown.

    Usage::

        worker = LiquidationContextWorker(redis_client)
        await worker.start()
        # runs until stop() is called
        await worker.stop()
    """

    def __init__(self, redis, *, cfg: dict[str, Any] | None = None) -> None:
        self._redis = redis
        cfg = cfg or {}

        self._stream_key: str = cfg.get("stream_key") or os.getenv("LIQ_CONTEXT_STREAM_KEY", _DEFAULT_STREAM_KEY)
        self._group: str = cfg.get("consumer_group") or os.getenv("LIQ_CONTEXT_CONSUMER_GROUP", _DEFAULT_CONSUMER_GROUP)
        self._consumer: str = cfg.get("consumer_name") or os.getenv("LIQ_CONTEXT_CONSUMER_NAME", _DEFAULT_CONSUMER_NAME)
        self._window_ms: int = int(cfg.get("window_ms") or os.getenv("LIQ_CONTEXT_WINDOW_MS", _DEFAULT_WINDOW_MS))
        self._ttl_s: int = int(cfg.get("redis_ttl_s") or os.getenv("LIQ_CONTEXT_REDIS_TTL_S", _DEFAULT_REDIS_TTL_S))
        self._batch_size: int = int(cfg.get("batch_size") or os.getenv("LIQ_CONTEXT_BATCH_SIZE", _DEFAULT_BATCH_SIZE))
        self._poll_ms: int = int(cfg.get("poll_ms") or os.getenv("LIQ_CONTEXT_POLL_MS", _DEFAULT_POLL_MS))
        self._stress_z_thr: float = float(cfg.get("stress_z_thr") or os.getenv("LIQ_CONTEXT_STRESS_Z_THR", _DEFAULT_STRESS_Z_THR))
        self._history_max: int = int(cfg.get("history_max") or os.getenv("LIQ_CONTEXT_HISTORY_MAX", _DEFAULT_HISTORY_MAX))

        self._windows: dict[str, _SymbolWindow] = {}
        self._stop_event = asyncio.Event()
        self._task: asyncio.Task | None = None

        # Publish interval: write snapshots every poll cycle for active symbols
        self._last_publish: dict[str, int] = {}
        self._publish_interval_ms = self._poll_ms  # publish each cycle

    # ── Lifecycle ──────────────────────────────────────────────────────────────

    async def start(self) -> None:
        await self._ensure_group()
        self._task = asyncio.create_task(self._run(), name="liq_ctx_worker")
        logger.info("liq_ctx: worker started (stream=%s group=%s window=%dms)",
                    self._stream_key, self._group, self._window_ms)

    async def stop(self) -> None:
        self._stop_event.set()
        if self._task:
            try:
                await asyncio.wait_for(self._task, timeout=5.0)
            except (TimeoutError, asyncio.CancelledError):
                self._task.cancel()
        logger.info("liq_ctx: worker stopped")

    # ── Core loop ──────────────────────────────────────────────────────────────

    async def _run(self) -> None:
        last_id = ">"
        poll_s = self._poll_ms / 1000.0

        while not self._stop_event.is_set():
            try:
                messages = await self._redis.xreadgroup(
                    groupname=self._group,
                    consumername=self._consumer,
                    streams={self._stream_key: last_id},
                    count=self._batch_size,
                    block=int(self._poll_ms),
                )

                ids_to_ack: list[str] = []
                if messages:
                    for _stream_name, entries in messages:
                        for msg_id, fields in entries:
                            evt = _parse_liq_event(fields)
                            if evt:
                                win = self._get_or_create_window(evt.symbol)
                                win.push(evt)
                            ids_to_ack.append(
                                msg_id.decode() if isinstance(msg_id, bytes) else msg_id
                            )

                    if ids_to_ack:
                        await self._redis.xack(self._stream_key, self._group, *ids_to_ack)

                # Publish snapshots for all known symbols
                await self._publish_all()

            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.warning("liq_ctx: loop error: %s", exc)
                await asyncio.sleep(poll_s)

    async def _publish_all(self) -> None:
        now_ms = _now_ms()
        for symbol, window in list(self._windows.items()):
            snap = window.build_snapshot(symbol, now_ms)
            key = CTX_LIQ_PREFIX + symbol
            try:
                await self._redis.set(key, snap.to_json(), ex=self._ttl_s)
            except Exception as exc:
                logger.debug("liq_ctx: redis SET %s error: %s", key, exc)

    # ── Helpers ────────────────────────────────────────────────────────────────

    def _get_or_create_window(self, symbol: str) -> _SymbolWindow:
        sym = symbol.upper()
        if sym not in self._windows:
            self._windows[sym] = _SymbolWindow(
                window_ms=self._window_ms,
                history_max=self._history_max,
                stress_z_thr=self._stress_z_thr,
            )
        return self._windows[sym]

    async def _ensure_group(self) -> None:
        try:
            await self._redis.xgroup_create(
                self._stream_key, self._group, id="$", mkstream=True
            )
            logger.info("liq_ctx: consumer group created: %s/%s", self._stream_key, self._group)
        except Exception as exc:
            # BUSYGROUP = group already exists, that's fine
            msg = str(exc)
            if "BUSYGROUP" not in msg:
                logger.warning("liq_ctx: xgroup_create warning: %s", exc)


# ─── Sync read helper for gate ─────────────────────────────────────────────────

def read_liq_context_sync(redis, *, symbol: str) -> dict[str, Any] | None:
    """Synchronous read of ctx:liq:{SYMBOL}. Returns dict or None (fail-open)."""
    try:
        raw = redis.get(CTX_LIQ_PREFIX + (symbol or "").upper())
        if raw is None:
            return None
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", errors="replace")
        return json.loads(raw)
    except Exception:
        return None


async def aread_liq_context(redis, *, symbol: str) -> dict[str, Any] | None:
    """Async read of ctx:liq:{SYMBOL}. Returns dict or None (fail-open)."""
    try:
        raw = await redis.get(CTX_LIQ_PREFIX + (symbol or "").upper())
        if raw is None:
            return None
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", errors="replace")
        return json.loads(raw)
    except Exception:
        return None
