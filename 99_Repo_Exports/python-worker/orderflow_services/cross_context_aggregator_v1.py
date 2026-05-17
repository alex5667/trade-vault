#!/usr/bin/env python3
"""cross_context_aggregator_v1.py — ADR-0006.

Aggregates BTC/ETH anchor returns + liquidation/OI/funding signals into
Redis hashes that of_confirm_engine.py hydrates via one pipelined HMGET
per signal.

Subscribes to:
  - stream:tick_BTCUSDT, stream:tick_ETHUSDT  (anchor mid-price ticks)
  - stream:liq_evt                            (liquidation events, all symbols)

Publishes (TTL 60s, refreshed continuously):
  ctx:anchor:btc:returns        { ret_30s, ret_1m, ret_5m, ts_ms }
  ctx:anchor:eth:returns        { ret_30s, ret_1m, ret_5m, ts_ms }
  ctx:liq:{symbol}:imb          { long_n_1m, short_n_1m, long_n_5m, short_n_5m,
                                  imb_1m, imb_5m, ts_ms }

OI / funding are sourced from ctx:deriv:{symbol} (REST-polled by derivs handler)
and re-published as ctx:oi:{symbol}:delta on a 30s refresh cycle.

ENV
  CROSS_CTX_PORT                  (default 9145)
  CROSS_CTX_GROUP                 (default "cross-context-aggregator")
  CROSS_CTX_BATCH                 (default 500)
  CROSS_CTX_ANCHORS               (default "BTCUSDT,ETHUSDT")
  CROSS_CTX_TTL_SEC               (default 60)
  CROSS_CTX_MAX_LAG_MS            (default 2000 — feature gate threshold)
  CROSS_CTX_LIQ_SYMBOLS           (default "" = all symbols)
  CROSS_CTX_OI_SYMBOLS            (default same as CROSS_CTX_ANCHORS)
  CROSS_CTX_OI_REFRESH_SEC        (default 30)
"""
from __future__ import annotations

import logging
import math
import os
import signal
import time
from collections import deque
from typing import Any

from prometheus_client import REGISTRY, Counter, Gauge, start_http_server  # type: ignore

from core.redis_client import get_redis
from utils.time_utils import get_ny_time_millis

logger = logging.getLogger("cross_context_aggregator")


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name) or default)
    except Exception:
        return default


def _get_or_create_gauge(name: str, doc: str, labels: list[str]) -> Gauge:
    try:
        return Gauge(name, doc, labels)
    except ValueError:
        for c in REGISTRY._collector_to_names:
            if name in REGISTRY._collector_to_names[c]:
                return c  # type: ignore
        raise


def _get_or_create_counter(name: str, doc: str, labels: list[str]) -> Counter:
    try:
        return Counter(name, doc, labels)
    except ValueError:
        for c in REGISTRY._collector_to_names:
            if name in REGISTRY._collector_to_names[c]:
                return c  # type: ignore
        raise


def _decode(val: Any) -> str:
    if isinstance(val, (bytes, bytearray)):
        return val.decode("utf-8", "ignore")
    return str(val) if val is not None else ""


def _safe_float(val: Any) -> float:
    try:
        if val is None:
            return float("nan")
        if isinstance(val, (bytes, bytearray)):
            val = val.decode("utf-8", "ignore")
        f = float(val)
        return f if math.isfinite(f) else float("nan")
    except Exception:
        return float("nan")


class AnchorReturnTracker:
    """Rolling deque of (ts_ms, mid, ofi_proxy, microprice_shift_bps) for anchor symbols."""

    WINDOWS_MS = {"30s": 30_000, "1m": 60_000, "5m": 300_000}
    MAX_LEN = 4096  # ~13 min at 30Hz tick rate

    def __init__(self) -> None:
        # tuple: (ts_ms, mid, ofi_proxy, microprice_shift_bps)
        self._buf: dict[str, deque[tuple[int, float, float, float]]] = {}

    def push(
        self,
        symbol: str,
        ts_ms: int,
        mid: float,
        *,
        ofi_proxy: float = 0.0,
        microprice_shift_bps: float = 0.0,
    ) -> dict[str, float]:
        if not math.isfinite(mid) or mid <= 0:
            return {}
        _ofi = ofi_proxy if math.isfinite(ofi_proxy) else 0.0
        _mps = microprice_shift_bps if math.isfinite(microprice_shift_bps) else 0.0
        buf = self._buf.setdefault(symbol, deque(maxlen=self.MAX_LEN))
        buf.append((ts_ms, mid, _ofi, _mps))

        cutoffs = {label: ts_ms - w for label, w in self.WINDOWS_MS.items()}
        anchors: dict[str, tuple[int, float, float, float]] = {}
        for sample in buf:
            for label, cutoff in cutoffs.items():
                if label in anchors:
                    continue
                if sample[0] >= cutoff:
                    anchors[label] = sample
                    break

        out: dict[str, float] = {}
        for label in self.WINDOWS_MS:
            ref = anchors.get(label)
            if ref is None or ref[1] <= 0:
                out[f"ret_{label}"] = 0.0
            else:
                out[f"ret_{label}"] = (mid - ref[1]) / ref[1]

        # 1m window OFI and microprice shift (EMA of last N samples)
        window_1m = [s for s in buf if s[0] >= ts_ms - 60_000]
        if window_1m:
            out["ofi_1m_ema"] = sum(s[2] for s in window_1m) / len(window_1m)
            out["microprice_shift_1m_ema"] = sum(s[3] for s in window_1m) / len(window_1m)
        else:
            out["ofi_1m_ema"] = 0.0
            out["microprice_shift_1m_ema"] = 0.0

        out["ts_ms"] = float(ts_ms)
        return out


class LiqImbalanceTracker:
    """Rolling deque of liquidation events per symbol → 1m / 5m imbalance.

    Event fields from stream:liq_evt:
      symbol      : str  (e.g. "BTCUSDT")
      liq_side    : str  ("long" | "short") — side of the liquidated position
      notional_usd: float

    Published to ctx:liq:{symbol}:imb:
      long_n_1m, short_n_1m   — notional liquidated longs/shorts in last 1 min
      long_n_5m, short_n_5m   — same, 5 min window
      imb_1m                  — (long_n - short_n) / total, ∈ [-1, 1], 0 if no data
      imb_5m
      ts_ms
    """

    WINDOWS_MS = {"1m": 60_000, "5m": 300_000}
    MAX_LEN = 8192  # ~136 events/s sustained → covers 60s safely

    def __init__(self, track_symbols: set[str] | None = None) -> None:
        # symbol → deque of (ts_ms, liq_side, notional_usd)
        self._buf: dict[str, deque[tuple[int, str, float]]] = {}
        self._track = track_symbols  # None = all symbols

    def _is_tracked(self, symbol: str) -> bool:
        return self._track is None or symbol in self._track

    def push(self, symbol: str, ts_ms: int, liq_side: str, notional_usd: float) -> dict[str, float] | None:
        if not self._is_tracked(symbol):
            return None
        if not math.isfinite(notional_usd) or notional_usd < 0:
            notional_usd = 0.0

        buf = self._buf.setdefault(symbol, deque(maxlen=self.MAX_LEN))
        buf.append((ts_ms, liq_side, notional_usd))

        out: dict[str, float] = {}
        for label, w in self.WINDOWS_MS.items():
            cutoff = ts_ms - w
            long_n = 0.0
            short_n = 0.0
            for evt_ts, side, notional in buf:
                if evt_ts < cutoff:
                    continue
                if side == "long":
                    long_n += notional
                elif side == "short":
                    short_n += notional
            total = long_n + short_n
            out[f"long_n_{label}"] = long_n
            out[f"short_n_{label}"] = short_n
            out[f"imb_{label}"] = (long_n - short_n) / total if total > 0 else 0.0

        out["ts_ms"] = float(ts_ms)
        return out

    def symbols(self) -> list[str]:
        return list(self._buf.keys())


def _refresh_oi_delta(
    redis_client: Any,
    symbols: list[str],
    ttl_sec: int,
    g_oi_delta: Gauge,
) -> None:
    """Periodic OI delta refresh: read ctx:deriv:{symbol} → write ctx:oi:{symbol}:delta.

    OI/funding come from REST-polled ctx:deriv:{symbol} hashes (written by the
    derivatives handler). The aggregator re-publishes a summary hash so the
    engine can HMGET a consistent view alongside anchor/liq data.
    """
    for symbol in symbols:
        try:
            raw = redis_client.hgetall(f"ctx:deriv:{symbol}")
            if not raw:
                continue
            fields = {
                (_decode(k) if isinstance(k, (bytes, bytearray)) else str(k)):
                (_decode(v) if isinstance(v, (bytes, bytearray)) else str(v))
                for k, v in raw.items()
            }
            oi_delta_1m = _safe_float(fields.get("oi_delta_1m", "nan"))
            oi_delta_5m = _safe_float(fields.get("oi_delta_5m", "nan"))
            ts_ms = _safe_float(fields.get("ts_ms", "0"))

            mapping = {
                "oi_delta_1m": f"{oi_delta_1m:.6f}" if math.isfinite(oi_delta_1m) else "0",
                "oi_delta_5m": f"{oi_delta_5m:.6f}" if math.isfinite(oi_delta_5m) else "0",
                "ts_ms": f"{ts_ms:.0f}",
            }
            redis_client.hset(f"ctx:oi:{symbol}:delta", mapping=mapping)
            redis_client.expire(f"ctx:oi:{symbol}:delta", ttl_sec)

            for label, val in [("1m", oi_delta_1m), ("5m", oi_delta_5m)]:
                if math.isfinite(val):
                    g_oi_delta.labels(symbol=symbol, window=label).set(val)
        except Exception as e:
            logger.debug("OI delta refresh failed for %s: %s", symbol, e)


def main() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    port = _env_int("CROSS_CTX_PORT", 9145)
    group = os.getenv("CROSS_CTX_GROUP", "cross-context-aggregator")
    batch = _env_int("CROSS_CTX_BATCH", 500)
    ttl_sec = _env_int("CROSS_CTX_TTL_SEC", 60)
    anchors_env = os.getenv("CROSS_CTX_ANCHORS", "BTCUSDT,ETHUSDT")
    anchors = [s.strip().upper() for s in anchors_env.split(",") if s.strip()]

    liq_syms_env = os.getenv("CROSS_CTX_LIQ_SYMBOLS", "")
    liq_track = {s.strip().upper() for s in liq_syms_env.split(",") if s.strip()} or None

    oi_syms_env = os.getenv("CROSS_CTX_OI_SYMBOLS", anchors_env)
    oi_symbols = [s.strip().upper() for s in oi_syms_env.split(",") if s.strip()]
    oi_refresh_sec = _env_int("CROSS_CTX_OI_REFRESH_SEC", 30)

    logger.info(
        "Starting cross-context aggregator: port=%d group=%s anchors=%s liq_track=%s",
        port, group, anchors, "all" if liq_track is None else liq_track,
    )

    g_anchor_ret = _get_or_create_gauge(
        "cross_ctx_anchor_return",
        "Anchor symbol return over rolling window",
        ["symbol", "window"],
    )
    g_anchor_age = _get_or_create_gauge(
        "cross_ctx_anchor_age_ms",
        "Age of latest anchor tick (ms)",
        ["symbol"],
    )
    c_ticks = _get_or_create_counter(
        "cross_ctx_ticks_total",
        "Anchor ticks processed",
        ["symbol"],
    )
    g_liq_imb = _get_or_create_gauge(
        "cross_ctx_liq_imbalance",
        "Liquidation imbalance (long_notional - short_notional) / total over window",
        ["symbol", "window"],
    )
    g_oi_delta = _get_or_create_gauge(
        "cross_ctx_oi_delta",
        "Open-interest delta over window (from ctx:deriv:{symbol})",
        ["symbol", "window"],
    )
    c_liq_events = _get_or_create_counter(
        "cross_ctx_liq_events_total",
        "Liquidation events processed",
        ["symbol", "side"],
    )

    tracker = AnchorReturnTracker()
    liq_tracker = LiqImbalanceTracker(track_symbols=liq_track)
    redis_client = get_redis()

    # XREADGROUP from anchor tick streams + liq_evt stream.
    LIQ_STREAM = "stream:liq_evt"
    streams: dict[str, str] = {}
    for sym in anchors:
        stream_key = f"stream:tick_{sym}"
        streams[stream_key] = ">"
        try:
            redis_client.xgroup_create(stream_key, group, id="$", mkstream=True)
        except Exception as e:
            if "BUSYGROUP" not in str(e):
                logger.error("xgroup_create failed for %s: %s", stream_key, e)

    # Subscribe to liq_evt stream
    streams[LIQ_STREAM] = ">"
    try:
        redis_client.xgroup_create(LIQ_STREAM, group, id="$", mkstream=True)
    except Exception as e:
        if "BUSYGROUP" not in str(e):
            logger.warning("xgroup_create failed for %s: %s", LIQ_STREAM, e)

    start_http_server(port)
    logger.info("HTTP /metrics on :%d", port)

    stop = {"flag": False}

    def _sig(_a: int, _b: Any) -> None:
        stop["flag"] = True

    signal.signal(signal.SIGTERM, _sig)
    signal.signal(signal.SIGINT, _sig)

    consumer = os.getenv("CROSS_CTX_CONSUMER", "cross-context-aggregator-1")
    last_age_refresh = 0
    last_oi_refresh = 0

    while not stop["flag"]:
        try:
            resp = redis_client.xreadgroup(
                groupname=group,
                consumername=consumer,
                streams=streams,  # type: ignore[arg-type]
                count=batch,
                block=2000,
            )
        except Exception as e:
            logger.error("XREADGROUP failed: %s", e)
            time.sleep(1.0)
            continue

        if resp:
            for stream_key, messages in resp:  # type: ignore[union-attr]
                sk = _decode(stream_key)
                ack_ids: list[Any] = []

                if sk == LIQ_STREAM:
                    # ── Liquidation events ───────────────────────────────────
                    for msg_id, fields in messages:
                        ack_ids.append(msg_id)
                        try:
                            fields = {_decode(k): _decode(v) for k, v in fields.items()}
                            symbol = fields.get("symbol", "").upper()
                            if not symbol:
                                continue
                            liq_side = fields.get("liq_side", "").lower()
                            notional = _safe_float(fields.get("notional_usd", "0"))
                            ts_ms = int(_safe_float(fields.get("ts_ms") or str(get_ny_time_millis())))

                            state = liq_tracker.push(symbol, ts_ms, liq_side, notional)
                            if state is not None:
                                c_liq_events.labels(symbol=symbol, side=liq_side).inc()
                                redis_key = f"ctx:liq:{symbol}:imb"
                                try:
                                    redis_client.hset(redis_key, mapping={
                                        k: f"{v:.8f}" for k, v in state.items()
                                    })
                                    redis_client.expire(redis_key, ttl_sec)
                                    for label in ("1m", "5m"):
                                        imb_val = state.get(f"imb_{label}", 0.0)
                                        g_liq_imb.labels(symbol=symbol, window=label).set(imb_val)
                                except Exception as re:
                                    logger.debug("Redis HSET liq %s failed: %s", symbol, re)
                        except Exception as e:
                            logger.warning("Failed to process liq_evt %s: %s", msg_id, e)

                else:
                    # ── Anchor tick events ───────────────────────────────────
                    symbol = sk.replace("stream:tick_", "").upper()
                    short = "btc" if symbol == "BTCUSDT" else ("eth" if symbol == "ETHUSDT" else symbol.lower())
                    latest_ret_state: dict[str, float] = {}

                    for msg_id, fields in messages:
                        ack_ids.append(msg_id)
                        try:
                            fields = {_decode(k): _decode(v) for k, v in fields.items()}
                            ts_ms = int(_safe_float(fields.get("ts_ms")) or get_ny_time_millis())
                            mid = _safe_float(fields.get("mid") or fields.get("price"))
                            if not math.isfinite(mid):
                                continue
                            _tbuy = _safe_float(fields.get("taker_buy_qty") or fields.get("buy_qty") or "0")
                            _tsell = _safe_float(fields.get("taker_sell_qty") or fields.get("sell_qty") or "0")
                            _ofi_proxy = (_tbuy - _tsell) if (math.isfinite(_tbuy) and math.isfinite(_tsell)) else 0.0
                            _mps = _safe_float(fields.get("microprice_shift_bps") or fields.get("l3_microprice_shift_bps_20") or "0")
                            latest_ret_state = tracker.push(
                                symbol, ts_ms, mid,
                                ofi_proxy=_ofi_proxy,
                                microprice_shift_bps=_mps if math.isfinite(_mps) else 0.0,
                            )
                            c_ticks.labels(symbol=symbol).inc()
                        except Exception as e:
                            logger.warning("Failed to process tick %s: %s", msg_id, e)

                    if latest_ret_state:
                        for window in ("30s", "1m", "5m"):
                            val = latest_ret_state.get(f"ret_{window}", 0.0)
                            g_anchor_ret.labels(symbol=symbol, window=window).set(val)
                        redis_key = f"ctx:anchor:{short}:returns"
                        try:
                            redis_client.hset(redis_key, mapping={
                                k: f"{v:.8f}" for k, v in latest_ret_state.items()
                            })
                            redis_client.expire(redis_key, ttl_sec)
                        except Exception as e:
                            logger.warning("Redis HSET %s failed: %s", redis_key, e)

                if ack_ids:
                    try:
                        redis_client.xack(sk, group, *ack_ids)
                    except Exception:
                        pass

        now_ms = get_ny_time_millis()

        # Anchor age refresh (every 5s)
        if now_ms - last_age_refresh >= 5_000:
            for sym, buf in tracker._buf.items():
                if buf:
                    g_anchor_age.labels(symbol=sym).set(now_ms - buf[-1][0])
            last_age_refresh = now_ms

        # OI delta refresh from ctx:deriv (every oi_refresh_sec)
        if now_ms - last_oi_refresh >= oi_refresh_sec * 1_000:
            _refresh_oi_delta(redis_client, oi_symbols, ttl_sec, g_oi_delta)
            last_oi_refresh = now_ms

    logger.info("Cross-context aggregator stopped")


if __name__ == "__main__":
    main()
