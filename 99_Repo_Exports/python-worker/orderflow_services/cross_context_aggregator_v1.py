#!/usr/bin/env python3
"""cross_context_aggregator_v1.py — ADR-0006 skeleton.

Aggregates BTC/ETH anchor returns + liquidation/OI/funding signals into
Redis hashes that of_confirm_engine.py hydrates via one pipelined HMGET
per signal.

Subscribes to:
  - stream:tick_BTCUSDT, stream:tick_ETHUSDT  (anchor mid-price ticks)
  - stream:liq_evt                            (liquidation events)
  - stream:oi_*, stream:funding_*             (existing handlers)

Publishes (TTL 60s, refreshed continuously):
  ctx:anchor:btc:returns        { ret_30s, ret_1m, ret_5m, ts_ms }
  ctx:anchor:eth:returns        { ret_30s, ret_1m, ret_5m, ts_ms }
  ctx:liq:{symbol}:imb          { long_n_1m, short_n_1m, long_n_5m, short_n_5m, ts_ms }

STATUS: SKELETON. Anchor return computation is implemented; liquidation,
OI, funding aggregation marked TODO — wire to existing Binance feed handlers.
See /home/alex/Apps/Obsidian/trade-vault/80_Research/ADR-0006 ...md

ENV
  CROSS_CTX_PORT                  (default 9145)
  CROSS_CTX_GROUP                 (default "cross-context-aggregator")
  CROSS_CTX_BATCH                 (default 500)
  CROSS_CTX_ANCHORS               (default "BTCUSDT,ETHUSDT")
  CROSS_CTX_TTL_SEC               (default 60)
  CROSS_CTX_MAX_LAG_MS            (default 2000 — feature gate threshold)
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
    """Rolling deque of (ts_ms, mid) for anchor symbols; computes 30s/1m/5m returns."""

    WINDOWS_MS = {"30s": 30_000, "1m": 60_000, "5m": 300_000}
    MAX_LEN = 4096  # ~13 min at 30Hz tick rate

    def __init__(self) -> None:
        self._buf: dict[str, deque[tuple[int, float]]] = {}

    def push(self, symbol: str, ts_ms: int, mid: float) -> dict[str, float]:
        if not math.isfinite(mid) or mid <= 0:
            return {}
        buf = self._buf.setdefault(symbol, deque(maxlen=self.MAX_LEN))
        buf.append((ts_ms, mid))

        cutoffs = {label: ts_ms - w for label, w in self.WINDOWS_MS.items()}
        anchors: dict[str, float] = {}
        # Walk buffer once, capture first sample older than each cutoff.
        for sample_ts, sample_mid in buf:
            for label, cutoff in cutoffs.items():
                if label in anchors:
                    continue
                if sample_ts >= cutoff:
                    anchors[label] = sample_mid
                    break

        out: dict[str, float] = {}
        for label in self.WINDOWS_MS:
            ref = anchors.get(label)
            if ref is None or ref <= 0:
                out[f"ret_{label}"] = 0.0
            else:
                out[f"ret_{label}"] = (mid - ref) / ref
        out["ts_ms"] = float(ts_ms)
        return out


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

    logger.info(
        "Starting cross-context aggregator: port=%d group=%s anchors=%s (SKELETON)",
        port, group, anchors,
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
    # TODO(ADR-0006): wire to stream:liq_evt
    g_liq_imb = _get_or_create_gauge(
        "cross_ctx_liq_imbalance",
        "Liquidation imbalance (long_notional - short_notional) / total over window",
        ["symbol", "window"],
    )
    # TODO(ADR-0006): wire to OI / funding handlers
    g_oi_delta = _get_or_create_gauge(
        "cross_ctx_oi_delta",
        "Open-interest delta over window (skeleton — wire to OI handler)",
        ["symbol", "window"],
    )

    tracker = AnchorReturnTracker()
    redis_client = get_redis()

    # XREADGROUP from each anchor's tick stream.
    streams: dict[str, str] = {}
    for sym in anchors:
        stream_key = f"stream:tick_{sym}"
        streams[stream_key] = ">"
        try:
            redis_client.xgroup_create(stream_key, group, id="$", mkstream=True)
        except Exception as e:
            if "BUSYGROUP" not in str(e):
                logger.error("xgroup_create failed for %s: %s", stream_key, e)

    start_http_server(port)
    logger.info("HTTP /metrics on :%d", port)

    stop = {"flag": False}

    def _sig(_a: int, _b: Any) -> None:
        stop["flag"] = True

    signal.signal(signal.SIGTERM, _sig)
    signal.signal(signal.SIGINT, _sig)

    consumer = os.getenv("CROSS_CTX_CONSUMER", "cross-context-aggregator-1")
    last_age_refresh = 0
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
                # stream:tick_BTCUSDT → BTCUSDT
                symbol = sk.replace("stream:tick_", "").upper()
                short = "btc" if symbol == "BTCUSDT" else ("eth" if symbol == "ETHUSDT" else symbol.lower())
                ack_ids: list[Any] = []
                latest_ret_state: dict[str, float] = {}

                for msg_id, fields in messages:
                    ack_ids.append(msg_id)
                    try:
                        fields = {_decode(k): _decode(v) for k, v in fields.items()}
                        ts_ms = int(_safe_float(fields.get("ts_ms")) or get_ny_time_millis())
                        mid = _safe_float(fields.get("mid") or fields.get("price"))
                        if not math.isfinite(mid):
                            continue
                        latest_ret_state = tracker.push(symbol, ts_ms, mid)
                        c_ticks.labels(symbol=symbol).inc()
                    except Exception as e:
                        logger.warning("Failed to process tick %s: %s", msg_id, e)

                if latest_ret_state:
                    # Publish per-window gauge + Redis hash for downstream HMGET
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

        # Anchor age publish
        now_ms = get_ny_time_millis()
        if now_ms - last_age_refresh >= 5_000:
            for sym, buf in tracker._buf.items():
                if buf:
                    g_anchor_age.labels(symbol=sym).set(now_ms - buf[-1][0])
            last_age_refresh = now_ms

        # TODO(ADR-0006): subscribe to stream:liq_evt, stream:oi_*, funding feeds
        # and populate g_liq_imb / g_oi_delta gauges + Redis hashes
        # `ctx:liq:{symbol}:imb`, `ctx:oi:{symbol}:delta`. Currently emitting
        # zero placeholders so downstream contract holds.
        _ = (g_liq_imb, g_oi_delta)

    logger.info("Cross-context aggregator stopped")


if __name__ == "__main__":
    main()
