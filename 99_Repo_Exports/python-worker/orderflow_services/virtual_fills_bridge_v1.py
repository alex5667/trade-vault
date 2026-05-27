#!/usr/bin/env python3
"""virtual_fills_bridge_v1.py — Bridge virtual trade closes → stream:fills:filled.

Reads `trades:closed` stream and publishes lightweight synthetic fill events
to `stream:fills:filled` so that `tca_priors_exporter_v1.py` can build
rolling TCA EMAs in shadow / paper-trading mode (where orders:exec has no
ENTRY_FILLED events because no real orders are submitted).

Synthetic fill format (compatible with tca_priors_exporter_v1._extract_tca_update):
  symbol              — trade symbol
  kind                — signal kind (of / iceberg / delta_spike …)
  ts_ms               — fill timestamp (fill_ts_ms from trades:closed)
  eff_spread_bps      — spread_bps_at_entry (pre-computed by analytics_db)
  is_bps              — realized_slippage_bps
  arrival_mid         — entry_px (proxy; book replay not available in shadow)
  fill_px             — entry_px (no actual slippage displacement)
  mid_after_1s_bps    — 0.0 (unknown without book replay)
  mid_after_5s_bps    — 0.0
  source              — "virtual_fills_bridge"

ENV
  VFB_PORT            (default 9148)
  VFB_TRADES_STREAM   (default "trades:closed")
  VFB_FILLS_STREAM    (default "stream:fills:filled")
  VFB_GROUP           (default "virtual-fills-bridge")
  VFB_CONSUMER        (default "virtual-fills-bridge-1")
  VFB_BATCH           (default 200)
  VFB_MAXLEN          (default 50000)
  VFB_BACKFILL_ID     (default "0" — start from beginning on first run)
  REDIS_URL           (default "redis://redis-worker-1:6379/0")
"""
from __future__ import annotations

import logging
import os
import signal
import time
from typing import Any

from prometheus_client import Counter, Gauge, start_http_server  # type: ignore

logger = logging.getLogger("virtual_fills_bridge")


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)) or str(default))
    except (TypeError, ValueError):
        return default


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        if v is None:
            return default
        return float(v)
    except (TypeError, ValueError):
        return default


def _kind_from_signal_id(signal_id: str) -> str:
    """Extract kind prefix from signal_id like 'of:BTCUSDT:123:B' → 'of'."""
    if not signal_id:
        return "unknown"
    prefix = signal_id.split(":")[0].lower()
    # Normalise common prefixes
    if prefix in ("of", "crypto-of"):
        return "of"
    if prefix in ("iceberg",):
        return "iceberg"
    if prefix in ("delta_spike", "delta-spike"):
        return "delta_spike"
    return prefix or "unknown"


def _process_batch(
    r: Any,
    items: list[tuple[str, dict[str, str]]],
    *,
    fills_stream: str,
    maxlen: int,
    c_published: Any,
    c_skipped: Any,
) -> int:
    published = 0
    for sid, fields in items:
        symbol = fields.get("symbol", "")
        if not symbol:
            c_skipped.labels(reason="no_symbol").inc()
            continue

        signal_id = fields.get("signal_id") or fields.get("sid") or ""
        kind = _kind_from_signal_id(signal_id) if signal_id else (fields.get("kind") or "of")

        fill_ts_ms = _safe_float(fields.get("fill_ts_ms") or fields.get("entry_ts_ms") or fields.get("ts_ms"))
        if fill_ts_ms <= 0:
            c_skipped.labels(reason="no_ts").inc()
            continue

        eff_spread_bps = _safe_float(fields.get("spread_bps_at_entry"))
        is_bps = _safe_float(fields.get("realized_slippage_bps") or fields.get("slippage_bps_est"))
        entry_px = _safe_float(fields.get("entry_px") or fields.get("entry_price"))

        fill_record = {
            "symbol": symbol,
            "kind": kind,
            "ts_ms": str(int(fill_ts_ms)),
            "eff_spread_bps": str(eff_spread_bps),
            "is_bps": str(is_bps),
            "arrival_mid": str(entry_px),
            "price": str(entry_px),       # tca_priors_exporter reads "price" or "avg_px"
            "mid_after_1s_bps": "0.0",
            "mid_after_5s_bps": "0.0",
            "source": "virtual_fills_bridge",
        }

        try:
            r.xadd(fills_stream, fill_record, maxlen=maxlen, approximate=True)
            c_published.labels(symbol=symbol, kind=kind).inc()
            published += 1
            # Also write a "default" bucket copy so of_confirm_engine's scenario-based
            # fallback can always find aggregate TCA priors regardless of kind.
            if kind != "default":
                r.xadd(
                    fills_stream,
                    {**fill_record, "kind": "default"},
                    maxlen=maxlen,
                    approximate=True,
                )
        except Exception as exc:
            logger.warning("XADD fills stream failed: %s", exc)
            c_skipped.labels(reason="xadd_error").inc()

    return published


def main() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    port = _env_int("VFB_PORT", 9148)
    trades_stream = os.getenv("VFB_TRADES_STREAM", "trades:closed")
    fills_stream = os.getenv("VFB_FILLS_STREAM", "stream:fills:filled")
    group = os.getenv("VFB_GROUP", "virtual-fills-bridge")
    consumer = os.getenv("VFB_CONSUMER", "virtual-fills-bridge-1")
    batch = _env_int("VFB_BATCH", 200)
    maxlen = _env_int("VFB_MAXLEN", 50000)
    poll_ms = _env_int("VFB_POLL_MS", 5000)

    logger.info(
        "Starting virtual_fills_bridge: port=%d in=%s out=%s",
        port, trades_stream, fills_stream,
    )

    start_http_server(port)

    c_published = Counter(
        "vfb_fills_published_total",
        "Virtual fills written to stream:fills:filled",
        ["symbol", "kind"],
    )
    c_skipped = Counter(
        "vfb_fills_skipped_total",
        "Virtual fills skipped",
        ["reason"],
    )
    g_lag = Gauge("vfb_consumer_lag", "Consumer lag (pending entries)")

    from core.redis_client import get_redis
    r = get_redis()

    # Create consumer group — start from 0 to backfill all history
    try:
        r.xgroup_create(trades_stream, group, id="0", mkstream=False)
        logger.info("Created consumer group '%s' starting from beginning (backfill mode)", group)
    except Exception as exc:
        if "BUSYGROUP" in str(exc):
            logger.info("Consumer group '%s' already exists — resuming from last ACK", group)
        else:
            logger.warning("xgroup_create error (ignored): %s", exc)

    running = True

    def _stop(sig, frame):  # noqa: ANN001
        nonlocal running
        running = False

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    while running:
        try:
            results = r.xreadgroup(
                groupname=group,
                consumername=consumer,
                streams={trades_stream: ">"},
                count=batch,
                block=poll_ms,
            )
            if not results:
                continue

            for _stream_name, items in results:
                if not items:
                    continue
                ack_ids = [sid for sid, _ in items]
                published = _process_batch(
                    r,
                    items,
                    fills_stream=fills_stream,
                    maxlen=maxlen,
                    c_published=c_published,
                    c_skipped=c_skipped,
                )
                if ack_ids:
                    r.xack(trades_stream, group, *ack_ids)
                if published:
                    logger.info("Published %d virtual fills from %d closes", published, len(items))

            # Update lag metric
            try:
                info = r.xpending(trades_stream, group)  # type: ignore[union-attr]
                pending = info.get("pending", 0) if isinstance(info, dict) else 0
                g_lag.set(int(pending))
            except Exception:
                pass

        except Exception as exc:
            logger.error("Main loop error: %s", exc)
            time.sleep(2)

    logger.info("virtual_fills_bridge stopped")


if __name__ == "__main__":
    main()
