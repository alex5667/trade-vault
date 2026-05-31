#!/usr/bin/env python3
"""fills_tca_enricher_v1.py — ADR-0005 fills→TCA enrichment service.

Subscribes to `orders:exec` Redis stream (executor's event log), filters for
ENTRY_FILLED / EXIT_FILLED / TP_FILLED events, captures arrival_mid at fill
time, schedules a 5s+tolerance delay, then computes TCA metrics via
book_replay_helper and publishes enriched events to `stream:fills:filled`.

Pipeline:
  orders:exec → consumer group → in-memory pending_fills queue → schedule
  6s wait → core.book_replay_helper.compute_tca_metrics → XADD stream:fills:filled
  → consumed by tca_priors_exporter_v1.py

Memory-bounded: pending_fills caps at FILLS_TCA_PENDING_MAX (default 10000)
to prevent unbounded growth on backpressure.

ENV
  FILLS_TCA_ENRICHER_PORT             (default 9147)
  FILLS_TCA_GROUP                     (default "fills-tca-enricher")
  FILLS_TCA_CONSUMER                  (default "fills-tca-enricher-1")
  FILLS_TCA_BATCH                     (default 200)
  FILLS_TCA_HORIZON_5S_DELAY_MS       (default 5500 — 5s + 500ms tolerance)
  FILLS_TCA_TOLERANCE_MS              (default 500)
  FILLS_TCA_PENDING_MAX               (default 10000)
  ORDERS_EXEC_STREAM                  (default "orders:exec")
  FILLS_FILLED_STREAM                 (default "stream:fills:filled")
"""
from __future__ import annotations

import logging
import math
import os
import signal
import threading
import time
from collections import deque
from dataclasses import dataclass
from typing import Any

from prometheus_client import REGISTRY, Counter, Gauge, start_http_server  # type: ignore

from core.book_replay_helper import compute_tca_metrics, get_mid_at
from core.redis_client import get_redis
from utils.time_utils import get_ny_time_millis

logger = logging.getLogger("fills_tca_enricher")


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name) or default)
    except Exception:
        return default


def _decode(v: Any) -> str:
    if isinstance(v, (bytes, bytearray)):
        return v.decode("utf-8", "ignore")
    return str(v) if v is not None else ""


def _safe_float(v: Any) -> float:
    try:
        if v is None:
            return float("nan")
        if isinstance(v, (bytes, bytearray)):
            v = v.decode("utf-8", "ignore")
        f = float(v)
        return f if math.isfinite(f) else float("nan")
    except Exception:
        return float("nan")


def _get_or_create_counter(name: str, doc: str, labels: list[str]) -> Counter:
    try:
        return Counter(name, doc, labels)
    except ValueError:
        for c in REGISTRY._collector_to_names:
            if name in REGISTRY._collector_to_names[c]:
                return c  # type: ignore
        raise


def _get_or_create_gauge(name: str, doc: str, labels: list[str]) -> Gauge:
    try:
        return Gauge(name, doc, labels)
    except ValueError:
        for c in REGISTRY._collector_to_names:
            if name in REGISTRY._collector_to_names[c]:
                return c  # type: ignore
        raise


@dataclass
class PendingFill:
    """Fill scheduled for TCA enrichment after a 5s+tolerance delay."""
    fill_id: str
    sid: str
    symbol: str
    side: str
    fill_price: float
    arrival_mid: float
    fill_ts_ms: int
    enrich_at_ms: int
    kind: str
    raw_fields: dict[str, str]


def _is_fill_event(event_type: str) -> bool:
    et = (event_type or "").upper()
    return et in (
        "ENTRY_FILLED",
        "EXIT_FILLED",
        "TP_FILLED",
        "TP1_FILLED",
        "TP2_FILLED",
        "TP3_FILLED",
        "FILLED",
        "EXCHANGE_FILL",
    )


def _extract_fill_event(fields: dict[str, str], redis_client: Any) -> PendingFill | None:
    """Build a PendingFill from execution event fields, capturing arrival_mid."""
    event_type = fields.get("event_type") or fields.get("status") or ""
    if not _is_fill_event(event_type):
        return None
    sid = fields.get("sid") or fields.get("signal_id") or ""
    symbol = (fields.get("symbol") or "").upper()
    side = (fields.get("side") or "").upper()
    fill_price = _safe_float(fields.get("avg_price") or fields.get("price") or fields.get("fill_price"))
    fill_ts_ms = int(_safe_float(fields.get("ts_ms") or fields.get("ts_event_ms")) or get_ny_time_millis())
    if not (symbol and side and math.isfinite(fill_price)):
        return None

    # arrival_mid: prefer field if executor populates it, else snapshot from book at exec_start_ts
    arrival_mid = _safe_float(fields.get("arrival_mid") or fields.get("mid_at_arrival"))
    if not math.isfinite(arrival_mid) or arrival_mid <= 0:
        # Capture mid at fill time as fallback (eff_spread will be ~0; flag via separate metric)
        ts_arrival_raw = _safe_float(fields.get("ts_exec_start_ms") or fields.get("ts_queue_ms"))
        ts_arrival = int(ts_arrival_raw) if math.isfinite(ts_arrival_raw) else fill_ts_ms
        am, _actual_ts = get_mid_at(redis_client, symbol, ts_arrival, tolerance_ms=500)
        arrival_mid = am if (am and am > 0) else fill_price

    horizon_delay_ms = _env_int("FILLS_TCA_HORIZON_5S_DELAY_MS", 5_500)
    enrich_at_ms = fill_ts_ms + horizon_delay_ms

    fill_id = sid + ":" + str(fill_ts_ms)
    kind = (fields.get("kind") or fields.get("scenario") or "default")

    return PendingFill(
        fill_id=fill_id,
        sid=sid,
        symbol=symbol,
        side=side,
        fill_price=fill_price,
        arrival_mid=arrival_mid,
        fill_ts_ms=fill_ts_ms,
        enrich_at_ms=enrich_at_ms,
        kind=kind,
        raw_fields=fields,
    )


def _enrich_and_publish(
    pending: PendingFill,
    redis_client: Any,
    *,
    out_stream: str,
    tolerance_ms: int,
    c_published: Counter,
    c_enrich_errors: Counter,
) -> None:
    try:
        tca = compute_tca_metrics(
            redis_client,
            symbol=pending.symbol,
            fill_price=pending.fill_price,
            arrival_mid=pending.arrival_mid,
            fill_ts_ms=pending.fill_ts_ms,
            side=pending.side,
            horizons_ms=(1_000, 5_000),
            tolerance_ms=tolerance_ms,
        )
        out: dict[str, str] = {
            "sid": pending.sid,
            "symbol": pending.symbol,
            "side": pending.side,
            "kind": pending.kind,
            "ts_ms": str(pending.fill_ts_ms),
            "price": f"{pending.fill_price:.10g}",
            "arrival_mid": f"{pending.arrival_mid:.10g}",
            # TCA metrics
            "eff_spread_bps": f"{tca['eff_spread_bps']:.6f}",
            "is_bps": f"{tca['is_bps']:.6f}",
            "mid_after_1s_bps": f"{tca['mid_after_1s_bps']:.6f}",
            "mid_after_5s_bps": f"{tca['mid_after_5s_bps']:.6f}",
            "realized_spread_1s_bps": f"{tca['realized_spread_1s_bps']:.6f}",
            "realized_spread_5s_bps": f"{tca['realized_spread_5s_bps']:.6f}",
            "perm_impact_1s_bps": f"{tca['perm_impact_1s_bps']:.6f}",
            "perm_impact_5s_bps": f"{tca['perm_impact_5s_bps']:.6f}",
        }
        try:
            redis_client.xadd(out_stream, out, maxlen=50_000, approximate=True)
            c_published.labels(symbol=pending.symbol, kind=pending.kind).inc()
        except Exception as e:
            logger.error("XADD %s failed: %s", out_stream, e)
            c_enrich_errors.labels(reason="xadd_failed").inc()
    except Exception as e:
        logger.warning("Enrichment failed for %s: %s", pending.fill_id, e)
        c_enrich_errors.labels(reason="exception").inc()


def _drain_due_fills(
    pending: deque[PendingFill],
    now_ms: int,
    redis_client: Any,
    *,
    out_stream: str,
    tolerance_ms: int,
    c_published: Counter,
    c_enrich_errors: Counter,
) -> int:
    drained = 0
    while pending and pending[0].enrich_at_ms <= now_ms:
        p = pending.popleft()
        _enrich_and_publish(
            p, redis_client,
            out_stream=out_stream, tolerance_ms=tolerance_ms,
            c_published=c_published, c_enrich_errors=c_enrich_errors,
        )
        drained += 1
    return drained


def main() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    port = _env_int("FILLS_TCA_ENRICHER_PORT", 9147)
    group = os.getenv("FILLS_TCA_GROUP", "fills-tca-enricher")
    consumer = os.getenv("FILLS_TCA_CONSUMER", "fills-tca-enricher-1")
    batch = _env_int("FILLS_TCA_BATCH", 200)
    tolerance_ms = _env_int("FILLS_TCA_TOLERANCE_MS", 500)
    pending_max = _env_int("FILLS_TCA_PENDING_MAX", 10_000)
    in_stream = os.getenv("ORDERS_EXEC_STREAM", "orders:exec")
    out_stream = os.getenv("FILLS_FILLED_STREAM", "stream:fills:filled")

    logger.info(
        "Starting fills TCA enricher: port=%d in=%s out=%s pending_max=%d",
        port, in_stream, out_stream, pending_max,
    )

    c_received = _get_or_create_counter(
        "fills_tca_received_total",
        "Execution events received from orders:exec",
        ["event_type"],
    )
    c_pended = _get_or_create_counter(
        "fills_tca_pended_total",
        "Fills enqueued for delayed enrichment",
        ["symbol", "kind"],
    )
    c_dropped = _get_or_create_counter(
        "fills_tca_dropped_total",
        "Fills dropped (queue full)",
        ["reason"],
    )
    c_published = _get_or_create_counter(
        "fills_tca_published_total",
        "Enriched fills published to stream:fills:filled",
        ["symbol", "kind"],
    )
    c_enrich_errors = _get_or_create_counter(
        "fills_tca_enrich_errors_total",
        "Errors during TCA enrichment",
        ["reason"],
    )
    g_pending_size = _get_or_create_gauge(
        "fills_tca_pending_size",
        "Number of fills awaiting 5s delay before enrichment",
        [],
    )
    g_oldest_pending_ms = _get_or_create_gauge(
        "fills_tca_oldest_pending_ms",
        "Age of oldest pending fill in queue (ms since enrich_at)",
        [],
    )

    pending: deque[PendingFill] = deque()
    redis_client = get_redis()
    try:
        redis_client.xgroup_create(in_stream, group, id="$", mkstream=True)
    except Exception as e:
        if "BUSYGROUP" not in str(e):
            logger.error("xgroup_create failed: %s", e)

    start_http_server(port)
    logger.info("HTTP /metrics on :%d", port)

    stop = {"flag": False}

    def _sig(_a: int, _b: Any) -> None:
        stop["flag"] = True

    signal.signal(signal.SIGTERM, _sig)
    signal.signal(signal.SIGINT, _sig)

    pending_lock = threading.Lock()

    def _drain_loop() -> None:
        """Background loop that drains due fills every 200ms."""
        while not stop["flag"]:
            now_ms = get_ny_time_millis()
            with pending_lock:
                drained = _drain_due_fills(
                    pending, now_ms, redis_client,
                    out_stream=out_stream, tolerance_ms=tolerance_ms,
                    c_published=c_published, c_enrich_errors=c_enrich_errors,
                )
                g_pending_size.set(len(pending))
                if pending:
                    g_oldest_pending_ms.set(max(0, now_ms - pending[0].enrich_at_ms))
                else:
                    g_oldest_pending_ms.set(0)
            if drained == 0:
                time.sleep(0.2)

    drain_thread = threading.Thread(target=_drain_loop, name="fills-tca-drain", daemon=True)
    drain_thread.start()

    while not stop["flag"]:
        try:
            resp = redis_client.xreadgroup(
                groupname=group,
                consumername=consumer,
                streams={in_stream: ">"},
                count=batch,
                block=2000,
            )
        except Exception as e:
            logger.error("XREADGROUP failed: %s", e)
            time.sleep(1.0)
            continue

        if not resp:
            continue

        ack_ids: list[Any] = []
        for _sk, messages in resp:  # type: ignore[union-attr]
            for msg_id, fields in messages:
                ack_ids.append(msg_id)
                try:
                    fields = {_decode(k): _decode(v) for k, v in fields.items()}
                    et = fields.get("event_type") or fields.get("status") or "unknown"
                    c_received.labels(event_type=et).inc()
                    pf = _extract_fill_event(fields, redis_client)
                    if pf is None:
                        continue
                    with pending_lock:
                        if len(pending) >= pending_max:
                            c_dropped.labels(reason="queue_full").inc()
                            continue
                        pending.append(pf)
                    c_pended.labels(symbol=pf.symbol, kind=pf.kind).inc()
                except Exception as e:
                    logger.warning("Failed to process exec event %s: %s", msg_id, e)

        if ack_ids:
            try:
                redis_client.xack(in_stream, group, *ack_ids)
            except Exception:
                pass

    logger.info("Fills TCA enricher stopped (pending=%d undelivered)", len(pending))


if __name__ == "__main__":
    main()
