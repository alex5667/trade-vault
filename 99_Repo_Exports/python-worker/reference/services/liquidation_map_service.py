# -*- coding: utf-8 -*-
"""services/liquidation_map_service.py

Фаза B: Python сервис построения "карты ликвидаций" (liquidation heatmap).

Пайплайн:
  Go-worker (WS ingest) -> Redis Streams stream:liq_evt -> this service (consumer group)
    -> Redis keys (snapshots) + optional snapshot stream.

Ключевые свойства (под ваш trade стек):
- Детерминизм времени: epoch ms, строгая валидация bad time.
- Контролируемое качество данных: validate → DLQ (dlq:<stream>) + ACK.
- Наблюдаемость: Prometheus метрики, лаги, throughput, snapshot sizes.

Запуск:
  python services/liquidation_map_service.py

ENV (минимум):
  REDIS_URL=redis://redis-worker-1:6379/0  (опционально)
  LIQ_EVT_STREAM=stream:liq_evt
  LIQMAP_GROUP=liqmap_group

  # windows
  LIQMAP_WINDOWS=5m,1h,4h,24h
  LIQMAP_PUBLISH_INTERVAL_MS=1000

  # bucket
  LIQMAP_BUCKET_MODE=log_bps|log_pct|abs
  LIQMAP_BUCKET_BPS=50

  # snapshots
  LIQMAP_SNAPSHOT_KEY_PREFIX=liqmap:snapshot
  LIQMAP_SNAPSHOT_TTL_SEC=30
  LIQMAP_MAX_LEVELS=250
  LIQMAP_RANGE_PCT=5

  # optional stream publishing (для WS push через backend)
  LIQMAP_PUBLISH_STREAM_ENABLED=0
  LIQMAP_SNAPSHOT_STREAM_PREFIX=stream:liqmap_snapshot
  LIQMAP_SNAPSHOT_STREAM_MAXLEN=20000

  # metrics
  LIQMAP_METRICS_PORT=9112

NOTE:
- Сервис intentionally "fail-soft": плохие сообщения уходят в DLQ, чтобы не стопорить группу.
"""

from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import json
import os
import time
import socket
import logging
from decimal import Decimal
from typing import Dict, List, Optional, Set, Tuple

from prometheus_client import Counter, Gauge, Histogram, start_http_server

# Bootstrap import paths (works for both repo-root/services and tick_flow_full/services copies)

def _bootstrap_paths() -> None:  # pragma: no cover
    import sys
    here = os.path.dirname(os.path.abspath(__file__))
    candidates = [
        os.path.abspath(os.path.join(here, '..')),
        os.path.abspath(os.path.join(here, '..', '..')),
        os.path.abspath(os.path.join(here, '..', '..', '..')),
    ]
    for root in candidates:
        if os.path.isfile(os.path.join(root, 'services', '__init__.py')) and os.path.isdir(os.path.join(root, 'tick_flow_full', 'core')):
            if root not in sys.path:
                sys.path.insert(0, root)
            tf = os.path.join(root, 'tick_flow_full')
            if tf not in sys.path:
                sys.path.insert(0, tf)
            return


try:
    from core.redis_client import get_redis
except Exception:  # pragma: no cover
    _bootstrap_paths()
    from core.redis_client import get_redis  # type: ignore

from services.redis_stream_runner_base import RedisStreamRunner, StreamMsg
from services.liquidation_map_core import (
    Bucketizer,
    LiqMapWindowAgg,
    normalize_liq_event,
    _safe_decimal_str,
    format_decimal,
    format_price,
)


# ─────────────────────────────────────────────────────────────────────────────
# Metrics
# ─────────────────────────────────────────────────────────────────────────────

liq_evt_read_total = Counter("liqmap_evt_read_total", "Total liquidation events read")
liq_evt_ok_total = Counter("liqmap_evt_ok_total", "Total liquidation events accepted", ["symbol"])
liq_evt_drop_total = Counter("liqmap_evt_drop_total", "Total liquidation events dropped", ["reason"])
liq_evt_dlq_total = Counter("liqmap_evt_dlq_total", "Total liquidation events sent to DLQ", ["reason"])

liqmap_levels_gauge = Gauge("liqmap_levels", "Number of levels in last snapshot", ["symbol", "window"])
liqmap_snapshot_bytes = Gauge("liqmap_snapshot_bytes", "Snapshot JSON bytes", ["symbol", "window"])
liqmap_snapshot_total = Counter("liqmap_snapshot_total", "Snapshots published", ["symbol", "window"])

liqmap_evt_lag_ms = Histogram(
    "liqmap_evt_lag_ms",
    "Event lag: now_ms - ts_event_ms (accepted events)",
    buckets=(50, 100, 250, 500, 1000, 2000, 5000, 10000, 30000, 60000, 120000, 300000, 600000),
)

liqmap_loop_sleep_ms = Histogram(
    "liqmap_loop_sleep_ms",
    "Main loop sleep (idle/backoff)",
    buckets=(0.0, 1, 5, 10, 50, 100, 250, 500, 1000),
)

liqmap_last_publish_ts_ms = Gauge("liqmap_last_publish_ts_ms", "Last publish wallclock ts (ms)")
liqmap_last_event_ts_ms = Gauge("liqmap_last_event_ts_ms", "Last accepted event ts_event (ms)")


# ─────────────────────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=os.getenv("LIQMAP_LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("liquidation_map_service")


def _parse_windows(s: str) -> List[Tuple[str, int]]:
    """Parse LIQMAP_WINDOWS like: '1h,4h,24h' or '15m,1h'."""

    out: List[Tuple[str, int]] = []
    for part in (s or "").split(","):
        p = part.strip().lower()
        if not p:
            continue
        if p.endswith("h"):
            n = int(p[:-1])
            out.append((p, n * 3600 * 1000))
        elif p.endswith("m"):
            n = int(p[:-1])
            out.append((p, n * 60 * 1000))
        else:
            raise ValueError(f"bad window: {p}")
    if not out:
        out = [("1h", 3600 * 1000), ("4h", 4 * 3600 * 1000), ("24h", 24 * 3600 * 1000)]
    return out


def _now_ms() -> int:
    return get_ny_time_millis()


class LiquidationMapService:
    def __init__(self) -> None:
        # Streams
        self.stream_in = os.getenv("LIQ_EVT_STREAM", "stream:liq_evt")
        self.group = os.getenv("LIQMAP_GROUP", "liqmap_group")
        self.consumer = os.getenv("LIQMAP_CONSUMER") or f"{socket.gethostname()}:{os.getpid()}"

        # DQ / time
        self.max_future_ms = int(os.getenv("LIQMAP_MAX_FUTURE_MS", "30000"))  # 30s
        # Keep a bit > 24h window to accept delayed events
        self.max_event_age_ms = int(os.getenv("LIQMAP_MAX_EVENT_AGE_MS", str(26 * 3600 * 1000)))

        # Windows
        self.windows = _parse_windows(os.getenv("LIQMAP_WINDOWS", "5m,1h,4h,24h"))
        self.max_window_ms = max(ms for _, ms in self.windows)

        # Publish cadence
        self.publish_interval_ms = int(os.getenv("LIQMAP_PUBLISH_INTERVAL_MS", "1000"))
        self.snapshot_key_prefix = os.getenv("LIQMAP_SNAPSHOT_KEY_PREFIX", "liqmap:snapshot")
        self.snapshot_ttl_sec = int(os.getenv("LIQMAP_SNAPSHOT_TTL_SEC", "30"))

        # Payload size control (UI)
        self.max_levels = int(os.getenv("LIQMAP_MAX_LEVELS", "250"))
        self.range_pct = float(os.getenv("LIQMAP_RANGE_PCT", "5"))

        # Allowlist symbols (optional)
        sym_s = os.getenv("LIQMAP_SYMBOLS", "").strip()
        self.symbol_allow: Optional[Set[str]] = None
        if sym_s:
            self.symbol_allow = {x.strip().upper() for x in sym_s.split(",") if x.strip()}

        # Bucket
        mode = os.getenv("LIQMAP_BUCKET_MODE", "log_bps").strip().lower()
        bps = int(os.getenv("LIQMAP_BUCKET_BPS", "50"))
        pct = float(os.getenv("LIQMAP_BUCKET_PCT", "0.1"))
        abs_step_s = os.getenv("LIQMAP_BUCKET_ABS", "")
        abs_step = Decimal(abs_step_s) if abs_step_s else None
        self.bucketizer = Bucketizer(mode=mode, abs_step=abs_step, bps=bps, pct=pct)

        # Optional snapshot stream
        self.publish_stream_enabled = os.getenv("LIQMAP_PUBLISH_STREAM_ENABLED", "0") == "1"
        self.snapshot_stream_prefix = os.getenv("LIQMAP_SNAPSHOT_STREAM_PREFIX", "stream:liqmap_snapshot")
        self.snapshot_stream_maxlen = int(os.getenv("LIQMAP_SNAPSHOT_STREAM_MAXLEN", "20000"))

        # Runner
        self.r = get_redis()
        self.runner = RedisStreamRunner(
            r=self.r,
            group=self.group,
            consumer=self.consumer,
            block_ms=int(os.getenv("LIQMAP_BLOCK_MS", "2000")),
            read_count=int(os.getenv("LIQMAP_READ_COUNT", "200")),
            autoclaim_min_idle_ms=int(os.getenv("LIQMAP_AUTOCLAIM_MIN_IDLE_MS", "45000")),
            autoclaim_count=int(os.getenv("LIQMAP_AUTOCLAIM_COUNT", "200")),
            dlq_prefix=os.getenv("LIQMAP_DLQ_PREFIX", "dlq"),
        )

        # Per-symbol per-window aggregators
        self.aggs: Dict[str, Dict[str, LiqMapWindowAgg]] = {}
        self._dirty_symbols: Set[str] = set()

        # Publish scheduler
        self._last_publish_ms = 0
        self._last_autoclaim_ms = 0
        self._autoclaim_every_ms = int(os.getenv("LIQMAP_AUTOCLAIM_EVERY_MS", "10000"))

        # Metrics server
        self.metrics_port = int(os.getenv("LIQMAP_METRICS_PORT", "9112"))

    def _ensure_symbol(self, symbol: str) -> None:
        if symbol in self.aggs:
            return
        self.aggs[symbol] = {}
        for wname, wms in self.windows:
            self.aggs[symbol][wname] = LiqMapWindowAgg(window_ms=wms, bucketizer=self.bucketizer)

    def _dq_time_check(self, ts_event_ms: int, now_ms: int) -> Optional[str]:
        # future
        if ts_event_ms > now_ms + self.max_future_ms:
            return "event_in_future"
        # too old
        if ts_event_ms < now_ms - self.max_event_age_ms:
            return "event_too_old"
        return None

    def _process_msg(self, msg: StreamMsg) -> None:
        liq_evt_read_total.inc()

        now_ms = _now_ms()
        ev, reason = normalize_liq_event(msg.fields)
        if reason is not None or ev is None:
            liq_evt_drop_total.labels(reason=reason or "normalize_failed").inc()
            # DLQ + ACK to avoid PEL blocking
            try:
                self.runner.to_dlq(msg.stream, msg, reason=reason or "normalize_failed")
                liq_evt_dlq_total.labels(reason=reason or "normalize_failed").inc()
            finally:
                self.runner.ack(msg.stream, msg.msg_id)
            return

        # allowlist
        if self.symbol_allow is not None and ev.symbol not in self.symbol_allow:
            liq_evt_drop_total.labels(reason="symbol_not_allowed").inc()
            self.runner.ack(msg.stream, msg.msg_id)
            return

        # time policy
        t_reason = self._dq_time_check(ev.ts_event_ms, now_ms)
        if t_reason is not None:
            liq_evt_drop_total.labels(reason=t_reason).inc()
            try:
                self.runner.to_dlq(msg.stream, msg, reason=t_reason)
                liq_evt_dlq_total.labels(reason=t_reason).inc()
            finally:
                self.runner.ack(msg.stream, msg.msg_id)
            return

        # Parse decimals
        px = _safe_decimal_str(ev.price_s)
        notional = _safe_decimal_str(ev.notional_usd_s)
        if px is None or notional is None:
            liq_evt_drop_total.labels(reason="decimal_parse_failed").inc()
            try:
                self.runner.to_dlq(msg.stream, msg, reason="decimal_parse_failed")
                liq_evt_dlq_total.labels(reason="decimal_parse_failed").inc()
            finally:
                self.runner.ack(msg.stream, msg.msg_id)
            return

        # Update aggregates
        try:
            self._ensure_symbol(ev.symbol)
            for _wname, agg in self.aggs[ev.symbol].items():
                agg.add(ts_event_ms=ev.ts_event_ms, price=px, liq_side=ev.liq_side, notional=notional)
            self._dirty_symbols.add(ev.symbol)

            liq_evt_ok_total.labels(symbol=ev.symbol).inc()
            liqmap_evt_lag_ms.observe(max(0, now_ms - ev.ts_event_ms))
            liqmap_last_event_ts_ms.set(ev.ts_event_ms)
        except Exception as e:
            # Any unexpected error -> DLQ (poison)
            liq_evt_drop_total.labels(reason="agg_error").inc()
            try:
                self.runner.to_dlq(msg.stream, msg, reason=f"agg_error:{type(e).__name__}")
                liq_evt_dlq_total.labels(reason="agg_error").inc()
            finally:
                self.runner.ack(msg.stream, msg.msg_id)
            return

        # ACK ok
        self.runner.ack(msg.stream, msg.msg_id)

    def _publish_symbol(self, symbol: str, now_ms: int) -> None:
        aggs = self.aggs.get(symbol)
        if not aggs:
            return

        for wname, agg in aggs.items():
            agg.evict(now_ms)
            levels = agg.levels(max_levels=self.max_levels, range_pct=self.range_pct)

            payload = {
                "ts_ms": now_ms,
                "symbol": symbol,
                "window": wname,
                "bucket_mode": self.bucketizer.mode,
                "bucket_bps": self.bucketizer.bps,
                "bucket_pct": self.bucketizer.pct,
                "bucket_abs": str(self.bucketizer.abs_step) if self.bucketizer.abs_step is not None else None,
                "range_pct": self.range_pct,
                "levels": [
                    {
                        "price": format_price(p),
                        "bucket": bk,
                        "long_usd": format_decimal(l),
                        "short_usd": format_decimal(s),
                        "total_usd": format_decimal(l + s),
                    }
                    for (p, bk, l, s) in levels
                ],
            }

            j = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
            key = f"{self.snapshot_key_prefix}:{symbol}:{wname}"
            self.r.set(key, j, ex=self.snapshot_ttl_sec)

            liqmap_levels_gauge.labels(symbol=symbol, window=wname).set(len(levels))
            liqmap_snapshot_bytes.labels(symbol=symbol, window=wname).set(len(j.encode("utf-8")))
            liqmap_snapshot_total.labels(symbol=symbol, window=wname).inc()

            if self.publish_stream_enabled:
                stream = f"{self.snapshot_stream_prefix}:{symbol}:{wname}"
                # Note: payload may be > 64KB if levels too big; max_levels limits it.
                self.r.xadd(stream, {"ts_ms": str(now_ms), "data": j}, maxlen=self.snapshot_stream_maxlen, approximate=True)

    def _publish_cycle(self, now_ms: int) -> None:
        # publish only dirty symbols to control cost; also evict on publish.
        dirty = list(self._dirty_symbols)
        self._dirty_symbols.clear()

        for sym in dirty:
            self._publish_symbol(sym, now_ms)

        self._last_publish_ms = now_ms
        liqmap_last_publish_ts_ms.set(now_ms)

    def run_forever(self) -> None:
        logger.info(
            "Starting LiquidationMapService stream=%s group=%s consumer=%s windows=%s bucket_mode=%s",
            self.stream_in,
            self.group,
            self.consumer,
            ",".join(w for w, _ in self.windows),
            self.bucketizer.mode,
        )

        # Ensure consumer group exists
        self.runner.ensure_groups([self.stream_in])

        # Metrics
        start_http_server(self.metrics_port)
        logger.info("Prometheus metrics on :%d", self.metrics_port)

        self._last_publish_ms = _now_ms()

        while True:
            loop_start = _now_ms()

            # Periodic autoclaim for PEL recovery
            if loop_start - self._last_autoclaim_ms >= self._autoclaim_every_ms:
                try:
                    claimed = self.runner.claim_cycle([self.stream_in])
                    for m in claimed:
                        # Handle claimed (previously pending) messages exactly like new ones.
                        self._process_msg(m)
                except Exception as e:
                    logger.warning("autoclaim failed: %s", e)
                self._last_autoclaim_ms = loop_start

            # Read new messages
            try:
                msgs = self.runner.read_new([self.stream_in])
                for m in msgs:
                    self._process_msg(m)
            except Exception as e:
                logger.exception("read/process failed: %s", e)
                # small backoff
                time.sleep(0.25)

            # Publish snapshots on interval even if idle (for TTL refresh + eviction)
            now_ms = _now_ms()
            if now_ms - self._last_publish_ms >= self.publish_interval_ms:
                try:
                    # If no dirty symbols, still evict periodically and refresh TTL for active symbols.
                    if not self._dirty_symbols and self.aggs:
                        # refresh all symbols to avoid stale UI when flow pauses
                        self._dirty_symbols = set(self.aggs.keys())
                    self._publish_cycle(now_ms)
                except Exception as e:
                    logger.exception("publish failed: %s", e)

            # Tiny idle sleep to avoid 100% CPU when block_ms==0
            elapsed = _now_ms() - loop_start
            if elapsed < 5:
                t0 = time.time()
                time.sleep(0.005)
                liqmap_loop_sleep_ms.observe((time.time() - t0) * 1000.0)


def main() -> None:
    svc = LiquidationMapService()
    svc.run_forever()


if __name__ == "__main__":
    main()
