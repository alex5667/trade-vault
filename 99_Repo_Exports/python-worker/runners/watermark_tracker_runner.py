"""Phase B.3: Watermark trailing tick consumer.

Подписывается на stream:tick_{SYMBOL} (redis-ticks), загружает активные FSM
из WatermarkStore (redis-worker-1), вызывает on_tick() и при движении SL
отправляет команду через OrderTrailingDispatcher.

ENV knobs:
  WATERMARK_TRACKER_SYMBOLS   CSV символов; "*" = авто по активным FSM (default *)
  WATERMARK_TRACKER_GROUP     имя consumer group                (default watermark-tracker)
  WATERMARK_TRACKER_BLOCK_MS  XREADGROUP block timeout ms       (default 500)
  WATERMARK_TRACKER_INDEX_TTL_SEC  период переиндексации активных FSM (default 60)
  REDIS_TICKS_URL             redis-ticks  (default redis://redis-ticks:6379/0)
  REDIS_URL                   redis-worker-1 для trail:wm:*     (default redis://redis-worker-1:6379/0)
  GATEWAY_URL                 go-gateway для trailing modify    (default http://scanner-go-gateway:8090)
"""

from __future__ import annotations

import logging
import os
import socket
import time
from typing import Any

import redis

from services.watermark_trailing import WMState
from services.watermark_trailing_store import WatermarkStore
from services.order_trailing_dispatcher import OrderTrailingDispatcher

log = logging.getLogger("watermark_tracker_runner")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")


# ─────────────────────────── prometheus (fail-open) ─────────────────────────
try:
    from prometheus_client import Counter, Gauge, start_http_server
    _ticks_processed = Counter("wm_tracker_ticks_total", "Tick messages processed", ["symbol"])
    _sl_moved = Counter("wm_tracker_sl_moved_total", "SL ratchet moves dispatched", ["symbol"])
    _dispatch_fail = Counter("wm_tracker_dispatch_fail_total", "Failed dispatcher calls", ["symbol"])
    _active_fsm = Gauge("wm_tracker_active_fsm", "Active FSMs currently tracked")
    _PROM_OK = True
except Exception:
    _ticks_processed = _sl_moved = _dispatch_fail = _active_fsm = None  # type: ignore[assignment]
    _PROM_OK = False


# ──────────────────────────────── runner ────────────────────────────────────
class WatermarkTrackerRunner:
    """Single-threaded tick consumer that drives WatermarkTrailingFSM."""

    GROUP: str = os.getenv("WATERMARK_TRACKER_GROUP", "watermark-tracker")

    def __init__(self) -> None:
        self.consumer_id = f"{self.GROUP}-{socket.gethostname()}-{os.getpid()}"

        ticks_url = os.getenv("REDIS_TICKS_URL", "redis://redis-ticks:6379/0")
        main_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
        gateway_url = os.getenv("GATEWAY_URL", "http://scanner-go-gateway:8090")

        self.r_ticks: redis.Redis = redis.from_url(ticks_url, decode_responses=True)
        self.r_main: redis.Redis = redis.from_url(main_url, decode_responses=True)
        self.store = WatermarkStore(self.r_main)
        self.dispatcher = OrderTrailingDispatcher(gateway_url)

        self.block_ms = int(os.getenv("WATERMARK_TRACKER_BLOCK_MS", "500"))
        self.index_ttl_s = int(os.getenv("WATERMARK_TRACKER_INDEX_TTL_SEC", "60"))

        # {symbol → {sid → position_id|None}} — refreshed periodically
        self._sym_index: dict[str, dict[str, str | None]] = {}
        self._index_ts: float = 0.0

        # symbols configured explicitly or auto-discovered
        _sym_env = os.getenv("WATERMARK_TRACKER_SYMBOLS", "*").strip()
        self._explicit_symbols: list[str] = (
            [] if _sym_env == "*" else [s.strip().upper() for s in _sym_env.split(",") if s.strip()]
        )

        # stream cursor per symbol; ">" = undelivered
        self._cursors: dict[str, str] = {}

        log.info(
            "WatermarkTrackerRunner init | ticks=%s main=%s gateway=%s symbols=%s",
            ticks_url, main_url, gateway_url,
            self._explicit_symbols or "auto",
        )

    # ──────────────────────────── index ─────────────────────────────────────

    def _refresh_index(self) -> None:
        """Scan trail:wm:* on redis-worker-1 and build symbol→sids index."""
        new_index: dict[str, dict[str, str | None]] = {}
        try:
            cursor = 0
            while True:
                cursor, keys = self.r_main.scan(cursor, match="trail:wm:*", count=200)
                for key in keys:
                    sid = key[len("trail:wm:"):]
                    snap = self.store.load(sid)
                    if snap is None or snap.state != WMState.TRAILING_ACTIVE:
                        continue
                    sym = snap.symbol.upper() if snap.symbol else ""
                    if not sym:
                        continue
                    new_index.setdefault(sym, {})[sid] = snap.position_id
                if cursor == 0:
                    break
        except Exception as e:
            log.warning("index refresh error: %s", e)
            return

        self._sym_index = new_index
        self._index_ts = time.monotonic()

        total = sum(len(v) for v in new_index.values())
        if _active_fsm is not None:
            _active_fsm.set(total)
        log.info("FSM index refreshed: %d symbols, %d active FSMs", len(new_index), total)

    def _maybe_refresh_index(self) -> None:
        if (time.monotonic() - self._index_ts) >= self.index_ttl_s:
            self._refresh_index()

    # ──────────────────────────── stream setup ──────────────────────────────

    def _ensure_group(self, stream: str) -> None:
        try:
            self.r_ticks.xgroup_create(stream, self.GROUP, id="$", mkstream=True)
        except redis.exceptions.ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                log.warning("xgroup_create %s: %s", stream, exc)

    def _watched_streams(self) -> list[str]:
        if self._explicit_symbols:
            return [f"stream:tick_{s}" for s in self._explicit_symbols]
        # auto: union of indexed symbols + existing streams to avoid racing
        return [f"stream:tick_{s}" for s in self._sym_index]

    # ──────────────────────────── tick processing ───────────────────────────

    def _process_tick(self, symbol: str, fields: dict[str, Any], msg_id: str) -> None:
        """Drive all active FSMs for this symbol on the incoming tick price."""
        if _ticks_processed is not None:
            try:
                _ticks_processed.labels(symbol=symbol).inc()
            except Exception:
                pass

        raw_price = fields.get("price") or fields.get("p") or fields.get("last_price")
        if raw_price is None:
            return
        try:
            price = float(raw_price)
        except (TypeError, ValueError):
            return
        if price <= 0:
            return

        raw_ts = fields.get("t") or fields.get("ts_ms") or fields.get("ts") or fields.get("time")
        try:
            now_ms = int(float(raw_ts)) if raw_ts else int(time.time() * 1000)
        except (TypeError, ValueError):
            now_ms = int(time.time() * 1000)

        sids_for_sym = self._sym_index.get(symbol, {})
        if not sids_for_sym:
            return

        for sid, position_id in list(sids_for_sym.items()):
            fsm = self.store.load_fsm(sid)
            if fsm is None:
                # sid disappeared — remove from index
                self._sym_index[symbol].pop(sid, None)
                continue

            if fsm.snap.state != WMState.TRAILING_ACTIVE:
                self._sym_index[symbol].pop(sid, None)
                continue

            try:
                decision = fsm.on_tick(price, now_ms=now_ms)
            except Exception as e:
                log.warning("on_tick error sid=%s: %s", sid, e)
                continue

            self.store.save(fsm.snap)

            if not decision.moved or decision.new_sl is None:
                continue

            log.info(
                "SL ratchet: sid=%s sym=%s price=%.5f new_sl=%.5f reason=%s",
                sid, symbol, price, decision.new_sl, decision.reason,
            )

            sent = self.dispatcher.send_trailing_modify(
                sid=sid,
                symbol=symbol,
                side=fsm.snap.side,
                position_id=position_id or fsm.snap.position_id,
                new_sl=decision.new_sl,
                tp_levels=[],
                metadata={
                    "source": "watermark_tracker_runner",
                    "watermark_reason": decision.reason,
                    "updates_total": fsm.snap.updates_total,
                    "high_wm": fsm.snap.high_wm,
                    "low_wm": fsm.snap.low_wm,
                },
            )
            if sent:
                if _sl_moved is not None:
                    try:
                        _sl_moved.labels(symbol=symbol).inc()
                    except Exception:
                        pass
            else:
                log.warning("dispatch failed: sid=%s sym=%s new_sl=%.5f", sid, symbol, decision.new_sl)
                if _dispatch_fail is not None:
                    try:
                        _dispatch_fail.labels(symbol=symbol).inc()
                    except Exception:
                        pass

    # ──────────────────────────── main loop ─────────────────────────────────

    def run(self) -> None:
        if _PROM_OK:
            try:
                port = int(os.getenv("WATERMARK_TRACKER_METRICS_PORT", "9840"))
                start_http_server(port)
                log.info("Prometheus metrics on :%d", port)
            except Exception as e:
                log.warning("Prometheus start failed: %s", e)

        log.info("Starting watermark tick consumer (consumer=%s)", self.consumer_id)

        # Bootstrap index
        self._refresh_index()

        while True:
            self._maybe_refresh_index()

            streams = self._watched_streams()
            if not streams:
                log.debug("No active watermark streams; sleeping 5s")
                time.sleep(5)
                continue

            # Ensure consumer groups exist for new streams
            for stream in streams:
                if stream not in self._cursors:
                    self._ensure_group(stream)
                    self._cursors[stream] = ">"

            streams_arg = {s: ">" for s in streams}
            try:
                results = self.r_ticks.xreadgroup(
                    groupname=self.GROUP,
                    consumername=self.consumer_id,
                    streams=streams_arg,
                    count=50,
                    block=self.block_ms,
                )
            except Exception as e:
                log.warning("xreadgroup error: %s", e)
                time.sleep(1)
                continue

            if not results:
                continue

            for stream_name, messages in results:
                # stream_name: "stream:tick_BTCUSDT"
                symbol = stream_name.replace("stream:tick_", "").upper()
                for msg_id, fields in messages:
                    try:
                        self._process_tick(symbol, fields, msg_id)
                    except Exception as e:
                        log.warning("process_tick error %s %s: %s", symbol, msg_id, e)
                    finally:
                        try:
                            self.r_ticks.xack(stream_name, self.GROUP, msg_id)
                        except Exception:
                            pass


# ────────────────────────────── entrypoint ──────────────────────────────────
if __name__ == "__main__":
    WatermarkTrackerRunner().run()
