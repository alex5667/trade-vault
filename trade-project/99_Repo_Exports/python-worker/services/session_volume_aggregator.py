"""session_volume_aggregator.py — P1 #16 producer: session_liquidity_z.

Reads stream:tick_{SYMBOL} from redis-ticks and tracks cumulative notional
volume per trading session (asia / europe / us). Maintains a rolling history
of past N completed session volumes per symbol, then computes a z-score for
the currently accumulating session vs that history.

Output key: ctx:session_vol:{SYMBOL}  (JSON, main redis)
Fields:
  session_liquidity_z   — z-score of current session volume vs history
  current_session       — "asia" | "europe" | "us"
  current_session_vol   — USD notional accumulated so far this session
  session_history_n     — number of completed sessions in history
  ts_ms                 — epoch ms at publish time
  quality_status        — "OK" | "insufficient_history" | "absent"

Port 9895 / metrics.
"""
from __future__ import annotations

import json
import logging
import math
import os
import signal as _signal
import sys
import time
from collections import deque

log = logging.getLogger("session_volume_aggregator")

# ── config ────────────────────────────────────────────────────────────────────
SYMBOLS: list[str] = [
    s.strip().upper()
    for s in os.getenv("SESSION_VOL_SYMBOLS", os.getenv("CRYPTO_SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT")).split(",")
    if s.strip()
]
INTERVAL_S: float = float(os.getenv("SESSION_VOL_INTERVAL_S", "10"))
HISTORY_LEN: int = int(os.getenv("SESSION_VOL_HISTORY_SESSIONS", "30"))
MIN_HISTORY: int = int(os.getenv("SESSION_VOL_MIN_HISTORY", "5"))
TTL_SEC: int = int(os.getenv("SESSION_VOL_TTL_SEC", "7200"))  # 2h
REDIS_TICKS_URL: str = os.getenv("REDIS_TICKS_URL", os.getenv("REDIS_URL", "redis://localhost:6379"))
REDIS_WRITE_URL: str = os.getenv("REDIS_URL", "redis://localhost:6379")
METRICS_PORT: int = int(os.getenv("SESSION_VOL_METRICS_PORT", "9895"))

_running = True


# ── session label (same mapping as pit_priors_rolling_v1._session) ────────────

def _session_for_ts(ts_ms: int) -> str:
    h = (ts_ms // 3_600_000) % 24
    if 13 <= h < 22:
        return "us"
    if 7 <= h < 16:
        return "europe"
    return "asia"


# ── per-symbol rolling state ──────────────────────────────────────────────────

class _SessionVolState:
    """Accumulates USD notional per session; z-score vs rolling history."""

    def __init__(self, history_len: int = HISTORY_LEN) -> None:
        self._current_session: str | None = None
        self._current_vol: float = 0.0          # USD notional (price × qty)
        self._session_start_ms: int = 0
        self._history: deque[float] = deque(maxlen=history_len)

    def on_tick(self, price: float, qty: float, ts_ms: int) -> None:
        sess = _session_for_ts(ts_ms)
        if sess != self._current_session:
            # Session boundary: archive completed session (only if we have a meaningful amount)
            if self._current_session is not None and self._current_vol > 0:
                self._history.append(self._current_vol)
            self._current_session = sess
            self._current_vol = 0.0
            self._session_start_ms = ts_ms
        if price > 0 and qty > 0:
            self._current_vol += price * qty

    def compute(self) -> dict:
        if self._current_session is None:
            return {"quality_status": "absent"}
        n = len(self._history)
        if n < MIN_HISTORY:
            return {
                "quality_status": "insufficient_history",
                "current_session": self._current_session,
                "current_session_vol": self._current_vol,
                "session_history_n": float(n),
                "session_liquidity_z": 0.0,
            }
        hist = list(self._history)
        mean = sum(hist) / n
        variance = sum((x - mean) ** 2 for x in hist) / n
        std = math.sqrt(variance)
        # z-score clamped to [-5, 5]; avoid div-by-zero with additive floor
        z = (self._current_vol - mean) / max(std, mean * 0.01 + 1.0)
        z = max(-5.0, min(5.0, z))
        return {
            "quality_status": "OK",
            "current_session": self._current_session,
            "current_session_vol": self._current_vol,
            "session_history_n": float(n),
            "session_liquidity_z": z,
        }


# ── Prometheus (idempotent) ───────────────────────────────────────────────────

def _start_metrics(port: int) -> None:
    try:
        from prometheus_client import Counter, Gauge, start_http_server
        global _ticks, _publishes, _z_gauge
        _ticks = Counter("session_vol_ticks_total", "Ticks processed", ["symbol"])
        _publishes = Counter("session_vol_publish_total", "Publishes", ["symbol", "status"])
        _z_gauge = Gauge("session_liquidity_z", "Session liquidity z-score", ["symbol"])
        start_http_server(port)
        log.info("metrics on :%d", port)
    except Exception as exc:
        log.warning("metrics unavailable: %s", exc)
        _ticks = _publishes = _z_gauge = None


def _inc(counter, sym: str) -> None:
    try:
        if counter is not None:
            counter.labels(symbol=sym).inc()
    except Exception:
        pass


def _set_gauge(gauge, sym: str, v: float) -> None:
    try:
        if gauge is not None:
            gauge.labels(symbol=sym).set(v)
    except Exception:
        pass


def _inc2(counter, sym: str, status: str) -> None:
    try:
        if counter is not None:
            counter.labels(symbol=sym, status=status).inc()
    except Exception:
        pass


# ── main service loop ─────────────────────────────────────────────────────────

def _service_main() -> None:
    import redis as redis_lib
    global _running

    def _sig(sig, frame):  # noqa: ANN001
        global _running
        _running = False

    _signal.signal(_signal.SIGTERM, _sig)
    _signal.signal(_signal.SIGINT, _sig)

    _start_metrics(METRICS_PORT)

    r_ticks = redis_lib.from_url(REDIS_TICKS_URL, decode_responses=True)
    r_write = redis_lib.from_url(REDIS_WRITE_URL, decode_responses=True)

    states: dict[str, _SessionVolState] = {s: _SessionVolState() for s in SYMBOLS}
    last_ids: dict[str, str] = {s: "$" for s in SYMBOLS}

    log.info("starting: symbols=%s interval=%ds history=%d min_history=%d",
             SYMBOLS, INTERVAL_S, HISTORY_LEN, MIN_HISTORY)

    last_publish = time.monotonic()
    while _running:
        try:
            streams = {f"stream:tick_{s}": last_ids[s] for s in SYMBOLS}
            try:
                resp = r_ticks.xread(streams, count=500, block=2000)
            except Exception as exc:
                log.debug("XREAD: %s", exc)
                resp = []

            for stream_key, entries in (resp or []):
                sym = stream_key.split("tick_", 1)[-1] if "tick_" in stream_key else None
                if sym is None or sym not in states:
                    continue
                for entry_id, fields in entries:
                    last_ids[sym] = entry_id
                    try:
                        px = float(fields.get("p") or fields.get("price") or 0.0)
                        qty = float(fields.get("q") or fields.get("qty") or 0.0)
                        if px <= 0 or qty <= 0:
                            continue
                        ts_ms = int(entry_id.split("-")[0])
                        states[sym].on_tick(px, qty, ts_ms)
                        _inc(_ticks, sym)
                    except Exception:
                        continue

            now = time.monotonic()
            if now - last_publish >= INTERVAL_S:
                last_publish = now
                now_ms = int(time.time() * 1000)
                for sym in SYMBOLS:
                    try:
                        payload = states[sym].compute()
                        payload["ts_ms"] = now_ms
                        status = str(payload.get("quality_status", ""))
                        r_write.set(f"ctx:session_vol:{sym}", json.dumps(payload), ex=TTL_SEC)
                        _inc2(_publishes, sym, status)
                        z = float(payload.get("session_liquidity_z", 0.0))
                        _set_gauge(_z_gauge, sym, z)
                        log.debug("published %s: z=%.3f session=%s hist_n=%s",
                                  sym, z,
                                  payload.get("current_session"),
                                  payload.get("session_history_n"))
                    except Exception as exc:
                        log.warning("publish %s: %s", sym, exc)
        except Exception as exc:
            log.error("loop error: %s", exc)

    log.info("stopped")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
        stream=sys.stderr,
    )
    _service_main()
