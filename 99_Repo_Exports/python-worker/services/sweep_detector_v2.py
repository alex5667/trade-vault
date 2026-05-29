"""sweep_detector_v2.py — per-symbol sweep + signal-cluster features.

Computes v14_of features by analysing recent tick stream + signal history:

  • sweep_div_match        — 1.0 if latest sweep direction matches CVD divergence sign, else 0.0
  • sweep_velocity_bps_s   — magnitude of sweep (bps) / duration (seconds)
  • signal_cluster_flag    — 1.0 if ≥3 signals fired in last 60s for this symbol
  • source_jump_usd        — largest single-tick price jump in USD notional (last 60s)

Source streams:
  - `stream:tick_{SYMBOL}` for sweep/jump computation
  - `signals:of:inputs` (filtered by symbol) for cluster detection

Writes: `sweep_v2:{SYMBOL}` JSON with TTL.

ENV:
  REDIS_URL                  ticks default redis-ticks:6379/0
  SDV2_PUBLISH_URL           snapshot target redis-worker-1
  SDV2_SYMBOLS               default common 4 + alts
  SDV2_TICK_WINDOW_SEC       default 60 (sweep velocity, jump scan)
  SDV2_SIGNAL_WINDOW_SEC     default 60 (cluster detection)
  SDV2_CLUSTER_MIN_COUNT     default 3
  SDV2_INTERVAL_S            default 15 (publish cadence)
  SDV2_SWEEP_MIN_BPS         default 5 (min move to count as sweep)
  SDV2_TTL_SEC               default 90
  METRICS_PORT               default 9883
"""
from __future__ import annotations

import json
import logging
import math
import os
import signal as _signal
import sys
import time
from collections import defaultdict, deque
from typing import Any

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("sweep_detector_v2")

REDIS_URL = os.getenv("REDIS_URL", "redis://redis-ticks:6379/0")
PUBLISH_URL = os.getenv("SDV2_PUBLISH_URL",
                       os.getenv("REDIS_PUBLISH_URL", "redis://redis-worker-1:6379/0"))
SIGNALS_REDIS_URL = os.getenv("SDV2_SIGNALS_URL",
                              os.getenv("REDIS_PUBLISH_URL", "redis://redis-worker-1:6379/0"))

SYMBOLS = [s.strip().upper() for s in os.getenv(
    "SDV2_SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT,1000PEPEUSDT"
).split(",") if s.strip()]
TICK_WINDOW_SEC = int(os.getenv("SDV2_TICK_WINDOW_SEC", "60"))
SIGNAL_WINDOW_SEC = int(os.getenv("SDV2_SIGNAL_WINDOW_SEC", "60"))
CLUSTER_MIN_COUNT = int(os.getenv("SDV2_CLUSTER_MIN_COUNT", "3"))
INTERVAL_S = int(os.getenv("SDV2_INTERVAL_S", "15"))
SWEEP_MIN_BPS = float(os.getenv("SDV2_SWEEP_MIN_BPS", "5"))
HASH_PREFIX = os.getenv("SDV2_HASH_PREFIX", "sweep_v2:")
TTL_SEC = int(os.getenv("SDV2_TTL_SEC", "90"))
METRICS_PORT = int(os.getenv("METRICS_PORT", "9883"))

try:
    from prometheus_client import Counter, Gauge, start_http_server
    _ticks = Counter("sdv2_ticks_total", "Ticks processed", ["symbol"])
    _publishes = Counter("sdv2_publishes_total", "Snapshots published")
    _last_ok = Gauge("sdv2_last_ok_ms", "Last publish ts ms")
except Exception:
    _ticks = _publishes = _last_ok = None  # type: ignore
    start_http_server = None  # type: ignore


def _inc(m, *labels):
    if m is None:
        return
    try:
        (m.labels(*labels) if labels else m).inc()
    except Exception:
        pass


# ── Pure feature computations ─────────────────────────────────────────────────


def compute_source_jump_usd(ticks: list[tuple[int, float, float, float]]) -> float:
    """Largest single-tick |Δprice| × qty over the window.

    ticks: list of (ts_ms, price, qty, signed_qty).
    """
    if len(ticks) < 2:
        return 0.0
    max_jump = 0.0
    for i in range(1, len(ticks)):
        dp = abs(ticks[i][1] - ticks[i - 1][1])
        usd = dp * ticks[i][2]
        if usd > max_jump:
            max_jump = usd
    return max_jump


def compute_sweep_velocity_bps_s(ticks: list[tuple[int, float, float, float]]) -> tuple[float, int]:
    """Find the largest sustained one-sided move in the window.

    Returns (velocity_bps_per_sec, sweep_direction).
    direction: +1 up, -1 down, 0 no sweep.
    Sweep ⇔ |Δprice| ≥ SWEEP_MIN_BPS over consecutive same-sign moves.
    """
    if len(ticks) < 3:
        return 0.0, 0
    n = len(ticks)
    # Walk forward, track running same-direction runs
    best_velocity = 0.0
    best_direction = 0
    i = 1
    while i < n:
        start_i = i - 1
        start_px = ticks[start_i][1]
        start_ts = ticks[start_i][0]
        # Detect direction from i
        dp = ticks[i][1] - start_px
        if dp == 0:
            i += 1
            continue
        direction = 1 if dp > 0 else -1
        # Extend run while same direction (or zero)
        j = i
        while j + 1 < n:
            nxt_dp = ticks[j + 1][1] - ticks[j][1]
            if direction > 0 and nxt_dp < 0:
                break
            if direction < 0 and nxt_dp > 0:
                break
            j += 1
        end_px = ticks[j][1]
        end_ts = ticks[j][0]
        total_dp = end_px - start_px
        bps_move = abs(10000.0 * total_dp / start_px) if start_px > 0 else 0.0
        duration_s = (end_ts - start_ts) / 1000.0
        if bps_move >= SWEEP_MIN_BPS and duration_s > 0:
            velocity = bps_move / duration_s
            if velocity > best_velocity:
                best_velocity = velocity
                best_direction = direction
        i = j + 1
    return best_velocity, best_direction


def compute_cvd(ticks: list[tuple[int, float, float, float]]) -> float:
    """Cumulative volume delta over window. Positive = net buying."""
    return sum(t[3] for t in ticks)


def compute_cvd_median_abs_delta_usd(ticks: list[tuple[int, float, float, float]]) -> float:
    """Rolling median of |signed_qty × price| per tick — typical CVD tick magnitude in USD.

    Used as normalisation denominator for CVD-z-score. Returns 0.0 when window empty.
    ticks: (ts_ms, price, qty, signed_qty).
    """
    if not ticks:
        return 0.0
    deltas = sorted(abs(t[3] * t[1]) for t in ticks)
    n = len(deltas)
    mid = n // 2
    return (deltas[mid] + deltas[mid - 1]) / 2.0 if n % 2 == 0 else deltas[mid]


def compute_cvd_divergence_from_price(ticks: list[tuple[int, float, float, float]]) -> float:
    """Signed divergence between net CVD direction and price direction over the window.

    Returns a value in [-1, 1]:
      > 0  bullish divergence: net buying pressure despite falling price
      < 0  bearish divergence: net selling pressure despite rising price
        0  aligned or insufficient data

    CVD norm = cvd / total_abs_flow → [-1, 1] direction fraction.
    Price norm = tanh(price_delta_bps / 5.0) — 5 bps ≈ 1σ for a typical 1-min move.
    Divergence = (cvd_norm - price_norm) / 2 → [-1, 1].
    """
    if len(ticks) < 10:
        return 0.0
    price_start = ticks[0][1]
    price_end = ticks[-1][1]
    if price_start <= 0:
        return 0.0
    cvd = compute_cvd(ticks)
    total_abs_flow = sum(abs(t[3]) for t in ticks)
    if total_abs_flow < 1e-9:
        return 0.0
    cvd_norm = max(-1.0, min(1.0, cvd / total_abs_flow))
    price_delta_bps = 10000.0 * (price_end - price_start) / price_start
    price_norm = math.tanh(price_delta_bps / 5.0)
    return (cvd_norm - price_norm) / 2.0


def compute_sweep_div_match(ticks: list[tuple[int, float, float, float]],
                            sweep_direction: int) -> float:
    """1.0 if CVD sign matches sweep direction, else 0.0.

    "Divergence match" means sweep up + buying CVD (or sweep down + selling CVD).
    No sweep → 0.0.
    """
    if sweep_direction == 0:
        return 0.0
    cvd = compute_cvd(ticks)
    cvd_sign = 1 if cvd > 0 else (-1 if cvd < 0 else 0)
    return 1.0 if cvd_sign == sweep_direction else 0.0


# ── Cluster detection (signals:of:inputs by symbol) ───────────────────────────


def _fetch_recent_signal_entries(r, since_ms: int) -> list[tuple]:
    """Fetch signals:of:inputs entries newer than since_ms in one call."""
    try:
        return r.xrevrange("signals:of:inputs", "+", f"{since_ms}-0", count=500)
    except Exception:
        return []


def count_recent_signals_for_symbol(
    symbol: str,
    cached_entries: list[tuple],
    since_ms: int,
) -> int:
    """Count signals for symbol from a pre-fetched entries list."""
    count = 0
    for entry_id, fields in cached_entries:
        try:
            ts_ms = int(entry_id.split("-")[0])
            if ts_ms < since_ms:
                break
            payload = fields.get("payload") if isinstance(fields, dict) else None
            if not payload:
                continue
            p = json.loads(payload)
            inner = p.get("data", p)
            if isinstance(inner, str):
                inner = json.loads(inner)
            sym = (inner.get("symbol") or "").upper()
            if sym == symbol:
                count += 1
        except Exception:
            continue
    return count


# ── Service ───────────────────────────────────────────────────────────────────


_running = True


def _sighandler(signum, _frame):
    global _running
    log.info("signal %d → exit", signum)
    _running = False


def run() -> int:
    if start_http_server is not None:
        try:
            start_http_server(METRICS_PORT)
        except Exception:
            pass

    _signal.signal(_signal.SIGTERM, _sighandler)
    _signal.signal(_signal.SIGINT, _sighandler)

    try:
        import redis
    except ImportError:
        log.error("redis-py not installed")
        return 2

    r_read = redis.from_url(REDIS_URL, decode_responses=True)
    r_write = redis.from_url(PUBLISH_URL, decode_responses=True)
    r_signals = redis.from_url(SIGNALS_REDIS_URL, decode_responses=True)

    # Per-symbol rolling window
    ticks: dict[str, deque[tuple[int, float, float, float]]] = {s: deque() for s in SYMBOLS}
    last_ids: dict[str, str] = {s: "$" for s in SYMBOLS}

    log.info("starting: symbols=%s tick_win=%ds signal_win=%ds interval=%ds",
             SYMBOLS, TICK_WINDOW_SEC, SIGNAL_WINDOW_SEC, INTERVAL_S)
    last_publish = time.monotonic()
    global _running
    _running = True

    while _running:
        try:
            streams = {f"stream:tick_{s}": last_ids[s] for s in SYMBOLS}
            try:
                resp = r_read.xread(streams, count=200, block=2000)
            except Exception as e:
                log.debug("XREAD: %s", e)
                resp = []
            now_ms = int(time.time() * 1000)
            cutoff_ms = now_ms - TICK_WINDOW_SEC * 1000
            for stream_key, entries in (resp or []):
                sym = stream_key.split("tick_", 1)[-1] if "tick_" in stream_key else None
                if sym is None or sym not in ticks:
                    continue
                for entry_id, fields in entries:
                    last_ids[sym] = entry_id
                    try:
                        px = float(fields.get("p") or fields.get("price") or 0.0)
                        if px <= 0:
                            continue
                        qty = float(fields.get("q") or fields.get("qty") or 0.0)
                        side = (fields.get("s") or fields.get("side") or "").lower()
                        sign = 1.0 if side.startswith("b") else (-1.0 if side.startswith("s") else 0.0)
                        ts_ms = int(entry_id.split("-")[0])
                        ticks[sym].append((ts_ms, px, qty, sign * qty))
                        _inc(_ticks, sym)
                    except Exception:
                        continue
                while ticks[sym] and ticks[sym][0][0] < cutoff_ms:
                    ticks[sym].popleft()

            now = time.monotonic()
            if now - last_publish >= INTERVAL_S:
                # One XREVRANGE for all symbols — time-bounded by since_ms.
                sig_since_ms = int(time.time() * 1000) - SIGNAL_WINDOW_SEC * 1000
                sig_entries = _fetch_recent_signal_entries(r_signals, sig_since_ms)
                for sym in SYMBOLS:
                    buf = list(ticks[sym])
                    while buf and buf[0][0] < cutoff_ms:
                        buf.pop(0)
                    if len(buf) < 5:
                        continue
                    velocity, direction = compute_sweep_velocity_bps_s(buf)
                    div_match = compute_sweep_div_match(buf, direction)
                    jump_usd = compute_source_jump_usd(buf)
                    cvd_mad = compute_cvd_median_abs_delta_usd(buf)
                    cvd_div = compute_cvd_divergence_from_price(buf)
                    n_sigs = count_recent_signals_for_symbol(sym, sig_entries, sig_since_ms)
                    cluster_flag = 1.0 if n_sigs >= CLUSTER_MIN_COUNT else 0.0
                    feats = {
                        "sweep_velocity_bps_s": velocity,
                        "sweep_div_match": div_match,
                        "source_jump_usd": jump_usd,
                        "signal_cluster_flag": cluster_flag,
                        "cvd_median_abs_delta_usd": cvd_mad,
                        "cvd_divergence_from_price": cvd_div,
                        "_n_signals_recent": float(n_sigs),
                        "ts_ms": now_ms,
                    }
                    try:
                        r_write.set(f"{HASH_PREFIX}{sym}", json.dumps(feats), ex=TTL_SEC)
                        _inc(_publishes)
                    except Exception as e:
                        log.warning("publish %s failed: %s", sym, e)
                if _last_ok is not None:
                    try:
                        _last_ok.set(now_ms)
                    except Exception:
                        pass
                last_publish = now

        except Exception as e:
            log.exception("loop error: %s", e)
            time.sleep(1)

    log.info("stopped")
    return 0


if __name__ == "__main__":
    sys.exit(run())
