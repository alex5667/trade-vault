"""Sweep-hunt detector (P2.C, 2026-05-27).

Detects stop-hunt sweeps from kline (1m) streams:

A sweep is a candle that pierces a recent N-bar high (or low) by ≥ THRESHOLD_BPS,
then closes back **inside** the range. This is a classic stop-hunt pattern —
liquidity grabbed above highs / below lows, then immediate reversal.

For each symbol we maintain a sliding window of last `LOOKBACK_BARS` 1m candles.
On a new closed candle:
  - prior_high = max(prev highs[1..N-1])
  - prior_low  = min(prev lows[1..N-1])
  - if candle.high > prior_high AND candle.close < prior_high:  → sweep UP
  - if candle.low  < prior_low  AND candle.close > prior_low:   → sweep DOWN
  magnitude_bps = (max(candle.high - prior_high, prior_low - candle.low) / mid) * 1e4

Writes to Redis HASH `ctx:sweep:{SYMBOL}` so EntryPolicyGate reader can veto LONG
when direction=up.

Source: kline 1m stream `stream:kline_1m:{SYMBOL}` (Go ingester).

ENV:
  SWEEP_DETECTOR_ENABLED            default 1
  SWEEP_DETECTOR_LOOKBACK_BARS      default 15  (last 15 closed bars before current)
  SWEEP_DETECTOR_THRESHOLD_BPS      default 30  (sweep size threshold)
  SWEEP_DETECTOR_TTL_SEC            default 900 (HASH key TTL)
  SWEEP_DETECTOR_POLL_SEC           default 5
  SWEEP_DETECTOR_REDIS_URL          fallback REDIS_URL
  SWEEP_DETECTOR_KLINE_STREAM_FMT   default stream:kline_1m:{SYMBOL}
  SWEEP_DETECTOR_SYMBOLS            CSV; fallback scan stream:kline_1m:*
  SWEEP_DETECTOR_PROM_PORT          default 9875
"""

from __future__ import annotations

import collections
import json
import logging
import os
import signal
import sys
import time
from typing import Any

logger = logging.getLogger("sweep_detector")

_KEY_PREFIX = "ctx:sweep:"

try:
    from prometheus_client import Counter, Gauge, start_http_server
    _sweeps_total = Counter(
        "sweep_detector_sweeps_total",
        "Sweeps detected",
        ["symbol", "direction"],
    )
    _scans_total = Counter(
        "sweep_detector_scans_total",
        "Symbol-scans performed",
        ["result"],
    )
    _last_cycle_ms = Gauge(
        "sweep_detector_last_cycle_ms",
        "Last cycle epoch ms",
    )
except Exception:
    Counter = Gauge = start_http_server = None  # type: ignore[assignment,misc]
    _sweeps_total = _scans_total = _last_cycle_ms = None  # type: ignore[assignment]


def _env_int(k: str, d: int) -> int:
    try:
        return int(os.environ.get(k, str(d)))
    except (TypeError, ValueError):
        return d


def _env_float(k: str, d: float) -> float:
    try:
        return float(os.environ.get(k, str(d)))
    except (TypeError, ValueError):
        return d


def _env_bool(k: str, d: bool) -> bool:
    raw = os.environ.get(k, "")
    if not raw:
        return d
    return raw.strip().lower() in ("1", "true", "yes", "on")


def _now_ms() -> int:
    return int(time.time() * 1000)


def _redis():
    import redis  # type: ignore
    url = (
        os.environ.get("SWEEP_DETECTOR_REDIS_URL")
        or os.environ.get("REDIS_URL")
        or "redis://redis-worker-1:6379/0"
    )
    return redis.from_url(url, decode_responses=True, socket_timeout=2.0)


def _scan_symbols(rc: Any, fmt: str, max_n: int) -> list[str]:
    """Scan stream:kline_1m:* keys; extract symbols. Limited to USDT pairs."""
    symbols: list[str] = []
    pattern = fmt.replace("{SYMBOL}", "*")
    try:
        cursor = 0
        while True:
            cursor, keys = rc.scan(cursor=cursor, match=pattern, count=200)
            for k in keys:
                sk = k.decode() if isinstance(k, bytes) else k
                prefix = fmt.split("{SYMBOL}", 1)[0]
                sym = sk[len(prefix):].split(":", 1)[0]
                if sym and sym.upper().endswith("USDT") and sym not in symbols:
                    symbols.append(sym.upper())
                    if len(symbols) >= max_n:
                        return symbols
            if cursor == 0:
                break
    except Exception as e:
        logger.debug("sweep_detector: scan fail: %s", e)
    return symbols


def _read_recent_klines(rc: Any, symbol: str, fmt: str, n: int) -> list[dict[str, float]]:
    """Pull recent N closed klines from `stream:kline_1m:{SYMBOL}`. Returns
    list of dicts {open_ms, high, low, close, open} oldest→newest. Empty list
    on failure."""
    key = fmt.replace("{SYMBOL}", symbol.upper())
    try:
        rows = rc.xrevrange(key, count=n)
    except Exception:
        return []
    if not rows:
        return []
    out: list[dict[str, float]] = []
    for _id, fields in rows:
        if not isinstance(fields, dict):
            continue
        norm: dict[str, Any] = {}
        for k, v in fields.items():
            ks = k.decode() if isinstance(k, bytes) else k
            vs = v.decode() if isinstance(v, bytes) else v
            norm[str(ks)] = vs
        # Two shapes possible: structured fields or {"payload": json}
        payload = None
        if "payload" in norm:
            try:
                payload = json.loads(norm["payload"]) if isinstance(norm["payload"], str) else norm["payload"]
            except Exception:
                payload = None
        if not isinstance(payload, dict):
            payload = norm
        try:
            kl = {
                "open_ms": float(payload.get("open_time") or payload.get("t") or payload.get("ts_ms") or 0),
                "open": float(payload.get("o") or payload.get("open") or 0),
                "high": float(payload.get("h") or payload.get("high") or 0),
                "low": float(payload.get("l") or payload.get("low") or 0),
                "close": float(payload.get("c") or payload.get("close") or 0),
            }
            if kl["high"] > 0 and kl["low"] > 0 and kl["close"] > 0:
                out.append(kl)
        except Exception:
            continue
    out.reverse()  # oldest -> newest
    return out


def _detect_sweep(klines: list[dict[str, float]], threshold_bps: float) -> dict[str, Any] | None:
    """Returns sweep details or None.

    Algorithm: на самой последней свече, проверяем выход за prior window high/low,
    и возврат внутрь.
    """
    if len(klines) < 5:
        return None
    last = klines[-1]
    prior = klines[:-1]
    prior_high = max(b["high"] for b in prior)
    prior_low = min(b["low"] for b in prior)
    if prior_high <= 0 or prior_low <= 0:
        return None
    mid = (prior_high + prior_low) / 2.0
    if mid <= 0:
        return None

    # Sweep UP: high > prior_high AND close < prior_high
    if last["high"] > prior_high and last["close"] < prior_high:
        mag = (last["high"] - prior_high) / mid * 1e4
        if mag >= threshold_bps:
            return {
                "direction": "up",
                "magnitude_bps": round(mag, 2),
                "ts_ms": int(last["open_ms"] or _now_ms()),
                "prior_high": prior_high,
                "high": last["high"],
                "close": last["close"],
            }

    # Sweep DOWN: low < prior_low AND close > prior_low
    if last["low"] < prior_low and last["close"] > prior_low:
        mag = (prior_low - last["low"]) / mid * 1e4
        if mag >= threshold_bps:
            return {
                "direction": "down",
                "magnitude_bps": round(mag, 2),
                "ts_ms": int(last["open_ms"] or _now_ms()),
                "prior_low": prior_low,
                "low": last["low"],
                "close": last["close"],
            }

    return None


def _write_sweep(rc: Any, symbol: str, sweep: dict[str, Any], ttl_sec: int) -> None:
    key = _KEY_PREFIX + symbol.upper()
    try:
        rc.hset(key, mapping={
            "last_sweep_ms": str(sweep["ts_ms"]),
            "direction": str(sweep["direction"]),
            "levels_swept": "1",  # рудиментарно — sweep одного уровня; producer может расширить
            "magnitude_bps": str(sweep["magnitude_bps"]),
            "source": "sweep_detector_v1",
        })
        rc.expire(key, max(60, ttl_sec))
        if _sweeps_total is not None:
            _sweeps_total.labels(symbol=symbol, direction=sweep["direction"]).inc()
        logger.info(
            "sweep_detector: %s direction=%s mag=%.1fbps",
            symbol, sweep["direction"], sweep["magnitude_bps"],
        )
    except Exception as e:
        logger.debug("sweep_detector: write fail %s: %s", symbol, e)


# Cache previous sweep ts_ms to avoid duplicate writes on the same candle.
_seen_ts: dict[str, int] = collections.defaultdict(int)


def _main_loop() -> int:
    if not _env_bool("SWEEP_DETECTOR_ENABLED", True):
        logger.info("sweep_detector: disabled")
        return 0

    lookback = max(5, _env_int("SWEEP_DETECTOR_LOOKBACK_BARS", 15))
    thr_bps = _env_float("SWEEP_DETECTOR_THRESHOLD_BPS", 30.0)
    ttl_sec = max(60, _env_int("SWEEP_DETECTOR_TTL_SEC", 900))
    poll_sec = max(2, _env_int("SWEEP_DETECTOR_POLL_SEC", 5))
    fmt = os.environ.get("SWEEP_DETECTOR_KLINE_STREAM_FMT", "stream:kline_1m:{SYMBOL}")
    csv_syms = (os.environ.get("SWEEP_DETECTOR_SYMBOLS") or "").strip()
    max_syms = _env_int("SWEEP_DETECTOR_MAX_SYMBOLS", 30)

    rc = _redis()

    if start_http_server is not None:
        try:
            start_http_server(_env_int("SWEEP_DETECTOR_PROM_PORT", 9875))
        except Exception as e:
            logger.warning("prom server fail: %s", e)

    stop = {"flag": False}

    def _sig(_s, _f):
        stop["flag"] = True

    signal.signal(signal.SIGTERM, _sig)
    signal.signal(signal.SIGINT, _sig)

    logger.info(
        "sweep_detector started: lookback=%d threshold_bps=%.1f ttl=%ds poll=%ds",
        lookback, thr_bps, ttl_sec, poll_sec,
    )

    while not stop["flag"]:
        try:
            symbols = (
                [s.strip().upper() for s in csv_syms.split(",") if s.strip()]
                if csv_syms
                else _scan_symbols(rc, fmt, max_syms)
            )
            detected = 0
            for sym in symbols:
                kls = _read_recent_klines(rc, sym, fmt, lookback)
                if len(kls) < 5:
                    if _scans_total is not None:
                        _scans_total.labels(result="insufficient_data").inc()
                    continue
                sweep = _detect_sweep(kls, thr_bps)
                if sweep is None:
                    if _scans_total is not None:
                        _scans_total.labels(result="no_sweep").inc()
                    continue
                # Avoid re-write on same candle.
                last_ts = sweep["ts_ms"]
                if _seen_ts.get(sym) == last_ts:
                    continue
                _seen_ts[sym] = last_ts
                _write_sweep(rc, sym, sweep, ttl_sec)
                detected += 1
                if _scans_total is not None:
                    _scans_total.labels(result="sweep").inc()
            if _last_cycle_ms is not None:
                _last_cycle_ms.set(float(_now_ms()))
            if detected > 0:
                logger.info("sweep_detector cycle: symbols=%d detected=%d", len(symbols), detected)
        except Exception as e:
            logger.warning("sweep_detector cycle error: %s", e)

        for _ in range(poll_sec):
            if stop["flag"]:
                break
            time.sleep(1.0)

    return 0


if __name__ == "__main__":
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    sys.exit(_main_loop())
