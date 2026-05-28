"""crossasset_ctx_writer.py — per-symbol cross-asset features vs BTC.

Unlocks v14_of features:
  • btc_corr_5m            — Pearson corr(returns_symbol, returns_BTC) on 1m klines, 5-bar window
  • cross_asset_vol_ratio  — stdev(returns_symbol) / stdev(returns_BTC) over 30 bars
  • alt_season_index       — fraction of tracked alts outperforming BTC over last 30 bars

Source: `stream:kline_1m:{SYMBOL}` (1-minute kline stream from go-worker).

Cadence: poll BTC + per-symbol klines every CCW_INTERVAL_S (default 60s).
Writes per-symbol `crossasset:ctx:{SYMBOL}` JSON snapshot. Also writes
`crossasset:ctx:_global` with alt_season_index (per-symbol entries inherit).

ENV:
  REDIS_URL                redis://redis-worker-1:6379/0
  CCW_KLINE_STREAM_PREFIX  default stream:kline_1m:
  CCW_BTC_SYMBOL           default BTCUSDT
  CCW_SYMBOLS              comma-separated alts (default ETHUSDT,SOLUSDT,1000PEPEUSDT)
  CCW_WINDOW_BARS          default 30 (1-min bars used for correlation + vol)
  CCW_INTERVAL_S           default 60
  CCW_TTL_SEC              default 180
  METRICS_PORT             default 9880
"""
from __future__ import annotations

import json
import logging
import math
import os
import signal as _signal
import sys
import time
from typing import Any

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("crossasset_ctx_writer")

REDIS_URL = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
KLINE_PREFIX = os.getenv("CCW_KLINE_STREAM_PREFIX", "stream:kline_1m:")
BTC_SYMBOL = os.getenv("CCW_BTC_SYMBOL", "BTCUSDT").upper()
SYMBOLS = [s.strip().upper() for s in os.getenv(
    "CCW_SYMBOLS", "ETHUSDT,SOLUSDT,1000PEPEUSDT"
).split(",") if s.strip()]
WINDOW_BARS = int(os.getenv("CCW_WINDOW_BARS", "30"))
INTERVAL_S = int(os.getenv("CCW_INTERVAL_S", "60"))
TTL_SEC = int(os.getenv("CCW_TTL_SEC", "180"))
HASH_PREFIX = os.getenv("CCW_HASH_PREFIX", "crossasset:ctx:")
METRICS_PORT = int(os.getenv("METRICS_PORT", "9880"))

try:
    from prometheus_client import Counter, Gauge, start_http_server
    _cycles_total = Counter("ccw_cycles_total", "Cycles run", ["outcome"])
    _btc_corr = Gauge("ccw_btc_corr_5m", "btc_corr_5m by symbol", ["symbol"])
    _alt_season = Gauge("ccw_alt_season_index", "Alt-season index (0..1)")
    _last_ok_ms = Gauge("ccw_last_ok_ms", "Last successful cycle ts ms")
    _METRICS_OK = True
except Exception:
    _cycles_total = _btc_corr = _alt_season = _last_ok_ms = None  # type: ignore
    start_http_server = None  # type: ignore
    _METRICS_OK = False


def _inc(m, *labels):
    if m is None:
        return
    try:
        (m.labels(*labels) if labels else m).inc()
    except Exception:
        pass


def _set(m, v, *labels):
    if m is None:
        return
    try:
        (m.labels(*labels) if labels else m).set(v)
    except Exception:
        pass


# ── Stats primitives ──────────────────────────────────────────────────────────


def _pearson(xs: list[float], ys: list[float]) -> float:
    """Returns 0.0 when degenerate (zero variance, mismatched length)."""
    n = len(xs)
    if n < 2 or n != len(ys):
        return 0.0
    mx = sum(xs) / n
    my = sum(ys) / n
    num = 0.0
    sx = 0.0
    sy = 0.0
    for x, y in zip(xs, ys):
        dx = x - mx
        dy = y - my
        num += dx * dy
        sx += dx * dx
        sy += dy * dy
    if sx == 0 or sy == 0:
        return 0.0
    return num / math.sqrt(sx * sy)


def _stdev(xs: list[float]) -> float:
    n = len(xs)
    if n < 2:
        return 0.0
    mean = sum(xs) / n
    var = sum((x - mean) ** 2 for x in xs) / (n - 1)
    return math.sqrt(var)


# ── Kline reader ──────────────────────────────────────────────────────────────


def _parse_kline_close(fields: dict[str, Any]) -> float | None:
    """Klines XADD fields include `close`/`c`/`close_price` variants."""
    for k in ("close", "c", "close_price"):
        v = fields.get(k)
        if v is None or v == "":
            continue
        try:
            f = float(v)
            if math.isfinite(f) and f > 0:
                return f
        except (TypeError, ValueError):
            continue
    return None


def load_closes_from_ticks(r, symbol: str, n_minutes: int) -> list[float]:
    """Aggregate last `n_minutes` of `stream:tick_{symbol}` into 1-minute closes.

    Falls back to this when kline streams aren't materialised. We use the LAST
    price seen in each 60-second bucket as the close.
    """
    import time as _t
    try:
        entries = r.xrevrange(f"stream:tick_{symbol}", "+", "-", count=n_minutes * 200)
    except Exception:
        return []
    if not entries:
        return []
    # Bucket ticks into 60s windows by stream-id ts
    buckets: dict[int, float] = {}
    for entry_id, fields in entries:
        try:
            ts_ms = int(entry_id.split("-")[0])
            bucket = ts_ms // 60_000
            px = float(fields.get("p") or fields.get("price") or 0.0)
            if px <= 0:
                continue
            # entries come desc; first hit per bucket = latest price in bucket
            if bucket not in buckets:
                buckets[bucket] = px
        except Exception:
            continue
    # Take last n_minutes buckets, oldest first
    sorted_buckets = sorted(buckets.items())[-n_minutes:]
    return [px for _, px in sorted_buckets]


def load_closes(r_kline, r_ticks, symbol: str, n: int) -> list[float]:
    """Return last `n` 1-minute close prices for symbol, oldest first.

    Tries `stream:kline_1m:{symbol}` on `r_kline` first; falls back to bucketed
    ticks from `r_ticks` when klines aren't populated.
    """
    closes: list[float] = []
    for prefix in (KLINE_PREFIX, "stream:kline:1m:", "stream:kline:"):
        key = f"{prefix}{symbol}"
        try:
            entries = r_kline.xrevrange(key, "+", "-", count=n + 5)
        except Exception:
            continue
        if not entries:
            continue
        for _id, fields in entries:
            c = _parse_kline_close(fields if isinstance(fields, dict) else {})
            if c is not None:
                closes.append(c)
            if len(closes) >= n:
                break
        if closes:
            break
    if closes:
        closes.reverse()
        return closes
    # Fallback: tick aggregation
    return load_closes_from_ticks(r_ticks, symbol, n)


def returns_from_closes(closes: list[float]) -> list[float]:
    """Simple returns r_t = ln(c_t / c_{t-1}). Drops invalid pairs."""
    out: list[float] = []
    for i in range(1, len(closes)):
        c_prev = closes[i - 1]
        c_now = closes[i]
        if c_prev > 0 and c_now > 0:
            try:
                out.append(math.log(c_now / c_prev))
            except (ValueError, ZeroDivisionError):
                continue
    return out


# ── Cycle ─────────────────────────────────────────────────────────────────────


def run_cycle(r, r_ticks=None) -> dict[str, dict[str, float]]:
    """Returns {symbol: features_dict} for all configured symbols.

    `r` — main worker Redis (klines if present, snapshot target).
    `r_ticks` — tick stream Redis (fallback source). Defaults to `r`.
    """
    if r_ticks is None:
        r_ticks = r
    btc_closes = load_closes(r, r_ticks, BTC_SYMBOL, WINDOW_BARS + 1)
    btc_rets = returns_from_closes(btc_closes)
    if len(btc_rets) < 5:
        log.warning("BTC returns insufficient (n=%d) — skipping cycle", len(btc_rets))
        _inc(_cycles_total, "btc_insufficient")
        return {}

    btc_5m = btc_rets[-5:]
    btc_30 = btc_rets[-WINDOW_BARS:]
    btc_stdev = _stdev(btc_30)
    btc_cum_ret = sum(btc_30)

    out: dict[str, dict[str, float]] = {}
    alt_outperform = 0
    alt_total = 0
    for sym in SYMBOLS:
        sym_closes = load_closes(r, r_ticks, sym, WINDOW_BARS + 1)
        sym_rets = returns_from_closes(sym_closes)
        if len(sym_rets) < 5:
            continue
        sym_5m = sym_rets[-5:]
        sym_30 = sym_rets[-WINDOW_BARS:]
        feats: dict[str, float] = {}
        # btc_corr_5m: correlation on last 5 bars vs BTC
        if len(sym_5m) == len(btc_5m) >= 2:
            feats["btc_corr_5m"] = _pearson(sym_5m, btc_5m)
        # cross_asset_vol_ratio
        sym_stdev = _stdev(sym_30)
        if btc_stdev > 1e-9:
            feats["cross_asset_vol_ratio"] = sym_stdev / btc_stdev
        feats["_window_bars"] = float(len(sym_30))
        feats["ts_ms"] = float(int(time.time() * 1000))
        out[sym] = feats
        # alt_season: alt out-performing BTC over the window?
        sym_cum_ret = sum(sym_30)
        alt_total += 1
        if sym_cum_ret > btc_cum_ret:
            alt_outperform += 1

    alt_season_index = (alt_outperform / alt_total) if alt_total > 0 else 0.5
    # Inject alt_season into each alt's features
    for sym in out:
        out[sym]["alt_season_index"] = alt_season_index

    # Also publish a global snapshot
    out["_global"] = {
        "alt_season_index": alt_season_index,
        "n_alts": float(alt_total),
        "n_outperform": float(alt_outperform),
        "ts_ms": float(int(time.time() * 1000)),
    }

    # Self-reference snapshot for BTC: alt_season inherited; btc_corr_5m=1.0
    # and vol_ratio=1.0 by definition. Downstream consumers (feature_enricher,
    # of_confirm_engine) expect a key for BTCUSDT — without it, BTC trades
    # land with empty crossasset:ctx, leaving btc_corr_5m / vol_ratio / alt_season
    # in indicators as None.
    if BTC_SYMBOL not in out:
        out[BTC_SYMBOL] = {
            "btc_corr_5m": 1.0,
            "cross_asset_vol_ratio": 1.0,
            "alt_season_index": alt_season_index,
            "_window_bars": float(len(btc_30)),
            "ts_ms": float(int(time.time() * 1000)),
            "_self_ref": 1.0,
        }

    if _alt_season is not None:
        _alt_season.set(alt_season_index)
    for sym, feats in out.items():
        if sym == "_global":
            continue
        corr = feats.get("btc_corr_5m")
        if corr is not None and _btc_corr is not None:
            try:
                _btc_corr.labels(symbol=sym).set(corr)
            except Exception:
                pass

    _inc(_cycles_total, "ok")
    _set(_last_ok_ms, int(time.time() * 1000))
    return out


def publish_snapshots(r, snapshots: dict[str, dict[str, float]]) -> int:
    n = 0
    for sym, feats in snapshots.items():
        try:
            r.set(f"{HASH_PREFIX}{sym}", json.dumps(feats), ex=TTL_SEC)
            n += 1
        except Exception as e:
            log.warning("publish %s failed: %s", sym, e)
    return n


_running = True


def _sighandler(signum, _frame):
    global _running
    log.info("signal %d → exit", signum)
    _running = False


def run() -> int:
    if _METRICS_OK and start_http_server is not None:
        try:
            start_http_server(METRICS_PORT)
        except Exception as e:
            log.warning("metrics server failed: %s", e)

    _signal.signal(_signal.SIGTERM, _sighandler)
    _signal.signal(_signal.SIGINT, _sighandler)

    try:
        import redis
    except ImportError:
        log.error("redis-py not installed")
        return 2

    r = redis.from_url(REDIS_URL, decode_responses=True)
    ticks_url = os.getenv("CCW_TICKS_URL", "redis://redis-ticks:6379/0")
    try:
        r_ticks = redis.from_url(ticks_url, decode_responses=True,
                                 socket_connect_timeout=2, socket_timeout=2)
        r_ticks.ping()
    except Exception as e:
        log.warning("ticks Redis unavailable (%s) — fallback disabled", e)
        r_ticks = r
    log.info("starting: symbols=%s btc=%s window=%d interval=%ds (ticks=%s)",
             SYMBOLS, BTC_SYMBOL, WINDOW_BARS, INTERVAL_S, ticks_url)

    while _running:
        try:
            snapshots = run_cycle(r, r_ticks)
            if snapshots:
                n = publish_snapshots(r, snapshots)
                log.info("cycle: published %d snapshots", n)
            else:
                log.warning("cycle empty")
        except Exception as e:
            log.exception("cycle error: %s", e)
            _inc(_cycles_total, "error")

        slept = 0
        while _running and slept < INTERVAL_S:
            time.sleep(min(5, INTERVAL_S - slept))
            slept += 5

    log.info("stopped")
    return 0


if __name__ == "__main__":
    sys.exit(run())
