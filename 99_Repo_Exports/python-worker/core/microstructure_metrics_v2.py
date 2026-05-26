"""microstructure_metrics_v2.py — pure functions + standalone service.

Computes microstructure features for v13_of / feature enricher:
  • kyle_lambda, kyle_x_vpin, taker_lambda
  • vpin_rolling, vpin_x_funding
  • tick_autocorr_lag1, roll_spread_est, hurst_exp_50, hurst_x_vol_regime
  • garman_klass_vol, parkinson_vol, yang_zhang_vol  — OHLC estimators (Group NA)
  • amihud_illiquidity                               — |ret|/volume_USD (Group NB)
  • pin_estimate                                     — |B−S|/(B+S) flow proxy (Group NC)

Two ways to use:
  (a) Pure functions — called from runtime tick handler with recent tick buffer.
  (b) Standalone consumer service — subscribes to `stream:tick_{SYMBOL}`,
      maintains in-memory rolling buffer, periodically writes
      `microstruct:ctx:{SYMBOL}` JSON snapshot. feature_enricher_v1 already
      knows how to read this key.

This file provides BOTH. The standalone service is opt-in via
`USE_MICROSTRUCTURE_V2_SERVICE=1`.
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
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger("microstructure_metrics_v2")

_LN2 = math.log(2.0)
_BAR_WINDOW = 20


@dataclass(frozen=True)
class OHLCBar:
    o: float
    h: float
    l: float
    c: float
    volume: float
    ts_ms: int = 0


class MinuteBarAggregator:
    """Aggregate ticks into 1-minute OHLC bars (newest at end of deque)."""

    def __init__(self, maxlen: int = _BAR_WINDOW + 5) -> None:
        self._bars: deque[OHLCBar] = deque(maxlen=maxlen)
        self._bucket_ms: int | None = None
        self._o = self._h = self._l = self._c = 0.0
        self._vol = 0.0

    def on_tick(self, px: float, qty: float, ts_ms: int) -> None:
        if px <= 0 or ts_ms <= 0:
            return
        bucket = (ts_ms // 60_000) * 60_000
        if self._bucket_ms is None:
            self._open_bar(bucket, px, qty)
            return
        if bucket != self._bucket_ms:
            self._close_bar()
            self._open_bar(bucket, px, qty)
            return
        self._h = max(self._h, px)
        self._l = min(self._l, px)
        self._c = px
        self._vol += max(0.0, qty)

    def _open_bar(self, bucket_ms: int, px: float, qty: float) -> None:
        self._bucket_ms = bucket_ms
        self._o = self._h = self._l = self._c = px
        self._vol = max(0.0, qty)

    def _close_bar(self) -> None:
        if self._bucket_ms is None or self._o <= 0:
            return
        self._bars.append(
            OHLCBar(
                o=self._o, h=self._h, l=self._l, c=self._c,
                volume=self._vol, ts_ms=self._bucket_ms,
            )
        )
        self._bucket_ms = None

    def bars(self) -> list[OHLCBar]:
        return list(self._bars)


# ── Pure stats ────────────────────────────────────────────────────────────────


def _mean(xs: list[float]) -> float:
    return sum(xs) / len(xs) if xs else 0.0


def _var(xs: list[float], mean: float | None = None) -> float:
    if len(xs) < 2:
        return 0.0
    if mean is None:
        mean = _mean(xs)
    return sum((x - mean) ** 2 for x in xs) / (len(xs) - 1)


def _cov(xs: list[float], ys: list[float]) -> float:
    n = len(xs)
    if n < 2 or n != len(ys):
        return 0.0
    mx, my = _mean(xs), _mean(ys)
    return sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / (n - 1)


def _corr(xs: list[float], ys: list[float]) -> float:
    vx, vy = _var(xs), _var(ys)
    if vx <= 0 or vy <= 0:
        return 0.0
    return _cov(xs, ys) / math.sqrt(vx * vy)


# ── Microstructure features ───────────────────────────────────────────────────


def kyle_lambda(prices: list[float], signed_vols: list[float]) -> float:
    """Price impact: OLS slope of `Δprice` on `signed_volume`.

    Returns 0.0 when either series has insufficient variance.
    """
    if len(prices) < 3 or len(prices) != len(signed_vols):
        return 0.0
    dprices = [prices[i] - prices[i - 1] for i in range(1, len(prices))]
    svol_aligned = signed_vols[1:]
    if not dprices:
        return 0.0
    var_svol = _var(svol_aligned)
    if var_svol <= 0:
        return 0.0
    cov_dp_sv = _cov(dprices, svol_aligned)
    return cov_dp_sv / var_svol


def taker_lambda(prices: list[float], taker_signed_vols: list[float]) -> float:
    """Same as kyle but only when trade was taker-initiated.

    Caller must zero out maker-initiated volumes in `taker_signed_vols`.
    """
    return kyle_lambda(prices, taker_signed_vols)


def vpin_rolling(buy_vols: list[float], sell_vols: list[float], n_buckets: int = 50) -> float:
    """Volume-synchronised PIN over N volume buckets.

    Methodology (Easley-Lopez-O'Hara 2012):
      1. Aggregate consecutive trades into N buckets of equal volume.
      2. For each bucket compute |buy - sell| / (buy + sell).
      3. Return mean across buckets.

    Returns 0.0 when total volume too small to fill > 1 bucket.
    """
    total_vol = sum(buy_vols) + sum(sell_vols)
    if total_vol <= 0 or len(buy_vols) != len(sell_vols):
        return 0.0
    bucket_vol = total_vol / n_buckets
    if bucket_vol <= 0:
        return 0.0
    # Cumulate trade-by-trade into V-buckets
    cum_buy = 0.0
    cum_sell = 0.0
    cum_total = 0.0
    bucket_results: list[float] = []
    for b, s in zip(buy_vols, sell_vols):
        cum_buy += b
        cum_sell += s
        cum_total += b + s
        # Emit a bucket each time we cross threshold
        while cum_total >= bucket_vol and len(bucket_results) < n_buckets:
            imbalance = abs(cum_buy - cum_sell) / max(1e-9, cum_buy + cum_sell)
            bucket_results.append(imbalance)
            cum_buy = 0.0
            cum_sell = 0.0
            cum_total = 0.0
    if not bucket_results:
        return 0.0
    return sum(bucket_results) / len(bucket_results)


def tick_autocorr_lag1(prices: list[float]) -> float:
    """Lag-1 autocorrelation of price increments."""
    if len(prices) < 3:
        return 0.0
    dprices = [prices[i] - prices[i - 1] for i in range(1, len(prices))]
    if len(dprices) < 2:
        return 0.0
    lag0 = dprices[:-1]
    lag1 = dprices[1:]
    return _corr(lag0, lag1)


def roll_spread_est(prices: list[float]) -> float:
    """Roll (1984) implicit-spread estimator: 2·√(-cov(Δp_t, Δp_{t-1})).

    Returns 0.0 when serial cov is non-negative (model unidentified).
    """
    if len(prices) < 3:
        return 0.0
    dprices = [prices[i] - prices[i - 1] for i in range(1, len(prices))]
    if len(dprices) < 2:
        return 0.0
    cov = _cov(dprices[:-1], dprices[1:])
    if cov >= 0:
        return 0.0
    return 2.0 * math.sqrt(-cov)


def hurst_exp(prices: list[float], window: int = 50) -> float:
    """Rescaled-range (R/S) estimator of the Hurst exponent.

    H ≈ 0.5 → random walk
    H > 0.5 → trending (persistent)
    H < 0.5 → mean-reverting (anti-persistent)

    Returns 0.5 when input is degenerate.
    """
    if len(prices) < max(window, 10):
        return 0.5
    series = prices[-window:]
    n = len(series)
    if n < 10:
        return 0.5
    # Compute log returns
    log_rets = []
    for i in range(1, n):
        if series[i - 1] > 0 and series[i] > 0:
            try:
                log_rets.append(math.log(series[i] / series[i - 1]))
            except (ValueError, ZeroDivisionError):
                continue
    if len(log_rets) < 8:
        return 0.5
    # Try chunks of size 4, 8, 16 (powers of 2 ≤ len/2)
    chunk_sizes = []
    sz = 4
    while sz <= len(log_rets) // 2:
        chunk_sizes.append(sz)
        sz *= 2
    if len(chunk_sizes) < 2:
        return 0.5
    log_n = []
    log_rs = []
    for sz in chunk_sizes:
        rs_vals = []
        for start in range(0, len(log_rets) - sz + 1, sz):
            chunk = log_rets[start:start + sz]
            mean = _mean(chunk)
            dev = [chunk[i] - mean for i in range(sz)]
            cum = [sum(dev[:i + 1]) for i in range(sz)]
            R = max(cum) - min(cum)
            S = math.sqrt(_var(chunk, mean))
            if S > 0:
                rs_vals.append(R / S)
        if rs_vals:
            log_n.append(math.log(sz))
            log_rs.append(math.log(_mean(rs_vals)))
    if len(log_n) < 2:
        return 0.5
    # OLS slope = Hurst exponent
    mx = _mean(log_n)
    my = _mean(log_rs)
    num = sum((log_n[i] - mx) * (log_rs[i] - my) for i in range(len(log_n)))
    den = sum((log_n[i] - mx) ** 2 for i in range(len(log_n)))
    if den <= 0:
        return 0.5
    hurst = num / den
    return max(0.0, min(1.0, hurst))


def ohlc_vol_estimators(bars: list[OHLCBar]) -> dict[str, float]:
    """Garman-Klass, Parkinson, Yang-Zhang from rolling OHLC bars (v13 parity)."""
    out = {"garman_klass_vol": 0.0, "parkinson_vol": 0.0, "yang_zhang_vol": 0.0}
    n = len(bars)
    if n < 3:
        return out
    try:
        gk_sum = 0.0
        pk_sum = 0.0
        closes: list[float] = []
        overnight_vars: list[float] = []
        for i, b in enumerate(bars):
            if b.h <= 0 or b.l <= 0 or b.o <= 0 or b.c <= 0:
                continue
            log_hl = math.log(b.h / b.l)
            log_co = math.log(b.c / b.o)
            gk_sum += 0.5 * log_hl ** 2 - (2 * _LN2 - 1) * log_co ** 2
            pk_sum += log_hl ** 2
            closes.append(b.c)
            if i > 0:
                prev_c = bars[i - 1].c
                if prev_c > 0:
                    overnight_vars.append(math.log(b.o / prev_c) ** 2)
        if n > 1:
            out["garman_klass_vol"] = math.sqrt(max(0.0, gk_sum / n))
            out["parkinson_vol"] = math.sqrt(max(0.0, pk_sum / (4.0 * n * _LN2)))
        if len(overnight_vars) >= 2 and len(closes) >= 2:
            k = 0.34 / (1.34 + (n + 1) / (n - 1)) if n > 1 else 0.34
            sigma_o2 = sum(overnight_vars) / len(overnight_vars)
            log_ret = [
                math.log(closes[i] / closes[i - 1])
                for i in range(1, len(closes))
                if closes[i - 1] > 0
            ]
            if log_ret:
                mean_lr = sum(log_ret) / len(log_ret)
                sigma_cc2 = sum((r - mean_lr) ** 2 for r in log_ret) / len(log_ret)
            else:
                sigma_cc2 = 0.0
            rs_sum = 0.0
            rs_cnt = 0
            for b in bars:
                if b.h > 0 and b.l > 0 and b.o > 0 and b.c > 0:
                    rs_sum += (
                        math.log(b.h / b.c) * math.log(b.h / b.o)
                        + math.log(b.l / b.c) * math.log(b.l / b.o)
                    )
                    rs_cnt += 1
            sigma_rs2 = rs_sum / rs_cnt if rs_cnt > 0 else 0.0
            yz_var = sigma_o2 + k * sigma_rs2 + (1 - k) * sigma_cc2
            out["yang_zhang_vol"] = math.sqrt(max(0.0, yz_var))
    except Exception:
        pass
    return out


def amihud_illiquidity(bars: list[OHLCBar]) -> float:
    """Mean |return| / notional volume over consecutive bars."""
    if len(bars) < 3:
        return 0.0
    try:
        ratios: list[float] = []
        for i in range(1, len(bars)):
            c_prev = bars[i - 1].c
            c_curr = bars[i].c
            vol = bars[i].volume * bars[i].c
            if c_prev > 0 and vol > 1e-6:
                ratios.append(abs(c_curr / c_prev - 1.0) / vol)
        return sum(ratios) / len(ratios) if ratios else 0.0
    except Exception:
        return 0.0


def pin_estimate_from_flow(buy_vols: list[float], sell_vols: list[float]) -> float:
    """Simplified PIN proxy: |Σbuy − Σsell| / (Σbuy + Σsell)."""
    if len(buy_vols) < 20 or len(buy_vols) != len(sell_vols):
        return 0.0
    try:
        b_total = sum(buy_vols)
        s_total = sum(sell_vols)
        total = b_total + s_total
        if total < 1e-6:
            return 0.0
        return abs(b_total - s_total) / total
    except Exception:
        return 0.0


# ── Aggregator ────────────────────────────────────────────────────────────────


def compute_all(
    *,
    prices: list[float],
    signed_vols: list[float],
    taker_signed_vols: list[float] | None = None,
    buy_vols: list[float] | None = None,
    sell_vols: list[float] | None = None,
    bars: list[OHLCBar] | None = None,
    funding_rate: float = 0.0,
    vol_regime_code: float = 0.0,
) -> dict[str, float]:
    """Compute all v2 features at once. Missing inputs → feature absent."""
    out: dict[str, float] = {}
    vol_feats = ohlc_vol_estimators(bars or [])
    out.update(vol_feats)
    out["amihud_illiquidity"] = amihud_illiquidity(bars or [])
    out["pin_estimate"] = pin_estimate_from_flow(buy_vols or [], sell_vols or [])
    if prices and signed_vols:
        k_lambda = kyle_lambda(prices, signed_vols)
        if k_lambda != 0.0:
            out["kyle_lambda"] = k_lambda
    if prices and taker_signed_vols:
        t_lambda = taker_lambda(prices, taker_signed_vols)
        if t_lambda != 0.0:
            out["taker_lambda"] = t_lambda
    if buy_vols and sell_vols:
        vpin = vpin_rolling(buy_vols, sell_vols)
        if vpin > 0:
            out["vpin_rolling"] = vpin
            # vpin_x_funding
            sign_f = 1.0 if funding_rate > 0 else (-1.0 if funding_rate < 0 else 0.0)
            out["vpin_x_funding"] = vpin * sign_f
            # kyle_x_vpin
            if "kyle_lambda" in out:
                out["kyle_x_vpin"] = out["kyle_lambda"] * vpin
    if prices:
        ac = tick_autocorr_lag1(prices)
        out["tick_autocorr_lag1"] = ac
        rs = roll_spread_est(prices)
        if rs > 0:
            out["roll_spread_est"] = rs
        h = hurst_exp(prices)
        out["hurst_exp_50"] = h
        out["hurst_x_vol_regime"] = h * vol_regime_code
    return out


# ── Standalone service (opt-in via USE_MICROSTRUCTURE_V2_SERVICE=1) ──────────


def _service_main() -> int:
    """Standalone tick-consumer service that writes microstruct:ctx:{symbol}.

    Subscribes to `stream:tick_{SYMBOL}` for each configured symbol,
    maintains in-memory rolling window of prices + signed volumes,
    writes JSON snapshot every INTERVAL_S.
    """
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    log = logger

    REDIS_URL = os.getenv("REDIS_URL", "redis://redis-ticks:6379/0")
    SYMBOLS = [s.strip().upper() for s in os.getenv(
        "MSV2_SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT,1000PEPEUSDT"
    ).split(",") if s.strip()]
    WINDOW = int(os.getenv("MSV2_WINDOW_TICKS", "200"))
    INTERVAL_S = int(os.getenv("MSV2_INTERVAL_S", "30"))
    TTL_SEC = int(os.getenv("MSV2_TTL_SEC", "120"))
    HASH_PREFIX = os.getenv("MSV2_HASH_PREFIX", "microstruct:ctx:")
    METRICS_PORT = int(os.getenv("METRICS_PORT", "9881"))
    MAIN_REDIS = os.getenv("MSV2_PUBLISH_URL", os.getenv("REDIS_PUBLISH_URL",
                          "redis://redis-worker-1:6379/0"))

    try:
        from prometheus_client import Counter, Gauge, start_http_server
        _ticks_total = Counter("msv2_ticks_total", "Ticks processed", ["symbol"])
        _publishes = Counter("msv2_publishes_total", "Snapshots published")
        _last_ok_ms = Gauge("msv2_last_ok_ms", "Last successful publish ts ms")
    except Exception:
        _ticks_total = _publishes = _last_ok_ms = None
        start_http_server = None  # type: ignore

    if start_http_server is not None:
        try:
            start_http_server(METRICS_PORT)
        except Exception:
            pass

    try:
        import redis
    except ImportError:
        log.error("redis-py not installed")
        return 2

    r_read = redis.from_url(REDIS_URL, decode_responses=True)
    # Some setups route publish to a different host (worker-1)
    r_write = redis.from_url(MAIN_REDIS, decode_responses=True)

    # Rolling buffers per symbol
    prices: dict[str, deque] = {s: deque(maxlen=WINDOW) for s in SYMBOLS}
    svols: dict[str, deque] = {s: deque(maxlen=WINDOW) for s in SYMBOLS}
    buys: dict[str, deque] = {s: deque(maxlen=WINDOW) for s in SYMBOLS}
    sells: dict[str, deque] = {s: deque(maxlen=WINDOW) for s in SYMBOLS}
    takers: dict[str, deque] = {s: deque(maxlen=WINDOW) for s in SYMBOLS}
    bar_aggs: dict[str, MinuteBarAggregator] = {
        s: MinuteBarAggregator() for s in SYMBOLS
    }
    last_ids: dict[str, str] = {s: "$" for s in SYMBOLS}

    log.info("microstruct_v2: symbols=%s window=%d interval=%ds", SYMBOLS, WINDOW, INTERVAL_S)
    last_publish = time.monotonic()
    global _running
    _running = True

    def _sighandler(signum, _frame):
        global _running
        log.info("signal %d → exit", signum)
        _running = False

    _signal.signal(_signal.SIGTERM, _sighandler)
    _signal.signal(_signal.SIGINT, _sighandler)

    while _running:
        try:
            # Build stream→id map for XREAD
            streams = {f"stream:tick_{s}": last_ids[s] for s in SYMBOLS}
            try:
                resp = r_read.xread(streams, count=200, block=2000)
            except Exception as e:
                log.debug("XREAD: %s", e)
                resp = []
            for stream_key, entries in (resp or []):
                # Stream key parsing: "stream:tick_BTCUSDT" → symbol
                sym = stream_key.split("tick_", 1)[-1] if "tick_" in stream_key else None
                if sym is None or sym not in prices:
                    continue
                for entry_id, fields in entries:
                    last_ids[sym] = entry_id
                    try:
                        px = float(fields.get("p") or fields.get("price") or 0.0)
                        if px <= 0:
                            continue
                        qty = float(fields.get("q") or fields.get("qty") or 0.0)
                        side = (fields.get("s") or fields.get("side") or "").lower()
                        is_taker = (fields.get("m") or fields.get("is_buyer_maker") or "0") in ("0", "false", "False")
                        sign = 1.0 if side.startswith("b") else (-1.0 if side.startswith("s") else 0.0)
                        try:
                            ts_ms = int(str(entry_id).split("-")[0])
                        except (TypeError, ValueError):
                            ts_ms = int(time.time() * 1000)
                        prices[sym].append(px)
                        svols[sym].append(sign * qty)
                        bar_aggs[sym].on_tick(px, qty, ts_ms)
                        if sign > 0:
                            buys[sym].append(qty)
                            sells[sym].append(0.0)
                        elif sign < 0:
                            buys[sym].append(0.0)
                            sells[sym].append(qty)
                        else:
                            buys[sym].append(0.0)
                            sells[sym].append(0.0)
                        takers[sym].append(sign * qty if is_taker else 0.0)
                        if _ticks_total is not None:
                            try:
                                _ticks_total.labels(symbol=sym).inc()
                            except Exception:
                                pass
                    except Exception:
                        continue

            now = time.monotonic()
            if now - last_publish >= INTERVAL_S:
                for sym in SYMBOLS:
                    if len(prices[sym]) < 20:
                        continue
                    # Read funding from ctx:deriv:{sym} (if any)
                    funding = 0.0
                    try:
                        raw_deriv = r_write.get(f"ctx:deriv:{sym}")
                        if raw_deriv:
                            funding = float((json.loads(raw_deriv) or {}).get("funding_rate") or 0.0)
                    except Exception:
                        pass
                    feats = compute_all(
                        prices=list(prices[sym]),
                        signed_vols=list(svols[sym]),
                        taker_signed_vols=list(takers[sym]),
                        buy_vols=list(buys[sym]),
                        sell_vols=list(sells[sym]),
                        bars=bar_aggs[sym].bars(),
                        funding_rate=funding,
                        vol_regime_code=0.0,
                    )
                    if not feats:
                        continue
                    feats["ts_ms"] = int(time.time() * 1000)
                    try:
                        r_write.set(f"{HASH_PREFIX}{sym}", json.dumps(feats), ex=TTL_SEC)
                        if _publishes is not None:
                            _publishes.inc()
                    except Exception as e:
                        log.warning("publish %s failed: %s", sym, e)
                if _last_ok_ms is not None:
                    try:
                        _last_ok_ms.set(int(time.time() * 1000))
                    except Exception:
                        pass
                last_publish = now

        except Exception as e:
            log.exception("loop error: %s", e)
            time.sleep(1)

    log.info("stopped")
    return 0


if __name__ == "__main__":
    sys.exit(_service_main())
