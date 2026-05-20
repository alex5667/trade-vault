from __future__ import annotations

"""Python-side cross-asset correlation reader (audit 2026-05-19 Phase 5).

Computes `eth_btc_corr_5m` from raw tick streams on redis-ticks when the
go-worker REST polling into `runtime:crossasset:{SYMBOL}` is unavailable.

Background
----------
`compute_group_md.eth_btc_corr_5m` expects `runtime.eth_btc_corr_5m` populated
by `SymbolRuntime.maybe_load_crossasset()` from a Redis hash written by
go-worker. The hash does not exist in prod (SCAN confirmed empty), so the
key is perma-zero.

This reader fills the gap entirely in Python: subscribe to
`stream:tick_BTCUSDT` and `stream:tick_ETHUSDT` (already published by
go-worker), keep a 5-minute rolling window of (ts_ms, log_return) pairs per
symbol, compute Pearson correlation on demand.

Design
------
- Singleton, thread-safe deque per symbol (BTCUSDT, ETHUSDT)
- Update strategy: opportunistic on-demand XREVRANGE refresh (TTL 5s) — no
  background thread, no extra Redis subscription
- Returns 0.0 when fewer than _MIN_SAMPLES paired observations
- Fail-open everywhere
"""

import math
import os
import time
from threading import Lock
from typing import Sequence

_WINDOW_MS = 5 * 60 * 1000  # 5-minute rolling correlation window
_TICK_REFRESH_S = 5.0       # refresh tick history at most every 5s
_CORR_TTL_S = 30.0          # cached correlation value valid 30s
_MIN_SAMPLES = 10           # min paired observations for meaningful Pearson
_XREV_COUNT = 1000          # max ticks per refresh
_BTC = "BTCUSDT"
_ETH = "ETHUSDT"


def _pearson(xs: Sequence[float], ys: Sequence[float]) -> float:
    n = min(len(xs), len(ys))
    if n < _MIN_SAMPLES:
        return 0.0
    xs = xs[:n]; ys = ys[:n]
    mx = sum(xs) / n
    my = sum(ys) / n
    cov = sum((xs[i] - mx) * (ys[i] - my) for i in range(n))
    vx = sum((xs[i] - mx) ** 2 for i in range(n))
    vy = sum((ys[i] - my) ** 2 for i in range(n))
    denom = math.sqrt(vx * vy)
    if denom <= 0.0:
        return 0.0
    return max(-1.0, min(1.0, cov / denom))


def _align_returns(
    btc: list[tuple[int, float]],
    eth: list[tuple[int, float]],
    bucket_ms: int = 1000,
) -> tuple[list[float], list[float]]:
    """Align ticks into 1-second buckets and compute log returns per bucket."""
    if len(btc) < 2 or len(eth) < 2:
        return [], []

    def _to_bucket(seq: list[tuple[int, float]]) -> dict[int, float]:
        # Last-price-wins per bucket
        out: dict[int, float] = {}
        for ts, px in seq:
            out[ts // bucket_ms] = px
        return out

    bb = _to_bucket(btc)
    eb = _to_bucket(eth)
    common = sorted(set(bb.keys()) & set(eb.keys()))
    if len(common) < _MIN_SAMPLES + 1:
        return [], []
    btc_ret: list[float] = []
    eth_ret: list[float] = []
    for i in range(1, len(common)):
        p_b0, p_b1 = bb[common[i - 1]], bb[common[i]]
        p_e0, p_e1 = eb[common[i - 1]], eb[common[i]]
        if p_b0 > 0 and p_b1 > 0 and p_e0 > 0 and p_e1 > 0:
            btc_ret.append(math.log(p_b1 / p_b0))
            eth_ret.append(math.log(p_e1 / p_e0))
    return btc_ret, eth_ret


class _CrossAssetCorrReader:
    """Singleton: rolling 5m BTC/ETH correlation reader."""

    def __init__(self) -> None:
        self._lock = Lock()
        self._tick_cache: dict[str, list[tuple[int, float]]] = {}
        self._tick_last_refresh: dict[str, float] = {}
        self._corr_cached_value: float = 0.0
        self._corr_cached_at: float = 0.0
        self._redis_factory = None

    def _get_redis(self):
        """Lazy redis client targeting redis-ticks (where tick streams live)."""
        if self._redis_factory is not None:
            try:
                return self._redis_factory()
            except Exception:
                return None
        try:
            import redis  # type: ignore
            host = os.getenv("REDIS_TICKS_HOST", "redis-ticks")
            port = int(os.getenv("REDIS_TICKS_PORT", "6379"))
            self._redis_factory = lambda: redis.Redis(
                host=host, port=port, decode_responses=True,
                socket_connect_timeout=2, socket_timeout=2,
            )
            return self._redis_factory()
        except Exception:
            return None

    def _refresh_ticks(self, symbol: str) -> list[tuple[int, float]]:
        """Refresh per-symbol tick cache; return list of (ts_ms, price)."""
        now = time.time()
        last = self._tick_last_refresh.get(symbol, 0.0)
        cached = self._tick_cache.get(symbol, [])
        if cached and (now - last) < _TICK_REFRESH_S:
            return cached
        r = self._get_redis()
        if r is None:
            return cached
        try:
            entries = r.xrevrange(f"stream:tick_{symbol}", count=_XREV_COUNT)
        except Exception:
            return cached
        cutoff_ms = int(now * 1000.0) - _WINDOW_MS
        out: list[tuple[int, float]] = []
        for msg_id, fields in entries:
            try:
                ts_ms = int(msg_id.split("-")[0]) if isinstance(msg_id, str) else int(msg_id.decode().split("-")[0])
                if ts_ms < cutoff_ms:
                    break
                price = float(fields.get("price", 0) or 0)
                if price > 0:
                    out.append((ts_ms, price))
            except Exception:
                continue
        out.sort(key=lambda kv: kv[0])
        self._tick_cache[symbol] = out
        self._tick_last_refresh[symbol] = now
        return out

    def eth_btc_corr_5m(self) -> float:
        """Return cached 5-minute log-return correlation between ETH and BTC."""
        now = time.time()
        with self._lock:
            if (now - self._corr_cached_at) < _CORR_TTL_S:
                return self._corr_cached_value
        try:
            btc = self._refresh_ticks(_BTC)
            eth = self._refresh_ticks(_ETH)
            btc_ret, eth_ret = _align_returns(btc, eth)
            corr = _pearson(btc_ret, eth_ret)
        except Exception:
            corr = 0.0
        with self._lock:
            self._corr_cached_value = corr
            self._corr_cached_at = now
        return corr


_READER = _CrossAssetCorrReader()


def get_eth_btc_corr_5m() -> float:
    """Public accessor — fail-open Pearson correlation ETH↔BTC (5m, 1s buckets)."""
    try:
        return _READER.eth_btc_corr_5m()
    except Exception:
        return 0.0
