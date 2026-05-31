from __future__ import annotations

"""Python-side rolling trade-stats reader (audit 2026-05-19 Phase 5).

Computes Group MA features that `compute_group_ma` reads from runtime attrs.
TickProcessor (which would normally maintain these rolling stats) lives in
reference/ — these stats are perma-zero in prod without an extra reader.

Provides per-symbol TTL-cached readers backed by `stream:tick_{SYMBOL}` on
redis-ticks:

  - large_trade_ratio  : share of trades with notional in top 5% by size
                         over the last 5 minutes
  - trade_size_entropy : Shannon entropy of trade sizes (10 log-spaced bins)

Returns 0.0 with fewer than _MIN_SAMPLES observations. Fail-open.
"""

import math
import os
import time
from threading import Lock

_WINDOW_MS = 5 * 60 * 1000   # 5-minute rolling window
_REFRESH_S = 5.0             # refresh per-symbol tick cache at most every 5s
_TTL_S = 10.0                # cached stats valid 10s after compute
_MIN_SAMPLES = 30            # min trades for meaningful stats
_XREV_COUNT = 2000           # cap ticks per refresh (5min × 6 ticks/sec ≈ 1800)
_LARGE_PCTL = 0.95           # "large" trade = size > p95
_ENTROPY_BINS = 10           # Shannon entropy bin count (log-spaced)


def _shannon_entropy(values: list[float], bins: int = _ENTROPY_BINS) -> float:
    n = len(values)
    if n < _MIN_SAMPLES:
        return 0.0
    # Log-space binning — trade sizes are heavy-tailed
    log_vals = [math.log(v) for v in values if v > 0]
    if len(log_vals) < _MIN_SAMPLES:
        return 0.0
    lo = min(log_vals)
    hi = max(log_vals)
    if hi <= lo:
        return 0.0
    width = (hi - lo) / bins
    counts = [0] * bins
    for v in log_vals:
        idx = int((v - lo) / width)
        if idx >= bins:
            idx = bins - 1
        if idx < 0:
            idx = 0
        counts[idx] += 1
    total = sum(counts)
    if total <= 0:
        return 0.0
    ent = 0.0
    for c in counts:
        if c <= 0:
            continue
        p = c / total
        ent -= p * math.log(p)
    # Normalize to [0, 1] by max possible entropy log(bins)
    return ent / math.log(bins)


def _quantile(sorted_vals: list[float], q: float) -> float:
    """Linear interpolation quantile on a pre-sorted list."""
    n = len(sorted_vals)
    if n == 0:
        return 0.0
    if n == 1:
        return sorted_vals[0]
    pos = q * (n - 1)
    lo = int(pos)
    hi = min(lo + 1, n - 1)
    frac = pos - lo
    return sorted_vals[lo] * (1.0 - frac) + sorted_vals[hi] * frac


class _TradeStatsReader:
    """Singleton: per-symbol rolling trade stats from stream:tick."""

    def __init__(self) -> None:
        self._lock = Lock()
        # symbol -> (large_ratio, entropy, expire_at_s)
        self._cache: dict[str, tuple[float, float, float]] = {}
        self._redis_factory = None

    def _get_redis(self):
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

    def _compute(self, symbol: str) -> tuple[float, float]:
        r = self._get_redis()
        if r is None:
            return 0.0, 0.0
        try:
            entries = r.xrevrange(f"stream:tick_{symbol}", count=_XREV_COUNT)
        except Exception:
            return 0.0, 0.0
        cutoff_ms = int(time.time() * 1000.0) - _WINDOW_MS
        sizes: list[float] = []
        for msg_id, fields in entries:
            try:
                ts_ms = int(msg_id.split("-")[0]) if isinstance(msg_id, str) else int(msg_id.decode().split("-")[0])
                if ts_ms < cutoff_ms:
                    break
                qty = float(fields.get("quantity", 0) or 0)
                price = float(fields.get("price", 0) or 0)
                notional = abs(qty * price)
                if notional > 0:
                    sizes.append(notional)
            except Exception:
                continue
        if len(sizes) < _MIN_SAMPLES:
            return 0.0, 0.0
        sorted_sizes = sorted(sizes)
        thresh = _quantile(sorted_sizes, _LARGE_PCTL)
        large_count = sum(1 for s in sizes if s >= thresh)
        large_ratio = float(large_count) / float(len(sizes))
        entropy = _shannon_entropy(sizes)
        return large_ratio, entropy

    def get_stats(self, symbol: str) -> tuple[float, float]:
        """Return cached (large_trade_ratio, trade_size_entropy)."""
        now = time.time()
        with self._lock:
            cached = self._cache.get(symbol)
            if cached is not None and cached[2] > now:
                return cached[0], cached[1]
        try:
            large_ratio, entropy = self._compute(symbol)
        except Exception:
            large_ratio, entropy = 0.0, 0.0
        with self._lock:
            self._cache[symbol] = (large_ratio, entropy, now + _TTL_S)
        return large_ratio, entropy


_READER = _TradeStatsReader()


def get_trade_stats(symbol: str) -> tuple[float, float]:
    """Public accessor — (large_trade_ratio, trade_size_entropy) per symbol.

    Fail-open: returns (0.0, 0.0) on any error or insufficient samples.
    """
    try:
        return _READER.get_stats(str(symbol or ""))
    except Exception:
        return 0.0, 0.0
