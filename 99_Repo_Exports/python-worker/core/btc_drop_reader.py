from __future__ import annotations

"""BTC 5-minute return reader (Plan 3.4 Cross-asset correlation gate).

Computes BTC 5m fractional return from raw tick stream `stream:tick_BTCUSDT`
on redis-ticks when the upstream `runtime.btc_ret_5m` indicator is unavailable
or stale (warm-up, gap, missing v14_of OE fields).

Return semantics:
    fractional change = (price_now - price_5m_ago) / price_5m_ago
e.g. -0.01 ≡ -1% drop over 5 min.

Design mirrors `core.cross_asset_corr_reader`:
- Singleton, thread-safe cache
- Opportunistic XREVRANGE refresh (TTL 5s); cached return value valid 10s
- Returns None when insufficient data (caller decides fail-open semantics)
- All Redis errors swallowed; never raises
"""

import os
import time
from threading import Lock

_BTC = "BTCUSDT"
_WINDOW_MS = 5 * 60 * 1000     # 5-minute look-back window
_BUCKET_MS = 1_000             # 1-second buckets to reduce tick noise
_TICK_REFRESH_S = 5.0          # refresh tick history at most every 5s
_RET_TTL_S = 10.0              # cached return value valid 10s
_XREV_COUNT = 1500             # max ticks per refresh (5min × ~5 ticks/s headroom)
_ANCHOR_TOLERANCE_MS = 30_000  # acceptable drift around 5m mark when picking anchor


class _BtcDropReader:
    """Singleton: rolling BTC 5m fractional-return reader from tick stream."""

    def __init__(self) -> None:
        self._lock = Lock()
        self._ticks: list[tuple[int, float]] = []  # (ts_ms, price) ascending
        self._last_refresh: float = 0.0
        self._cached_ret: float | None = None
        self._cached_at: float = 0.0
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

    def _refresh_ticks(self) -> list[tuple[int, float]]:
        now = time.time()
        if self._ticks and (now - self._last_refresh) < _TICK_REFRESH_S:
            return self._ticks
        r = self._get_redis()
        if r is None:
            return self._ticks
        try:
            entries = r.xrevrange(f"stream:tick_{_BTC}", count=_XREV_COUNT)
        except Exception:
            return self._ticks
        cutoff_ms = int(now * 1000.0) - _WINDOW_MS - _ANCHOR_TOLERANCE_MS
        bucket: dict[int, float] = {}
        for msg_id, fields in entries:
            try:
                raw_id = msg_id if isinstance(msg_id, str) else msg_id.decode()
                ts_ms = int(raw_id.split("-")[0])
                if ts_ms < cutoff_ms:
                    break
                price = float(fields.get("price", 0) or 0)
                if price > 0:
                    bucket[ts_ms // _BUCKET_MS] = price  # last-px-wins per bucket
            except Exception:
                continue
        out = sorted(((b * _BUCKET_MS, px) for b, px in bucket.items()), key=lambda kv: kv[0])
        self._ticks = out
        self._last_refresh = now
        return out

    def btc_ret_5m(self) -> float | None:
        """Latest BTC 5m fractional return, or None when data is insufficient.

        Caller fails-open on None (treats missing data as "no drop").
        """
        now = time.time()
        with self._lock:
            if self._cached_ret is not None and (now - self._cached_at) < _RET_TTL_S:
                return self._cached_ret
        try:
            ticks = self._refresh_ticks()
        except Exception:
            ticks = []
        if len(ticks) < 2:
            return None

        latest_ts, latest_px = ticks[-1]
        target_ts = latest_ts - _WINDOW_MS
        anchor_ts, anchor_px = ticks[0]
        # Pick the oldest bucket whose ts is >= target_ts (i.e. the bucket
        # closest to "5 min ago" without going too far back). If no bucket sits
        # at/after the target, fall back to the oldest available — that's the
        # widest window we have and is still safe (return magnitude only grows).
        for ts, px in ticks:
            if ts >= target_ts:
                anchor_ts, anchor_px = ts, px
                break

        if anchor_px <= 0 or latest_px <= 0:
            return None
        # Require at least 60s span so we don't return noisy 0.0 on cold start.
        if (latest_ts - anchor_ts) < 60_000:
            return None

        ret = (latest_px - anchor_px) / anchor_px
        with self._lock:
            self._cached_ret = ret
            self._cached_at = now
        return ret


_READER = _BtcDropReader()


def get_btc_ret_5m() -> float | None:
    """Public accessor — fail-open BTC 5m fractional return (None if no data)."""
    try:
        return _READER.btc_ret_5m()
    except Exception:
        return None
