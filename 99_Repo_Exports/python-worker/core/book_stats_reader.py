from __future__ import annotations

"""Python-side rolling book-stats reader (audit 2026-05-19 Phase 6).

Computes Group MB features `depth_migration_bps` and `quote_stuffing_score`
that `compute_group_mb` would normally read from a BookProcessor rolling
tracker — but the active BookProcessor doesn't maintain these attrs.

Backed by `stream:book_{SYMBOL}` on redis-ticks (L2 snapshots every few
hundred ms, fields include `bids`, `asks`, `first_id`, `final_id`,
`prev_final`).

Per-symbol TTL-cached. Fail-open everywhere.

  - depth_migration_bps : mean |Δbest_bid + Δbest_ask| in bps between
                          consecutive snapshots in the last 10s window;
                          captures price-edge migration velocity.

  - quote_stuffing_score: median (final_id - prev_final) in the same
                          window, normalized by snapshot rate Hz —
                          proxy for L2-update intensity. High in stuffing
                          regimes.
"""

import json
import os
import time
from threading import Lock

_WINDOW_S = 10.0           # 10-second rolling window
_TTL_S = 5.0               # cache 5s
_MIN_SAMPLES = 5           # min snapshots needed
_XREV_COUNT = 200          # last 200 snapshots (~10s @ ~20 Hz)


def _parse_levels(raw):
    """Decode bids/asks JSON list-of-pairs; return first price or None."""
    if raw is None:
        return None
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode()
    if isinstance(raw, str):
        try:
            arr = json.loads(raw)
        except Exception:
            return None
    else:
        arr = raw
    if not isinstance(arr, list) or not arr:
        return None
    try:
        return float(arr[0][0])
    except Exception:
        return None


def _median(xs: list[float]) -> float:
    n = len(xs)
    if n == 0:
        return 0.0
    s = sorted(xs)
    mid = n // 2
    if n % 2 == 0:
        return (s[mid - 1] + s[mid]) / 2.0
    return s[mid]


class _BookStatsReader:
    def __init__(self) -> None:
        self._lock = Lock()
        # symbol -> (depth_migration_bps, quote_stuffing_score, expire_at_s)
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
            entries = r.xrevrange(f"stream:book_{symbol}", count=_XREV_COUNT)
        except Exception:
            return 0.0, 0.0
        cutoff_ms = int(time.time() * 1000.0) - int(_WINDOW_S * 1000.0)
        snaps: list[tuple[int, float, float, int]] = []
        for msg_id, fields in entries:
            try:
                ts_ms = int(msg_id.split("-")[0]) if isinstance(msg_id, str) else int(msg_id.decode().split("-")[0])
                if ts_ms < cutoff_ms:
                    break
                bid = _parse_levels(fields.get("bids"))
                ask = _parse_levels(fields.get("asks"))
                if bid is None or ask is None or bid <= 0 or ask <= 0:
                    continue
                # delta count between consecutive snapshots from server
                try:
                    delta_n = int(fields.get("final_id", 0) or 0) - int(fields.get("prev_final", 0) or 0)
                except Exception:
                    delta_n = 0
                if delta_n < 0:
                    delta_n = 0
                snaps.append((ts_ms, bid, ask, delta_n))
            except Exception:
                continue
        if len(snaps) < _MIN_SAMPLES:
            return 0.0, 0.0
        snaps.sort(key=lambda x: x[0])  # chronological
        # Depth migration: mean |Δbest_bid + Δbest_ask| / mid in bps per snapshot pair
        bps_changes: list[float] = []
        delta_counts: list[int] = []
        for i in range(1, len(snaps)):
            t0, b0, a0, _ = snaps[i - 1]
            t1, b1, a1, dn = snaps[i]
            mid = (b1 + a1) / 2.0
            if mid <= 0:
                continue
            chg = (abs(b1 - b0) + abs(a1 - a0)) / mid * 10_000.0
            bps_changes.append(chg)
            delta_counts.append(dn)
        if not bps_changes:
            return 0.0, 0.0
        depth_migration_bps = sum(bps_changes) / len(bps_changes)
        # quote_stuffing_score: median delta count normalised by per-second rate
        ts_first = snaps[0][0]
        ts_last = snaps[-1][0]
        secs = max(0.001, (ts_last - ts_first) / 1000.0)
        snaps_per_s = len(snaps) / secs
        median_delta = _median([float(x) for x in delta_counts])
        # Rate = (median deltas per snapshot) × (snapshots per second)
        # Scale: divide by 1000 so typical values land in [0, ~10] range.
        quote_stuffing_score = (median_delta * snaps_per_s) / 1000.0
        return float(depth_migration_bps), float(quote_stuffing_score)

    def get_stats(self, symbol: str) -> tuple[float, float]:
        now = time.time()
        with self._lock:
            cached = self._cache.get(symbol)
            if cached is not None and cached[2] > now:
                return cached[0], cached[1]
        try:
            dm, qs = self._compute(symbol)
        except Exception:
            dm, qs = 0.0, 0.0
        with self._lock:
            self._cache[symbol] = (dm, qs, now + _TTL_S)
        return dm, qs


_READER = _BookStatsReader()


def get_book_stats(symbol: str) -> tuple[float, float]:
    """(depth_migration_bps, quote_stuffing_score) per symbol — fail-open."""
    try:
        return _READER.get_stats(str(symbol or ""))
    except Exception:
        return 0.0, 0.0
