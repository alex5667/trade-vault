from __future__ import annotations

"""TTL-cached reader for last_trade_outcome_raw (audit 2026-05-19).

Background
----------
`compute_group_me.last_trade_outcome_raw` expects `runtime.last_trade_pnl_bps`,
but no trade_close hook writes it in the active pipeline (TickProcessor lives
in reference/, trade_close pipeline writes to Redis `trades:closed` stream
without back-propagating into runtime).

This module provides a per-symbol TTL-cached XREVRANGE reader that fetches
the last closed trade for the symbol from `trades:closed` and exposes
`pnl_bps` (computed from `pnl_pct` × 100). Wired into
`signal_pipeline.publish_signal()` so the value lands in
`signal["indicators"]["last_trade_outcome_raw"]` before v12_of inject.

Design
------
- Bounded XREVRANGE (COUNT=200) per cache miss; ~1ms typical.
- Per-symbol TTL cache (60s) — trades close infrequently per symbol.
- Module-level singleton; safe across signal_pipeline instances.
- Fail-open: any exception returns 0.0; never blocks signal publish.
"""

import time
from threading import Lock


_TTL_S = 60.0    # cache per-symbol pnl_bps for 60s
_XREV_COUNT = 500  # scan up to 500 recent closed trades per lookup
# High-symbol environments: 200 was too narrow (200 entries / ~60 symbols ≈ 3
# closes per symbol before the scan exhausts). 500 keeps p99 miss-rate low
# while staying well under 1ms on warm Redis (stream in memory).
_STREAM = "trades:closed"
# Optional per-symbol last-outcome hash key written by the trade close joiner
# (key: "trades:last_outcome:{symbol}", field: "pnl_bps"). Falls back to
# XREVRANGE scan if absent (zero-write-side-effect on reader).
_HASH_PREFIX = "trades:last_outcome:"


class _LastTradeOutcomeReader:
    """Module-level singleton with per-symbol TTL cache."""

    def __init__(self) -> None:
        # symbol -> (pnl_bps, expire_at_s)
        self._cache: dict[str, tuple[float, float]] = {}
        self._lock = Lock()
        self._redis_factory = None

    def _get_redis(self):
        """Lazy redis client. Reuses core.redis_client.get_redis() factory
        (project-standard, sync) if available; otherwise constructs a sync
        client from REDIS_WORKER_HOST/PORT env."""
        if self._redis_factory is not None:
            try:
                return self._redis_factory()
            except Exception:
                return None
        try:
            from core.redis_client import get_redis  # type: ignore
            self._redis_factory = get_redis
            return self._redis_factory()
        except Exception:
            pass
        try:
            import os
            import redis  # type: ignore
            host = os.getenv("REDIS_WORKER_HOST", "redis-worker-1")
            port = int(os.getenv("REDIS_WORKER_PORT", "6379"))
            self._redis_factory = lambda: redis.Redis(
                host=host, port=port, decode_responses=True,
                socket_connect_timeout=2, socket_timeout=2,
            )
            return self._redis_factory()
        except Exception:
            return None

    def _read_pnl_bps(self, symbol: str) -> float:
        """Read pnl_bps for the symbol's last closed trade.

        Strategy:
        1. Try per-symbol hash key `trades:last_outcome:{symbol}` (O(1)).
           Written by trade close joiner if JOINER_WRITE_LAST_OUTCOME=1.
        2. Fall back to XREVRANGE scan (up to _XREV_COUNT entries).
        """
        r = self._get_redis()
        if r is None:
            return 0.0
        # Fast path: per-symbol hash key
        try:
            raw = r.hget(f"{_HASH_PREFIX}{symbol}", "pnl_bps")
            if raw is not None:
                return float(raw)
        except Exception:
            pass
        # Slow path: scan stream
        try:
            entries = r.xrevrange(_STREAM, count=_XREV_COUNT)
        except Exception:
            return 0.0
        for _msg_id, data in entries:
            try:
                sym = data.get("symbol") if isinstance(data, dict) else None
                if isinstance(sym, bytes):
                    sym = sym.decode()
                if sym != symbol:
                    continue
                pnl_pct = data.get("pnl_pct") if isinstance(data, dict) else None
                if isinstance(pnl_pct, bytes):
                    pnl_pct = pnl_pct.decode()
                if pnl_pct in (None, ""):
                    # Fallback: compute from entry/exit/side
                    side = data.get("side", "")
                    if isinstance(side, bytes):
                        side = side.decode()
                    try:
                        entry = float(data.get("entry_px", 0) or 0)
                        exit_ = float(data.get("exit_px", 0) or 0)
                    except Exception:
                        continue
                    if entry <= 0 or exit_ <= 0:
                        continue
                    sign = 1.0 if str(side).upper() in ("LONG", "BUY") else -1.0
                    return sign * (exit_ - entry) / entry * 10_000.0
                try:
                    return float(pnl_pct) * 100.0  # pct → bps
                except Exception:
                    continue
            except Exception:
                continue
        return 0.0

    def get_pnl_bps(self, symbol: str) -> float:
        """Return cached pnl_bps for the symbol's last closed trade.

        Returns 0.0 if no closed trade is found within the scan window or on
        any error (fail-open).
        """
        now = time.time()
        with self._lock:
            cached = self._cache.get(symbol)
            if cached is not None and cached[1] > now:
                return cached[0]
        try:
            pnl_bps = self._read_pnl_bps(symbol)
        except Exception:
            pnl_bps = 0.0
        with self._lock:
            self._cache[symbol] = (pnl_bps, now + _TTL_S)
        return pnl_bps


_READER = _LastTradeOutcomeReader()


def get_last_trade_outcome_bps(symbol: str) -> float:
    """Public accessor — fail-open per-symbol pnl_bps of last closed trade."""
    try:
        return _READER.get_pnl_bps(str(symbol or ""))
    except Exception:
        return 0.0
