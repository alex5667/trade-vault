"""ATR cache / reader for multiple legacy key shapes.

We keep this module read-optimised because production uses many key shapes:

  - ``ATR:{symbol}:{TF}``          hash  (tracker-style, has ``lastCloseTime``)
  - ``atr:{symbol}:{tf}``          string
  - ``atr:val:{symbol}:{tf}``      string (legacy mirror)
  - ``atr:json:{symbol}:{tf}``     JSON  (includes ``ts``)
  - ``ta:last:atr:{symbol}``       JSON  (includes ``tf`` + ``ts``)
  - ``cfg:atr_sel_meta:{symbol}``  JSON  (pre-selected meta from sanity calibrator)

Public API
----------
- :class:`ATRCache` — main cache class
- :func:`get_atr_cache` — module-level singleton factory
"""
from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import json
import logging
import os
import time
from typing import Any

import redis

from core.redis_client import get_redis
from utils.helpers import _f, _i

_log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Timeframe normalisation table (lowercase input -> tracker uppercase key)
# ---------------------------------------------------------------------------
_TF_MAP: dict[str, str] = {
    "1m": "M1",  "m1": "M1"
    "5m": "M5",  "m5": "M5"
    "15m": "M15", "m15": "M15"
    "30m": "M30", "m30": "M30"
    "1h": "H1",  "h1": "H1"
    "4h": "H4",  "h4": "H4"
    "1d": "D1",  "d1": "D1"
}


class ATRCache:
    """Read ATR values from Redis, supporting multiple legacy key shapes.

    Args:
        ttl:          TTL (seconds) used when writing ATR values.
        redis_client: Pre-built Redis client (injected mainly in tests).
                      If ``None``, resolved from ``ATR_REDIS_URL`` env var
                      or :func:`~core.redis_client.get_redis`.
    """

    def __init__(
        self
        ttl: int = 3600
        redis_client: redis.Redis | None = None,  # type: ignore[type-arg]
    ) -> None:
        if redis_client is not None:
            self.redis_client = redis_client
        else:
            url = os.getenv("ATR_REDIS_URL")
            self.redis_client = (
                redis.from_url(url, decode_responses=True) if url else get_redis()
            )
        self.ttl = ttl

    # ------------------------------------------------------------------
    # Public read API
    # ------------------------------------------------------------------

    def get(self, symbol: str, timeframe: str) -> float | None:
        """Return ATR float (best-effort) or ``None``."""
        atr, _ = self.get_with_meta(symbol=symbol, timeframe=timeframe)
        return atr

    def get_with_meta(
        self
        symbol: str
        timeframe: str | None = None
        now_ms: int | None = None
        prefer_src: str = ""
    ) -> tuple[float | None, dict[str, Any]]:
        """Return ``(atr_value, meta)`` using the best available source.

        Meta dict always contains:
            ``src``, ``tf``, ``ts_ms``, ``age_ms``.

        Args:
            symbol:     Trading symbol (e.g. ``"BTCUSDT"``).
            timeframe:  Requested timeframe string. When ``None``, resolved
                        from ``cfg:atr_tf:{symbol}`` Redis key.
            now_ms:     Current epoch-ms. Defaults to ``get_ny_time_millis()``.
            prefer_src: Force-select a specific candidate source name.
        """
        sym = str(symbol)
        nm = int(now_ms) if now_ms is not None else get_ny_time_millis()

        if timeframe is None:
            tf_cfg = self.redis_client.get(f"cfg:atr_tf:{sym}")
            tf: str | None = str(tf_cfg).strip() or None if tf_cfg else None
        else:
            tf = str(timeframe)

        # 1. Prefer sanity-calibrated selection (pre-selected meta)
        meta_raw = self.redis_client.get(f"cfg:atr_sel_meta:{sym}")
        if meta_raw:
            try:
                meta: dict[str, Any] = json.loads(meta_raw)
                atr = _f(meta.get("atr"), 0.0)
                ts_ms = _i(meta.get("ts_ms"), 0)
                age_ms = max(0, nm - ts_ms) if ts_ms > 0 else 0
                meta["age_ms"] = age_ms
                meta.setdefault("src", meta.pop("source", "selected"))
                if atr > 0:
                    return atr, meta
            except Exception:  # noqa: BLE001
                pass

        if not tf:
            return None, {"src": "none", "tf": "na", "ts_ms": 0, "age_ms": 0}

        # 2. prefer_src — pick a specific candidate by name
        candidates = self.get_candidates(symbol=sym, timeframe=tf, now_ms=nm)

        if prefer_src:
            for c in candidates:
                if c.get("src") == prefer_src and float(c.get("atr", 0) or 0) > 0:
                    return float(c["atr"]), _candidate_meta(c, tf)
            # Fallthrough to normal freshness selection if prefer_src not found

        if not candidates:
            return None, {"src": "none", "tf": tf, "ts_ms": 0, "age_ms": 0}

        # 3. Pick freshest (timestamped first, then smallest age)
        best = _pick_best_candidate(candidates)
        if best and float(best.get("atr", 0) or 0) > 0:
            return float(best["atr"]), _candidate_meta(best, tf)

        return None, {"src": "none", "tf": tf, "ts_ms": 0, "age_ms": 0}

    def get_candidates(
        self
        *
        symbol: str
        timeframe: str
        now_ms: int | None = None
    ) -> list[dict[str, Any]]:
        """Return ALL available candidates with metadata for source selection.

        Each candidate dict contains:
            ``atr``, ``src``, ``key``, ``tf``, ``ts_ms``, ``age_ms``, ``has_ts``.
        """
        out: list[dict[str, Any]] = []
        sym = str(symbol or "").upper()
        tf_raw = str(timeframe or "1m")
        tf_norm = _normalize_tracker_tf(tf_raw)
        nm = int(now_ms) if now_ms is not None else get_ny_time_millis()

        # --- 1. Tracker hash ATR:{SYM}:{TFN} ---
        tracker_key = f"ATR:{sym}:{tf_norm}"
        try:
            v_atr, v_ts = self.redis_client.hmget(tracker_key, "atr", "lastCloseTime")
            if v_atr:
                atr = _f(v_atr, 0.0)
                ts_ms = _i(v_ts, 0) if v_ts else 0
                age = max(0, nm - ts_ms) if ts_ms > 0 else 0
                out.append({
                    "atr": atr, "src": "atr_tracker", "key": tracker_key
                    "tf": tf_norm, "ts_ms": ts_ms, "age_ms": age
                    "has_ts": int(ts_ms > 0)
                })
        except Exception:  # noqa: BLE001
            pass

        # --- 2. atr:{sym}:{tf} string ---
        key2 = f"atr:{sym}:{tf_raw}"
        try:
            raw = self.redis_client.get(key2)
            if raw:
                atr = _f(raw, 0.0)
                pttl = self._pttl_ms(key2)
                out.append({
                    "atr": atr, "src": "atr_string", "key": key2
                    "tf": tf_norm, "ts_ms": 0, "age_ms": 0
                    "has_ts": 0, "pttl_ms": pttl
                })
        except Exception:  # noqa: BLE001
            pass

        # --- 3. atr:val:{sym}:{tf} string mirror ---
        key2b = f"atr:val:{sym}:{tf_raw}"
        try:
            raw = self.redis_client.get(key2b)
            if raw:
                atr = _f(raw, 0.0)
                pttl = self._pttl_ms(key2b)
                out.append({
                    "atr": atr, "src": "atr_val", "key": key2b
                    "tf": tf_norm, "ts_ms": 0, "age_ms": 0
                    "has_ts": 0, "pttl_ms": pttl
                })
        except Exception:  # noqa: BLE001
            pass

        # --- 4. atr:json:{sym}:{tf} (atr + ts) ---
        key3 = f"atr:json:{sym}:{tf_raw}"
        try:
            raw = self.redis_client.get(key3)
            if raw:
                d = json.loads(raw)
                atr = _f(d.get("atr", 0.0) or 0.0, 0.0)
                ts_ms = _i(d.get("ts", 0) or 0, 0)
                if atr > 0:
                    age = max(0, nm - ts_ms) if ts_ms > 0 else 0
                    out.append({
                        "atr": atr, "src": "atr_json", "key": key3
                        "tf": tf_norm, "ts_ms": ts_ms, "age_ms": age
                        "has_ts": int(ts_ms > 0)
                    })
        except Exception:  # noqa: BLE001
            pass

        # --- 5. ta:last:atr:{sym} (atr + tf + ts) ---
        last_key = f"ta:last:atr:{sym}"
        try:
            raw = self.redis_client.get(last_key)
            if raw:
                d = json.loads(raw)
                atr = _f(d.get("atr", 0.0) or 0.0, 0.0)
                ts_ms = _i(d.get("ts", 0) or 0, 0)
                src_tf = str(d.get("tf", "") or "").upper()
                if atr > 0:
                    age = max(0, nm - ts_ms) if ts_ms > 0 else 0
                    tf_mismatch = int(bool(src_tf) and src_tf != tf_norm)
                    out.append({
                        "atr": atr, "src": "ta_last", "key": last_key
                        "tf": src_tf if src_tf else tf_norm
                        "ts_ms": ts_ms, "age_ms": age
                        "has_ts": int(ts_ms > 0)
                        "tf_mismatch": tf_mismatch
                    })
        except Exception:  # noqa: BLE001
            pass

        return out

    # ------------------------------------------------------------------
    # Write API
    # ------------------------------------------------------------------

    def set(self, symbol: str, timeframe: str, atr_value: float) -> bool:
        """Store ATR in cache (keeps both primary and legacy key)."""
        if atr_value <= 0:
            return False
        try:
            self.redis_client.set(f"atr:{symbol}:{timeframe}", str(atr_value), ex=self.ttl)
            self.redis_client.set(f"atr:val:{symbol}:{timeframe}", str(atr_value), ex=self.ttl)
            return True
        except Exception:  # noqa: BLE001
            return False

    def delete(self, symbol: str, timeframe: str) -> bool:
        """Delete the primary ATR key for *symbol*/*timeframe*."""
        try:
            self.redis_client.delete(f"atr:{symbol}:{timeframe}")
            return True
        except Exception:  # noqa: BLE001
            return False

    def clear_all(self) -> int:
        """Delete all ``atr:*`` keys. Returns count of deleted keys."""
        try:
            keys = list(self.redis_client.scan_iter(match="atr:*", count=10000))
            return int(self.redis_client.delete(*keys)) if keys else 0
        except Exception:  # noqa: BLE001
            return 0

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _pttl_ms(self, key: str) -> int:
        try:
            v = self.redis_client.pttl(key)
            return int(v) if isinstance(v, int) else -1
        except Exception:  # noqa: BLE001
            return -1


# ---------------------------------------------------------------------------
# Module-level helpers (pure functions, importable for testing)
# ---------------------------------------------------------------------------

def _normalize_tracker_tf(tf: str) -> str:
    """Map a timeframe string to the uppercase tracker format (e.g. ``'1m'`` → ``'M1'``)."""
    if not tf:
        return "M1"
    return _TF_MAP.get(tf.strip().lower(), tf.strip().upper())


def _candidate_meta(c: dict[str, Any], requested_tf: str) -> dict[str, Any]:
    """Build a normalised meta dict from a candidate entry."""
    return {
        "src": str(c.get("src", "unknown"))
        "key": str(c.get("key", ""))
        "tf": str(c.get("tf", requested_tf))
        "ts_ms": int(c.get("ts_ms", 0) or 0)
        "age_ms": int(c.get("age_ms", 0) or 0)
        "tf_mismatch": int(c.get("tf_mismatch", 0))
    }


def _pick_best_candidate(candidates: list[dict[str, Any]]) -> dict[str, Any] | None:
    """Pick the best candidate using freshness + tf-match priority.

    Priority order:
    1. Timestamped candidates with ``tf_mismatch=0`` (smallest age wins)
    2. Timestamped candidates with ``tf_mismatch=1`` (tf mismatch penalty)
    3. Non-timestamped candidates (first in list)
    """
    if not candidates:
        return None
    # Split by has_ts and tf_mismatch
    ts_matched = [c for c in candidates if c.get("has_ts") and not c.get("tf_mismatch")]
    ts_mismatch = [c for c in candidates if c.get("has_ts") and c.get("tf_mismatch")]
    no_ts = [c for c in candidates if not c.get("has_ts")]

    if ts_matched:
        return min(ts_matched, key=lambda c: c.get("age_ms", 0))
    if ts_mismatch:
        return min(ts_mismatch, key=lambda c: c.get("age_ms", 0))
    return no_ts[0] if no_ts else None



# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

_atr_cache_instance: ATRCache | None = None


def get_atr_cache() -> ATRCache:
    """Return the module-level :class:`ATRCache` singleton (lazy-init)."""
    global _atr_cache_instance  # noqa: PLW0603
    if _atr_cache_instance is None:
        _atr_cache_instance = ATRCache()
    return _atr_cache_instance
