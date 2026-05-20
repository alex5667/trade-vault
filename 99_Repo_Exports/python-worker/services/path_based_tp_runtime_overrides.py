from __future__ import annotations

"""path_based_tp_runtime_overrides.py — read-side adapter for path-tp autocal.

Loads `autocal:path_tp:state` (written by
`orderflow_services/path_based_tp_autocal_v1.py`) and exposes a per-call
lookup that performs the (symbol, regime, direction) → tp1_R fallback
hierarchy *inside* the reader, so callers don't repeat that logic.

Design:
  - Disabled by default (`AUTOCAL_PATH_TP_READ_ENABLED=0`); fail-open.
  - TTL cache (30s default) — Redis GET on miss only.
  - HMAC verify if `RECS_HMAC_SECRET`/`LAYERS_CAL_HMAC_SECRET` set; mismatch → ignore snapshot.
  - Per-bucket `enforce` flag — only if 1, override is returned; else default.

Public surface:
  get_path_based_tp1_r(symbol, regime, direction, default) -> float
    Returns autocal recommendation if a passing+enforced bucket exists
    along the fallback hierarchy; otherwise `default`.

  get_bucket_for_inspection(symbol, regime, direction) -> dict | None
    Returns the matched bucket dict (or None) regardless of enforce flag —
    used for shadow-mode indicator emission, NOT for changing exec.
"""

import hashlib
import hmac
import json
import logging
import os
import threading
import time
from typing import Any

from core.path_based_tp_cdf import (
    ALL,
    BucketKey,
    lookup_recommendation,
)

logger = logging.getLogger(__name__)

_DEFAULT_KEY = "autocal:path_tp:state"
_DEFAULT_REFRESH_MS = 30_000
_DEFAULT_STALE_MS = 6 * 60 * 60 * 1000  # 6 hours — path-tp is slow-moving (window 168h)


def _env(k: str, d: str = "") -> str:
    return os.environ.get(k, d)


def _env_bool(k: str, d: bool) -> bool:
    raw = _env(k, "")
    if not raw:
        return d
    return raw.strip().lower() in ("1", "true", "yes", "on")


def _env_int(k: str, d: int) -> int:
    try:
        return int(_env(k, str(d)))
    except Exception:
        return d


class PathBasedTpReader:
    """TTL-cached Redis snapshot reader. Thread-safe."""

    def __init__(
        self,
        redis_client: Any,
        *,
        redis_key: str = _DEFAULT_KEY,
        refresh_ms: int = _DEFAULT_REFRESH_MS,
        stale_ms: int = _DEFAULT_STALE_MS,
        hmac_secret: str = "",
    ) -> None:
        self._redis = redis_client
        self._key = redis_key
        self._refresh_ms = max(1000, refresh_ms)
        self._stale_ms = max(self._refresh_ms, stale_ms)
        self._hmac_secret = hmac_secret
        self._lock = threading.Lock()
        self._buckets: dict[str, dict[str, Any]] = {}
        self._snapshot_ts_ms: int = 0
        self._last_refresh_ms: int = 0

    def _maybe_refresh(self) -> None:
        now_ms = int(time.time() * 1000)
        if (now_ms - self._last_refresh_ms) < self._refresh_ms:
            return
        with self._lock:
            if (now_ms - self._last_refresh_ms) < self._refresh_ms:
                return
            self._last_refresh_ms = now_ms
            try:
                raw = self._redis.get(self._key)
                if not raw:
                    return
                if isinstance(raw, (bytes, bytearray)):
                    raw = raw.decode()
                data = json.loads(raw)
                if self._hmac_secret and "sig" in data:
                    expected_sig = data.pop("sig")
                    canon = json.dumps(
                        data, sort_keys=True, separators=(",", ":")
                    ).encode()
                    actual = hmac.new(
                        self._hmac_secret.encode(), canon, hashlib.sha256
                    ).hexdigest()
                    if not hmac.compare_digest(actual, str(expected_sig)):
                        logger.warning("path_tp overrides: HMAC mismatch — ignoring")
                        return
                self._buckets = data.get("buckets") or {}
                self._snapshot_ts_ms = int(data.get("ts_ms") or 0)
            except Exception as e:
                logger.debug("path_tp overrides: refresh fail (fail-open): %s", e)

    def _fresh(self) -> bool:
        if not self._buckets:
            return False
        age_ms = int(time.time() * 1000) - self._snapshot_ts_ms
        return age_ms <= self._stale_ms

    def get_tp1_r(
        self,
        *,
        symbol: str,
        regime: str,
        direction: str,
        default: float,
        require_enforce: bool = True,
    ) -> float:
        """Return autocal-recommended TP1_R if a matching bucket exists.

        `require_enforce=True` (the default) walks the fallback hierarchy
        and returns the first bucket that BOTH passes AND has enforce=1.
        When False, returns the first passing bucket regardless of enforce
        (useful for emitting `tp1_target_r_path_shadow` indicators).
        """
        self._maybe_refresh()
        if not self._fresh():
            return default
        sym = (symbol or ALL).upper()
        rg = (regime or ALL).lower()
        dr = (direction or ALL).upper()
        candidates = [
            BucketKey(sym, rg, dr),
            BucketKey(ALL, rg, dr),
            BucketKey(sym, ALL, dr),
            BucketKey(ALL, ALL, dr),
            BucketKey(ALL, ALL, ALL),
        ]
        for bk in candidates:
            entry = self._buckets.get(bk.encode())
            if not entry or int(entry.get("passes", 0)) != 1:
                continue
            if require_enforce and int(entry.get("enforce", 0)) != 1:
                continue
            try:
                v = entry.get("tp1_r")
                if v is not None:
                    return float(v)
            except Exception:
                continue
        return default

    def get_bucket(
        self,
        *,
        symbol: str,
        regime: str,
        direction: str,
    ) -> dict[str, Any] | None:
        """Inspection helper — returns matched bucket dict (passing, any enforce)."""
        self._maybe_refresh()
        if not self._fresh():
            return None
        return lookup_recommendation(
            self._buckets,
            symbol=symbol,
            regime=regime,
            direction=direction,
        )


# Module singleton.
_READER: PathBasedTpReader | None = None
_READER_LOCK = threading.Lock()


def _make_reader() -> PathBasedTpReader | None:
    if not _env_bool("AUTOCAL_PATH_TP_READ_ENABLED", False):
        return None
    try:
        import redis  # type: ignore
        url = _env("AUTOCAL_PATH_TP_REDIS_URL",
                   _env("REDIS_URL", "redis://redis-worker-1:6379/0"))
        client = redis.from_url(url, decode_responses=True)
        secret = (_env("PATH_TP_AUTOCAL_HMAC_SECRET", "")
                  or _env("RECS_HMAC_SECRET", "")
                  or _env("LAYERS_CAL_HMAC_SECRET", ""))
        return PathBasedTpReader(
            client,
            redis_key=_env("AUTOCAL_PATH_TP_KEY", _DEFAULT_KEY),
            refresh_ms=_env_int("AUTOCAL_PATH_TP_REFRESH_MS", _DEFAULT_REFRESH_MS),
            stale_ms=_env_int("AUTOCAL_PATH_TP_STALE_MS", _DEFAULT_STALE_MS),
            hmac_secret=secret,
        )
    except Exception as e:
        logger.debug("path_tp overrides: reader init fail (fail-open): %s", e)
        return None


def get_reader() -> PathBasedTpReader | None:
    """Lazy singleton. None when disabled or unavailable."""
    global _READER
    if _READER is not None:
        return _READER
    with _READER_LOCK:
        if _READER is None:
            _READER = _make_reader()
        return _READER


def get_path_based_tp1_r(
    symbol: str,
    regime: str,
    direction: str,
    default: float,
    *,
    require_enforce: bool = True,
) -> float:
    """Convenience function. Returns default if reader disabled or no override."""
    rdr = get_reader()
    if rdr is None:
        return default
    try:
        return rdr.get_tp1_r(
            symbol=symbol,
            regime=regime,
            direction=direction,
            default=default,
            require_enforce=require_enforce,
        )
    except Exception:
        return default


def get_bucket_for_inspection(
    symbol: str,
    regime: str,
    direction: str,
) -> dict[str, Any] | None:
    rdr = get_reader()
    if rdr is None:
        return None
    try:
        return rdr.get_bucket(symbol=symbol, regime=regime, direction=direction)
    except Exception:
        return None


def reset_reader_for_tests() -> None:
    """Test helper — clear singleton."""
    global _READER
    with _READER_LOCK:
        _READER = None


# Re-export for callers that want to construct keys without depending on core.
__all__ = [
    "PathBasedTpReader",
    "get_reader",
    "get_path_based_tp1_r",
    "get_bucket_for_inspection",
    "reset_reader_for_tests",
    "BucketKey",
    "ALL",
]
