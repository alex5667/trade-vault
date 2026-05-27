from __future__ import annotations

"""crossvenue_agree_runtime_overrides.py — reader for autocal:crossvenue:agree:state.

Provides streaming median/MAD of `cross_venue_direction_agree` per symbol,
written by `orderflow_services/crossvenue_agree_calibrator_v1.py` (P1.11).

Design mirrors `services/path_based_tp_runtime_overrides.py`:
  - Disabled by default (`AUTOCAL_CROSSVENUE_AGREE_READ_ENABLED=0`).
  - TTL cache + HMAC verify; fail-open.
  - Public surface returns `(median, mad, n_total)`; callers decide gating.
"""

import hashlib
import hmac
import json
import logging
import os
import threading
import time
from typing import Any

logger = logging.getLogger(__name__)

_DEFAULT_KEY = "autocal:crossvenue:agree:state"
_DEFAULT_REFRESH_MS = 30_000
_DEFAULT_STALE_MS = 6 * 60 * 60 * 1000


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
    except (TypeError, ValueError):
        return d


class CrossVenueAgreeReader:
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
                    canon = json.dumps(data, sort_keys=True, separators=(",", ":")).encode()
                    actual = hmac.new(self._hmac_secret.encode(), canon, hashlib.sha256).hexdigest()
                    if not hmac.compare_digest(actual, str(expected_sig)):
                        logger.warning("crossvenue_agree: HMAC mismatch — ignoring")
                        return
                self._buckets = data.get("buckets") or {}
                self._snapshot_ts_ms = int(data.get("ts_ms") or 0)
            except Exception as e:
                logger.debug("crossvenue_agree: refresh fail (fail-open): %s", e)

    def _fresh(self) -> bool:
        if not self._buckets:
            return False
        age_ms = int(time.time() * 1000) - self._snapshot_ts_ms
        return age_ms <= self._stale_ms

    def get_thresholds(
        self,
        symbol: str,
        *,
        require_enforce: bool = True,
    ) -> tuple[float, float, int] | None:
        """Return (median_agree, mad_agree, n_total) for symbol, else None."""
        self._maybe_refresh()
        if not self._fresh():
            return None
        sym = (symbol or "").upper()
        entry = self._buckets.get(sym)
        if not entry:
            return None
        if require_enforce and int(entry.get("enforce", 0)) != 1:
            return None
        try:
            return (
                float(entry.get("median_agree", 0.0)),
                float(entry.get("mad_agree", 0.0)),
                int(entry.get("n_total", 0)),
            )
        except (TypeError, ValueError):
            return None


_READER: CrossVenueAgreeReader | None = None
_READER_LOCK = threading.Lock()


def _make_reader() -> CrossVenueAgreeReader | None:
    if not _env_bool("AUTOCAL_CROSSVENUE_AGREE_READ_ENABLED", False):
        return None
    try:
        import redis  # type: ignore
        url = _env("AUTOCAL_CROSSVENUE_AGREE_REDIS_URL",
                   _env("REDIS_URL", "redis://redis-worker-1:6379/0"))
        client = redis.from_url(url, decode_responses=True)
        secret = (_env("CROSSVENUE_AGREE_CAL_HMAC_SECRET", "")
                  or _env("RECS_HMAC_SECRET", "")
                  or _env("LAYERS_CAL_HMAC_SECRET", ""))
        return CrossVenueAgreeReader(
            client,
            redis_key=_env("AUTOCAL_CROSSVENUE_AGREE_KEY", _DEFAULT_KEY),
            refresh_ms=_env_int("AUTOCAL_CROSSVENUE_AGREE_REFRESH_MS", _DEFAULT_REFRESH_MS),
            stale_ms=_env_int("AUTOCAL_CROSSVENUE_AGREE_STALE_MS", _DEFAULT_STALE_MS),
            hmac_secret=secret,
        )
    except Exception as e:
        logger.debug("crossvenue_agree: reader init fail (fail-open): %s", e)
        return None


def get_reader() -> CrossVenueAgreeReader | None:
    global _READER
    if _READER is not None:
        return _READER
    with _READER_LOCK:
        if _READER is None:
            _READER = _make_reader()
        return _READER


def get_thresholds(symbol: str, *, require_enforce: bool = True) -> tuple[float, float, int] | None:
    rdr = get_reader()
    if rdr is None:
        return None
    try:
        return rdr.get_thresholds(symbol, require_enforce=require_enforce)
    except Exception:
        return None


def reset_reader_for_tests() -> None:
    global _READER
    with _READER_LOCK:
        _READER = None
