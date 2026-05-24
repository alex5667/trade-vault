from __future__ import annotations

"""ml_canary_runtime_overrides.py — read-side adapter for ml-canary autopromoter.

Loads `autocal:ml_canary:state` (written by
`orderflow_services/ml_canary_autopromoter_v1.py`) and exposes a per-call
lookup for the dynamic `ML_SCORER_CANARY_RATE`.

Design (mirrors path_based_tp_runtime_overrides):
  - Disabled by default (`AUTOCAL_ML_CANARY_READ_ENABLED=0`); fail-open to env default.
  - TTL cache (30s default) — Redis GET on miss only.
  - HMAC verify if `ML_CANARY_AUTOCAL_HMAC_SECRET`/`LAYERS_CAL_HMAC_SECRET` set; mismatch → ignore.
  - Override applied only if state has `enforce=1`; else default.
  - Clamped to [0.0, 1.0].

Public surface:
  get_canary_rate(default: float) -> float
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

_DEFAULT_KEY = "autocal:ml_canary:state"
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
    except Exception:
        return d


def _clamp01(v: float) -> float:
    if v < 0.0:
        return 0.0
    if v > 1.0:
        return 1.0
    return float(v)


class MLCanaryReader:
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
        self._state: dict[str, Any] = {}
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
                        logger.warning("ml_canary overrides: HMAC mismatch — ignoring")
                        return
                self._state = data
                self._snapshot_ts_ms = int(data.get("ts_ms") or 0)
            except Exception as e:
                logger.debug("ml_canary overrides: refresh fail (fail-open): %s", e)

    def _fresh(self) -> bool:
        if not self._state:
            return False
        age_ms = int(time.time() * 1000) - self._snapshot_ts_ms
        return age_ms <= self._stale_ms

    def get_canary_rate(self, default: float) -> float:
        """Return autocal-recommended canary rate if enforce=1, else default.

        `default` is normally the value of `ML_SCORER_CANARY_RATE` env var.
        Both override and default are clamped to [0.0, 1.0].
        """
        d = _clamp01(default)
        self._maybe_refresh()
        if not self._fresh():
            return d
        if int(self._state.get("enforce", 0)) != 1:
            return d
        v = self._state.get("current_rate")
        if v is None:
            return d
        try:
            return _clamp01(float(v))
        except Exception:
            return d

    def get_state_for_inspection(self) -> dict[str, Any]:
        """Return current state dict (or empty) — for shadow indicator emission."""
        self._maybe_refresh()
        if not self._fresh():
            return {}
        return dict(self._state)


_READER: MLCanaryReader | None = None
_READER_LOCK = threading.Lock()


def _make_reader() -> MLCanaryReader | None:
    if not _env_bool("AUTOCAL_ML_CANARY_READ_ENABLED", False):
        return None
    try:
        import redis  # type: ignore
        url = _env(
            "AUTOCAL_ML_CANARY_REDIS_URL",
            _env("REDIS_URL", "redis://redis-worker-1:6379/0"),
        )
        client = redis.from_url(url, decode_responses=True)
        secret = (
            _env("ML_CANARY_AUTOCAL_HMAC_SECRET", "")
            or _env("LAYERS_CAL_HMAC_SECRET", "")
            or _env("RECS_HMAC_SECRET", "")
        )
        return MLCanaryReader(
            client,
            redis_key=_env("AUTOCAL_ML_CANARY_KEY", _DEFAULT_KEY),
            refresh_ms=_env_int("AUTOCAL_ML_CANARY_REFRESH_MS", _DEFAULT_REFRESH_MS),
            stale_ms=_env_int("AUTOCAL_ML_CANARY_STALE_MS", _DEFAULT_STALE_MS),
            hmac_secret=secret,
        )
    except Exception as e:
        logger.debug("ml_canary overrides: reader init fail (fail-open): %s", e)
        return None


def get_reader() -> MLCanaryReader | None:
    global _READER
    if _READER is not None:
        return _READER
    with _READER_LOCK:
        if _READER is None:
            _READER = _make_reader()
        return _READER


def get_canary_rate(default: float = 0.05) -> float:
    """Convenience function. Returns env-driven default if reader disabled or no override."""
    rdr = get_reader()
    if rdr is None:
        return _clamp01(default)
    try:
        return rdr.get_canary_rate(default=default)
    except Exception:
        return _clamp01(default)


def reset_reader_for_tests() -> None:
    """Test helper — clear singleton."""
    global _READER
    with _READER_LOCK:
        _READER = None


__all__ = [
    "MLCanaryReader",
    "get_reader",
    "get_canary_rate",
    "reset_reader_for_tests",
]
