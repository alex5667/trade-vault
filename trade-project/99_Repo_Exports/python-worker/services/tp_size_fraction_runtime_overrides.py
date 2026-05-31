from __future__ import annotations

"""tp_size_fraction_runtime_overrides.py — read-side for TPSizeFractionCalibrator.

Loads `autocal:tp_size_fractions:state` with TTL cache.
Exposes `get_fractions(regime)` → (f1, f2, f3) | None.

Default OFF (`AUTOCAL_TP_SIZE_FRAC_READ_ENABLED=0`), fail-open → None.
Replaces hardcoded TP_SIZE_FRAC_{1,2,3} defaults.
"""

import json
import logging
import os
import threading
import time
from typing import Any

logger = logging.getLogger(__name__)

_DEFAULT_KEY = "autocal:tp_size_fractions:state"
_DEFAULT_REFRESH_MS = 60_000  # slower refresh — changes on 5-min snapshot
_DEFAULT_STALE_MS = 30 * 60 * 1000


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


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        import math
        f = float(v)
        return f if math.isfinite(f) else default
    except (TypeError, ValueError):
        return default


class TPSizeFractionReader:
    """TTL-cached per-regime TP size fraction reader."""

    def __init__(
        self,
        redis_client: Any,
        *,
        redis_key: str = _DEFAULT_KEY,
        refresh_ms: int = _DEFAULT_REFRESH_MS,
        stale_ms: int = _DEFAULT_STALE_MS,
    ) -> None:
        self._redis = redis_client
        self._key = redis_key
        self._refresh_ms = max(1000, refresh_ms)
        self._stale_ms = max(self._refresh_ms, stale_ms)
        self._lock = threading.Lock()
        # key: "regime" → dict with committed_f1/f2/f3
        self._snapshot: dict[str, dict[str, Any]] = {}
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
                data = json.loads(raw.decode("utf-8") if isinstance(raw, bytes) else raw)
                parsed: dict[str, dict[str, Any]] = {}
                bins = data.get("bins", []) if isinstance(data, dict) else []
                for row in bins:
                    if not isinstance(row, dict):
                        continue
                    reg = (row.get("regime") or "*").lower()
                    parsed[reg] = row
                self._snapshot = parsed
            except Exception as e:
                logger.debug("tp_size_frac overrides: refresh fail: %s", e)

    def get_fractions(self, regime: str = "*") -> tuple[float, float, float] | None:
        """Return (f1, f2, f3) committed fractions or None if unavailable."""
        self._maybe_refresh()
        reg = (regime or "*").strip().lower()

        for key in (reg, "*"):
            state = self._snapshot.get(key)
            if not state:
                continue
            updated_ms = int(state.get("updated_ts_ms") or 0)
            if updated_ms > 0:
                age_ms = int(time.time() * 1000) - updated_ms
                if age_ms > self._stale_ms:
                    continue
            f1 = _safe_float(state.get("committed_f1"), -1.0)
            f2 = _safe_float(state.get("committed_f2"), -1.0)
            f3 = _safe_float(state.get("committed_f3"), -1.0)
            if f1 > 0 and f2 > 0 and f3 > 0:
                return f1, f2, f3
        return None


_READER: TPSizeFractionReader | None = None
_READER_LOCK = threading.Lock()


def _make_reader() -> TPSizeFractionReader | None:
    if not _env_bool("AUTOCAL_TP_SIZE_FRAC_READ_ENABLED", False):
        return None
    try:
        import redis  # type: ignore
        url = _env("AUTOCAL_TP_SIZE_FRAC_REDIS_URL", _env("REDIS_URL", "redis://redis-worker-1:6379/0"))
        client = redis.from_url(url, decode_responses=False)
        return TPSizeFractionReader(
            client,
            redis_key=_env("AUTOCAL_TP_SIZE_FRAC_KEY", _DEFAULT_KEY),
            refresh_ms=_env_int("AUTOCAL_TP_SIZE_FRAC_REFRESH_MS", _DEFAULT_REFRESH_MS),
            stale_ms=_env_int("AUTOCAL_TP_SIZE_FRAC_STALE_MS", _DEFAULT_STALE_MS),
        )
    except Exception as e:
        logger.debug("tp_size_frac overrides: reader init fail: %s", e)
        return None


def get_reader() -> TPSizeFractionReader | None:
    global _READER
    if _READER is not None:
        return _READER
    with _READER_LOCK:
        if _READER is None:
            _READER = _make_reader()
        return _READER


def get_fractions(regime: str = "*") -> tuple[float, float, float] | None:
    rdr = get_reader()
    if rdr is None:
        return None
    try:
        return rdr.get_fractions(regime)
    except Exception:
        return None
