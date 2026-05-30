from __future__ import annotations

"""exec_latency_timeout_runtime_overrides.py — read-side for ExecLatencyTimeoutCalibrator.

Loads `autocal:exec_latency_timeout:state` with TTL cache.
Exposes `get_timeouts()` → (executor_ms, router_ms) | None.

Default OFF (`AUTOCAL_EXEC_LATENCY_READ_ENABLED=0`), fail-open → None.
Replaces hardcoded PROTECTION_ARM_TIMEOUT_MS.
"""

import json
import logging
import os
import threading
import time
from typing import Any

logger = logging.getLogger(__name__)

_DEFAULT_KEY = "autocal:exec_latency_timeout:state"
_DEFAULT_REFRESH_MS = 60_000
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


class ExecLatencyTimeoutReader:
    """TTL-cached execution latency timeout reader."""

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
        self._snapshot: dict[str, Any] = {}
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
                if isinstance(data, dict):
                    self._snapshot = data
            except Exception as e:
                logger.debug("exec_latency overrides: refresh fail: %s", e)

    def get_timeouts(self) -> tuple[float, float] | None:
        """Return (executor_ms, router_ms) or None if unavailable."""
        self._maybe_refresh()
        if not self._snapshot:
            return None
        updated_ms = int(self._snapshot.get("updated_ts_ms") or 0)
        if updated_ms > 0:
            age_ms = int(time.time() * 1000) - updated_ms
            if age_ms > self._stale_ms:
                return None
        exec_ms = _safe_float(self._snapshot.get("committed_executor_ms"), -1.0)
        router_ms = _safe_float(self._snapshot.get("committed_router_ms"), -1.0)
        if exec_ms > 0 and router_ms > 0:
            return exec_ms, router_ms
        return None


_READER: ExecLatencyTimeoutReader | None = None
_READER_LOCK = threading.Lock()


def _make_reader() -> ExecLatencyTimeoutReader | None:
    if not _env_bool("AUTOCAL_EXEC_LATENCY_READ_ENABLED", False):
        return None
    try:
        import redis  # type: ignore
        url = _env("AUTOCAL_EXEC_LATENCY_REDIS_URL", _env("REDIS_URL", "redis://redis-worker-1:6379/0"))
        client = redis.from_url(url, decode_responses=False)
        return ExecLatencyTimeoutReader(
            client,
            redis_key=_env("AUTOCAL_EXEC_LATENCY_KEY", _DEFAULT_KEY),
            refresh_ms=_env_int("AUTOCAL_EXEC_LATENCY_REFRESH_MS", _DEFAULT_REFRESH_MS),
            stale_ms=_env_int("AUTOCAL_EXEC_LATENCY_STALE_MS", _DEFAULT_STALE_MS),
        )
    except Exception as e:
        logger.debug("exec_latency overrides: reader init fail: %s", e)
        return None


def get_reader() -> ExecLatencyTimeoutReader | None:
    global _READER
    if _READER is not None:
        return _READER
    with _READER_LOCK:
        if _READER is None:
            _READER = _make_reader()
        return _READER


def get_timeouts() -> tuple[float, float] | None:
    rdr = get_reader()
    if rdr is None:
        return None
    try:
        return rdr.get_timeouts()
    except Exception:
        return None
