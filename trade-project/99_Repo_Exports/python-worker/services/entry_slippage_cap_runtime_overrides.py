from __future__ import annotations

"""entry_slippage_cap_runtime_overrides.py — read-side for EntrySlippageCapCalibrator.

Loads `autocal:entry_slip_cap:state` (HGETALL per-symbol JSON) with TTL cache.
Exposes `get_cap_bps(symbol, session)` → float | None.

Default OFF (`AUTOCAL_ENTRY_SLIP_CAP_READ_ENABLED=0`), fail-open → None.
"""

import json
import logging
import os
import threading
import time
from typing import Any

logger = logging.getLogger(__name__)

_DEFAULT_KEY = "autocal:entry_slip_cap:state"
_DEFAULT_REFRESH_MS = 30_000
_DEFAULT_STALE_MS = 10 * 60 * 1000


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


class EntrySlippageCapReader:
    """TTL-cached per-(symbol × session) entry slippage cap reader. Thread-safe."""

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
                raw = self._redis.hgetall(self._key)
                if not raw:
                    return
                parsed: dict[str, dict[str, Any]] = {}
                for key_raw, state_raw in raw.items():
                    try:
                        k = key_raw.decode("utf-8") if isinstance(key_raw, bytes) else str(key_raw)
                        s = state_raw.decode("utf-8") if isinstance(state_raw, bytes) else str(state_raw)
                        state = json.loads(s)
                        if isinstance(state, dict):
                            parsed[k] = state
                    except Exception:
                        pass
                self._snapshot = parsed
            except Exception as e:
                logger.debug("entry_slip_cap overrides: refresh fail: %s", e)

    def get_cap_bps(self, symbol: str, session: str = "*") -> float | None:
        """Return committed_cap_bps for (symbol, session) or None if unavailable."""
        self._maybe_refresh()
        sym = (symbol or "").strip().upper()
        sess = (session or "*").strip().lower()

        # Try exact key, then wildcard session
        for key in (f"{sym}:{sess}", f"{sym}:*"):
            state = self._snapshot.get(key)
            if state:
                updated_ms = int(state.get("updated_ts_ms") or 0)
                if updated_ms > 0:
                    age_ms = int(time.time() * 1000) - updated_ms
                    if age_ms > self._stale_ms:
                        continue
                cap = _safe_float(state.get("committed_cap_bps"), -1.0)
                if cap > 0:
                    return cap
        return None


_READER: EntrySlippageCapReader | None = None
_READER_LOCK = threading.Lock()


def _make_reader() -> EntrySlippageCapReader | None:
    if not _env_bool("AUTOCAL_ENTRY_SLIP_CAP_READ_ENABLED", False):
        return None
    try:
        import redis  # type: ignore
        url = _env("AUTOCAL_ENTRY_SLIP_CAP_REDIS_URL", _env("REDIS_URL", "redis://redis-worker-1:6379/0"))
        client = redis.from_url(url, decode_responses=False)
        return EntrySlippageCapReader(
            client,
            redis_key=_env("AUTOCAL_ENTRY_SLIP_CAP_KEY", _DEFAULT_KEY),
            refresh_ms=_env_int("AUTOCAL_ENTRY_SLIP_CAP_REFRESH_MS", _DEFAULT_REFRESH_MS),
            stale_ms=_env_int("AUTOCAL_ENTRY_SLIP_CAP_STALE_MS", _DEFAULT_STALE_MS),
        )
    except Exception as e:
        logger.debug("entry_slip_cap overrides: reader init fail: %s", e)
        return None


def get_reader() -> EntrySlippageCapReader | None:
    global _READER
    if _READER is not None:
        return _READER
    with _READER_LOCK:
        if _READER is None:
            _READER = _make_reader()
        return _READER


def get_cap_bps(symbol: str, session: str = "*") -> float | None:
    rdr = get_reader()
    if rdr is None:
        return None
    try:
        return rdr.get_cap_bps(symbol, session)
    except Exception:
        return None
