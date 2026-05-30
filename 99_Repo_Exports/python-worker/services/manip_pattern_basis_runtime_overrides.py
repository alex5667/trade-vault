from __future__ import annotations

"""manip_pattern_basis_runtime_overrides.py — read-side for ManipPatternBasisCalibrator.

Loads `autocal:manip_pattern_basis:state` with TTL cache.
Exposes `get_params(symbol)` → dict[str, float] | None.

Default OFF (`AUTOCAL_MANIP_PATTERN_BASIS_READ_ENABLED=0`), fail-open → None.
None → caller uses ENV-based defaults (LAYERING_BUILD_MULT, etc.).
"""

import json
import logging
import os
import threading
import time
from typing import Any

logger = logging.getLogger(__name__)

_DEFAULT_KEY = "autocal:manip_pattern_basis:state"
_DEFAULT_REFRESH_MS = 60_000
_DEFAULT_STALE_MS = 20 * 60 * 1000


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


class ManipPatternBasisReader:
    """TTL-cached per-symbol manip pattern basis reader. Thread-safe."""

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
        # symbol → {"build_mult": ..., "revert_frac": ..., ...}
        self._by_symbol: dict[str, dict[str, float]] = {}
        self._ts_ms: int = 0
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
                if not isinstance(data, dict):
                    return
                self._ts_ms = int(data.get("ts_ms") or 0)
                by_sym: dict[str, dict[str, float]] = {}
                for row in data.get("bins", []):
                    if not isinstance(row, dict):
                        continue
                    sym = str(row.get("symbol", "*")).upper()
                    by_sym[sym] = {
                        "build_mult": _safe_float(row.get("committed_build_mult"), 0.0),
                        "revert_frac": _safe_float(row.get("committed_revert_frac"), 0.0),
                        "revert_ms": _safe_float(row.get("committed_revert_ms"), 0.0),
                        "qs_msg_z": _safe_float(row.get("committed_qs_msg_z"), 0.0),
                        "qs_cancel_z": _safe_float(row.get("committed_qs_cancel_z"), 0.0),
                    }
                self._by_symbol = by_sym
            except Exception as e:
                logger.debug("manip_basis overrides: refresh fail: %s", e)

    def get_params(self, symbol: str) -> dict[str, float] | None:
        """Return calibrated params for symbol or None if unavailable/stale."""
        self._maybe_refresh()
        if not self._by_symbol:
            return None
        if self._ts_ms > 0:
            age_ms = int(time.time() * 1000) - self._ts_ms
            if age_ms > self._stale_ms:
                return None
        sym = (symbol or "*").strip().upper()
        for key in (sym, "*"):
            p = self._by_symbol.get(key)
            if p and p.get("build_mult", 0.0) > 0:
                return p
        return None


_READER: ManipPatternBasisReader | None = None
_READER_LOCK = threading.Lock()


def _make_reader() -> ManipPatternBasisReader | None:
    if not _env_bool("AUTOCAL_MANIP_PATTERN_BASIS_READ_ENABLED", False):
        return None
    try:
        import redis  # type: ignore
        url = _env("AUTOCAL_MANIP_PATTERN_BASIS_REDIS_URL", _env("REDIS_URL", "redis://redis-worker-1:6379/0"))
        client = redis.from_url(url, decode_responses=False)
        return ManipPatternBasisReader(
            client,
            redis_key=_env("AUTOCAL_MANIP_PATTERN_BASIS_KEY", _DEFAULT_KEY),
            refresh_ms=_env_int("AUTOCAL_MANIP_PATTERN_BASIS_REFRESH_MS", _DEFAULT_REFRESH_MS),
            stale_ms=_env_int("AUTOCAL_MANIP_PATTERN_BASIS_STALE_MS", _DEFAULT_STALE_MS),
        )
    except Exception as e:
        logger.debug("manip_basis overrides: reader init fail: %s", e)
        return None


def get_reader() -> ManipPatternBasisReader | None:
    global _READER
    if _READER is not None:
        return _READER
    with _READER_LOCK:
        if _READER is None:
            _READER = _make_reader()
        return _READER


def get_params(symbol: str) -> dict[str, float] | None:
    rdr = get_reader()
    if rdr is None:
        return None
    try:
        return rdr.get_params(symbol)
    except Exception:
        return None
