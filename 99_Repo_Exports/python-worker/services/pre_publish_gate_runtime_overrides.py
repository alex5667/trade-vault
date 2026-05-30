"""
pre_publish_gate_runtime_overrides.py — TTL-cached reader for PrePublishGateCalibrator.

Reads `autocal:pre_publish_gate:state` (STRING, JSON), exposes
`get_delta_z_thr(symbol, regime)` and `get_obi_thr(symbol, regime)`
with hierarchical fallback: (symbol, regime) → (symbol, *) → (*, *) → None.

Disabled by default (AUTOCAL_PRE_PUBLISH_GATE_READ_ENABLED=0); fail-open → returns None
(caller uses ENV default DELTA_Z_THRESHOLD / OBI_THRESHOLD).
"""
from __future__ import annotations

import json
import logging
import os
import threading
import time
from typing import Any

logger = logging.getLogger(__name__)

_DEFAULT_REFRESH_MS = 60_000
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


class PrePublishGateReader:
    """TTL-cached reader for per-(symbol × regime) delta_z and OBI thresholds. Thread-safe."""

    def __init__(
        self,
        redis_client: Any,
        *,
        redis_key: str,
        refresh_ms: int = _DEFAULT_REFRESH_MS,
        stale_ms: int = _DEFAULT_STALE_MS,
    ) -> None:
        self._redis = redis_client
        self._key = redis_key
        self._refresh_ms = max(1000, refresh_ms)
        self._stale_ms = max(self._refresh_ms, stale_ms)
        self._lock = threading.Lock()
        # (symbol, regime) → (delta_z_thr, obi_thr)
        self._bins: dict[tuple[str, str], tuple[float, float]] = {}
        self._enforce: bool = False
        self._default_delta_z_thr: float = 2.0
        self._default_obi_thr: float = 0.35
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
                if isinstance(raw, bytes):
                    raw = raw.decode("utf-8")
                state = json.loads(raw)
                self._enforce = bool(state.get("enforce", False))
                self._default_delta_z_thr = _safe_float(state.get("default_delta_z_thr"), 2.0)
                self._default_obi_thr = _safe_float(state.get("default_obi_thr"), 0.35)
                self._ts_ms = int(state.get("ts_ms", 0))
                new_bins: dict[tuple[str, str], tuple[float, float]] = {}
                for row in state.get("bins", []):
                    sym = str(row.get("symbol", "*"))
                    reg = str(row.get("regime", "*"))
                    z_thr = _safe_float(row.get("committed_delta_z_thr"), 0.0)
                    obi_thr = _safe_float(row.get("committed_obi_thr"), 0.0)
                    if z_thr > 0 or obi_thr > 0:
                        new_bins[(sym, reg)] = (z_thr, obi_thr)
                self._bins = new_bins
            except Exception as e:
                logger.debug("pre_publish_gate_reader refresh fail: %s", e)

    def _is_fresh(self) -> bool:
        if self._ts_ms == 0:
            return True  # not yet loaded, allow
        age_ms = int(time.time() * 1000) - self._ts_ms
        return age_ms <= self._stale_ms

    def get_delta_z_thr(self, symbol: str, regime: str) -> float | None:
        """Returns calibrated delta_z threshold or None (caller uses ENV default)."""
        self._maybe_refresh()
        if not self._enforce:
            return None
        if not self._is_fresh():
            return None
        sym = (symbol or "*").strip().upper()
        reg = (regime or "*").strip().lower()
        for key in [(sym, reg), (sym, "*"), ("*", "*")]:
            entry = self._bins.get(key)
            if entry and entry[0] > 0:
                return entry[0]
        return None

    def get_obi_thr(self, symbol: str, regime: str) -> float | None:
        """Returns calibrated OBI threshold or None (caller uses ENV default)."""
        self._maybe_refresh()
        if not self._enforce:
            return None
        if not self._is_fresh():
            return None
        sym = (symbol or "*").strip().upper()
        reg = (regime or "*").strip().lower()
        for key in [(sym, reg), (sym, "*"), ("*", "*")]:
            entry = self._bins.get(key)
            if entry and entry[1] > 0:
                return entry[1]
        return None


# ── Module singleton ──────────────────────────────────────────────────────────

_READER: PrePublishGateReader | None = None
_READER_LOCK = threading.Lock()


def _make_reader() -> PrePublishGateReader | None:
    if not _env_bool("AUTOCAL_PRE_PUBLISH_GATE_READ_ENABLED", False):
        return None
    try:
        import redis  # type: ignore
        url = _env("AUTOCAL_PRE_PUBLISH_GATE_REDIS_URL", _env("REDIS_URL", "redis://redis-worker-1:6379/0"))
        key = _env("AUTOCAL_PRE_PUBLISH_GATE_KEY", "autocal:pre_publish_gate:state")
        client = redis.from_url(url, decode_responses=False)
        return PrePublishGateReader(
            client,
            redis_key=key,
            refresh_ms=_env_int("AUTOCAL_PRE_PUBLISH_GATE_REFRESH_MS", _DEFAULT_REFRESH_MS),
            stale_ms=_env_int("AUTOCAL_PRE_PUBLISH_GATE_STALE_MS", _DEFAULT_STALE_MS),
        )
    except Exception as e:
        logger.debug("pre_publish_gate_reader init fail: %s", e)
        return None


def get_reader() -> PrePublishGateReader | None:
    global _READER
    if _READER is not None:
        return _READER
    with _READER_LOCK:
        if _READER is None:
            _READER = _make_reader()
        return _READER


def get_delta_z_thr(symbol: str, regime: str) -> float | None:
    """Returns calibrated delta_z threshold for (symbol, regime), or None (fail-open)."""
    rdr = get_reader()
    if rdr is None:
        return None
    try:
        return rdr.get_delta_z_thr(symbol, regime)
    except Exception:
        return None


def get_obi_thr(symbol: str, regime: str) -> float | None:
    """Returns calibrated OBI threshold for (symbol, regime), or None (fail-open)."""
    rdr = get_reader()
    if rdr is None:
        return None
    try:
        return rdr.get_obi_thr(symbol, regime)
    except Exception:
        return None
