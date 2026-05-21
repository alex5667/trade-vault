from __future__ import annotations

"""TTL-cached read-side adapter for CrossVenueCalibratorCore snapshots.

Reads ``autocal:crossvenue:state`` from Redis with a configurable refresh
interval and exposes ``thresholds_for(symbol)`` for use in the hot signal path.

Design
------
- Fail-open: any Redis error, missing/stale snapshot, or disabled flag falls
  back to the ENV-derived defaults supplied by the caller.
- Module-level singleton via ``get_reader()`` — one Redis GET per refresh_ms
  regardless of call frequency.
- When ``AUTOCAL_CROSSVENUE_READ_ENABLED=0`` (default), ``get_reader()`` returns
  ``None`` — the gate uses ENV values unchanged.  Turn on after the feed service
  has accumulated ≥ MIN_SAMPLES (30) observations per active symbol (~30 min).

ENV
---
  AUTOCAL_CROSSVENUE_READ_ENABLED   0|1   (default 0 — shadow mode default)
  AUTOCAL_CROSSVENUE_REFRESH_MS     int   (default 60 000 — 1 min)
  AUTOCAL_CROSSVENUE_STALE_MS       int   (default 1 800 000 — 30 min)
"""

import json
import logging
import os
import threading
import time
from typing import Any

from core.cross_venue_calibrator import (
    CrossVenueCalibratorCore,
    DEFAULT_DISLOC_Z,
    DEFAULT_MIN_AGREE,
)
from core.redis_keys import RK

logger = logging.getLogger(__name__)


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name, "")
    return raw.strip().lower() in ("1", "true", "yes", "on") if raw else default


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name, "")
    try:
        return int(raw) if raw else default
    except ValueError:
        return default


class CrossVenueCalibReader:
    """TTL-cached snapshot reader.  Thread-safe; one instance per process."""

    def __init__(
        self,
        redis_client: Any,
        *,
        redis_key: str = RK.AUTOCAL_CROSSVENUE_STATE,
        refresh_ms: int = 60_000,
        stale_ms:   int = 30 * 60_000,
    ) -> None:
        self._redis      = redis_client
        self._redis_key  = redis_key
        self._refresh_ms = max(1_000, refresh_ms)
        self._stale_ms   = max(self._refresh_ms, stale_ms)

        self._lock            = threading.Lock()
        self._calibrator:     CrossVenueCalibratorCore | None = None
        self._last_refresh_ms: int = 0
        self._last_ok_ms:      int = 0

    # ── public API ────────────────────────────────────────────────────────── #

    def thresholds_for(
        self,
        symbol: str,
        *,
        default_disloc_z:  float = DEFAULT_DISLOC_Z,
        default_min_agree: float = DEFAULT_MIN_AGREE,
    ) -> tuple[float, float]:
        """Return ``(adaptive_disloc_z, adaptive_min_agree)``.

        Falls back to supplied defaults on any failure or when the calibrator
        has not yet accumulated enough samples for ``symbol``.
        """
        now_ms = int(time.time() * 1000)
        self._maybe_refresh(now_ms)
        cal = self._calibrator
        if cal is None:
            return default_disloc_z, default_min_agree
        if (now_ms - self._last_ok_ms) > self._stale_ms:
            return default_disloc_z, default_min_agree
        return cal.thresholds_for(
            symbol,
            default_disloc_z=default_disloc_z,
            default_min_agree=default_min_agree,
        )

    def is_healthy(self) -> bool:
        cal = self._calibrator
        if cal is None:
            return False
        return (int(time.time() * 1000) - self._last_ok_ms) <= self._stale_ms

    def force_refresh(self) -> bool:
        return self._refresh(int(time.time() * 1000), force=True)

    # ── internals ─────────────────────────────────────────────────────────── #

    def _maybe_refresh(self, now_ms: int) -> None:
        if (now_ms - self._last_refresh_ms) < self._refresh_ms:
            return
        self._refresh(now_ms, force=False)

    def _refresh(self, now_ms: int, *, force: bool) -> bool:
        with self._lock:
            if not force and (now_ms - self._last_refresh_ms) < self._refresh_ms:
                return self._calibrator is not None
            self._last_refresh_ms = now_ms
            try:
                raw = self._redis.get(self._redis_key)
            except Exception as e:
                logger.warning("crossvenue_calib reader: redis GET failed: %s", e)
                return self._calibrator is not None
            if raw is None:
                return self._calibrator is not None
            try:
                if isinstance(raw, (bytes, bytearray)):
                    raw = raw.decode("utf-8", "ignore")
                state = json.loads(raw)
            except Exception as e:
                logger.warning("crossvenue_calib reader: parse failed: %s", e)
                return self._calibrator is not None
            try:
                cal = CrossVenueCalibratorCore.load_state(state)
            except Exception as e:
                logger.warning("crossvenue_calib reader: load_state failed: %s", e)
                return self._calibrator is not None
            self._calibrator  = cal
            self._last_ok_ms  = now_ms
            return True


# ── module-level singleton ─────────────────────────────────────────────────── #

_READER: CrossVenueCalibReader | None = None
_READER_LOCK = threading.Lock()


def get_reader() -> CrossVenueCalibReader | None:
    """Return the process-wide reader, or None when disabled.

    Toggle via ``AUTOCAL_CROSSVENUE_READ_ENABLED=1``.
    """
    global _READER
    if not _env_bool("AUTOCAL_CROSSVENUE_READ_ENABLED", False):
        return None
    if _READER is not None:
        return _READER
    with _READER_LOCK:
        if _READER is not None:
            return _READER
        try:
            from core.redis_client import get_redis
            client = get_redis()
        except Exception as e:
            logger.warning("crossvenue_calib reader: cannot get Redis: %s", e)
            return None
        refresh_ms = _env_int("AUTOCAL_CROSSVENUE_REFRESH_MS", 60_000)
        stale_ms   = _env_int("AUTOCAL_CROSSVENUE_STALE_MS",   30 * 60_000)
        _READER = CrossVenueCalibReader(client, refresh_ms=refresh_ms, stale_ms=stale_ms)
        return _READER


def reset_reader_for_tests() -> None:
    """Reset singleton — pytest fixtures only."""
    global _READER
    with _READER_LOCK:
        _READER = None
