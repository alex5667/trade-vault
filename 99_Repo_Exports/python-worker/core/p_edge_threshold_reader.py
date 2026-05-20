from __future__ import annotations

"""
Read-side adapter for `PEdgeThresholdCalibrator` snapshots produced by
`orderflow_services/p_edge_threshold_calibrator_v1.py`.

Loads `autocal:p_edge:state` from Redis with a TTL-cached refresh, exposes
`p_min_for(symbol, regime, kind, default)` for use inside `EdgeCostGate.
_p_min_for_kind()`.

Design notes:
  - The hot path (gate.evaluate) hits this reader thousands of times per minute.
    Redis GET is fast (~0.1ms LAN) but we still cache for `refresh_ms` to
    avoid per-call traffic.
  - Fail-open: any Redis error or missing/expired snapshot falls back to the
    caller-supplied `default` — gate behaviour reverts to the historic ENV
    cutoff, never blocks signals.
  - Module-level singleton via `get_reader()` so the gate's `from_env()`
    constructor doesn't have to thread a Redis client through every layer.
  - When `AUTOCAL_P_EDGE_READ_ENABLED=0` (or missing), `get_reader()` returns
    `None` — disabled by default, must be explicitly turned on per service.
"""

import json
import logging
import os
import threading
import time
from typing import Any

from core.p_edge_threshold_calibrator import (
    DEFAULT_P_MIN,
    PEdgeThresholdCalibrator,
)
from core.redis_keys import RK

logger = logging.getLogger(__name__)


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name, "")
    if not raw:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name, "")
    try:
        return int(raw) if raw else default
    except ValueError:
        return default


class PEdgeThresholdReader:
    """TTL-cached snapshot reader. Thread-safe; one instance per process."""

    def __init__(
        self,
        redis_client: Any,
        *,
        redis_key: str = RK.AUTOCAL_P_EDGE_STATE,
        refresh_ms: int = 60_000,
        stale_ms: int = 30 * 60 * 1000,   # 30 min — beyond this, treat snapshot as gone
    ) -> None:
        self._redis = redis_client
        self._redis_key = redis_key
        self._refresh_ms = max(1_000, refresh_ms)
        self._stale_ms = max(self._refresh_ms, stale_ms)

        self._lock = threading.Lock()
        # Held under lock — only updated by _refresh().
        self._calibrator: PEdgeThresholdCalibrator | None = None
        self._last_refresh_ms: int = 0
        self._last_load_ok_ms: int = 0

    # ----- public API -------------------------------------------------

    def p_min_for(
        self,
        *,
        symbol: str,
        regime: str,
        kind: str,
        default: float = DEFAULT_P_MIN,
        direction: str = "*",
    ) -> float:
        """Return calibrated cutoff with fail-open semantics.

        When snapshot unavailable / stale / disabled → returns `default`.

        `direction` (Phase B, default "*") is forwarded to the calibrator's
        fallback hierarchy. Callers that haven't been upgraded keep getting
        the direction-agnostic aggregate bin exactly as before.
        """
        now_ms = int(time.time() * 1000)
        self._maybe_refresh(now_ms)
        cal = self._calibrator
        if cal is None:
            return default
        # Stale snapshot — keep using last known good for some grace period
        # via stale_ms, but past it we drop back to default.
        if (now_ms - self._last_load_ok_ms) > self._stale_ms:
            return default
        if not cal.enforce:
            return default
        # PEdgeThresholdCalibrator.p_min_for returns default_p_min when no
        # bin matches in the fallback chain — we override that with the
        # caller's default so the gate's per-kind ENV value remains the floor.
        val = cal.p_min_for(
            symbol=symbol, regime=regime, kind=kind, direction=direction,
        )
        if val is None or val <= 0.0:
            return default
        # If the calibrator returned its OWN default_p_min (i.e. no real
        # calibration data for this hierarchy), prefer the caller default.
        if abs(val - cal.default_p_min) < 1e-12:
            return default
        return val

    def shadow_p_min(
        self,
        *,
        symbol: str,
        regime: str,
        kind: str,
        direction: str = "*",
    ) -> float:
        """Latest proposed cutoff regardless of enforce — for reports/metrics."""
        self._maybe_refresh(int(time.time() * 1000))
        cal = self._calibrator
        if cal is None:
            return 0.0
        return cal.shadow_p_min(
            symbol=symbol, regime=regime, kind=kind, direction=direction,
        )

    def is_healthy(self) -> bool:
        """True iff we have a fresh, parsed snapshot."""
        cal = self._calibrator
        if cal is None:
            return False
        return (int(time.time() * 1000) - self._last_load_ok_ms) <= self._stale_ms

    def force_refresh(self) -> bool:
        """Test/admin hook — refresh now, return True if loaded OK."""
        return self._refresh(int(time.time() * 1000), force=True)

    # ----- internals --------------------------------------------------

    def _maybe_refresh(self, now_ms: int) -> None:
        if (now_ms - self._last_refresh_ms) < self._refresh_ms:
            return
        self._refresh(now_ms, force=False)

    def _refresh(self, now_ms: int, *, force: bool) -> bool:
        # Single-flight: under the lock, re-check the timer to avoid stampedes.
        with self._lock:
            if not force and (now_ms - self._last_refresh_ms) < self._refresh_ms:
                return self._calibrator is not None
            self._last_refresh_ms = now_ms
            try:
                raw = self._redis.get(self._redis_key)
            except Exception as e:  # noqa: BLE001 — boundary fail-open
                logger.warning("p_edge reader: redis GET failed: %s", e)
                return self._calibrator is not None
            if raw is None:
                # No snapshot yet — keep the previous calibrator if we had one,
                # but its staleness will expire it via `stale_ms`.
                return self._calibrator is not None
            try:
                if isinstance(raw, (bytes, bytearray)):
                    raw = raw.decode("utf-8", "ignore")
                state = json.loads(raw)
            except Exception as e:  # noqa: BLE001
                logger.warning("p_edge reader: parse failed: %s", e)
                return self._calibrator is not None

            # Rebuild calibrator from scratch — guards against accumulated
            # bins for symbols/regimes no longer present in the snapshot.
            cal = PEdgeThresholdCalibrator()
            try:
                cal.load_state(state)
            except Exception as e:  # noqa: BLE001
                logger.warning("p_edge reader: load_state failed: %s", e)
                return self._calibrator is not None
            self._calibrator = cal
            self._last_load_ok_ms = now_ms
            return True


# ----- module-level singleton ---------------------------------------------

_READER: PEdgeThresholdReader | None = None
_READER_LOCK = threading.Lock()


def get_reader() -> PEdgeThresholdReader | None:
    """Return the process-wide reader, or None when disabled / Redis unavailable.

    Toggle via ENV `AUTOCAL_P_EDGE_READ_ENABLED=1`.
    """
    global _READER
    if not _env_bool("AUTOCAL_P_EDGE_READ_ENABLED", False):
        return None
    if _READER is not None:
        return _READER
    with _READER_LOCK:
        if _READER is not None:
            return _READER
        try:
            from core.redis_client import get_redis  # local import to avoid hard dep at import-time
            client = get_redis()
        except Exception as e:  # noqa: BLE001
            logger.warning("p_edge reader: cannot obtain Redis client: %s", e)
            return None
        refresh_ms = _env_int("AUTOCAL_P_EDGE_REFRESH_MS", 60_000)
        stale_ms = _env_int("AUTOCAL_P_EDGE_STALE_MS", 30 * 60 * 1000)
        _READER = PEdgeThresholdReader(
            client, refresh_ms=refresh_ms, stale_ms=stale_ms,
        )
        return _READER


def reset_reader_for_tests() -> None:
    """Reset the module-level singleton — pytest fixtures only."""
    global _READER
    with _READER_LOCK:
        _READER = None
