from __future__ import annotations

"""funding_basis_z_runtime_overrides.py — read-side for FundingBasisZCalibrator.

Loads `autocal:funding_basis_z:state` with TTL cache.
Exposes `get_thresholds(symbol, vol_regime)` → (funding_z_max, basis_bps_max) | None.

Default OFF (`AUTOCAL_FUNDING_Z_READ_ENABLED=0`), fail-open → None.
Replaces hardcoded DERIV_CTX_FUNDING_Z_MAX=3.0 / DERIV_CTX_BASIS_BPS_MAX=10.0.
"""

import json
import logging
import os
import threading
import time
from typing import Any

logger = logging.getLogger(__name__)

_DEFAULT_KEY = "autocal:funding_basis_z:state"
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


class FundingBasisZReader:
    """TTL-cached per-(symbol × vol_regime) funding z / basis bps reader."""

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
        # key: "SYMBOL:vol_regime" → dict with committed_funding_z / committed_basis_bps
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
                # State stored as JSON list of bins
                raw = self._redis.get(self._key)
                if not raw:
                    return
                data = json.loads(raw.decode("utf-8") if isinstance(raw, bytes) else raw)
                parsed: dict[str, dict[str, Any]] = {}
                bins = data.get("bins", []) if isinstance(data, dict) else []
                for row in bins:
                    if not isinstance(row, dict):
                        continue
                    sym = (row.get("symbol") or "*").upper()
                    vr = (row.get("vol_regime") or "*").lower()
                    k = f"{sym}:{vr}"
                    parsed[k] = row
                self._snapshot = parsed
            except Exception as e:
                logger.debug("funding_z overrides: refresh fail: %s", e)

    def get_thresholds(self, symbol: str, vol_regime: str = "*") -> tuple[float, float] | None:
        """Return (funding_z_max, basis_bps_max) or None if unavailable."""
        self._maybe_refresh()
        sym = (symbol or "").strip().upper()
        vr = (vol_regime or "*").strip().lower()

        for key in (f"{sym}:{vr}", f"{sym}:*", f"*:{vr}", "*:*"):
            state = self._snapshot.get(key)
            if not state:
                continue
            updated_ms = int(state.get("updated_ts_ms") or 0)
            if updated_ms > 0:
                age_ms = int(time.time() * 1000) - updated_ms
                if age_ms > self._stale_ms:
                    continue
            fz = _safe_float(state.get("committed_funding_z"), -1.0)
            bb = _safe_float(state.get("committed_basis_bps"), -1.0)
            if fz > 0 and bb > 0:
                return fz, bb
        return None


_READER: FundingBasisZReader | None = None
_READER_LOCK = threading.Lock()


def _make_reader() -> FundingBasisZReader | None:
    if not _env_bool("AUTOCAL_FUNDING_Z_READ_ENABLED", False):
        return None
    try:
        import redis  # type: ignore
        url = _env("AUTOCAL_FUNDING_Z_REDIS_URL", _env("REDIS_URL", "redis://redis-worker-1:6379/0"))
        client = redis.from_url(url, decode_responses=False)
        return FundingBasisZReader(
            client,
            redis_key=_env("AUTOCAL_FUNDING_Z_KEY", _DEFAULT_KEY),
            refresh_ms=_env_int("AUTOCAL_FUNDING_Z_REFRESH_MS", _DEFAULT_REFRESH_MS),
            stale_ms=_env_int("AUTOCAL_FUNDING_Z_STALE_MS", _DEFAULT_STALE_MS),
        )
    except Exception as e:
        logger.debug("funding_z overrides: reader init fail: %s", e)
        return None


def get_reader() -> FundingBasisZReader | None:
    global _READER
    if _READER is not None:
        return _READER
    with _READER_LOCK:
        if _READER is None:
            _READER = _make_reader()
        return _READER


def get_thresholds(symbol: str, vol_regime: str = "*") -> tuple[float, float] | None:
    rdr = get_reader()
    if rdr is None:
        return None
    try:
        return rdr.get_thresholds(symbol, vol_regime)
    except Exception:
        return None
