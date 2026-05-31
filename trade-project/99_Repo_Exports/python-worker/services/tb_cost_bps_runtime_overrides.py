"""
tb_cost_bps_runtime_overrides.py — TTL-cached reader for TbCostBpsCalibrator.

Reads `autocal:tb_cost_bps:state` (STRING, JSON), exposes `get_cost_bps(symbol)`
with hierarchical fallback: (symbol) → global (*) → None (ENV default TB_COST_BPS).

Disabled by default (AUTOCAL_TB_COST_BPS_READ_ENABLED=0); fail-open → returns None
(caller uses ENV default TB_COST_BPS).
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


class TbCostBpsReader:
    """TTL-cached reader for per-symbol TB cost estimate. Thread-safe."""

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
        self._bins: dict[str, float] = {}   # symbol → committed_cost_bps
        self._enforce: bool = False
        self._default_cost_bps: float = 7.0
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
                self._default_cost_bps = _safe_float(state.get("default_cost_bps"), 7.0)
                self._ts_ms = int(state.get("ts_ms", 0))
                new_bins: dict[str, float] = {}
                for row in state.get("bins", []):
                    sym = str(row.get("symbol", "*"))
                    cost = _safe_float(row.get("committed_cost_bps"), 0.0)
                    if cost > 0:
                        new_bins[sym] = cost
                self._bins = new_bins
            except Exception as e:
                logger.debug("tb_cost_bps_reader refresh fail: %s", e)

    def get_cost_bps(self, symbol: str) -> float | None:
        """Returns calibrated cost_bps or None (caller uses ENV default TB_COST_BPS)."""
        self._maybe_refresh()
        if not self._enforce:
            return None
        age_ms = int(time.time() * 1000) - self._ts_ms
        if self._ts_ms > 0 and age_ms > self._stale_ms:
            return None
        sym = (symbol or "*").strip().upper()
        for key in [sym, "*"]:
            cost = self._bins.get(key)
            if cost and cost > 0:
                return cost
        return None


# ── Module singleton ──────────────────────────────────────────────────────────

_READER: TbCostBpsReader | None = None
_READER_LOCK = threading.Lock()


def _make_reader() -> TbCostBpsReader | None:
    if not _env_bool("AUTOCAL_TB_COST_BPS_READ_ENABLED", False):
        return None
    try:
        import redis  # type: ignore
        url = _env("AUTOCAL_TB_COST_BPS_REDIS_URL", _env("REDIS_URL", "redis://redis-worker-1:6379/0"))
        key = _env("AUTOCAL_TB_COST_BPS_KEY", "autocal:tb_cost_bps:state")
        client = redis.from_url(url, decode_responses=False)
        return TbCostBpsReader(
            client,
            redis_key=key,
            refresh_ms=_env_int("AUTOCAL_TB_COST_BPS_REFRESH_MS", _DEFAULT_REFRESH_MS),
            stale_ms=_env_int("AUTOCAL_TB_COST_BPS_STALE_MS", _DEFAULT_STALE_MS),
        )
    except Exception as e:
        logger.debug("tb_cost_bps_reader init fail: %s", e)
        return None


def get_reader() -> TbCostBpsReader | None:
    global _READER
    if _READER is not None:
        return _READER
    with _READER_LOCK:
        if _READER is None:
            _READER = _make_reader()
        return _READER


def get_cost_bps(symbol: str) -> float | None:
    """Returns calibrated TB cost estimate for the symbol, or None (fail-open)."""
    rdr = get_reader()
    if rdr is None:
        return None
    try:
        return rdr.get_cost_bps(symbol)
    except Exception:
        return None
