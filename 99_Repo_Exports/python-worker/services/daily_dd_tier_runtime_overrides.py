"""
daily_dd_tier_runtime_overrides.py — TTL-cached reader for DailyDDPerTierCalibrator.

Reads `autocal:daily_dd_tier:state`, exposes `get_soft_limit(tier, regime)` and
`get_hard_limit(tier, regime)` for risk_policy_engine.

Disabled by default (AUTOCAL_DAILY_DD_TIER_READ_ENABLED=0); fail-open → None
(caller uses ENV RISK_MAX_DAILY_LOSS_PCT).
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


class DailyDDTierReader:
    """TTL-cached per-(tier × regime) DD limit reader. Thread-safe."""

    def __init__(self, redis_client: Any, *, redis_key: str,
                 refresh_ms: int = _DEFAULT_REFRESH_MS, stale_ms: int = _DEFAULT_STALE_MS) -> None:
        self._redis = redis_client
        self._key = redis_key
        self._refresh_ms = max(1000, refresh_ms)
        self._stale_ms = max(self._refresh_ms, stale_ms)
        self._lock = threading.Lock()
        self._soft: dict[tuple[str, str], float] = {}
        self._hard: dict[tuple[str, str], float] = {}
        self._enforce: bool = False
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
                self._ts_ms = int(state.get("ts_ms", 0))
                soft: dict[tuple[str, str], float] = {}
                hard: dict[tuple[str, str], float] = {}
                for row in state.get("bins", []):
                    t = str(row.get("tier", "*")).upper()
                    r = str(row.get("regime", "*")).lower()
                    s = _safe_float(row.get("committed_soft_pct"), 0.0)
                    h = _safe_float(row.get("committed_hard_pct"), 0.0)
                    if s > 0:
                        soft[(t, r)] = s
                    if h > 0:
                        hard[(t, r)] = h
                self._soft = soft
                self._hard = hard
            except Exception as e:
                logger.debug("daily_dd_tier_reader refresh fail: %s", e)

    def _lookup(self, d: dict[tuple[str, str], float], tier: str, regime: str) -> float | None:
        t = (tier or "*").strip().upper()
        r = (regime or "*").strip().lower()
        for key in [(t, r), (t, "*"), ("*", r), ("*", "*")]:
            v = d.get(key)
            if v and v > 0:
                return v
        return None

    def get_soft_limit(self, tier: str, regime: str) -> float | None:
        self._maybe_refresh()
        if not self._enforce:
            return None
        age_ms = int(time.time() * 1000) - self._ts_ms
        if self._ts_ms > 0 and age_ms > self._stale_ms:
            return None
        return self._lookup(self._soft, tier, regime)

    def get_hard_limit(self, tier: str, regime: str) -> float | None:
        self._maybe_refresh()
        if not self._enforce:
            return None
        age_ms = int(time.time() * 1000) - self._ts_ms
        if self._ts_ms > 0 and age_ms > self._stale_ms:
            return None
        return self._lookup(self._hard, tier, regime)


_READER: DailyDDTierReader | None = None
_READER_LOCK = threading.Lock()


def _make_reader() -> DailyDDTierReader | None:
    if not _env_bool("AUTOCAL_DAILY_DD_TIER_READ_ENABLED", False):
        return None
    try:
        import redis  # type: ignore
        url = _env("AUTOCAL_DAILY_DD_TIER_REDIS_URL", _env("REDIS_URL", "redis://redis-worker-1:6379/0"))
        key = _env("AUTOCAL_DAILY_DD_TIER_KEY", "autocal:daily_dd_tier:state")
        client = redis.from_url(url, decode_responses=False)
        return DailyDDTierReader(
            client, redis_key=key,
            refresh_ms=_env_int("AUTOCAL_DAILY_DD_TIER_REFRESH_MS", _DEFAULT_REFRESH_MS),
            stale_ms=_env_int("AUTOCAL_DAILY_DD_TIER_STALE_MS", _DEFAULT_STALE_MS),
        )
    except Exception as e:
        logger.debug("daily_dd_tier_reader init fail: %s", e)
        return None


def get_reader() -> DailyDDTierReader | None:
    global _READER
    if _READER is not None:
        return _READER
    with _READER_LOCK:
        if _READER is None:
            _READER = _make_reader()
        return _READER
