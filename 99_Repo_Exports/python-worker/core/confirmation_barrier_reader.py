from __future__ import annotations

"""
ConfirmationBarrierReader — read-side cache для ConfirmationBarrierCalibrator.

Читает snapshot из Redis (AUTOCAL_CONFIRM_BARRIER_STATE) с TTL-кэшем.
Используется в L2ConfirmCfg.from_env() когда CONFIRM_BARRIER_CAL_ENABLED=1.

ENV
  CONFIRM_BARRIER_CAL_ENABLED    0 = shadow (default); 1 = enforce
  CONFIRM_BARRIER_CAL_CACHE_TTL  кеш в секундах (default 30)
  CONFIRM_BARRIER_CAL_REDIS_URL  overrides REDIS_URL
"""

import json
import logging
import math
import os
import time
from typing import Any

logger = logging.getLogger("confirm-barrier-reader")


def _env(k: str, d: str = "") -> str:
    return os.environ.get(k, d)


def _env_float(k: str, d: float) -> float:
    try:
        return float(_env(k, str(d)))
    except (ValueError, TypeError):
        return d


def _env_bool(k: str, d: bool) -> bool:
    v = _env(k, "")
    if not v:
        return d
    return v.strip().lower() in ("1", "true", "yes")


class ConfirmationBarrierReader:
    """TTL-cached reader для калиброванных OBI-порогов.

    Потокобезопасность: НЕ thread-safe сам по себе.
    Для async-хендлеров — инстанцируй в одном event loop; для sync — один
    инстанс per thread.
    """

    _BREAKOUT_DEFAULT = 1.15
    _ABSORPTION_DEFAULT = 1.20
    _KIND_DEFAULTS: dict[str, float] = {
        "breakout": _BREAKOUT_DEFAULT,
        "absorption": _ABSORPTION_DEFAULT,
    }

    def __init__(
        self,
        redis_client: Any = None,
        *,
        cache_ttl_sec: float = 30.0,
        enforce: bool = False,
        redis_key: str = "autocal:confirm_barrier:state",
    ) -> None:
        self._redis = redis_client
        self._cache_ttl = float(cache_ttl_sec)
        self.enforce = bool(enforce)
        self._key = redis_key

        self._cache: dict[str, Any] = {}  # parsed snapshot
        self._loaded_at: float = 0.0

    @classmethod
    def from_env(cls, redis_client: Any = None) -> "ConfirmationBarrierReader":
        from core.redis_keys import RK
        enforce = _env_bool("CONFIRM_BARRIER_CAL_ENABLED", False)
        ttl = _env_float("CONFIRM_BARRIER_CAL_CACHE_TTL", 30.0)
        if redis_client is None and enforce:
            try:
                from core.redis_client import get_redis
                url = _env("CONFIRM_BARRIER_CAL_REDIS_URL", "") or _env("REDIS_URL", "")
                redis_client = get_redis(url=url or None)
            except Exception:
                pass
        return cls(
            redis_client,
            cache_ttl_sec=ttl,
            enforce=enforce,
            redis_key=RK.AUTOCAL_CONFIRM_BARRIER,
        )

    def threshold_for(self, symbol: str, kind: str) -> float:
        """Return calibrated threshold для (symbol, kind).

        Если enforce=False → возвращает hardcoded default.
        Если calibration холодная / нет в кэше → возвращает hardcoded default.
        """
        default = self._KIND_DEFAULTS.get(kind, 1.15)
        if not self.enforce:
            return default
        snapshot = self._maybe_refresh()
        if not snapshot:
            return default
        bins = snapshot.get("bins") or {}
        # (sym, kind) → (*, kind) → default
        for sym_key in (symbol.upper(), "*"):
            bin_key = f"{sym_key}:{kind.lower()}"
            entry = bins.get(bin_key)
            if isinstance(entry, dict):
                tau = entry.get("committed_tau")
                if tau is not None:
                    try:
                        t = float(tau)
                        if math.isfinite(t) and t > 0.0:
                            return t
                    except (TypeError, ValueError):
                        pass
        return default

    def _maybe_refresh(self) -> dict[str, Any]:
        now = time.monotonic()
        if self._cache and (now - self._loaded_at) < self._cache_ttl:
            return self._cache
        if self._redis is None:
            return {}
        try:
            raw = self._redis.get(self._key)
            if raw:
                data = json.loads(raw)
                if isinstance(data, dict):
                    self._cache = data
                    self._loaded_at = now
        except Exception as e:
            logger.debug("confirm-barrier-reader refresh failed: %s", e)
        return self._cache
