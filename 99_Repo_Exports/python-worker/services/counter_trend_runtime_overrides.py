from __future__ import annotations

"""counter_trend_runtime_overrides.py — read-side adapter for counter-trend cal.

Loads `autocal:counter_trend:state` (written by
`orderflow_services/counter_trend_regime_calibrator_v1.py`) and exposes
per-direction block-regime lists.

Design:
  - Disabled by default (`AUTOCAL_COUNTER_TREND_READ_ENABLED=0`); fail-open.
  - TTL cache (60s default) — Redis GET on miss only.
  - HMAC verify if `RECS_HMAC_SECRET`/`LAYERS_CAL_HMAC_SECRET` set; mismatch → ignore.
  - require_enforce: only enforced buckets contribute to live override.

Public surface:
  get_block_regimes(direction, default_set) -> frozenset[str]
    Returns autocal-recommended block regimes for the given direction.
    Falls back to default_set if reader disabled, snapshot stale, or no
    enforced buckets exist.
"""

import hashlib
import hmac
import json
import logging
import os
import threading
import time
from typing import Any

logger = logging.getLogger(__name__)

_DEFAULT_KEY = "autocal:counter_trend:state"
_DEFAULT_REFRESH_MS = 60_000
_DEFAULT_STALE_MS = 6 * 60 * 60 * 1000  # 6 hours


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


class CounterTrendReader:
    """TTL-cached Redis snapshot reader. Thread-safe."""

    def __init__(
        self,
        redis_client: Any,
        *,
        redis_key: str = _DEFAULT_KEY,
        refresh_ms: int = _DEFAULT_REFRESH_MS,
        stale_ms: int = _DEFAULT_STALE_MS,
        hmac_secret: str = "",
    ) -> None:
        self._redis = redis_client
        self._key = redis_key
        self._refresh_ms = max(1000, refresh_ms)
        self._stale_ms = max(self._refresh_ms, stale_ms)
        self._hmac_secret = hmac_secret
        self._lock = threading.Lock()
        self._short_block: list[str] = []
        self._long_block: list[str] = []
        self._buckets: dict[str, dict[str, Any]] = {}
        self._snapshot_ts_ms: int = 0
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
                if isinstance(raw, (bytes, bytearray)):
                    raw = raw.decode()
                data = json.loads(raw)
                if self._hmac_secret and "sig" in data:
                    expected_sig = data.pop("sig")
                    canon = json.dumps(
                        data, sort_keys=True, separators=(",", ":")
                    ).encode()
                    actual = hmac.new(
                        self._hmac_secret.encode(), canon, hashlib.sha256
                    ).hexdigest()
                    if not hmac.compare_digest(actual, str(expected_sig)):
                        logger.warning(
                            "counter_trend overrides: HMAC mismatch — ignoring"
                        )
                        return
                self._buckets = data.get("buckets") or {}
                self._short_block = list(data.get("short_block_regimes") or [])
                self._long_block = list(data.get("long_block_regimes") or [])
                self._snapshot_ts_ms = int(data.get("ts_ms") or 0)
            except Exception as e:
                logger.debug(
                    "counter_trend overrides: refresh fail (fail-open): %s", e
                )

    def _fresh(self) -> bool:
        if not (self._short_block or self._long_block or self._buckets):
            return False
        age_ms = int(time.time() * 1000) - self._snapshot_ts_ms
        return age_ms <= self._stale_ms

    def get_block_regimes(
        self,
        *,
        direction: str,
        default_set: frozenset[str],
        require_enforce: bool = True,
    ) -> frozenset[str]:
        """Return autocal-recommended block regimes for `direction`.

        When `require_enforce=True` (default), uses `short_block_regimes` /
        `long_block_regimes` published by the calibrator (which already
        filters to enforced buckets). When False, intersects all `block=1`
        buckets regardless of enforce (shadow-mode signal collection).
        """
        self._maybe_refresh()
        if not self._fresh():
            return default_set
        d = (direction or "").strip().upper()
        if require_enforce:
            src = self._short_block if d == "SHORT" else (
                self._long_block if d == "LONG" else []
            )
            if not src:
                return default_set
            return frozenset(str(x).strip().lower() for x in src if str(x).strip())
        out: set[str] = set()
        for key, b in self._buckets.items():
            if int(b.get("block", 0)) != 1:
                continue
            try:
                bk_dir, bk_reg = key.split("|", 1)
            except ValueError:
                continue
            if bk_dir.upper() == d:
                out.add(bk_reg.strip().lower())
        return frozenset(out) if out else default_set

    def get_bucket_ev(self, *, direction: str, regime: str, default: float = 0.0) -> float:
        """Return calibrator avg_r for (direction × regime) — P2.1 EV weighting.

        Returns `default` when reader is disabled, snapshot stale, or bucket unknown.
        """
        self._maybe_refresh()
        if not self._fresh():
            return default
        key = f"{direction.strip().upper()}|{regime.strip().lower()}"
        b = self._buckets.get(key)
        if not b:
            return default
        try:
            v = float(b.get("avg_r", default))
            import math as _m
            return v if _m.isfinite(v) else default
        except Exception:
            return default

    def get_snapshot_meta(self) -> dict[str, Any]:
        """Inspection helper: return ts/age/sizes for shadow-mode metrics."""
        self._maybe_refresh()
        now_ms = int(time.time() * 1000)
        return {
            "ts_ms": self._snapshot_ts_ms,
            "age_ms": now_ms - self._snapshot_ts_ms if self._snapshot_ts_ms else None,
            "short_block_n": len(self._short_block),
            "long_block_n": len(self._long_block),
            "buckets_n": len(self._buckets),
            "fresh": self._fresh(),
        }


_CT_MODE_REDIS_KEY = "cfg:counter_trend:mode"
_CT_MODE_REFRESH_MS = 15_000  # re-read mode from Redis every 15 s


class _ModeCache:
    """TTL-cached reader for cfg:counter_trend:mode. Thread-safe, fail-open."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._value: str | None = None
        self._last_ms: int = 0

    def get(self, redis_client: Any, env_fallback: str) -> str:
        now_ms = int(time.time() * 1000)
        if now_ms - self._last_ms < _CT_MODE_REFRESH_MS and self._value is not None:
            return self._value
        with self._lock:
            if now_ms - self._last_ms < _CT_MODE_REFRESH_MS and self._value is not None:
                return self._value
            self._last_ms = now_ms
            try:
                raw = redis_client.get(_CT_MODE_REDIS_KEY)
                if raw:
                    self._value = str(raw).strip().lower()
                    return self._value
            except Exception as e:
                logger.debug("ct mode cache: redis read fail (fail-open): %s", e)
            return env_fallback


_MODE_CACHE = _ModeCache()

# Module singleton.
_READER: CounterTrendReader | None = None
_READER_LOCK = threading.Lock()


def _make_reader() -> CounterTrendReader | None:
    if not _env_bool("AUTOCAL_COUNTER_TREND_READ_ENABLED", False):
        return None
    try:
        import redis  # type: ignore
        url = _env(
            "AUTOCAL_COUNTER_TREND_REDIS_URL",
            _env("REDIS_URL", "redis://redis-worker-1:6379/0"),
        )
        client = redis.from_url(url, decode_responses=True)
        secret = (
            _env("CT_CAL_HMAC_SECRET", "")
            or _env("RECS_HMAC_SECRET", "")
            or _env("LAYERS_CAL_HMAC_SECRET", "")
        )
        return CounterTrendReader(
            client,
            redis_key=_env("AUTOCAL_COUNTER_TREND_KEY", _DEFAULT_KEY),
            refresh_ms=_env_int("AUTOCAL_COUNTER_TREND_REFRESH_MS", _DEFAULT_REFRESH_MS),
            stale_ms=_env_int("AUTOCAL_COUNTER_TREND_STALE_MS", _DEFAULT_STALE_MS),
            hmac_secret=secret,
        )
    except Exception as e:
        logger.debug("counter_trend overrides: reader init fail (fail-open): %s", e)
        return None


def get_reader() -> CounterTrendReader | None:
    """Lazy singleton. None when disabled or unavailable."""
    global _READER
    if _READER is not None:
        return _READER
    with _READER_LOCK:
        if _READER is None:
            _READER = _make_reader()
        return _READER


def reset_reader_for_tests() -> None:
    """Test helper: force re-init on next get_reader() call."""
    global _READER
    with _READER_LOCK:
        _READER = None


def get_block_regimes(
    *,
    direction: str,
    default_set: frozenset[str],
    require_enforce: bool = True,
) -> frozenset[str]:
    """Module-level convenience accessor. Returns default_set on any failure."""
    r = get_reader()
    if r is None:
        return default_set
    try:
        return r.get_block_regimes(
            direction=direction,
            default_set=default_set,
            require_enforce=require_enforce,
        )
    except Exception:
        return default_set


def get_bucket_ev(*, direction: str, regime: str, default: float = 0.0) -> float:
    """Module-level convenience — P2.1. Returns default on any failure/disabled."""
    r = get_reader()
    if r is None:
        return default
    try:
        return r.get_bucket_ev(direction=direction, regime=regime, default=default)
    except Exception:
        return default


def get_mode(redis_client: Any, env_fallback: str = "shadow") -> str:
    """Return current counter-trend gate mode ('shadow'|'enforce').

    Priority: cfg:counter_trend:mode in Redis (refreshed every 15 s)
    → env_fallback (typically COUNTER_TREND_HARD_VETO_MODE env var).
    Fail-open: returns env_fallback on any Redis error.
    """
    return _MODE_CACHE.get(redis_client, env_fallback)
