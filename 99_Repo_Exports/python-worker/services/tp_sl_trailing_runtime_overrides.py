from __future__ import annotations

"""tp_sl_trailing_runtime_overrides.py — read-side adapter for tp-sl-trail autocal.

Loads `autocal:tp_sl_trailing:state` (written by
`orderflow_services/tp_sl_trailing_autocal_v1.py`) with TTL cache and exposes
`get_override(knob, default)` for use inside trailing_profiles, signal_pipeline,
trade_monitor, layer_d_early_arm_hook.

Design:
  - Disabled by default (`AUTOCAL_TP_SL_TRAIL_READ_ENABLED=0`); fail-open.
  - TTL cache (30s default) — Redis GET on miss only.
  - HMAC verify if `RECS_HMAC_SECRET`/`LAYERS_CAL_HMAC_SECRET` set; mismatch → ignore snapshot.
  - Per-knob `enforce` flag — only if 1, runtime override active; else default.
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

_DEFAULT_KEY = "autocal:tp_sl_trailing:state"
_DEFAULT_REFRESH_MS = 30_000
_DEFAULT_STALE_MS = 30 * 60 * 1000  # 30 min


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


class TpSlTrailOverridesReader:
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
        self._snapshot: dict[str, dict[str, Any]] = {}
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
                data = json.loads(raw if isinstance(raw, (str, bytes, bytearray))
                                  else str(raw))
                if self._hmac_secret and "sig" in data:
                    expected_sig = data.pop("sig")
                    canon = json.dumps(
                        data, sort_keys=True, separators=(",", ":")
                    ).encode()
                    actual = hmac.new(
                        self._hmac_secret.encode(), canon, hashlib.sha256
                    ).hexdigest()
                    if not hmac.compare_digest(actual, str(expected_sig)):
                        logger.warning("tp_sl_trail overrides: HMAC mismatch — ignoring")
                        return
                self._snapshot = data.get("knobs") or {}
                self._snapshot_ts_ms = int(data.get("ts_ms") or 0)
            except Exception as e:
                logger.debug("tp_sl_trail overrides: refresh fail (fail-open): %s", e)

    def get_override(self, knob: str, default: Any) -> Any:
        """Return autocal-applied override if enforce=1 and snapshot fresh; else default."""
        self._maybe_refresh()
        knobs = self._snapshot
        if not knobs:
            return default
        # Staleness guard.
        age_ms = int(time.time() * 1000) - self._snapshot_ts_ms
        if age_ms > self._stale_ms:
            return default
        entry = knobs.get(knob)
        if not entry:
            return default
        if not int(entry.get("enforce") or 0):
            return default
        return entry.get("value", default)


# Module singleton.
_READER: TpSlTrailOverridesReader | None = None
_READER_LOCK = threading.Lock()


def _make_reader() -> TpSlTrailOverridesReader | None:
    if not _env_bool("AUTOCAL_TP_SL_TRAIL_READ_ENABLED", False):
        return None
    try:
        import redis  # type: ignore
        url = _env("AUTOCAL_TP_SL_TRAIL_REDIS_URL",
                   _env("REDIS_URL", "redis://redis-worker-1:6379/0"))
        client = redis.from_url(url, decode_responses=True)
        secret = _env("TP_SL_TRAIL_AUTOCAL_HMAC_SECRET", "") \
                  or _env("RECS_HMAC_SECRET", "") \
                  or _env("LAYERS_CAL_HMAC_SECRET", "")
        return TpSlTrailOverridesReader(
            client,
            redis_key=_env("AUTOCAL_TP_SL_TRAIL_KEY", _DEFAULT_KEY),
            refresh_ms=_env_int("AUTOCAL_TP_SL_TRAIL_REFRESH_MS", _DEFAULT_REFRESH_MS),
            stale_ms=_env_int("AUTOCAL_TP_SL_TRAIL_STALE_MS", _DEFAULT_STALE_MS),
            hmac_secret=secret,
        )
    except Exception as e:
        logger.debug("tp_sl_trail overrides: reader init fail (fail-open): %s", e)
        return None


def get_reader() -> TpSlTrailOverridesReader | None:
    """Lazy singleton. None when disabled or unavailable."""
    global _READER
    if _READER is not None:
        return _READER
    with _READER_LOCK:
        if _READER is None:
            _READER = _make_reader()
        return _READER


def get_override(knob: str, default: Any) -> Any:
    """Convenience function. Returns default if reader disabled or no override."""
    rdr = get_reader()
    if rdr is None:
        return default
    try:
        return rdr.get_override(knob, default)
    except Exception:
        return default


def reset_reader_for_tests() -> None:
    """Test helper — clear singleton."""
    global _READER
    with _READER_LOCK:
        _READER = None
