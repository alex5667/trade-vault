from __future__ import annotations

"""daily_dd_reader.py — read-side adapter for Daily Equity-Drawdown Kill-Switch.

Loads `risk:daily_dd:state` (written by services/daily_dd_kill_switch_v1.py)
with TTL cache and exposes `is_armed()` / `get_state()` for use inside
EntryPolicyGate.

Design:
  - Fail-open: if Redis missing/invalid → returns (False, "") (gate passes).
  - TTL cache (5s default) — Redis HGETALL on miss only.
  - Stale guard: if last update older than DAILY_DD_STALE_MS → not armed.
  - Mode-aware: only returns armed when state.mode == 'enforce'.
"""

import logging
import os
import threading
import time
from typing import Any

from core.redis_keys import RK

logger = logging.getLogger(__name__)

_DEFAULT_REFRESH_MS = 5_000
_DEFAULT_STALE_MS = 5 * 60 * 1000  # 5 min


def _env(k: str, d: str = "") -> str:
    return os.environ.get(k, d)


def _env_int(k: str, d: int) -> int:
    try:
        return int(_env(k, str(d)))
    except Exception:
        return d


class DailyDdReader:
    """TTL-cached Redis snapshot reader. Thread-safe. Fail-open by design."""

    def __init__(
        self,
        redis_client: Any,
        *,
        redis_key: str = RK.DAILY_DD_STATE,
        refresh_ms: int = _DEFAULT_REFRESH_MS,
        stale_ms: int = _DEFAULT_STALE_MS,
    ) -> None:
        self._redis = redis_client
        self._key = redis_key
        self._refresh_ms = max(500, refresh_ms)
        self._stale_ms = max(self._refresh_ms, stale_ms)
        self._lock = threading.Lock()
        self._snapshot: dict[str, str] = {}
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
                raw: Any = self._redis.hgetall(self._key) or {}
                snap: dict[str, str] = {}
                for k, v in raw.items():
                    if isinstance(k, (bytes, bytearray)):
                        k = k.decode("utf-8", "ignore")
                    if isinstance(v, (bytes, bytearray)):
                        v = v.decode("utf-8", "ignore")
                    snap[str(k)] = str(v)
                self._snapshot = snap
            except Exception as e:
                logger.debug("daily_dd_reader: refresh fail (fail-open): %s", e)

    def get_state(self) -> dict[str, str]:
        self._maybe_refresh()
        return dict(self._snapshot)

    def is_armed(self) -> tuple[bool, str]:
        """Возвращает (armed, reason).

        armed=True только если:
          - state.kill_armed == '1'
          - state.mode == 'enforce'
          - updated_at_ms свежее DAILY_DD_STALE_MS

        Fail-open: любая аномалия → (False, "").
        """
        self._maybe_refresh()
        snap = self._snapshot
        if not snap:
            return False, ""
        try:
            if snap.get("kill_armed", "0") != "1":
                return False, ""
            mode = (snap.get("mode", "") or "").strip().lower()
            if mode != "enforce":
                return False, ""
            updated = int(snap.get("updated_at_ms", "0") or "0")
            now_ms = int(time.time() * 1000)
            if updated <= 0 or (now_ms - updated) > self._stale_ms:
                return False, ""
            reason = snap.get("reason", "") or "daily_dd_breach"
            return True, reason
        except Exception:
            return False, ""


# Module singleton.
_READER: DailyDdReader | None = None
_READER_LOCK = threading.Lock()


def _make_reader_for_ctx(ctx: Any) -> DailyDdReader | None:
    """Build reader using ctx.redis (if sync) or a fresh sync client.

    Returns None if no sync client available or daily_dd_reader is disabled.
    """
    if _env("DAILY_DD_READER_ENABLED", "1").strip().lower() not in ("1", "true", "on", "yes"):
        return None

    rc = None
    if ctx is not None:
        rc = getattr(ctx, "redis", None)
        # Detect async client — fall back to sync below.
        try:
            mod = type(rc).__module__ if rc is not None else ""
            if rc is not None and ("asyncio" in mod or "aioredis" in mod):
                rc = None
        except Exception:
            rc = None

    if rc is None:
        try:
            from handlers.crypto_orderflow.config.handler_config import _get_sync_redis
            rc = _get_sync_redis()
        except Exception:
            rc = None

    if rc is None:
        try:
            import redis  # type: ignore
            url = _env("REDIS_URL", "redis://redis-worker-1:6379/0")
            rc = redis.from_url(url, decode_responses=True, socket_timeout=2)
        except Exception:
            return None

    try:
        return DailyDdReader(
            rc,
            refresh_ms=_env_int("DAILY_DD_REFRESH_MS", _DEFAULT_REFRESH_MS),
            stale_ms=_env_int("DAILY_DD_STALE_MS", _DEFAULT_STALE_MS),
        )
    except Exception:
        return None


def get_reader(ctx: Any = None) -> DailyDdReader | None:
    """Lazy singleton."""
    global _READER
    if _READER is not None:
        return _READER
    with _READER_LOCK:
        if _READER is None:
            _READER = _make_reader_for_ctx(ctx)
        return _READER


def is_armed(ctx: Any = None) -> tuple[bool, str]:
    """Convenience entrypoint for gates. Fail-open."""
    try:
        rdr = get_reader(ctx)
        if rdr is None:
            return False, ""
        return rdr.is_armed()
    except Exception:
        return False, ""


def reset_reader_for_tests() -> None:
    """Test helper — clear singleton."""
    global _READER
    with _READER_LOCK:
        _READER = None
