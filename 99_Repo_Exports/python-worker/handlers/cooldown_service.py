# cooldown_service.py
"""
Cooldown management functionality extracted from base_orderflow_handler.py
"""

from __future__ import annotations
from utils.time_utils import get_ny_time_millis

from typing import Any, Dict, Tuple
import time
import uuid

from common.log import setup_logger


class CooldownService:
    """
    Service for managing signal cooldowns and rate limiting.
    """

    def __init__(self, symbol: str, redis_client: Any = None):
        self.symbol = symbol
        self.redis = redis_client
        self.logger = setup_logger(f"CooldownService:{symbol}")
        # memory fallback: k -> expires_at_ms
        self._cooldowns: Dict[str, int] = {}
        # memory fallback: k -> token (для release по токену)
        self._cooldown_tokens: Dict[str, str] = {}

        self._default_cooldowns_ms = {
            "breakout": 30_000
            "sweep": 15_000
            "extreme": 90_000
            "absorption": 45_000
            "obi_spike": 20_000
            "weak_progress": 60_000
            "default": 10_000
        }

        self._RELEASE_LUA = """
        if redis.call('get', KEYS[1]) == ARGV[1] then
          return redis.call('del', KEYS[1])
        else
          return 0
        end
        """

    def _mem_key(self, *, family: str, timeframe_s: int, kind_lc: str, level_key: str) -> str:
        return f"{family}:{timeframe_s}:{kind_lc}:{level_key}"

    def _redis_key(self, mem_key: str) -> str:
        return f"cooldown:{self.symbol}:{mem_key}"

    def acquire(self, *, kind: str, level_key: str, ts_ms: int, family: str = "of", timeframe_s: int = 60) -> bool:
        """
        Atomically acquire cooldown slot for (family, timeframe_s, kind, level_key).
        True  -> allowed and reserved for cooldown period
        False -> still in cooldown

        Key format: cooldown:{symbol}:{family}:{timeframe_s}:{kind}:{level_key}
        """
        kind_n = (kind or "default").lower()
        ok, _, _ = self.reserve(
            family=family
            timeframe_s=int(timeframe_s)
            kind_lc=kind_n
            level_key=level_key
            ts_ms=int(ts_ms)
        )
        return bool(ok)

    def reserve(
        self
        *
        family: str
        timeframe_s: int
        kind_lc: str
        level_key: str
        ts_ms: int
    ) -> Tuple[bool, str, str]:
        """
        Вариант B (по-взрослому):
          - Redis: atomic SET key token NX PX=period
          - Возвращает (ok, redis_key, token) для safe release()
        """
        kind_n = (kind_lc or "default").lower()
        period = self._get_cooldown_ms(kind_n, timeframe_s=timeframe_s)

        k = f"{family}:{timeframe_s}:{kind_n}:{level_key}"
        redis_key = f"cooldown:{self.symbol}:{k}"
        token = uuid.uuid4().hex

        if self.redis:
            try:
                ok = self.redis.set(redis_key, token, nx=True, px=int(period))
                return bool(ok), redis_key, token
            except Exception as e:
                self.logger.warning("Redis cooldown reserve failed: %s", e)
                # fallthrough to memory

        # Memory fallback (per-process) - use wall-clock for consistency
        now = get_ny_time_millis()
        expires_at = int(self._cooldowns.get(k, 0) or 0)
        if now < expires_at:
            return False, redis_key, token
        self._cooldowns[k] = now + int(period)
        self._cooldown_tokens[k] = token
        if len(self._cooldowns) > 1000:
            self._cleanup(now)
        return True, redis_key, token

    def release(self, redis_key: str, token: str) -> bool:
        """
        Safe release() для multi-process:
          - Redis: compare-and-del (Lua)
          - Memory: удаляем только если token совпал
        """
        if self.redis:
            try:
                res = self.redis.eval(self._RELEASE_LUA, 1, redis_key, token)
                return bool(res)
            except Exception as e:
                self.logger.warning("Redis cooldown release failed: %s", e)
                return False

        # Memory fallback
        prefix = f"cooldown:{self.symbol}:"
        k = redis_key[len(prefix):] if redis_key.startswith(prefix) else redis_key
        if self._cooldown_tokens.get(k) != token:
            return False
        self._cooldowns.pop(k, None)
        self._cooldown_tokens.pop(k, None)
        return True

    def is_allowed(self, *, kind: str, level_key: str, ts_ms: int, family: str = "of", timeframe_s: int = 60) -> bool:
        """Check if signal type and level combination is allowed (not in cooldown)."""
        # keep as pure check for backwards-compat; prefer acquire() in generator
        kind_n = (kind or "default").lower()
        _ = self._get_cooldown_ms(kind_n, timeframe_s=timeframe_s)
        k = self._mem_key(family=family, timeframe_s=timeframe_s, kind_lc=kind_n, level_key=level_key)

        if self.redis:
            redis_key = self._redis_key(k)
            try:
                # если ключ существует, значит cooldown активен (TTL не истёк)
                return self.redis.exists(redis_key) == 0
            except Exception:
                pass

        now = get_ny_time_millis()
        expires_at = int(self._cooldowns.get(k, 0) or 0)
        return now >= expires_at

    def mark(self, *, kind: str, level_key: str, ts_ms: int, family: str = "of", timeframe_s: int = 60) -> None:
        """Mark cooldown timestamp for signal type and level combination."""
        # With acquire() you typically don't need mark() at all.
        # Keep for compatibility (memory mode) but normalize kind and store expiry.
        kind_n = (kind or "default").lower()
        period = self._get_cooldown_ms(kind_n, timeframe_s=timeframe_s)
        k = self._mem_key(family=family, timeframe_s=timeframe_s, kind_lc=kind_n, level_key=level_key)
        now = get_ny_time_millis()
        self._cooldowns[k] = now + int(period)

        if self.redis:
            # If you still call mark() in Redis mode, set TTL=period (not max*2).
            redis_key = self._redis_key(k)
            try:
                self.redis.set(redis_key, str(now), px=int(period))
            except Exception as e:
                self.logger.warning("Failed to set Redis cooldown: %s", e)

        if len(self._cooldowns) > 1000:
            self._cleanup(now)

    def clear_all(self) -> None:
        """Clear all cooldowns (for testing or emergency)."""
        self._cooldowns.clear()
        self._cooldown_tokens.clear()
        if not self.redis:
            return
        try:
            # SCAN вместо KEYS
            pattern = f"cooldown:{self.symbol}:*"
            for key in self.redis.scan_iter(match=pattern, count=10000):
                self.redis.delete(key)
        except Exception as e:
            self.logger.warning("Failed to clear Redis cooldowns: %s", e)

    def _get_cooldown_ms(self, kind: str, *, timeframe_s: int = 60) -> int:
        """Get cooldown period for signal type (with timeframe scaling)."""
        kind_n = (kind or "").lower()
        base = int(self._default_cooldowns_ms.get(kind_n, self._default_cooldowns_ms["default"]))

        # Простая шкала по timeframe:
        # 1m: x1, 5m: x2, 15m: x3, 1h+: x4
        tf = int(timeframe_s or 60)
        if tf >= 3600:
            mul = 4
        elif tf >= 900:
            mul = 3
        elif tf >= 300:
            mul = 2
        else:
            mul = 1
        return int(base * mul)

    def _max_cooldown_ms(self) -> int:
        """Get maximum cooldown period."""
        return max(self._default_cooldowns_ms.values())

    def _cleanup(self, now_ms: int) -> None:
        """Remove expired cooldown entries from memory."""
        # now we store expires_at, so cleanup is simpler
        expired = [k for k, expires_at in self._cooldowns.items() if now_ms >= int(expires_at)]
        for k in expired:
            self._cooldowns.pop(k, None)
            self._cooldown_tokens.pop(k, None)
