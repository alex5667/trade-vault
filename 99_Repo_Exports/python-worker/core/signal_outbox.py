
# core/signal_outbox.py
import json
import logging
from dataclasses import dataclass
from typing import Any

from common.time_utils import normalize_epoch_ms_best_effort
from core.performance_optimizer import get_optimized_redis_client
from core.redis_keys import RedisStreams as RS
from utils.time_utils import get_ny_time_millis

logger = logging.getLogger(__name__)


@dataclass
class OutboxSettings:
    outbox_stream: str = RS.SIGNAL_OUTBOX
    outbox_maxlen: int = 20000
    dedup_ttl_ms: int = 60000
    dedup_bucket_ms: int = 60000


# Lua script: atomic dedup check + XADD + marker SET.
# Rollback XADD if marker SET fails.
_LUA_DEDUP_AND_OUTBOX = r"""
-- KEYS[1] = dedup_key
-- KEYS[2] = outbox_stream
-- ARGV[1] = dedup_ttl_ms
-- ARGV[2] = maxlen
-- ARGV[3] = envelope_json

if redis.call('EXISTS', KEYS[1]) == 1 then
  return {0}
end

local id = redis.call('XADD', KEYS[2], 'MAXLEN', '~', ARGV[2], '*', 'data', ARGV[3])

local ok = redis.pcall('SET', KEYS[1], '1', 'PX', ARGV[1])
if type(ok) == 'table' and ok['err'] then
  redis.pcall('XDEL', KEYS[2], id)
  return {0}
end

return {1, id}
"""


class SignalOutboxPublisher:
    def __init__(
        self,
        redis_url: str | None = None,
        settings: OutboxSettings | None = None,
        redis_client: Any | None = None,
    ):
        if redis_client is not None:
            self._redis = redis_client
        else:
            self._redis = get_optimized_redis_client(redis_url)
        self.settings = settings or OutboxSettings()
        self._sha_dedup: str | None = None

    @staticmethod
    def _normalize_epoch_ms(ts_ms: Any) -> int:
        """
        Delegate to canonical normalize_epoch_ms_best_effort from common.time_utils.
        """
        return normalize_epoch_ms_best_effort(ts_ms)

    @staticmethod
    def _normalize_ts_ms(ts_ms: Any, envelope: dict[str, Any]) -> int:
        """
        CRITICAL (D): ts_ms может быть 0/мусор/не-epoch → bucket=0 → ложный дедуп (глушит сигналы).
        Нормализация:
          - если ts_ms невалидный/не epoch → берем fallback:
              1) envelope.get("ts_ms") / envelope.get("meta",{}).get("ts_ms")
              2) now_ms
        """
        now_ms = get_ny_time_millis()
        lo = 946684800000       # 2000-01-01
        hi = 4102444800000      # 2100-01-01

        def _to_int(x: Any) -> int | None:
            try:
                if x is None:
                    return None
                if isinstance(x, bool):
                    return None
                return int(x)
            except Exception:
                return None

        cand = _to_int(ts_ms)
        if cand is None or cand <= 0 or cand < lo or cand > hi:
            # try envelope fallbacks
            e1 = _to_int(envelope.get("ts_ms"))
            if e1 is not None and lo <= e1 <= hi:
                return e1
            meta = envelope.get("meta") or {}
            if isinstance(meta, dict):
                e2 = _to_int(meta.get("ts_ms"))
                if e2 is not None and lo <= e2 <= hi:
                    return e2
            # last resort
            return 0
        return cand

    @staticmethod
    def build_dedup_key(
        source: str,
        strategy: str,
        symbol: str,
        side: str,
        kind: str,
        level_key: str,
        reason: str,
        ts_ms: int,
        bucket_ms: int,
    ) -> str:
        if ts_ms <= 0:
            import hashlib
            fallback_hash = hashlib.md5(f"{source}:{strategy}:{symbol}:{side}:{kind}:{level_key}:{reason}".encode()).hexdigest()
            bucket_str = f"bypass_{fallback_hash[:8]}"
        else:
            bucket_str = str(int(ts_ms // max(bucket_ms, 1)))

        # reason может быть "" (тогда дедуп по умолчанию в бакете)
        # level_key может быть "" (тогда дедуп шире)
        return f"dedup:{strategy}:{source}:{symbol}:{side}:{kind}:{level_key}:{reason}:{bucket_str}"

    def _ensure_lua_sha(self) -> str:
        """Load Lua script if not already cached."""
        if self._sha_dedup is None:
            self._sha_dedup = self._redis.script_load(_LUA_DEDUP_AND_OUTBOX)
        return self._sha_dedup  # type: ignore

    def publish(
        self,
        *,
        source: str,
        strategy: str,
        symbol: str,
        side: str,
        kind: str,
        level_key: str,
        ts_ms: int,
        envelope: dict[str, Any],
        dedup_ttl_ms: int | None = None,
    ) -> str | None:
        """
        Atomic dedup + XADD to outbox stream via Lua script.

        Returns:
            msg_id (str) on successful publish, None if deduped or failed.
        """
        settings = self.settings
        dedup_ttl = int(dedup_ttl_ms or settings.dedup_ttl_ms)

        norm_ts_ms = self._normalize_ts_ms(ts_ms, envelope)

        # P1-8: Include fingerprint to avoid losing legitimate signals in 60s bucket
        reason = (envelope.get("fingerprint") or "")

        dedup_key = self.build_dedup_key(
            source=source,
            strategy=strategy,
            symbol=symbol,
            side=side,
            kind=kind,
            level_key=level_key or "",
            reason=reason,
            ts_ms=norm_ts_ms,
            bucket_ms=settings.dedup_bucket_ms,
        )

        # Ensure schema_version is present — dispatcher rejects envelopes without it.
        if "schema_version" not in envelope:
            envelope = {**envelope, "schema_version": 1}

        envelope_json = json.dumps(envelope, ensure_ascii=False, separators=(",", ":"))

        keys = [dedup_key, settings.outbox_stream]
        args = [str(dedup_ttl), str(settings.outbox_maxlen), envelope_json]

        try:
            sha = self._ensure_lua_sha()
            result = self._redis.evalsha(sha, 2, *keys, *args)
        except Exception as e:
            err_str = str(e)
            if "NOSCRIPT" in err_str:
                # SHA evicted — reload + retry
                self._sha_dedup = None
                try:
                    result = self._redis.eval(_LUA_DEDUP_AND_OUTBOX, 2, *keys, *args)
                except Exception as e2:
                    logger.error("Lua EVAL fallback failed for %s/%s: %s", symbol, kind, e2)
                    return None
            else:
                logger.error("Outbox publish EVALSHA failed for %s/%s: %s", symbol, kind, e)
                return None

        # Lua returns: {0} on dedup, {1, id} on success
        if isinstance(result, (list, tuple)):
            if len(result) >= 2 and result[0] == 1:
                msg_id = result[1]
                if isinstance(msg_id, bytes):
                    msg_id = msg_id.decode("utf-8", errors="replace")
                return str(msg_id)
            # result[0] == 0 => dedup hit
            return None
        # unexpected shape — treat as failure
        return None



