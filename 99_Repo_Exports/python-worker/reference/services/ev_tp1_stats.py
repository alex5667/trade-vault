from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import os
import time
from dataclasses import dataclass
from typing import Any, Optional, Tuple


def _env_bool(name: str, default: bool) -> bool:
    v = (os.getenv(name, "1" if default else "0") or "").strip().lower()
    return v in {"1", "true", "yes", "on"}


def _clamp(x: float, lo: float, hi: float) -> float:
    try:
        xf = float(x)
    except Exception:
        return lo
    return max(lo, min(hi, xf))


@dataclass(frozen=True)
class EvTp1StatsConfig:
    """
    Minimal TP1 hit-rate stats for EV/cost gates.

    Stored per (kind,strategy)×symbol×tf×regime:
      key: evtp1:{kind}:{symbol}:{tf}:{regime}
      fields:
        - total_trades (int)
        - tp1_hits (int)
        - ema_tp1 (float in [0,1])
        - last_ts_ms (int)

    Writer is invoked only when StatsAggregator main Lua returns applied=1.
    """

    enabled: bool = _env_bool("EV_TP1_STATS_ENABLED", True)
    alpha: float = float(os.getenv("EV_TP1_EMA_ALPHA", "0.05") or 0.05)
    ttl_sec: int = int(os.getenv("EV_TP1_STATS_TTL_SEC", str(60 * 60 * 24 * 30)) or (60 * 60 * 24 * 30))

    @classmethod
    def from_env(cls) -> "EvTp1StatsConfig":
        return cls(
            enabled=_env_bool("EV_TP1_STATS_ENABLED", True),
            alpha=float(os.getenv("EV_TP1_EMA_ALPHA", "0.05") or 0.05),
            ttl_sec=int(os.getenv("EV_TP1_STATS_TTL_SEC", str(60 * 60 * 24 * 30)) or (60 * 60 * 24 * 30)),
        )


_LUA_TP1_EMA = r"""
-- KEYS[1] = hash key
-- ARGV[1] = alpha
-- ARGV[2] = tp1_hit (0/1)
-- ARGV[3] = now_ms
-- ARGV[4] = ttl_sec (optional; 0 means no expire)

local key = KEYS[1]
local alpha = tonumber(ARGV[1]) or 0.05
if alpha < 0 then alpha = 0 end
if alpha > 1 then alpha = 1 end
local hit = tonumber(ARGV[2]) or 0
if hit ~= 1 then hit = 0 end
local now_ms = tostring(ARGV[3] or "0")
local ttl = tonumber(ARGV[4]) or 0

local total = redis.call('HINCRBY', key, 'total_trades', 1)
if hit == 1 then
  redis.call('HINCRBY', key, 'tp1_hits', 1)
end

local prev = tonumber(redis.call('HGET', key, 'ema_tp1') or '0')
local ema = prev
if total <= 1 then
  ema = hit
else
  ema = prev + alpha * (hit - prev)
end

redis.call('HSET', key, 'ema_tp1', tostring(ema), 'last_ts_ms', now_ms)
if ttl and ttl > 0 then
  redis.call('EXPIRE', key, ttl)
end
return {total, ema}
"""


def _key(kind: str, symbol: str, tf: str, regime: str) -> str:
    # Keep keys short and stable (do not include strategy/source here: kind already equals strategy in your pipeline).
    rg = (regime or "na").strip().lower() or "na"
    return f"evtp1:{kind}:{symbol}:{tf}:{rg}"


def update_tp1_hit_ema(
    redis_client: Any,
    *,
    cfg: EvTp1StatsConfig,
    kind: str,
    symbol: str,
    tf: str,
    regime: str,
    tp1_hit: int,
    now_ms: Optional[int] = None,
) -> Tuple[int, float]:
    """
    Update TP1 hit-rate stats.

    Production path:
      - uses a tiny Lua for atomicity: total_trades/tp1_hits/ema update together.

    Test/fallback path:
      - if Redis client doesn't support eval/script_load, do best-effort Python ops.
        (Not strictly atomic, but enough for unit tests and fail-open semantics.)
    """
    if not cfg.enabled or redis_client is None:
        return (0, 0.0)

    now = int(now_ms or get_ny_time_millis())
    hit = 1 if int(tp1_hit) == 1 else 0
    alpha = _clamp(cfg.alpha, 0.0, 1.0)
    k = _key(str(kind), str(symbol), str(tf), str(regime))

    # Prefer Lua if available
    try:
        if hasattr(redis_client, "script_load") and hasattr(redis_client, "evalsha"):
            sha = redis_client.script_load(_LUA_TP1_EMA)
            res = redis_client.evalsha(sha, 1, k, str(alpha), str(hit), str(now), str(int(cfg.ttl_sec or 0)))
            total = int(res[0]) if res and len(res) > 0 else 0
            ema = float(res[1]) if res and len(res) > 1 else 0.0
            return (total, ema)
    except Exception:
        # fall through to Python fallback
        pass

    # Fallback (non-atomic): used in tests / fake redis
    try:
        total = int(redis_client.hincrby(k, "total_trades", 1))
        if hit == 1:
            redis_client.hincrby(k, "tp1_hits", 1)
        try:
            prev_raw = redis_client.hget(k, "ema_tp1")
            prev = float(prev_raw) if prev_raw is not None else 0.0
        except Exception:
            prev = 0.0
        ema = float(hit) if total <= 1 else (prev + alpha * (float(hit) - prev))
        redis_client.hset(k, mapping={"ema_tp1": str(ema), "last_ts_ms": str(now)})
        if int(cfg.ttl_sec or 0) > 0:
            try:
                redis_client.expire(k, int(cfg.ttl_sec))
            except Exception:
                pass
        return (total, float(ema))
    except Exception:
        return (0, 0.0)


def get_tp1_hit_prob(
    redis_client: Any,
    *,
    kind: str,
    symbol: str,
    tf: str,
    regime: str,
    cfg: EvTp1StatsConfig,
) -> Optional[float]:
    """
    Retrieve TP1 hit probability from Redis.
    
    Returns:
        float in [0,1] if data exists, None otherwise
    """
    if not cfg.enabled or redis_client is None:
        return None
    
    k = _key(str(kind), str(symbol), str(tf), str(regime))
    
    try:
        ema_raw = redis_client.hget(k, "ema_tp1")
        if ema_raw is None:
            return None
        ema = float(ema_raw)
        return max(0.0, min(1.0, ema))
    except Exception:
        return None


def extract_regime_label_from_ctx(ctx: Any) -> str:
    """
    Extract regime label from context.
    
    Checks multiple possible attribute names for regime.
    Returns empty string if not found.
    """
    for attr in ("entry_regime", "regime", "market_regime"):
        try:
            val = getattr(ctx, attr, None)
            if val is not None:
                s = str(val).strip()
                if s:
                    return s
        except Exception:
            pass
    return ""


def attach_tp1_hit_prob_to_ctx(
    ctx: Any,
    *,
    redis_client: Any,
    kind: str,
    symbol: str,
    tf: str,
    cfg: EvTp1StatsConfig,
) -> None:
    """
    Attach TP1 hit probability to context for EV gate evaluation.
    
    Sets ctx.p_hit_tp1 if probability data is available.
    """
    regime = extract_regime_label_from_ctx(ctx)
    p_hit = get_tp1_hit_prob(
        redis_client,
        kind=kind,
        symbol=symbol,
        tf=tf,
        regime=regime,
        cfg=cfg,
    )
    if p_hit is not None:
        try:
            setattr(ctx, "p_hit_tp1", float(p_hit))
        except Exception:
            pass


class RedisEvTp1StatsProvider:
    def __init__(self, redis_client: Any, cfg: EvTp1StatsConfig) -> None:
        self.redis = redis_client
        self.cfg = cfg

    def key(self, kind: str, symbol: str, tf: str, regime: str) -> str:
        rg = regime if getattr(self.cfg, "use_regime_dim", True) else "na"
        return _key(kind, symbol, tf, rg)

    def get_p_hit_tp1(self, kind: str, symbol: str, tf: str, regime: str) -> Optional[float]:
        try:
            k = self.key(kind, symbol, tf, regime)
            data = self.redis.hgetall(k)
            if not data:
                return None
            
            # handle possible dict vs bytes-dict
            def _get(f):
                v = data.get(f) or data.get(f.encode())
                if v is not None and isinstance(v, bytes): return v.decode()
                return v
            
            total = int(_get("total_trades") or 0)
            if total < getattr(self.cfg, "min_n", 0):
                return None
            
            ema = _get("ema_tp1")
            return float(ema) if ema is not None else None
        except Exception:
            return None


# Compatibility alias
update_evstats_on_close = update_tp1_hit_ema
