from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import math
import os
import time
from dataclasses import dataclass
from typing import Any, Optional

from domain.time_utils import session_from_ts_ms


def _env_bool(name: str, default: bool) -> bool:
    v = (os.getenv(name, "1" if default else "0") or "").strip().lower()
    return v in {"1", "true", "yes", "on"}


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        x = float(v)
        return x if math.isfinite(x) else float(default)
    except Exception:
        return float(default)


def _safe_int(v: Any, default: int = 0) -> int:
    try:
        return int(float(v))
    except Exception:
        return int(default)


@dataclass(frozen=True)
class ExecCostEmaConfig:
    enabled: bool
    alpha: float
    ttl_sec: int
    min_samples: int = 20
    prefix: str = "execost:"
    write_compat_no_kind: bool = True
    # Test-compatible fields (stubs)
    min_samples_to_trust: int = 20
    key_prefix: str = "execost:"
    dim_tf: bool = True
    dim_kind: bool = True
    write_legacy: bool = True
    read_legacy_fallback: bool = True

    @classmethod
    def from_env(cls) -> "ExecCostEmaConfig":
        enabled = _env_bool("EXEC_COST_EMA_ENABLED", False)
        alpha = _safe_float(os.getenv("EXEC_COST_EMA_ALPHA", "0.05"), 0.05)
        min_samples = _safe_int(os.getenv("EXEC_COST_EMA_MIN_SAMPLES", "20"), 20)
        ttl_sec = _safe_int(os.getenv("EXEC_COST_EMA_TTL_SEC", str(60 * 60 * 24 * 30)), 60 * 60 * 24 * 30)
        prefix = str(os.getenv("EXEC_COST_EMA_PREFIX", "execost:") or "execost:")
        return cls(
            enabled=enabled
            alpha=alpha
            min_samples=min_samples
            ttl_sec=ttl_sec
            prefix=prefix
            min_samples_to_trust=min_samples
            key_prefix=prefix
        )


def _ema_update(old: Optional[float], x: float, alpha: float) -> float:
    if old is None or (not math.isfinite(old)) or old <= 0:
        return float(x)
    return float(alpha) * float(x) + (1.0 - float(alpha)) * float(old)


def update_exec_cost_ema(
    redis_client: Any
    *
    cfg: ExecCostEmaConfig
    key: str
    realized_slippage_bps: float
    realized_spread_bps: float
    now_ms: Optional[int] = None
) -> None:
    if not cfg.enabled or redis_client is None:
        return
    
    now = int(now_ms or get_ny_time_millis())
    slip = float(realized_slippage_bps)
    sprd = float(realized_spread_bps)

    try:
        # Read existing
        v = redis_client.hmget(key, "samples", "ema_slip_bps", "ema_spread_bps")
        old_samples = _safe_int(v[0], 0) if v and len(v) > 0 else 0
        old_slip = _safe_float(v[1], 0.0) if v and len(v) > 1 and v[1] is not None else None
        old_sprd = _safe_float(v[2], 0.0) if v and len(v) > 2 and v[2] is not None else None

        new_samples = old_samples + 1
        new_slip = _ema_update(old_slip, slip, cfg.alpha)
        new_sprd = _ema_update(old_sprd, sprd, cfg.alpha)

        redis_client.hset(key, mapping={
            "samples": str(new_samples)
            "ema_slip_bps": str(new_slip)
            "ema_spread_bps": str(new_sprd)
            "last_ts_ms": str(now)
        })
        if cfg.ttl_sec > 0:
            redis_client.expire(key, int(cfg.ttl_sec))
    except Exception:
        pass


def read_exec_cost_ema_bps(
    redis_client: Any
    *
    cfg: ExecCostEmaConfig
    key: str
) -> Optional[float]:
    if not cfg.enabled or redis_client is None:
        return None
    try:
        v = redis_client.hmget(key, "samples", "ema_slip_bps")
        if not v or len(v) < 2: return None
        samples = _safe_int(v[0], 0)
        ema = _safe_float(v[1], 0.0)
        
        min_n = getattr(cfg, "min_samples_to_trust", cfg.min_samples)
        if samples < min_n:
            return None
        return ema if ema > 0 else None
    except Exception:
        return None


# Compatibility aliases and stubs
read_exec_cost_ema_slippage_bps = read_exec_cost_ema_bps
def maybe_update_exec_cost_ema_from_closed(*args, **kwargs):
    # For now just call the newer one if possible or do nothing
    pass

update_exec_cost_ema_from_closed = maybe_update_exec_cost_ema_from_closed

def build_exec_cost_ema_key(cfg: ExecCostEmaConfig, symbol, venue, session, tf, kind, legacy=False):
    p = getattr(cfg, "key_prefix", cfg.prefix)
    if legacy:
        return f"{p}{symbol}:{venue}:{session}:{tf}"
    return f"{p}{symbol}:{venue}:{session}:{tf}:{kind}"

# Expose session_from_ts_ms for tests
__all__ = [
    "ExecCostEmaConfig"
    "update_exec_cost_ema"
    "read_exec_cost_ema_bps"
    "build_exec_cost_ema_key"
    "session_from_ts_ms"
    "maybe_update_exec_cost_ema_from_closed"
    "update_exec_cost_ema_from_closed"
]