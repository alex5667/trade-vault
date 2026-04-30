from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import os
import time
import math
from dataclasses import dataclass
from typing import Any, Optional, Dict


def _env_bool(name: str, default: bool) -> bool:
    v = (os.getenv(name, "1" if default else "0") or "").strip().lower()
    return v in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)) or default)
    except Exception:
        return int(default)


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)) or default)
    except Exception:
        return float(default)


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        v = float(x)
        return v if math.isfinite(v) else float(default)
    except Exception:
        return float(default)


@dataclass(frozen=True)
class SpreadEmaConfig:
    """
    EMA of realized spread (bps) at exit tick, used as baseline for 'spread shock' entry gating.
    """
    enabled: bool
    alpha: float
    ttl_s: int
    prefix: str
    min_samples: int

    @staticmethod
    def from_env() -> "SpreadEmaConfig":
        return SpreadEmaConfig(
            enabled=_env_bool("EXEC_SPREAD_EMA_ENABLED", True)
            alpha=_env_float("EXEC_SPREAD_EMA_ALPHA", 0.05)
            ttl_s=_env_int("EXEC_SPREAD_EMA_TTL_S", 60 * 60 * 24 * 30),  # 30 days
            prefix=str(os.getenv("EXEC_SPREAD_EMA_PREFIX", "spreadema:") or "spreadema:")
            min_samples=_env_int("EXEC_SPREAD_EMA_MIN_SAMPLES", 10)
        )


def _key(cfg: SpreadEmaConfig, *, symbol: str, venue: str, session: str, tf: str, kind: str) -> str:
    sym = (symbol or "").strip()
    ven = (venue or "na").strip()
    ses = (session or "na").strip()
    tfv = (tf or "na").strip()
    knd = (kind or "na").strip()
    # New (preferred) format includes kind dimension:
    return f"{cfg.prefix}{sym}:{ven}:{ses}:{tfv}:{knd}"


def update_spread_ema(
    redis_client: Any
    *
    cfg: SpreadEmaConfig
    symbol: str
    venue: str
    session: str
    tf: str
    kind: str
    now_ms: Optional[int]
    realized_spread_bps: Any
) -> None:
    """
    Update EMA in Redis hash:
      fields:
        - samples
        - ema_spread_bps
        - last_ts_ms

    Fail-open: all errors are swallowed.
    """
    try:
        if not cfg.enabled or redis_client is None:
            return
        val = _safe_float(realized_spread_bps, 0.0)
        if val <= 0:
            return
        alpha = float(cfg.alpha)
        if not (0 < alpha <= 1):
            alpha = 0.05
        ts = int(now_ms or get_ny_time_millis())
        k = _key(cfg, symbol=symbol, venue=venue, session=session, tf=tf, kind=kind)

        try:
            cur = redis_client.hgetall(k) or {}
        except Exception:
            cur = {}

        def _dec(x: Any) -> str:
            if isinstance(x, bytes):
                return x.decode("utf-8", errors="ignore")
            return str(x)

        cur_s: Dict[str, str] = {}
        if isinstance(cur, dict):
            for kk, vv in cur.items():
                cur_s[_dec(kk)] = _dec(vv)

        n = 0
        try:
            n = int(float(cur_s.get("samples", "0") or 0))
        except Exception:
            n = 0
        old = None
        try:
            if "ema_spread_bps" in cur_s:
                old = float(cur_s["ema_spread_bps"])
        except Exception:
            old = None

        if old is None or not math.isfinite(old) or old <= 0:
            ema = float(val)
        else:
            ema = float(alpha) * float(val) + (1.0 - float(alpha)) * float(old)

        n2 = int(n + 1)
        redis_client.hset(k, mapping={
            "samples": str(n2)
            "ema_spread_bps": str(float(ema))
            "last_ts_ms": str(int(ts))
        })
        try:
            redis_client.expire(k, int(max(60, cfg.ttl_s)))
        except Exception:
            pass
    except Exception:
        return
