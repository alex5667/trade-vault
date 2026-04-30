from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any


def _env_bool(name: str, default: bool) -> bool:
    v = (os.getenv(name, "1" if default else "0") or "").strip().lower()
    return v in {"1", "true", "yes", "on"}


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        if v is None:
            return float(default)
        return float(v)
    except Exception:
        return float(default)


def _canon_regime(v: Any) -> str:
    if v is None:
        return "na"
    if isinstance(v, str):
        s = v.strip().lower()
        return s if s else "na"
    try:
        s = str(getattr(v, "name", None) or getattr(v, "value", None) or v).strip().lower()
        return s if s else "na"
    except Exception:
        return "na"


@dataclass(frozen=True)
class TrailStatsConfig:
    enabled: bool = True
    alpha: float = 0.08
    use_regime_dim: bool = True
    ttl_sec: int = 60 * 60 * 24 * 60  # 60d

    @classmethod
    def from_env(cls) -> "TrailStatsConfig":
        return cls(
            enabled=_env_bool("TRAIL_STATS_ENABLED", True)
            alpha=float(os.getenv("TRAIL_STATS_EMA_ALPHA", "0.08") or 0.08)
            use_regime_dim=_env_bool("TRAIL_STATS_USE_REGIME_DIM", True)
            ttl_sec=int(os.getenv("TRAIL_STATS_TTL_SEC", str(60 * 60 * 24 * 60)) or (60 * 60 * 24 * 60))
        )


def _key(kind: str, symbol: str, tf: str, regime: str, *, use_regime_dim: bool) -> str:
    rg = _canon_regime(regime)
    if not use_regime_dim:
        rg = "na"
    return f"trailstats:{kind}:{symbol}:{tf}:{rg}"


def update_trail_giveback_ema(
    redis_client: Any
    *
    cfg: TrailStatsConfig
    kind: str
    symbol: str
    tf: str
    regime: str
    giveback_r: float
    trailing_stop: int
    now_ms: int
) -> None:
    """
    Writes EMA giveback-risk stats used by TrailConditionalEvaluator.
    Stored in Redis HASH:
      trailstats:{kind}:{symbol}:{tf}:{regime}:
        - total_trades
        - ema_giveback_r
        - ema_trailing_stop
        - last_ts_ms
    Fail-open: never raises.
    """
    if redis_client is None or not cfg.enabled:
        return

    try:
        k = _key(kind, symbol, tf, regime, use_regime_dim=cfg.use_regime_dim)
        alpha = max(0.001, min(float(cfg.alpha), 1.0))

        # Read old EMA (best-effort; atomicity is not critical here).
        old = redis_client.hmget(k, "ema_giveback_r", "ema_trailing_stop")
        og = None
        osx = None
        try:
            if old and len(old) >= 2:
                og = float(old[0]) if old[0] is not None else None
                osx = float(old[1]) if old[1] is not None else None
        except Exception:
            og = None
            osx = None

        x_gb = max(0.0, float(giveback_r))
        x_ts = 1.0 if int(trailing_stop) else 0.0

        new_gb = x_gb if og is None else ((1.0 - alpha) * float(og) + alpha * x_gb)
        new_ts = x_ts if osx is None else ((1.0 - alpha) * float(osx) + alpha * x_ts)

        pipe = redis_client.pipeline(transaction=False)
        pipe.hincrby(k, "total_trades", 1)
        pipe.hset(k, mapping={
            "ema_giveback_r": str(float(new_gb))
            "ema_trailing_stop": str(float(new_ts))
            "last_ts_ms": str(int(now_ms))
        })
        if cfg.ttl_sec and cfg.ttl_sec > 0:
            pipe.expire(k, int(cfg.ttl_sec))
        pipe.execute()
    except Exception:
        return
