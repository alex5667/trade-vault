from __future__ import annotations

import os
import math
from dataclasses import dataclass
from typing import Any, Optional, Dict

# This module is intentionally "fail-open":
# - If Redis is not available or fields are missing -> do nothing.
# - Never break StatsAggregator.update_stats().


def _env_bool(name: str, default: bool) -> bool:
    v = (os.getenv(name, "1" if default else "0") or "").strip().lower()
    return v in {"1", "true", "yes", "on"}


def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        f = float(x)
        if not math.isfinite(f):
            return None
        return f
    except Exception:
        return None


def _canon_regime(v: Any) -> str:
    if v is None:
        return "na"
    if isinstance(v, str):
        s = v.strip().lower()
        return s if s else "na"
    s = str(getattr(v, "name", None) or getattr(v, "value", None) or v).strip().lower()
    return s if s else "na"


def _estimate_notional(entry_price: Optional[float], qty: Optional[float], notional: Optional[float]) -> Optional[float]:
    # Prefer explicit notional (notional_usd). Fallback: |qty| * entry_price.
    if notional is not None and notional > 1e-12:
        return float(notional)
    if entry_price is None or qty is None:
        return None
    ep = float(entry_price)
    q = abs(float(qty))
    if ep > 1e-12 and q > 1e-12:
        return ep * q
    return None


def _pnl_to_bps(pnl: Optional[float], *, notional: Optional[float]) -> Optional[float]:
    if pnl is None or notional is None or notional <= 1e-12:
        return None
    bps = abs(float(pnl)) / float(notional) * 10_000.0
    return float(bps) if math.isfinite(bps) and bps > 0 else None


@dataclass(frozen=True)
class GivebackEmaConfig:
    """
    Maintains EMA of giveback risk in bps:
      giveback_bps ~= |giveback_pnl| / notional * 10000

    Stored as Redis HASH:
      trailgb:{kind}:{symbol}:{tf}:{regime}
        - samples: int
        - ema_giveback_bps: float
        - last_ts_ms: int

    This is used by conditional trailing:
      if giveback-risk is high for this kind/regime -> enable trailing more often.
    """

    enabled: bool
    alpha: float
    min_samples_for_use: int
    use_regime_dim: bool
    ttl_sec: int

    key_prefix: str = "trailgb"

    @classmethod
    def from_env(cls) -> "GivebackEmaConfig":
        enabled = _env_bool("TRAIL_GIVEBACK_EMA_ENABLED", True)
        try:
            alpha = float(os.getenv("TRAIL_GIVEBACK_EMA_ALPHA", "0.05") or "0.05")
        except Exception:
            alpha = 0.05
        alpha = max(0.001, min(alpha, 0.5))
        try:
            min_samples = int(os.getenv("TRAIL_GIVEBACK_MIN_SAMPLES", "30") or "30")
        except Exception:
            min_samples = 30
        use_regime_dim = _env_bool("TRAIL_GIVEBACK_USE_REGIME_DIM", True)
        try:
            ttl_sec = int(os.getenv("TRAIL_GIVEBACK_TTL_SEC", str(60 * 60 * 24 * 30)) or str(60 * 60 * 24 * 30))
        except Exception:
            ttl_sec = 60 * 60 * 24 * 30
        return cls(
            enabled=enabled
            alpha=float(alpha)
            min_samples_for_use=int(max(min_samples, 0))
            use_regime_dim=bool(use_regime_dim)
            ttl_sec=int(max(ttl_sec, 0))
        )

    def key(self, *, kind: str, symbol: str, tf: str, regime: str) -> str:
        rg = regime if self.use_regime_dim else "na"
        return f"{self.key_prefix}:{kind}:{symbol}:{tf}:{rg}"


def _decode_hgetall(h: Dict[Any, Any]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for k, v in (h or {}).items():
        try:
            if isinstance(k, (bytes, bytearray)):
                k = k.decode("utf-8", "ignore")
            if isinstance(v, (bytes, bytearray)):
                v = v.decode("utf-8", "ignore")
            out[str(k)] = str(v)
        except Exception:
            pass
    return out


def update_giveback_ema(
    redis_client: Any
    *
    cfg: GivebackEmaConfig
    kind: str
    symbol: str
    tf: str
    regime: str
    now_ms: int
    giveback_pnl: Optional[float]
    entry_price: Optional[float]
    qty: Optional[float]
    notional: Optional[float]
) -> None:
    """
    Fail-open writer called from StatsAggregator.update_stats() after applied==1.
    """
    if not cfg.enabled:
        return
    if redis_client is None:
        return
    kb = (kind or "").strip().lower()
    sym = (symbol or "").strip().upper()
    tfk = (tf or "").strip().lower() or "1m"
    rg = _canon_regime(regime)
    if not kb or not sym:
        return

    nt = _estimate_notional(entry_price, qty, notional)
    gb_bps = _pnl_to_bps(_safe_float(giveback_pnl), notional=nt)
    if gb_bps is None or gb_bps <= 0:
        return

    key = cfg.key(kind=kb, symbol=sym, tf=tfk, regime=rg)

    # Minimal dependency implementation (no Lua required).
    # Atomicity is "nice-to-have", but we prefer simplicity and fail-open semantics.
    try:
        h = _decode_hgetall(redis_client.hgetall(key) or {})
        try:
            old_ema = float(h.get("ema_giveback_bps", "0") or "0")
        except Exception:
            old_ema = 0.0
        try:
            samples = int(float(h.get("samples", "0") or "0"))
        except Exception:
            samples = 0

        new_ema = (cfg.alpha * float(gb_bps)) + ((1.0 - cfg.alpha) * float(old_ema)) if old_ema > 0 else float(gb_bps)
        samples_new = samples + 1

        pipe = redis_client.pipeline(transaction=False) if hasattr(redis_client, "pipeline") else None
        if pipe is None:
            # fallback without pipeline
            redis_client.hset(key, mapping={"samples": str(samples_new), "ema_giveback_bps": f"{new_ema:.8f}", "last_ts_ms": str(int(now_ms))})
            if cfg.ttl_sec > 0:
                try:
                    redis_client.expire(key, int(cfg.ttl_sec))
                except Exception:
                    pass
            return

        pipe.hset(key, mapping={"samples": str(samples_new), "ema_giveback_bps": f"{new_ema:.8f}", "last_ts_ms": str(int(now_ms))})
        if cfg.ttl_sec > 0:
            pipe.expire(key, int(cfg.ttl_sec))
        pipe.execute()
    except Exception:
        return


def read_giveback_ema(redis_client: Any, *, cfg: GivebackEmaConfig, kind: str, symbol: str, tf: str, regime: str) -> Optional[Dict[str, Any]]:
    """
    Reader used by conditional trailing evaluator.
    Returns dict: {"samples": int, "ema_giveback_bps": float} or None.
    """
    if redis_client is None:
        return None
    kb = (kind or "").strip().lower()
    sym = (symbol or "").strip().upper()
    tfk = (tf or "").strip().lower() or "1m"
    rg = _canon_regime(regime)
    if not kb or not sym:
        return None
    try:
        h = _decode_hgetall(redis_client.hgetall(cfg.key(kind=kb, symbol=sym, tf=tfk, regime=rg)) or {})
        if not h:
            return None
        samples = int(float(h.get("samples", "0") or "0"))
        ema = float(h.get("ema_giveback_bps", "0") or "0")
        if samples <= 0 or not math.isfinite(ema) or ema <= 0:
            return None
        return {"samples": samples, "ema_giveback_bps": float(ema)}
    except Exception:
        return None
