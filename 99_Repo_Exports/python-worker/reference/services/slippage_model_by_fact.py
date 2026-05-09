from __future__ import annotations

import math
import os
from dataclasses import dataclass
from typing import Any

from domain.time_utils import normalize_ts_ms
from services.session_service import session_key_from_epoch_ms


def _env_bool(name: str, default: bool) -> bool:
    v = (os.getenv(name, "1" if default else "0") or "").strip().lower()
    return v in {"1", "true", "yes", "on"}


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        f = float(v)
        return f if math.isfinite(f) else default
    except Exception:
        return default


def _extract_spread_bps_from_ctx(ctx: Any) -> float:
    """
    Best-effort spread extraction in bps.
    Supports:
      - ctx.spread_bps (already computed upstream)
      - ctx.ask/ctx.bid and common aliases (best_ask/best_bid, l1_ask/l1_bid, a/b)
    Fail-open: returns 0.0 if not available.
    """
    if ctx is None:
        return 0.0
    try:
        v = getattr(ctx, "spread_bps", None)
        if v is not None:
            x = float(v)
            if math.isfinite(x) and x > 0:
                return float(x)
    except Exception:
        pass

    def _get(*names: str) -> float | None:
        for n in names:
            try:
                vv = getattr(ctx, n, None)
                if vv is None:
                    continue
                f = float(vv)
                if math.isfinite(f) and f > 0:
                    return f
            except Exception:
                continue
        return None

    ask = _get("ask", "best_ask", "l1_ask", "a")
    bid = _get("bid", "best_bid", "l1_bid", "b")
    if ask is None or bid is None:
        return 0.0
    if ask <= bid or ask <= 0 or bid <= 0:
        return 0.0
    mid = (ask + bid) / 2.0
    if mid <= 0:
        return 0.0
    return float((ask - bid) / mid * 10_000.0)


@dataclass(frozen=True)
class SlippageModelCfg:
    """
    Gate-side model:
      slippage_bps = max(default, half_spread, EMA(realized_slippage_bps@dims))

    Dims:
      - legacy: symbol×venue×session
      - optional extension (no protocol break): ×tf×kind, enabled by env
    """
    ema_enabled: bool = _env_bool("SLIPPAGE_EMA_ENABLED", True)
    ema_min_samples: int = int(os.getenv("SLIPPAGE_EMA_MIN_SAMPLES", "30"))
    ema_key_prefix: str = os.getenv("SLIPPAGE_EMA_KEY_PREFIX", "slipema")
    # backward compatible extension
    ema_dim_tf_kind: bool = _env_bool("SLIPPAGE_EMA_DIM_TF_KIND", False)


def _ema_key_legacy(cfg: SlippageModelCfg, *, symbol: str, venue: str, session: str) -> str:
    return f"{cfg.ema_key_prefix}:{symbol}:{venue}:{session}"


def _ema_key_tf_kind(
    cfg: SlippageModelCfg, *, symbol: str, venue: str, session: str, tf: str, kind: str
) -> str:
    k = (kind or "").strip().lower() or "na"
    t = (tf or "").strip().lower() or "na"
    return f"{cfg.ema_key_prefix}:{symbol}:{venue}:{session}:{t}:{k}"


def _read_ema(redis_client: Any, key: str, *, min_samples: int) -> tuple[int, float]:
    """
    Read EMA from Redis (fail-open).
    Supports multiple field spellings to avoid silent breakages:
      samples: "samples" or "n"
      ema:     "ema_bps" or "ema"
    Returns (samples, ema_bps) or (0,0.0).
    """
    try:
        if redis_client is None:
            return 0, 0.0
        # Prefer HGET (hash protocol). If your impl uses GET, you can extend here.
        hget = getattr(redis_client, "hget", None)
        if hget is None:
            return 0, 0.0

        s_raw = hget(key, "samples")
        if s_raw is None:
            s_raw = hget(key, "n")
        samples = int(float(s_raw)) if s_raw is not None else 0
        if samples < int(min_samples):
            return samples, 0.0

        e_raw = hget(key, "ema_bps")
        if e_raw is None:
            e_raw = hget(key, "ema")
        ema_bps = _safe_float(e_raw, 0.0)
        if ema_bps <= 0:
            return samples, 0.0
        return samples, float(ema_bps)
    except Exception:
        return 0, 0.0


def estimate_slippage_bps_ctx(
    ctx: Any,
    *,
    redis_client: Any,
    symbol: str,
    venue: str,
    ts_ms: Any,
    tf: str = "na",
    kind: str = "na",
    default_bps: float,
    use_spread_half: bool,
) -> float:
    """
    HARDENED estimator (single entrypoint for gates):

    Required hard rules:
      - use normalize_ts_ms() always (no local heuristics)
      - if normalized ts <= 0 => session="na" and DO NOT read EMA
      - if ts was in seconds (<1e12) => normalized to ms (and EMA is allowed)

    Fail-open:
      - no redis / parse errors => fallback to max(default, spread/2)
    """
    cfg = SlippageModelCfg()

    spread_bps = _extract_spread_bps_from_ctx(ctx)
    half_spread = (0.5 * float(spread_bps)) if (use_spread_half and spread_bps > 0) else 0.0
    base = float(max(float(default_bps), float(half_spread)))

    if not cfg.ema_enabled or redis_client is None:
        return base

    t = normalize_ts_ms(ts_ms)
    if t <= 0:
        # hard rule: invalid timestamp -> no EMA dimensioning
        return base

    session = session_key_from_epoch_ms(t)
    if session == "na":
        return base

    sym = (symbol or "").strip() or str(getattr(ctx, "symbol", "") or "")
    ven = (venue or "").strip() or "na"

    # First try extended dims if enabled; otherwise try legacy key directly.
    ema_bps = 0.0
    if cfg.ema_dim_tf_kind:
        key2 = _ema_key_tf_kind(cfg, symbol=sym, venue=ven, session=session, tf=tf, kind=kind)
        _, ema2 = _read_ema(redis_client, key2, min_samples=cfg.ema_min_samples)
        ema_bps = ema2
        # Backward compat fallback: if no EMA yet, try legacy.
        if ema_bps <= 0:
            key1 = _ema_key_legacy(cfg, symbol=sym, venue=ven, session=session)
            _, ema1 = _read_ema(redis_client, key1, min_samples=cfg.ema_min_samples)
            ema_bps = ema1
    else:
        key1 = _ema_key_legacy(cfg, symbol=sym, venue=ven, session=session)
        _, ema1 = _read_ema(redis_client, key1, min_samples=cfg.ema_min_samples)
        ema_bps = ema1

    if ema_bps > 0:
        return float(max(base, ema_bps))
    return base
