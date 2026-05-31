from __future__ import annotations

import math
import os
from dataclasses import dataclass
from typing import Any

from domain.time_utils import ctx_epoch_ms
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


@dataclass(frozen=True)
class SlippageModelConfig:
    """
    slippage_bps = max(default, half_spread, EMA(realized_slippage_bps))
    Key dims (requested): symbol×venue×session×tf×kind
    """
    enabled: bool = _env_bool("SLIPPAGE_MODEL_ENABLED", True)
    default_slippage_bps: float = float(os.getenv("SLIPPAGE_DEFAULT_BPS", "1.5"))
    half_spread_mult: float = float(os.getenv("SLIPPAGE_HALF_SPREAD_MULT", "0.5"))
    use_ema: bool = _env_bool("SLIPPAGE_USE_EMA", True)
    ema_min_samples: int = int(os.getenv("SLIPPAGE_EMA_MIN_SAMPLES", "30"))
    key_prefix: str = os.getenv("SLIPPAGE_EMA_KEY_PREFIX", "slipema")


def spread_bps_from_tick(tick: Any, mid: float | None = None) -> float | None:
    """
    Compute spread bps from a tick-like object.
    Supported fields (per your tick_parser):
      - tick.bid / tick.ask
      - tick.b / tick.a
    """
    try:
        bid = getattr(tick, "bid", None)
        ask = getattr(tick, "ask", None)
        if bid is None:
            bid = getattr(tick, "b", None)
        if ask is None:
            ask = getattr(tick, "a", None)
        b = float(bid)  # type: ignore
        a = float(ask)  # type: ignore
        if not (math.isfinite(b) and math.isfinite(a)):
            return None
        if b <= 0 or a <= 0 or a <= b:
            return None
        m = float(mid) if mid is not None else (a + b) / 2.0
        if m <= 0:
            return None
        return float((a - b) / m * 10_000.0)
    except Exception:
        return None


def _ema_key(
    cfg: SlippageModelConfig,
    *,
    symbol: str,
    venue: str,
    session: str,
    tf: str,
    kind: str,
) -> str:
    # Back-compat: if kind empty -> "na"
    k = (kind or "").strip().lower() or "na"
    return f"{cfg.key_prefix}:{symbol}:{venue}:{session}:{tf}:{k}"


def estimate_slippage_bps(
    *,
    cfg: SlippageModelConfig,
    ctx: Any,
    tick: Any,
    symbol: str,
    venue: str,
    tf: str,
    kind: str,
    redis_client: Any = None,
    mid: float | None = None,
) -> float:
    """
    Uses strict ts normalization:
      - ts_ms invalid -> session="na" -> EMA not used (fail-open)
      - ts in seconds -> normalized to ms by normalize_ts_ms()
    """
    spread_bps = spread_bps_from_tick(tick, mid=mid)
    half_spread = (cfg.half_spread_mult * float(spread_bps)) if spread_bps is not None else 0.0
    base = max(float(cfg.default_slippage_bps), float(half_spread))

    if not cfg.enabled or not cfg.use_ema or redis_client is None:
        return float(base)

    ts_ms = ctx_epoch_ms(ctx)
    if ts_ms <= 0:
        # Important safety: invalid ts => no EMA dimensioning
        return float(base)

    session = session_key_from_epoch_ms(ts_ms)
    if session == "na":
        return float(base)

    key = _ema_key(
        cfg,
        symbol=symbol,
        venue=(venue or "na"),
        session=str(session),
        tf=(tf or "na"),
        kind=str(kind),
    )

    try:
        # Expected format:
        #   HSET key samples <int> ema_bps <float> last_ts_ms <int>
        # Fail-open on any mismatch.
        samples_raw = redis_client.hget(key, "samples") if hasattr(redis_client, "hget") else None
        ema_raw = redis_client.hget(key, "ema_bps") if hasattr(redis_client, "hget") else None

        samples = int(float(samples_raw)) if samples_raw is not None else 0
        if samples < int(cfg.ema_min_samples):
            return float(base)

        ema_bps = _safe_float(ema_raw, 0.0)
        if ema_bps > 0:
            return float(max(base, ema_bps))
        return float(base)
    except Exception:
        return float(base)
