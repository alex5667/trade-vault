from __future__ import annotations

import math
import os
from dataclasses import dataclass
from typing import Any

# Shared timestamp/session utils (hardening)
try:
    from domain.time_utils import normalize_ts_ms, session_from_ts_ms
except Exception:
    normalize_ts_ms = None  # type: ignore
    session_from_ts_ms = None  # type: ignore


def _env_bool(name: str, default: bool) -> bool:
    v = (os.getenv(name, "1" if default else "0") or "").strip().lower()
    return v in {"1", "true", "yes", "on"}


@dataclass(frozen=True)
class SlippageEmaConfig:
    """
    Execution slippage EMA writer.

    This is the upstream source of truth for EdgeCostGate's "slippage by fact" model.
    Keys (v2):
      slipema:{symbol}:{venue}:{session}:{tf}:{kind}
    Hash fields:
      samples, ema_slip_bps, ema_spread_bps, last_ts_ms
    """

    enabled: bool
    alpha: float
    min_samples_to_trust: int
    ttl_s: int
    prefix: str = "slipema:"

    @staticmethod
    def from_env() -> SlippageEmaConfig:
        return SlippageEmaConfig(
            enabled=_env_bool("EXEC_SLIPPAGE_EMA_ENABLED", True),
            alpha=float(os.getenv("EXEC_SLIPPAGE_EMA_ALPHA", "0.05")),
            min_samples_to_trust=int(os.getenv("EXEC_SLIPPAGE_EMA_MIN_SAMPLES", "20")),
            ttl_s=int(os.getenv("EXEC_SLIPPAGE_EMA_TTL_S", str(3600 * 24 * 30))),
            prefix=os.getenv("EXEC_SLIPPAGE_EMA_PREFIX", "slipema:"),
        )


def _canon_tf(tf: str) -> str:
    s = (tf or "").strip().lower()
    if not s:
        return "na"
    # keep simple normalization (TradeMonitor already canon_tf)
    return s


def _canon_dim(x: Any, default: str = "na") -> str:
    s = (x or "").strip()
    return s if s else default


def build_key(*, cfg: SlippageEmaConfig, symbol: str, venue: str, session: str, tf: str, kind: str) -> str:
    return f"{cfg.prefix}{symbol}:{venue}:{session}:{tf}:{kind}"


def update_slippage_ema(
    redis_client: Any,
    *,
    cfg: SlippageEmaConfig,
    symbol: str,
    venue: str,
    tf: str,
    kind: str,
    ts_ms: Any,
    realized_slippage_bps: float,
    realized_spread_bps: float = 0.0,
) -> None:
    """
    Best-effort EMA update (fail-open).

    Inputs:
      realized_slippage_bps: |fill_price - mid| / mid * 10000
      realized_spread_bps: (ask-bid)/mid * 10000 (optional, may be 0)

    Timestamp hardening:
      - normalize_ts_ms(ts) is used for session extraction
      - invalid ts => skip update (avoid polluting EMA with 'na' session)
    """
    if not cfg.enabled:
        return
    if redis_client is None:
        return

    try:
        slip = float(realized_slippage_bps or 0.0)
        spr = float(realized_spread_bps or 0.0)
        if slip <= 0 or not math.isfinite(slip):
            return
        if spr < 0 or not math.isfinite(spr):
            spr = 0.0

        # Normalize ts -> epoch ms
        tsm = 0
        if normalize_ts_ms is not None:
            tsm = int(normalize_ts_ms(ts_ms))
        else:
            tsm = int(float(ts_ms or 0))
        if tsm <= 0:
            return

        sess = "na"
        if session_from_ts_ms is not None:
            sess = str(session_from_ts_ms(tsm))

        sym = _canon_dim(symbol)
        ven = _canon_dim(venue)
        tfv = _canon_tf(tf)
        knd = _canon_dim(kind).lower()

        key = build_key(cfg=cfg, symbol=sym, venue=ven, session=sess, tf=tfv, kind=knd)

        # Read current state (hash)
        h = redis_client.hgetall(key) or {}
        # redis may return bytes
        def _s(x: Any) -> str:
            if isinstance(x, bytes):
                return x.decode("utf-8", errors="ignore")
            return str(x)
        hh: dict[str, str] = { _s(k): _s(v) for k, v in dict(h).items() } if isinstance(h, dict) else {}

        n0 = int(float(hh.get("samples") or 0))
        ema0 = float(hh.get("ema_slip_bps") or 0.0)
        ema_sp0 = float(hh.get("ema_spread_bps") or 0.0)

        a = float(cfg.alpha)
        if a <= 0 or a > 1 or not math.isfinite(a):
            a = 0.05

        if n0 <= 0 or ema0 <= 0 or not math.isfinite(ema0):
            ema1 = slip
        else:
            ema1 = (1.0 - a) * ema0 + a * slip

        if spr > 0:
            if n0 <= 0 or ema_sp0 <= 0 or not math.isfinite(ema_sp0):
                ema_sp1 = spr
            else:
                ema_sp1 = (1.0 - a) * ema_sp0 + a * spr
        else:
            ema_sp1 = ema_sp0

        # Write back
        pipe = None
        try:
            pipe = redis_client.pipeline()
        except Exception:
            pipe = None

        if pipe is not None:
            pipe.hset(key, mapping={
                "samples": str(n0 + 1),
                "ema_slip_bps": str(float(ema1)),
                "ema_spread_bps": str(float(ema_sp1)),
                "last_ts_ms": str(int(tsm)),
            })
            pipe.expire(key, int(cfg.ttl_s))
            pipe.execute()
        else:
            redis_client.hset(key, mapping={
                "samples": str(n0 + 1),
                "ema_slip_bps": str(float(ema1)),
                "ema_spread_bps": str(float(ema_sp1)),
                "last_ts_ms": str(int(tsm)),
            })
            redis_client.expire(key, int(cfg.ttl_s))
    except Exception:
        return
