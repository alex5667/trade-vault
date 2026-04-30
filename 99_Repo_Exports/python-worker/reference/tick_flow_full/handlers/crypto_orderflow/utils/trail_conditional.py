from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Optional, Dict, Tuple


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


def _pick_attr(obj: Any, *names: str) -> Any:
    """
    Best-effort getter for evolving ctx schemas.
    Returns first found attribute or None.
    """
    for n in names:
        try:
            if hasattr(obj, n):
                return getattr(obj, n)
        except Exception:
            continue
    return None


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


def _sym_base(symbol: str) -> str:
    s = (symbol or "").strip().upper()
    if s.endswith("USDT") and len(s) > 4:
        return s[:-4]
    return s or "NA"


def _kind_key(kind: str) -> str:
    return (kind or "na").strip().upper()


def _cfg_pick(symbol: str, kind: str, key: str, default: Any) -> Any:
    """
    Resolution order (most specific first):
      1) <SYM>_<KIND>_<KEY>
      2) <SYM>_<KEY>
      3) <KIND>_<KEY>
      4) <KEY>
      5) default
    """
    sym = _sym_base(symbol)
    kd = _kind_key(kind)
    for nm in (f"{sym}_{kd}_{key}", f"{sym}_{key}", f"{kd}_{key}", key):
        v = os.getenv(nm)
        if v is None or str(v).strip() == "":
            continue
        return v
    return default


@dataclass(frozen=True)
class TrailDecision:
    enabled: bool
    reason: str


@dataclass(frozen=True)
class TrailConditionalEvaluator:
    """
    Decide whether trailing should START after TP1 for a specific signal.

    Main idea:
      - If momentum is still strong -> allow trailing.
      - Else allow trailing only if giveback-risk EMA is high (historically large giveback).
      - Otherwise veto trailing -> finish with fixed TP2/TP3 behavior.

    Fail-open:
      - If evaluator disabled or any read fails -> enabled=True.
    """
    redis: Any = None
    tf_default: str = "1m"

    @classmethod
    def from_env(cls, *, redis: Any = None) -> "TrailConditionalEvaluator":
        return cls(redis=redis, tf_default=str(os.getenv("TRAIL_TF_DEFAULT", "1m") or "1m"))

    def _trailstats_key(self, *, kind: str, symbol: str, tf: str, regime: str) -> str:
        # writer is in services/trail_giveback_stats.py
        rg = _canon_regime(regime)
        use_rg = _env_bool("TRAIL_STATS_USE_REGIME_DIM", True)
        if not use_rg:
            rg = "na"
        return f"trailstats:{kind}:{symbol}:{tf}:{rg}"

    def _read_ema_giveback_r(self, *, kind: str, symbol: str, tf: str, regime: str) -> Optional[float]:
        r = self.redis
        if r is None:
            return None
        key = self._trailstats_key(kind=kind, symbol=symbol, tf=tf, regime=regime)
        try:
            v = r.hget(key, "ema_giveback_r")
            if isinstance(v, (bytes, bytearray)):
                v = v.decode("utf-8", "ignore")
            f = float(v)
            return f if f >= 0 else None
        except Exception:
            return None

    def evaluate(
        self
        ctx: Any
        *
        side: str
        symbol: str
        kind: str
        tf: str
        regime: str
    ) -> TrailDecision:
        # Global hard switch: preserve legacy behavior when disabled.
        if not _env_bool("TRAIL_COND_EVAL_ENABLED", True):
            return TrailDecision(True, "EVAL_DISABLED")

        try:
            # ---------------------------------------------------------------
            # 1) Momentum features (best-effort reads, because ctx schema evolves).
            # ---------------------------------------------------------------
            z = _safe_float(_pick_attr(ctx, "z", "z_delta", "delta_z", "raw_score"), 0.0)

            # OBI may have many names across versions:
            obi = _safe_float(_pick_attr(ctx, "obi_avg", "obi_20", "obi", "obi_score"), 0.0)
            obi_sust = bool(_pick_attr(ctx, "obi_sustained", "obi_is_sustained", "obi_stable") or False)

            # Normalize sign expectation: LONG wants positive momentum, SHORT wants negative.
            side_u = (side or "LONG").strip().upper()
            sign = 1.0 if side_u in {"LONG", "BUY"} else -1.0
            z_signed = z * sign
            obi_signed = obi * sign

            z_min = float(_cfg_pick(symbol, kind, "TRAIL_Z_MIN", "1.2"))
            obi_min = float(_cfg_pick(symbol, kind, "TRAIL_OBI_MIN", "0.0"))
            require_sust = _cfg_pick(symbol, kind, "TRAIL_REQUIRE_OBI_SUST", "0")
            require_sust_b = str(require_sust).strip().lower() in {"1", "true", "yes", "on"}

            momentum_ok = (z_signed >= z_min) and (obi_signed >= obi_min)
            if require_sust_b:
                momentum_ok = momentum_ok and bool(obi_sust)

            if momentum_ok:
                rs = []
                rs.append(f"mom z={z_signed:.2f}>={z_min}")
                if obi_min > 0:
                    rs.append(f"obi={obi_signed:.2f}>={obi_min}")
                if require_sust_b:
                    rs.append("sust=1")
                return TrailDecision(True, "MOMENTUM_OK " + " ".join(rs))

            # ---------------------------------------------------------------
            # 2) Giveback-risk fallback: allow trailing if historical giveback is high.
            # ---------------------------------------------------------------
            use_stats = _env_bool("TRAIL_USE_GIVEBACK_STATS", True)
            if use_stats:
                ema_gb = self._read_ema_giveback_r(kind=kind, symbol=symbol, tf=tf or self.tf_default, regime=regime)
                if ema_gb is not None:
                    gb_min = float(_cfg_pick(symbol, kind, "TRAIL_GIVEBACK_R_MIN", "0.35"))
                    if ema_gb >= gb_min:
                        return TrailDecision(True, f"GIVEBACK_OK ema_giveback_r={ema_gb:.3f}>={gb_min:.3f}")
                    return TrailDecision(False, f"VETO ema_giveback_r={ema_gb:.3f}<{gb_min:.3f}")

            # If we have no stats -> conservative choice for quality: veto trailing.
            veto_default = _env_bool("TRAIL_VETO_IF_NO_STATS", True)
            if veto_default:
                return TrailDecision(False, "VETO_NO_STATS")
            return TrailDecision(True, "ALLOW_NO_STATS")

        except Exception:
            # Fail-open: never block trades due to evaluator errors.
            return TrailDecision(True, "EVAL_ERROR_FAIL_OPEN")


def apply_trailing_policy_to_payload(
    *
    payload: Dict[str, Any]
    ctx: Any
    evaluator: Any
    side: str
    symbol: str
    kind: str
    tf: str
    regime: str
    reason_max_len: int = 256
) -> Tuple[bool, str]:
    """
    Single source of truth for propagating conditional trailing decision.

    Writes:
      payload["trail_after_tp1"]: bool
      payload["trail_after_tp1_reason"]: str
      ctx.trail_after_tp1 / ctx.trail_after_tp1_reason (dynamic attrs, best-effort)

    Fail-open:
      - If evaluator is missing or throws -> default True.
    """
    enabled = True
    reason = "NO_EVAL"
    try:
        if evaluator is not None:
            dec = evaluator.evaluate(ctx, side=side, symbol=symbol, kind=kind, tf=tf, regime=regime)
            enabled = bool(getattr(dec, "enabled", True))
            reason = str(getattr(dec, "reason", ""))
        else:
            enabled = True
            reason = "NO_EVAL"
    except Exception:
        enabled = True
        reason = "EVAL_ERROR"

    try:
        payload["trail_after_tp1"] = bool(enabled)
        payload["trail_after_tp1_reason"] = (reason or "")[:reason_max_len]
    except Exception:
        # last resort: keep protocol sane
        payload["trail_after_tp1"] = True
        payload["trail_after_tp1_reason"] = "PAYLOAD_SET_ERROR"

    # ctx mirrors payload (useful for logs/telegram note/debug)
    try:
        setattr(ctx, "trail_after_tp1", bool(payload.get("trail_after_tp1", True)))
        setattr(ctx, "trail_after_tp1_reason", str(payload.get("trail_after_tp1_reason", ""))[:reason_max_len])
    except Exception:
        pass

    return bool(payload.get("trail_after_tp1", True)), str(payload.get("trail_after_tp1_reason", ""))
