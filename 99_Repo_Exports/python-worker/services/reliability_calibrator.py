from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import os
import time
import math
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from domain.time_utils import normalize_ts_ms, session_from_ts_ms


# ---------------------------------------------------------------------------
# Reliability calibration (conf_pct -> realized hit-rate) per dims:
#   symbol × venue × session × tf × kind × regime
#
# Why:
#   - One calibrator per symbol is often too coarse: kinds behave differently,
#     regimes behave differently, and microstructure varies by venue/session.
#   - We store simple empirical reliability curves:
#       bucket(confidence_pct) -> hit_rate(outcome)
#   - This is WRITE-side only (fail-open). Read-side mapping can be added later
#     without changing the stored protocol.
#
# Outcomes supported (4):
#   1) tp1            : TP1 hit (entry quality / most stable)
#   2) tp2            : TP2 hit (default compromise)
#   3) win            : pnl_net > 0 (profitability target)
#   4) nosl_after_tp1 : TP1 hit AND close_reason != "SL"
#
# OPTIONAL strict outcome (enabled by listing it in REL_CAL_OUTCOMES):
#   5) nosl_after_tp1_t{T} :
#        TP1 hit AND not SL AND trade survived at least T ms after TP1.
#
# Examples:
#   REL_CAL_OUTCOMES=tp2,nosl_after_tp1,nosl_after_tp1_t500,nosl_after_tp1_t2000
#
# Note:
#   We do NOT have full path info "SL happened within first T" unless explicitly
#   recorded. This strict proxy uses tp1_hit_ts_ms + exit_ts_ms.
#
# Pipeline recommendation:
#   - Keep two curves:
#       entry curve  : tp2 (default)   -> best compromise between stability and profitability
#       mgmt curve   : nosl_after_tp1  -> directly reflects "giveback / hold quality"
#   - This file implements that by default via REL_CAL_OUTCOMES default = "tp2,nosl_after_tp1".
# ---------------------------------------------------------------------------


def _env_bool(name: str, default: bool) -> bool:
    v = (os.getenv(name, "1" if default else "0") or "").strip().lower()
    return v in {"1", "true", "yes", "on"}


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        f = float(x)
        return f if math.isfinite(f) else default
    except Exception:
        return default


def _canon(s: Any, default: str = "na") -> str:
    try:
        if s is None:
            return default
        out = str(s).strip().lower()
        return out if out else default
    except Exception:
        return default


def _canon_regime(v: Any) -> str:
    # Keep consistent with services.stats_aggregator.canon_regime, but avoid import cycles.
    if v is None:
        return "na"
    if isinstance(v, str):
        s = v.strip().lower()
        return s if s else "na"
    s = str(getattr(v, "name", None) or getattr(v, "value", None) or v).strip().lower()
    return s if s else "na"


def _parse_csv(s: str) -> List[str]:
    out: List[str] = []
    for part in (s or "").split(","):
        p = part.strip().lower()
        if p:
            out.append(p)
    return out


def _parse_nosl_t(outcome: str) -> Optional[int]:
    """
    Parse "nosl_after_tp1_t{T}" where T is milliseconds integer.
    Examples:
      - nosl_after_tp1_t500   -> 500
      - nosl_after_tp1_t2000  -> 2000
    """
    o = (outcome or "").strip().lower()
    pref = "nosl_after_tp1_t"
    if not o.startswith(pref):
        return None
    tail = o[len(pref):].strip()
    if not tail:
        return None
    if not tail.isdigit():
        return None
    try:
        t = int(tail)
        return t if t > 0 else None
    except Exception:
        return None


def _bucket_conf_pct(conf_pct: float, step: int) -> int:
    """
    Bucketize confidence in [0..100] into integer bucket start.
    Example step=5: 0,5,10,...,100
    """
    if step <= 0:
        step = 5
    c = _safe_float(conf_pct, 0.0)
    if c < 0:
        c = 0.0
    if c > 100:
        c = 100.0
    b = int((c // step) * step)
    if b > 100:
        b = 100
    if b < 0:
        b = 0
    return b


@dataclass(frozen=True)
class RelCalConfig:
    enabled: bool
    prefix: str
    outcomes: List[str]
    bucket_step_pct: int
    ttl_sec: int

    # Dim toggles (to reduce cardinality if needed)
    use_kind_dim: bool
    use_regime_dim: bool
    use_venue_dim: bool
    use_session_dim: bool
    use_tf_dim: bool

    @staticmethod
    def from_env() -> "RelCalConfig":
        enabled = _env_bool("REL_CAL_ENABLED", True)
        prefix = (os.getenv("REL_CAL_PREFIX", "relcal") or "relcal").strip()
        # Default implements the pipeline recommendation:
        #   - entry curve: tp2
        #   - mgmt curve : nosl_after_tp1
        outcomes = _parse_csv(os.getenv("REL_CAL_OUTCOMES", "tp2,nosl_after_tp1"))
        bucket_step_pct = int(float(os.getenv("REL_CAL_BUCKET_STEP_PCT", "5") or 5))
        ttl_sec = int(float(os.getenv("REL_CAL_TTL_SEC", str(60 * 60 * 24 * 30)) or 0))  # 30d

        use_kind_dim = _env_bool("REL_CAL_USE_KIND_DIM", True)
        use_regime_dim = _env_bool("REL_CAL_USE_REGIME_DIM", True)
        use_venue_dim = _env_bool("REL_CAL_USE_VENUE_DIM", True)
        use_session_dim = _env_bool("REL_CAL_USE_SESSION_DIM", True)
        use_tf_dim = _env_bool("REL_CAL_USE_TF_DIM", True)

        # Sanity: if outcomes is empty -> keep safe default.
        if not outcomes:
            outcomes = ["tp2", "nosl_after_tp1"]
        return RelCalConfig(
            enabled=enabled,
            prefix=prefix,
            outcomes=outcomes,
            bucket_step_pct=bucket_step_pct,
            ttl_sec=ttl_sec,
            use_kind_dim=use_kind_dim,
            use_regime_dim=use_regime_dim,
            use_venue_dim=use_venue_dim,
            use_session_dim=use_session_dim,
            use_tf_dim=use_tf_dim,
        )


def _extract_confidence_pct(pos: Dict[str, Any], closed: Dict[str, Any]) -> Optional[float]:
    """
    Confidence is typically in the original signal payload.
    Your emit protocol uses:
      payload["confidence"] = 0..100
    TradeMonitor keeps raw payload in pos["signal_payload"].
    """
    try:
        # Prefer signal payload first (closest to emitted protocol).
        sp = pos.get("signal_payload")
        if isinstance(sp, dict):
            if "confidence" in sp:
                c = _safe_float(sp.get("confidence"), float("nan"))
                if math.isfinite(c):
                    return c
            if "confidence_pct" in sp:
                c = _safe_float(sp.get("confidence_pct"), float("nan"))
                if math.isfinite(c):
                    return c
        # Fallback: sometimes confidence is copied into closed or pos root.
        for k in ("confidence", "confidence_pct"):
            if k in pos:
                c = _safe_float(pos.get(k), float("nan"))
                if math.isfinite(c):
                    return c
            if k in closed:
                c = _safe_float(closed.get(k), float("nan"))
                if math.isfinite(c):
                    return c
    except Exception:
        pass
    return None


def _extract_dims(pos: Dict[str, Any], closed: Dict[str, Any]) -> Tuple[str, str, str, str, str, str, int]:
    """
    Returns:
      kind, symbol, venue, session, tf, regime, entry_ts_ms_norm

    Notes:
      - TradeClosed does NOT have 'kind'; in your pipeline strategy==kind.
      - session is computed from entry_ts_ms (not exit) to make curves consistent.
      - venue may be absent in TradeClosed; we try payload/pos dynamic fields.
    """
    kind = _canon(closed.get("strategy") or pos.get("strategy") or "na")
    symbol = _canon(closed.get("symbol") or pos.get("symbol") or "na")
    tf = _canon(closed.get("tf") or pos.get("tf") or "na")

    # venue: prefer explicit closed field, else pos dynamic, else payload.
    venue = "na"
    try:
        if "venue" in closed:
            venue = _canon(closed.get("venue"), "na")
        elif "venue" in pos:
            venue = _canon(pos.get("venue"), "na")
        else:
            sp = pos.get("signal_payload")
            if isinstance(sp, dict):
                venue = _canon(sp.get("venue") or sp.get("exchange") or sp.get("venue_id"), "na")
    except Exception:
        venue = "na"

    # entry ts
    ts_raw = closed.get("entry_ts_ms") or pos.get("entry_ts_ms") or 0
    try:
        entry_ts = normalize_ts_ms(int(float(ts_raw)) if ts_raw else 0)
    except Exception:
        entry_ts = 0

    # session: fail-open if ts invalid
    if entry_ts <= 0 or entry_ts < 10**12:
        session = "na"
    else:
        try:
            session = _canon(pos.get("session") or closed.get("session") or session_from_ts_ms(int(entry_ts)), "na")
        except Exception:
            session = "na"

    # regime
    rg = closed.get("entry_regime") or closed.get("regime") or pos.get("entry_regime") or pos.get("regime")
    regime = _canon_regime(rg)

    return kind, symbol, venue, session, tf, regime, int(entry_ts)


def _compute_hit(outcome: str, pos: Dict[str, Any], closed: Dict[str, Any]) -> bool:
    """
    Compute boolean hit for a given outcome.
    Must be deterministic and stable under partial/missing fields (fail-open => False).
    """
    o = (outcome or "").strip().lower()
    try:
        if o == "tp1":
            return bool(closed.get("tp1_hit") or pos.get("tp1_hit"))
        if o == "tp2":
            return bool(closed.get("tp2_hit") or pos.get("tp2_hit"))
        if o == "win":
            return _safe_float(closed.get("pnl_net"), 0.0) > 0.0
        if o in {"nosl_after_tp1", "no_sl_after_tp1", "tp1_not_sl"}:
            tp1 = bool(closed.get("tp1_hit") or pos.get("tp1_hit"))
            if not tp1:
                return False
            # We treat "SL after TP1" as the final close bucket being SL.
            # This is a practical proxy; a stricter horizon-based definition can be added later.
            cr = str(closed.get("close_reason") or "").strip().upper()
            return cr != "SL"

        # Strict-by-horizon proxy:
        #   hit if:
        #     - TP1 was hit
        #     - final close is not SL
        #     - exit_ts_ms - tp1_hit_ts_ms >= T
        #
        # Rationale:
        #   Without an explicit "SL occurred within first T after TP1" marker,
        #   the best deterministic proxy is "trade survived >= T and not SL".
        #   This is conservative: short-lived trades (<T) are not counted as hit.
        t = _parse_nosl_t(o)
        if t is not None:
            tp1 = bool(closed.get("tp1_hit") or pos.get("tp1_hit"))
            if not tp1:
                return False
            cr = str(closed.get("close_reason") or "").strip().upper()
            if cr == "SL":
                return False
            tp1_ts = closed.get("tp1_hit_ts_ms") or pos.get("tp1_hit_ts_ms")
            exit_ts = closed.get("exit_ts_ms") or pos.get("exit_ts_ms")
            try:
                tp1_ts_i = int(float(tp1_ts)) if tp1_ts else 0
                exit_ts_i = int(float(exit_ts)) if exit_ts else 0
            except Exception:
                return False
            if tp1_ts_i <= 0 or exit_ts_i <= 0:
                return False
            return (exit_ts_i - tp1_ts_i) >= int(t)
    except Exception:
        return False
    return False


def _build_key(cfg: RelCalConfig, *, outcome: str, kind: str, symbol: str, venue: str, session: str, tf: str, regime: str) -> str:
    """
    Key format (explicit dims, stable, human-auditable):
      {prefix}:{outcome}:{kind}:{symbol}:{venue}:{session}:{tf}:{regime}
    Dim toggles allow reducing cardinality without changing code elsewhere.
    """
    parts = [cfg.prefix, _canon(outcome, "na")]
    parts.append(kind if cfg.use_kind_dim else "na")
    parts.append(symbol)
    parts.append(venue if cfg.use_venue_dim else "na")
    parts.append(session if cfg.use_session_dim else "na")
    parts.append(tf if cfg.use_tf_dim else "na")
    parts.append(regime if cfg.use_regime_dim else "na")
    return ":".join(parts)


def update_reliability_curves(
    redis_client: Any,
    *,
    cfg: Optional[RelCalConfig] = None,
    pos: Dict[str, Any],
    trade_closed: Dict[str, Any],
    now_ms: Optional[int] = None,
) -> None:
    """
    Fail-open writer:
      - if disabled / missing redis / missing confidence -> do nothing
      - never raises
    Redis structure per key:
      HASH fields:
        samples_total
        hits_total
        b{bucket}:n
        b{bucket}:h
        last_ts_ms
    """
    try:
        cfg2 = cfg or RelCalConfig.from_env()
        if not cfg2.enabled:
            return
        if redis_client is None:
            return
        conf = _extract_confidence_pct(pos, trade_closed)
        if conf is None or not math.isfinite(float(conf)):
            return
        kind, symbol, venue, session, tf, regime, _ = _extract_dims(pos, trade_closed)
        bucket = _bucket_conf_pct(float(conf), cfg2.bucket_step_pct)
        now = int(now_ms or get_ny_time_millis())

        pipe = redis_client.pipeline(transaction=False)
        for outcome in (cfg2.outcomes or []):
            hit = _compute_hit(outcome, pos, trade_closed)
            key = _build_key(
                cfg2,
                outcome=outcome,
                kind=kind,
                symbol=symbol,
                venue=venue,
                session=session,
                tf=tf,
                regime=regime,
            )
            # Global totals
            pipe.hincrby(key, "samples_total", 1)
            if hit:
                pipe.hincrby(key, "hits_total", 1)
            # Per-bucket counters
            bn = f"b{bucket}:n"
            bh = f"b{bucket}:h"
            pipe.hincrby(key, bn, 1)
            if hit:
                pipe.hincrby(key, bh, 1)
            # Audit timestamp (not used in math, but useful for maintenance)
            try:
                pipe.hset(key, "last_ts_ms", str(now))
            except Exception:
                pass
            # TTL to avoid unbounded growth (default 30d)
            if cfg2.ttl_sec and cfg2.ttl_sec > 0:
                try:
                    pipe.expire(key, int(cfg2.ttl_sec))
                except Exception:
                    pass
        try:
            pipe.execute()
        except Exception:
            # fail-open
            return
    except Exception:
        return