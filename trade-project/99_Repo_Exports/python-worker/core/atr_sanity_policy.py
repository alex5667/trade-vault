from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any


@dataclass
class AtrSanityDecision:
    ok: bool
    atr: float
    reason: str
    used_fallback: int = 0

def _tf_to_ms(tf: str) -> int:
    t = (tf or "").strip().lower()
    m = {
        "1m": 60_000, "m1": 60_000,
        "5m": 300_000, "m5": 300_000,
        "15m": 900_000, "m15": 900_000,
        "30m": 1_800_000, "m30": 1_800_000,
        "1h": 3_600_000, "h1": 3_600_000,
        "4h": 14_400_000, "h4": 14_400_000,
        "1d": 86_400_000, "d1": 86_400_000,
    }
    return m.get(t, 300_000)  # default 5m

def sanitize_atr(
    *,
    atr: float,
    entry: float,
    atr_meta: dict[str, Any],
    atr_tf: str,
    runtime_last_atr: float,
    runtime_last_atr_ts_ms: int,
    now_ms: int,
    cfg: dict[str, Any],
) -> tuple[AtrSanityDecision, dict[str, Any]]:
    """
    Sanity rules:
      - reject non-finite/<=0 ATR
      - reject stale ATR (age_ms > max_age_ms)
      - reject absurd ATR_bps (<min_bps_abs or >max_bps_abs) as data issue
    Fallback order:
      1) runtime.last_atr if fresh enough
      2) pct fallback: entry * ATR_SANITY_FALLBACK_PCT (default 0.0003)
    """
    out_ind: dict[str, Any] = {}
    eps = 1e-12
    entry = float(entry or 0.0)
    a = float(atr or 0.0)

    tf_ms = _tf_to_ms((atr_tf or "1m"))
    max_age_mult = float(cfg.get("atr_sanity_max_age_mult", 3.0) or 3.0)
    max_age_ms = int(cfg.get("atr_sanity_max_age_ms", int(max_age_mult * tf_ms)) or int(max_age_mult * tf_ms))

    min_bps_abs = float(cfg.get("atr_sanity_min_bps_abs", 0.5) or 0.5)
    max_bps_abs = float(cfg.get("atr_sanity_max_bps_abs", 500.0) or 500.0)
    fallback_pct = float(cfg.get("atr_sanity_fallback_pct", 0.0003) or 0.0003)

    age_ms = int(atr_meta.get("age_ms", 0) or 0) if isinstance(atr_meta, dict) else 0
    stale = int(age_ms > max_age_ms) if age_ms > 0 else 0

    def _bps(x: float) -> float:
        return (10000.0 * x / max(eps, entry)) if entry > 0 else 0.0

    ok = 1
    reason = "OK"
    if not math.isfinite(a) or a <= 0:
        ok = 0
        reason = "ATR_INVALID"
    elif stale == 1:
        ok = 0
        reason = "ATR_STALE"
    else:
        bps = _bps(a)
        if bps > 0 and (bps < min_bps_abs or bps > max_bps_abs):
            ok = 0
            reason = "ATR_BPS_ABSURD"

    out_ind["atr_sanity_tf_ms"] = tf_ms
    out_ind["atr_sanity_max_age_ms"] = max_age_ms
    out_ind["atr_sanity_age_ms"] = age_ms
    out_ind["atr_sanity_stale"] = stale

    if ok == 1:
        out_ind["atr_sanity_used_fallback"] = 0
        return AtrSanityDecision(ok=True, atr=a, reason="OK", used_fallback=0), out_ind

    # fallback #1: runtime last_atr (if not too old vs now_ms)
    la = float(runtime_last_atr or 0.0)
    lts = int(runtime_last_atr_ts_ms or 0)
    if la > 0 and lts > 0 and (now_ms - lts) <= max_age_ms:
        out_ind["atr_sanity_used_fallback"] = 1
        out_ind["atr_sanity_fallback_src"] = "runtime_last_atr"
        return AtrSanityDecision(ok=True, atr=la, reason=reason, used_fallback=1), out_ind

    # fallback #2: pct
    if entry > 0:
        fb = entry * fallback_pct
        out_ind["atr_sanity_used_fallback"] = 1
        out_ind["atr_sanity_fallback_src"] = "pct"
        return AtrSanityDecision(ok=True, atr=float(fb), reason=reason, used_fallback=1), out_ind

    out_ind["atr_sanity_used_fallback"] = 1
    out_ind["atr_sanity_fallback_src"] = "none"
    return AtrSanityDecision(ok=False, atr=0.0, reason=reason, used_fallback=1), out_ind
