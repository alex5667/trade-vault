from __future__ import annotations

"""ExecutionHealthGate (P6).

Purpose
-------
Use *post-trade* execution quality rollups (TCA) to prevent or tighten trades
in regimes where paper-edge gets eaten by execution.

Inputs
------
Redis rollups produced by TCA worker (Phase B):
  - is_p95_bps
  - perm_impact_p95_bps (delta=1s/5s)
  - realized_spread_p50_bps (delta=1s/5s) for adverse selection

Actions
-------
- default/soft: annotate only
- strict: tighten (increase expected_slippage_bps and optionally EDGE_COST_K)
- hard: veto when (IS_p95 AND perm_impact_p95) exceed thresholds simultaneously

This module is fail-open: if Redis keys are missing, no veto.

Notes on dimensions
-------------------
The TCA rollup keys are dimensioned by:
  (sym, venue, session, tf, kind, side)
In practice we need robust fallbacks because upstream may not always provide
all dims consistently. Therefore we attempt keys in this order:
  1) exact
  2) tf=all
  3) kind=all
  4) session=all
  5) tf=all+kind=all+session=all

The last fallback is what EntryPolicy should rely on.
"""

import math
import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple


def _f(x: Any, d: float = 0.0) -> float:
    try:
        v = float(x)
    except Exception:
        return float(d)
    if not math.isfinite(v):
        return float(d)
    return float(v)


def _profile() -> str:
    return str(os.getenv("GATE_PROFILE", os.getenv("EXEC_HEALTH_PROFILE", "default")) or "default").strip().lower()


def _mode() -> str:
    # explicit override for this gate
    m = str(os.getenv("EXEC_HEALTH_MODE", "auto") or "auto").strip().lower()
    if m in ("monitor", "tighten", "veto"):
        return m
    p = _profile()
    if p == "hard":
        return "veto"
    if p == "strict":
        return "tighten"
    return "monitor"


@dataclass
class ExecHealthThresholds:
    max_is_p95_bps: float = 0.0
    max_perm_impact_p95_bps: float = 0.0
    min_realized_spread_p50_bps: float = -999.0
    tighten_add_mult: float = 1.0
    tighten_add_cap_bps: float = 8.0

    @staticmethod
    def from_env(prefix: str = "EXEC_") -> "ExecHealthThresholds":
        return ExecHealthThresholds(
            max_is_p95_bps=_f(os.getenv(f"{prefix}MAX_IS_P95_BPS", "0"), 0.0)
            max_perm_impact_p95_bps=_f(os.getenv(f"{prefix}MAX_PERM_IMPACT_P95_BPS", "0"), 0.0)
            min_realized_spread_p50_bps=_f(os.getenv(f"{prefix}MIN_REALIZED_SPREAD_P50_BPS", "-999"), -999.0)
            tighten_add_mult=_f(os.getenv(f"{prefix}TIGHTEN_ADD_MULT", "1.0"), 1.0)
            tighten_add_cap_bps=_f(os.getenv(f"{prefix}TIGHTEN_ADD_CAP_BPS", "8.0"), 8.0)
        )


@dataclass
class ExecHealthDecision:
    apply: bool
    veto: bool
    flags: List[str]
    reason_code: str = ""
    tighten_add_bps: float = 0.0


def build_rollup_keys(
    *
    metric: str
    sym: str
    venue: str
    session: str
    tf: str
    kind: str
    side: str
) -> List[str]:
    """Return fallback key list for one metric."""
    sym = (sym or "").upper()
    venue = (venue or "na").lower()
    session = (session or "na").lower()
    tf = (tf or "all").lower()
    kind = (kind or "all").lower()
    side = (side or "na").upper()

    def k(_sess: str, _tf: str, _kind: str) -> str:
        return f"tca:{metric}:{sym}:{venue}:{_sess}:{_tf}:{_kind}:{side}"

    keys = [
        k(session, tf, kind)
        k(session, "all", kind)
        k(session, tf, "all")
        k("all", tf, kind)
        k("all", "all", "all")
    ]
    # Deduplicate while preserving order
    out = []
    seen = set()
    for x in keys:
        if x not in seen:
            seen.add(x)
            out.append(x)
    return out


async def redis_mget_first(redis, keys: Sequence[str]) -> Optional[float]:
    """Return first found numeric value across fallback keys."""
    try:
        vals = await redis.mget(list(keys))
    except Exception:
        # fallback to sequential GET
        for k in keys:
            try:
                v = await redis.get(k)
            except Exception:
                v = None
            fv = _f(v, float("nan"))
            if math.isfinite(fv):
                return float(fv)
        return None

    for v in vals or []:
        fv = _f(v, float("nan"))
        if math.isfinite(fv):
            return float(fv)
    return None


async def read_exec_rollups(
    *
    redis
    sym: str
    venue: str
    session: str
    tf: str
    kind: str
    side: str
    delta_sec: int = 1
) -> Dict[str, float]:
    """Read the minimal set of rollups needed for P6.

    Returns empty dict if data is missing.
    """
    # NOTE: metric naming is fixed in Phase B rollups.
    # We keep it strict and provide fallback keys.
    out: Dict[str, float] = {}
    try:
        is_k = build_rollup_keys(metric="is_p95_bps", sym=sym, venue=venue, session=session, tf=tf, kind=kind, side=side)
        pi_k = build_rollup_keys(metric=f"perm_impact_p95_bps:{int(delta_sec)}", sym=sym, venue=venue, session=session, tf=tf, kind=kind, side=side)
        rs_k = build_rollup_keys(metric=f"realized_spread_p50_bps:{int(delta_sec)}", sym=sym, venue=venue, session=session, tf=tf, kind=kind, side=side)

        v_is = await redis_mget_first(redis, is_k)
        v_pi = await redis_mget_first(redis, pi_k)
        v_rs = await redis_mget_first(redis, rs_k)

        if v_is is not None:
            out["is_p95_bps"] = float(v_is)
        if v_pi is not None:
            out[f"perm_impact_p95_bps_{int(delta_sec)}"] = float(v_pi)
        if v_rs is not None:
            out[f"realized_spread_p50_bps_{int(delta_sec)}"] = float(v_rs)
    except Exception:
        return {}

    return out


def decide_execution_health(
    *
    rollups: Dict[str, float]
    thr: ExecHealthThresholds
) -> ExecHealthDecision:
    """Pure policy decision for execution-health gate."""
    flags: List[str] = []

    v_is = _f(rollups.get("is_p95_bps"), float("nan"))
    v_pi = _f(rollups.get("perm_impact_p95_bps_1"), float("nan"))
    v_rs = _f(rollups.get("realized_spread_p50_bps_1"), float("nan"))

    if math.isfinite(v_is) and thr.max_is_p95_bps > 0 and v_is >= thr.max_is_p95_bps:
        flags.append("is_p95_high")
    if math.isfinite(v_pi) and thr.max_perm_impact_p95_bps > 0 and v_pi >= thr.max_perm_impact_p95_bps:
        flags.append("perm_impact_p95_high")
    if math.isfinite(v_rs) and thr.min_realized_spread_p50_bps > -900 and v_rs <= thr.min_realized_spread_p50_bps:
        flags.append("adverse_realized")

    mode = _mode()
    if not flags:
        return ExecHealthDecision(apply=False, veto=False, flags=[])

    if mode == "monitor":
        return ExecHealthDecision(apply=True, veto=False, flags=flags, reason_code="EXEC_HEALTH_MONITOR")

    # tighten: add slippage proportional to severity (bounded)
    tighten_add = 0.0
    if mode in ("tighten", "veto"):
        # simple heuristic: max of offending metrics / thresholds
        sev = 0.0
        if "is_p95_high" in flags and thr.max_is_p95_bps > 0 and math.isfinite(v_is):
            sev = max(sev, v_is / thr.max_is_p95_bps)
        if "perm_impact_p95_high" in flags and thr.max_perm_impact_p95_bps > 0 and math.isfinite(v_pi):
            sev = max(sev, v_pi / thr.max_perm_impact_p95_bps)
        if "adverse_realized" in flags and thr.min_realized_spread_p50_bps > -900 and math.isfinite(v_rs):
            # adverse selection: treat as severity 1.0
            sev = max(sev, 1.0)

        tighten_add = min(float(thr.tighten_add_cap_bps), float(thr.tighten_add_mult) * max(0.0, sev - 1.0) * 5.0)

    if mode == "tighten":
        return ExecHealthDecision(apply=True, veto=False, flags=flags, reason_code="EXEC_HEALTH_TIGHTEN", tighten_add_bps=float(tighten_add))

    # Veto policy (hard): require both IS_p95 and perm_impact_p95 to be bad.
    do_veto = ("is_p95_high" in flags) and ("perm_impact_p95_high" in flags)
    if do_veto:
        return ExecHealthDecision(
            apply=True
            veto=True
            flags=flags
            reason_code="VETO_IMPL_SHORTFALL_P95" if "is_p95_high" in flags else "VETO_EXEC_HEALTH"
            tighten_add_bps=float(tighten_add)
        )

    # Otherwise still tighten (but do not veto)
    return ExecHealthDecision(apply=True, veto=False, flags=flags, reason_code="EXEC_HEALTH_TIGHTEN", tighten_add_bps=float(tighten_add))


def apply_exec_health_to_indicators(*, indicators: Dict[str, Any], dec: ExecHealthDecision) -> None:
    """Mutate indicators (fail-open)."""
    try:
        indicators["exec_health_apply"] = int(1 if dec.apply else 0)
        indicators["exec_health_veto"] = int(1 if dec.veto else 0)
        indicators["exec_health_flags"] = ",".join(dec.flags)
        indicators["exec_health_reason"] = str(dec.reason_code or "")
        indicators["exec_health_tighten_add_bps"] = float(dec.tighten_add_bps or 0.0)

        if dec.tighten_add_bps and float(dec.tighten_add_bps) > 0:
            cur = _f(indicators.get("expected_slippage_bps"), 0.0)
            indicators["expected_slippage_bps"] = float(cur + float(dec.tighten_add_bps))
    except Exception:
        pass
