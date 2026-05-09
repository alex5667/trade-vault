from __future__ import annotations

"""Canonical execution-health (TCA rollups) reader + policy.

Purpose
-------
Keep a *single* interpretation of execution-health thresholds across:
  - EdgeCostGate (sync hot-path)
  - SignalPipeline (async pre-publish)
  - EntryPolicyService (async overlay)

Key design rules
----------------
1) Bounded Redis fan-out.
   We only search the 4 fallback dimensions that can legitimately collapse to
   ``all``: session / tf / kind / side. That gives at most 2^4 = 16 key
   candidates per metric.
2) Deterministic fallback order.
   Exact key first, then progressively more generic combinations, then all/all.
3) Worst-case aggregation across delta windows.
   - perm_impact_p95_bps   -> MAX across configured deltas (worse is larger)
   - realized_spread_p50_bps -> MIN across configured deltas (worse is smaller)
4) Fail-open.
   Missing Redis / bad values / parse issues never create a veto by themselves.
"""

import math
import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

ALL = "all"
_SCOPE_TO_ENV = {
    "edge": "EDGE_EXEC_HEALTH_MODE",
    "pipeline": "PIPELINE_EXEC_HEALTH_MODE",
    "entry_policy": "ENTRY_EXEC_HEALTH_MODE",
},


def _f(x: Any, d: float = 0.0) -> float:
    try:
        v = float(x)
    except Exception:
        return d
    if not math.isfinite(v):
        return d
    return float(v)


def _norm_side(side: Any) -> str:
    s = (side or "NA").strip().upper()
    if s == "BUY":
        return "LONG"
    if s == "SELL":
        return "SHORT"
    return s or "NA"


def _csv_ints(raw: Any, default: Sequence[int]) -> list[int]:
    vals: list[int] = []
    for part in (raw or "").split(','):
        p = part.strip()
        if not p:
            continue
        try:
            vals.append(int(float(p)))
        except Exception:
            continue
    out: list[int] = []
    seen = set()
    for v in vals or list(default):
        if int(v) <= 0:
            continue
        if int(v) not in seen:
            seen.add(int(v))
            out.append(int(v))
    return out or list(default)


@dataclass(frozen=True)
class ExecHealthThresholds:
    max_is_p95_bps: float = 0.0
    max_perm_impact_p95_bps: float = 0.0
    min_realized_spread_p50_bps: float = -999.0
    tighten_add_mult: float = 1.0
    tighten_add_cap_bps: float = 8.0
    tighten_k_mult: float = 1.0
    veto_require_both_is_and_impact: bool = True
    veto_on_adverse: bool = False
    delta_sec_list: tuple[int, ...] = (1, 5)

    @staticmethod
    def from_env(prefix: str = "EXEC_") -> ExecHealthThresholds:
        deltas = tuple(_csv_ints(os.getenv(f"{prefix}TCA_DELTA_SEC_LIST", "1,5"), default=(1, 5)))
        return ExecHealthThresholds(
            max_is_p95_bps=_f(os.getenv(f"{prefix}MAX_IS_P95_BPS", "0"), 0.0),
            max_perm_impact_p95_bps=_f(os.getenv(f"{prefix}MAX_PERM_IMPACT_P95_BPS", "0"), 0.0),
            min_realized_spread_p50_bps=_f(os.getenv(f"{prefix}MIN_REALIZED_SPREAD_P50_BPS", "-999"), -999.0),
            tighten_add_mult=_f(os.getenv(f"{prefix}TIGHTEN_ADD_MULT", "1.0"), 1.0),
            tighten_add_cap_bps=_f(os.getenv(f"{prefix}TIGHTEN_ADD_CAP_BPS", "8.0"), 8.0),
            tighten_k_mult=_f(os.getenv(f"{prefix}TIGHTEN_K_MULT", "1.0"), 1.0),
            veto_require_both_is_and_impact=str(
                os.getenv(f"{prefix}VETO_REQUIRE_BOTH_IS_AND_IMPACT", "1") or "1"
            ).strip().lower() not in {"0", "false", "no", "off"},
            veto_on_adverse=str(
                os.getenv(f"{prefix}VETO_ON_ADVERSE", "0") or "0"
            ).strip().lower() in {"1", "true", "yes", "on"},
            delta_sec_list=deltas,
        )


@dataclass(frozen=True)
class ExecHealthPolicySnapshot:
    profile: str
    scope: str
    mode: str
    thresholds: ExecHealthThresholds


@dataclass
class ExecHealthDecision:
    apply: bool
    veto: bool
    mode: str = ""
    flags: list[str] = field(default_factory=list)
    reason_code: str = ""
    tighten_add_bps: float = 0.0
    tighten_k_mult: float = 1.0
    scope: str = ""
    rollups: dict[str, float] = field(default_factory=dict)


def build_rollup_keys(
    *,
    metric: str,
    sym: str,
    venue: str,
    session: str,
    tf: str,
    kind: str,
    side: str,
) -> list[str]:
    """Build bounded fallback keys for a single rollup metric.

    The search space is strictly capped at 16 combinations:
      session exact|all × tf exact|all × kind exact|all × side exact|all
    """
    sym_n = (sym or "").upper()
    venue_n = (venue or "na").lower()
    dims = [
        str(session or ALL).lower(),
        str(tf or ALL).lower(),
        str(kind or ALL).lower(),
        _norm_side(side),
    ]

    out: list[str] = []
    seen = set()
    for mask in range(16):
        vals = []
        for idx, val in enumerate(dims):
            vals.append(ALL if ((mask >> idx) & 1) else val)
        key = f"tca:{metric}:{sym_n}:{venue_n}:{vals[0]}:{vals[1]}:{vals[2]}:{vals[3]}"
        if key not in seen:
            seen.add(key)
            out.append(key)
    return out


def _metric_key_map(
    *, sym: str, venue: str, session: str, tf: str, kind: str, side: str, deltas: Sequence[int]
) -> dict[str, list[str]]:
    metric_keys: dict[str, list[str]] = {
        "is_p95_bps": build_rollup_keys(
            metric="is_p95_bps", sym=sym, venue=venue, session=session, tf=tf, kind=kind, side=side
        )
    },
    for delta_sec in deltas:
        metric_keys[f"perm_impact_p95_bps_{int(delta_sec)}"] = build_rollup_keys(
            metric=f"perm_impact_p95_bps:{int(delta_sec)}",
            sym=sym, venue=venue, session=session, tf=tf, kind=kind, side=side,
        )
        metric_keys[f"realized_spread_p50_bps_{int(delta_sec)}"] = build_rollup_keys(
            metric=f"realized_spread_p50_bps:{int(delta_sec)}",
            sym=sym, venue=venue, session=session, tf=tf, kind=kind, side=side,
        )
    return metric_keys


def _pick_first_numeric(keys: Sequence[str], values_by_key: Mapping[str, Any]) -> float | None:
    for key in keys:
        fv = _f(values_by_key.get(key), float("nan"))
        if math.isfinite(fv):
            return float(fv)
    return None


def _aggregate_rollups(metric_values: Mapping[str, float | None], deltas: Sequence[int]) -> dict[str, float]:
    out: dict[str, float] = {}
    v_is = metric_values.get("is_p95_bps")
    if v_is is not None and math.isfinite(float(v_is)):
        out["is_p95_bps"] = float(v_is)

    perm_pairs: list[tuple[int, float]] = []
    rs_pairs: list[tuple[int, float]] = []
    for delta_sec in deltas:
        k_pi = f"perm_impact_p95_bps_{int(delta_sec)}"
        k_rs = f"realized_spread_p50_bps_{int(delta_sec)}"
        v_pi = metric_values.get(k_pi)
        v_rs = metric_values.get(k_rs)
        if v_pi is not None and math.isfinite(float(v_pi)):
            out[k_pi] = float(v_pi)
            perm_pairs.append((int(delta_sec), float(v_pi)))
        if v_rs is not None and math.isfinite(float(v_rs)):
            out[k_rs] = float(v_rs)
            rs_pairs.append((int(delta_sec), float(v_rs)))

    # Worst-case aggregation: max perm_impact (higher = worse), min realized_spread (lower = worse)
    if perm_pairs:
        worst_delta, worst_val = max(perm_pairs, key=lambda kv: kv[1])
        out["perm_impact_p95_bps"] = float(worst_val)
        out["perm_impact_p95_bps_delta_sec"] = float(worst_delta)
    if rs_pairs:
        worst_delta, worst_val = min(rs_pairs, key=lambda kv: kv[1])
        out["realized_spread_p50_bps"] = float(worst_val)
        out["realized_spread_p50_bps_delta_sec"] = float(worst_delta)
    return out


def _sync_mget_values(redis: Any, keys: Sequence[str]) -> dict[str, Any]:
    flat = list(keys)
    if not flat:
        return {}
    try:
        vals = redis.mget(flat)
        if isinstance(vals, tuple):
            vals = list(vals)
        return {k: v for k, v in zip(flat, vals or [])}
    except Exception:
        out: dict[str, Any] = {}
        for key in flat:
            try:
                out[key] = redis.get(key)
            except Exception:
                out[key] = None
        return out


async def _async_mget_values(redis: Any, keys: Sequence[str]) -> dict[str, Any]:
    flat = list(keys)
    if not flat:
        return {}
    try:
        vals = await redis.mget(flat)
        if isinstance(vals, tuple):
            vals = list(vals)
        return {k: v for k, v in zip(flat, vals or [])}
    except Exception:
        out: dict[str, Any] = {}
        for key in flat:
            try:
                out[key] = await redis.get(key)
            except Exception:
                out[key] = None
        return out


def read_exec_health_rollups_sync(
    *,
    redis: Any,
    sym: str,
    venue: str,
    session: str,
    tf: str,
    kind: str,
    side: str,
    delta_sec_list: Sequence[int] | None = None,
) -> dict[str, float]:
    """Synchronous reader for TCA rollups — used in EdgeCostGate hot-path."""
    if redis is None:
        return {}
    deltas = tuple(delta_sec_list or ExecHealthThresholds.from_env().delta_sec_list)
    metric_keys = _metric_key_map(
        sym=sym, venue=venue, session=session, tf=tf, kind=kind, side=side, deltas=deltas
    )
    flat = [k for keys in metric_keys.values() for k in keys]
    values_by_key = _sync_mget_values(redis, flat)
    metric_values = {metric: _pick_first_numeric(keys, values_by_key) for metric, keys in metric_keys.items()}
    return _aggregate_rollups(metric_values, deltas)


async def aread_exec_health_rollups(
    *,
    redis: Any,
    sym: str,
    venue: str,
    session: str,
    tf: str,
    kind: str,
    side: str,
    delta_sec_list: Sequence[int] | None = None,
) -> dict[str, float]:
    """Async reader for TCA rollups — used in SignalPipeline and EntryPolicyService."""
    if redis is None:
        return {}
    deltas = tuple(delta_sec_list or ExecHealthThresholds.from_env().delta_sec_list)
    metric_keys = _metric_key_map(
        sym=sym, venue=venue, session=session, tf=tf, kind=kind, side=side, deltas=deltas
    )
    flat = [k for keys in metric_keys.values() for k in keys]
    values_by_key = await _async_mget_values(redis, flat)
    metric_values = {metric: _pick_first_numeric(keys, values_by_key) for metric, keys in metric_keys.items()}
    return _aggregate_rollups(metric_values, deltas)


def _resolve_mode(*, profile: str, scope: str) -> str:
    """Resolve the effective mode for a given scope.

    Priority:
      1. Scope-specific ENV var (EDGE_EXEC_HEALTH_MODE / PIPELINE_EXEC_HEALTH_MODE / ENTRY_EXEC_HEALTH_MODE)
      2. Global EXEC_HEALTH_MODE
      3. Auto-mapping from GATE_PROFILE: hard->veto, strict->tighten, else monitor
    """
    env_name = _SCOPE_TO_ENV.get((scope or "").strip().lower())
    if env_name:
        raw = str(
            os.getenv(env_name, os.getenv("EXEC_HEALTH_MODE", "auto")) or "auto"
        ).strip().lower()
    else:
        raw = (os.getenv("EXEC_HEALTH_MODE", "auto") or "auto").strip().lower()

    if raw in {"off", "monitor", "tighten", "veto"}:
        return raw

    # Auto-map from profile
    prof = str(profile or os.getenv("GATE_PROFILE", "default") or "default").strip().lower()
    if prof == "hard":
        return "veto"
    if prof == "strict":
        return "tighten"
    return "monitor"


def get_exec_health_policy_from_env(*, profile: str, scope: str = "edge") -> ExecHealthPolicySnapshot:
    prof = str(profile or os.getenv("GATE_PROFILE", "default") or "default").strip().lower()
    thr = ExecHealthThresholds.from_env(prefix="EXEC_")
    mode = _resolve_mode(profile=prof, scope=scope)
    return ExecHealthPolicySnapshot(profile=prof, scope=str(scope), mode=mode, thresholds=thr)


def decide_exec_health_from_env(
    *, profile: str, rollups: Mapping[str, Any], scope: str = "edge"
) -> ExecHealthDecision:
    """Single-source-of-truth policy decision given TCA rollups.

    Fail-open: bad/missing rollup values never produce a veto.
    """
    pol = get_exec_health_policy_from_env(profile=profile, scope=scope)
    thr = pol.thresholds
    mode = pol.mode
    if mode == "off":
        return ExecHealthDecision(
            apply=False, veto=False, mode=mode, scope=scope, rollups=dict(rollups or {})
        )

    v_is = _f(rollups.get("is_p95_bps"), float("nan"))
    v_pi = _f(rollups.get("perm_impact_p95_bps"), float("nan"))
    v_rs = _f(rollups.get("realized_spread_p50_bps"), float("nan"))

    flags: list[str] = []
    if math.isfinite(v_is) and thr.max_is_p95_bps > 0.0 and v_is >= thr.max_is_p95_bps:
        flags.append("is_p95_high")
    if math.isfinite(v_pi) and thr.max_perm_impact_p95_bps > 0.0 and v_pi >= thr.max_perm_impact_p95_bps:
        flags.append("perm_impact_p95_high")
    if math.isfinite(v_rs) and thr.min_realized_spread_p50_bps > -900.0 and v_rs <= thr.min_realized_spread_p50_bps:
        flags.append("adverse_realized")

    if not flags:
        return ExecHealthDecision(
            apply=False, veto=False, mode=mode, scope=scope, rollups=dict(rollups or {})
        )

    # Severity ratio for tighten amount calculation
    sev = 1.0
    if "is_p95_high" in flags and thr.max_is_p95_bps > 0.0 and math.isfinite(v_is):
        sev = max(sev, float(v_is) / max(thr.max_is_p95_bps, 1e-9))
    if "perm_impact_p95_high" in flags and thr.max_perm_impact_p95_bps > 0.0 and math.isfinite(v_pi):
        sev = max(sev, float(v_pi) / max(thr.max_perm_impact_p95_bps, 1e-9))
    if "adverse_realized" in flags and thr.min_realized_spread_p50_bps > -900.0 and math.isfinite(v_rs):
        denom = max(abs(thr.min_realized_spread_p50_bps), 1.0)
        sev = max(sev, 1.0 + abs(float(v_rs) - thr.min_realized_spread_p50_bps) / denom)

    tighten_add_bps = 0.0
    tighten_k_mult = 1.0
    if mode in {"tighten", "veto"}:
        tighten_add_bps = min(
            float(thr.tighten_add_cap_bps),
            max(0.0, sev - 1.0) * 5.0 * max(0.0, float(thr.tighten_add_mult))
        )
        tighten_k_mult = max(1.0, float(thr.tighten_k_mult or 1.0))

    veto = False
    reason_code = "EXEC_HEALTH_MONITOR" if mode == "monitor" else "EXEC_HEALTH_TIGHTEN"
    if mode == "veto":
        hit_core = ("is_p95_high" in flags) and ("perm_impact_p95_high" in flags)
        hit_adverse = ("adverse_realized" in flags) and bool(thr.veto_on_adverse)
        veto = bool(hit_adverse or hit_core) if bool(thr.veto_require_both_is_and_impact) else bool(flags)
        if veto:
            if hit_core:
                reason_code = "VETO_IMPL_SHORTFALL_P95"
            elif hit_adverse:
                reason_code = "VETO_EXEC_ADVERSE_SELECTION"
            else:
                reason_code = "VETO_EXEC_HEALTH"

    return ExecHealthDecision(
        apply=True,
        veto=bool(veto),
        mode=mode,
        flags=flags,
        reason_code=str(reason_code),
        tighten_add_bps=float(tighten_add_bps),
        tighten_k_mult=float(tighten_k_mult),
        scope=str(scope),
        rollups={
            k: float(v)
            for k, v in dict(rollups or {}).items()
            if isinstance(v, (int, float)) and math.isfinite(float(v))
        },
    )
