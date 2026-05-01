# -*- coding: utf-8 -*-
from __future__ import annotations
"""
Autopilot fields normalizer (unit-testable).

Goal
-----
TradeMonitor / analytics wants a small stable set of fields on every closed trade:
  - symbol, regime, scenario
  - abs_lvl_tier, dn_tier
  - of_confirm_ok, book_health_ok
  - pressure_sps (optional)

Reality
-------
Signals come from different pipelines (crypto_orderflow, smt_entry_policy, etc).
Some fields may be located:
  - at top-level of signal payload
  - inside config_snapshot.indicators
  - inside ctx / micro blocks

This module:
  - extracts "best-effort" values
  - normalizes scenario taxonomy to: {"continuation","reversal"} (or "na")
  - merges fields back into payload root to make persistence trivial

Design rules:
  - FAIL-OPEN: if extraction fails -> do not raise, keep original payload
  - Determinism-friendly: prefer fields that are tied to ts_ms (indicators on emission)
"""


from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple


def _s(x: Any, default: str = "") -> str:
    try:
        return str(x or default)
    except Exception:
        return default


def _i(x: Any, default: int = 0) -> int:
    try:
        return int(float(x))
    except Exception:
        return default


def _f(x: Any, default: float = 0.0) -> float:
    try:
        v = float(x)
        return v
    except Exception:
        return default


def _get(d: Any, *path: str) -> Any:
    """
    Safe nested getter for dicts.
    Example: _get(payload, "config_snapshot", "indicators", "of_confirm_ok")
    """
    cur = d
    for k in path:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(k)
    return cur


def normalize_scenario(raw: Any) -> str:
    """
    Normalize scenario taxonomy to what EntryPolicy expects:
      - "continuation"
      - "reversal"
    Everything else -> "na"
    """
    v = _s(raw, "").strip().lower()
    if v in ("continuation", "cont", "trend", "trend_continuation"):
        return "continuation"
    if v in ("reversal", "rev", "counter", "countertrend", "smt_reversal"):
        return "reversal"
    # Some pipelines may store StrongGate scenario directly
    if v in ("reversal_gate", "strong_reversal"):
        return "reversal"
    if v in ("continuation_gate", "strong_continuation"):
        return "continuation"
    return "na"


@dataclass
class AutopilotFields:
    symbol=""
    regime: str = "na"
    scenario: str = "na"
    abs_lvl_tier: int = -1
    dn_tier: int = -1
    of_confirm_ok: int = 0
    book_health_ok: int = 1
    pressure_sps: float = 0.0


def extract_autopilot_fields(payload: Dict[str, Any]) -> AutopilotFields:
    """
    Extract best-effort fields from heterogeneous signal payload.
    """
    if not isinstance(payload, dict):
        return AutopilotFields()

    # 1) Most reliable: config_snapshot.indicators (tied to emission)
    ind = _get(payload, "config_snapshot", "indicators")
    if not isinstance(ind, dict):
        # Some pipelines might keep indicators at top-level
        ind = payload.get("indicators") if isinstance(payload.get("indicators"), dict) else {}

    # 2) ctx block (used by SMT entry policy)
    ctx = payload.get("ctx") if isinstance(payload.get("ctx"), dict) else {}

    symbol = _s(payload.get("symbol") or ctx.get("symbol") or "", "").upper()
    regime = _s(payload.get("regime") or ctx.get("regime") or payload.get("entry_regime") or "na", "na").lower()

    # Scenario can live in multiple places:
    # - payload["scenario"] (preferred)
    # - payload["decision"] (SMT uses "decision" as scenario)
    # - indicators["strong_gate_scn"] (crypto OF strong gate)
    # - strong_gate_scn inside payload
    raw_scn = (
        payload.get("scenario")
        or payload.get("decision")
        or ind.get("scenario")
        or ind.get("strong_gate_scn")
        or payload.get("strong_gate_scn")
        or ctx.get("scenario")
        or ctx.get("decision")
    )
    scenario = normalize_scenario(raw_scn)

    # abs_lvl_tier: preferred key "abs_lvl_tier", fallback "abs_lvl_tier_used"
    # also check "tier" as common alias
    raw_tier = (
        payload.get("abs_lvl_tier")
        if payload.get("abs_lvl_tier") is not None
        else payload.get("abs_lvl_tier_used")
    )
    if raw_tier is None:
        raw_tier = ind.get("abs_lvl_tier") if ind.get("abs_lvl_tier") is not None else payload.get("tier")

    abs_lvl_tier = _i(raw_tier, default=-1)

    dn_tier = _i(
        payload.get("dn_tier", None)
        if payload.get("dn_tier", None) is not None
        else ind.get("dn_tier", None),
        default=-1,
    )
    of_confirm_ok = _i(
        payload.get("of_confirm_ok", None)
        if payload.get("of_confirm_ok", None) is not None
        else ind.get("of_confirm_ok", ind.get("strong_gate_ok", None)),
        default=0, # Default to 0 (False) if unknown, to be safe/conservative
    )
    book_health_ok = _i(
        payload.get("book_health_ok", None)
        if payload.get("book_health_ok", None) is not None
        else ind.get("book_health_ok", 1), # Default to 1 (True) if unknown
        default=1,
    )
    pressure_sps = _f(
        payload.get("pressure_sps", None)
        if payload.get("pressure_sps", None) is not None
        else ind.get("pressure_sps", payload.get("micro", {}).get("pressure_sps") if isinstance(payload.get("micro"), dict) else None),
        default=0.0,
    )

    return AutopilotFields(
        symbol=symbol,
        regime=regime,
        scenario=scenario,
        abs_lvl_tier=abs_lvl_tier,
        dn_tier=dn_tier,
        of_confirm_ok=of_confirm_ok,
        book_health_ok=book_health_ok,
        pressure_sps=pressure_sps,
    )


def enrich_signal_payload_for_autopilot(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Merge extracted autopilot fields into payload root.
    This makes persistence easy (PositionState.signal_payload / events:trades root payload).
    """
    if not isinstance(payload, dict):
        return payload
    try:
        af = extract_autopilot_fields(payload)
        # Do not overwrite if caller already set explicit values.
        payload.setdefault("regime", af.regime)
        payload.setdefault("scenario", af.scenario)
        if "abs_lvl_tier" not in payload and af.abs_lvl_tier != -1:
            payload["abs_lvl_tier"] = int(af.abs_lvl_tier)
        if "dn_tier" not in payload and af.dn_tier != -1:
            payload["dn_tier"] = int(af.dn_tier)
        if "of_confirm_ok" not in payload and af.of_confirm_ok != -1:
            payload["of_confirm_ok"] = int(af.of_confirm_ok)
        if "book_health_ok" not in payload and af.book_health_ok != -1:
            payload["book_health_ok"] = int(af.book_health_ok)
        if "pressure_sps" not in payload and af.pressure_sps > 0:
            payload["pressure_sps"] = float(af.pressure_sps)
        return payload
    except Exception:
        return payload
