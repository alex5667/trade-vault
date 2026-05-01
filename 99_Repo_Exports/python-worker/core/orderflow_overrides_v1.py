# -*- coding: utf-8 -*-
from __future__ import annotations
"""
OrderflowOverridesV1
Strict schema + validation + deterministic selection.

Goals:
  - No silent config drift (versioned sid).
  - Fail-open (invalid overrides => ignore).
  - Deterministic application (same inputs => same result).
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple


def _s(x: Any, d: str = "") -> str:
    try:
        return str(x if x is not None else d)
    except Exception:
        return d


def _i(x: Any, d: int = 0) -> int:
    try:
        return int(float(x))
    except Exception:
        return int(d)


def _f(x: Any, d: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return float(d)


@dataclass
class RolloutV1:
    mode: str = "full"  # "full" | "canary"
    canary_symbols: List[str] = field(default_factory=list)
    canary_share: float = 1.0  # used only if mode="canary" and symbols empty (hash-share)
    promote_after_hours: int = 0

    def validate(self) -> Tuple[bool, str]:
        m = (self.mode or "full").lower()
        if m not in ("full", "canary"):
            return False, "rollout.mode"
        cs = []
        for s in self.canary_symbols or []:
            ss = _s(s).upper().strip()
            if ss:
                cs.append(ss)
        self.canary_symbols = cs
        self.canary_share = max(0.0, min(1.0, float(self.canary_share or 0.0)))
        self.promote_after_hours = max(0, int(self.promote_after_hours or 0))
        return True, "ok"


@dataclass
class OrderflowOverridesV1:
    v: int = 1
    enabled: int = 1
    updated_ts_ms: int = 0
    # policy knobs (examples; extend as needed)
    abs_lvl_tier_trend: Optional[int] = None
    abs_lvl_tier_range: Optional[int] = None
    abs_lvl_tier_thin: Optional[int] = None

    strong_need_reversal: Optional[int] = None
    strong_need_continuation: Optional[int] = None

    burst_window_min_ms: Optional[int] = None
    burst_window_max_ms: Optional[int] = None

    # ATR gate floors override (optional)
    atr_floor_t0_bps: Optional[float] = None
    atr_floor_t1_bps: Optional[float] = None
    atr_floor_t2_bps: Optional[float] = None

    rollout: RolloutV1 = field(default_factory=RolloutV1)

    @staticmethod
    def from_json(raw: str) -> Tuple[Optional["OrderflowOverridesV1"], str]:
        import json
        try:
            d = json.loads(raw or "")
            if not isinstance(d, dict):
                return None, "not_dict"
            o = OrderflowOverridesV1()
            o.v = _i(d.get("v", 1), 1)
            o.enabled = _i(d.get("enabled", 1), 1)
            o.updated_ts_ms = _i(d.get("updated_ts_ms", 0), 0)
            o.abs_lvl_tier_trend = d.get("abs_lvl_tier_trend", None)
            o.abs_lvl_tier_range = d.get("abs_lvl_tier_range", None)
            o.abs_lvl_tier_thin = d.get("abs_lvl_tier_thin", None)
            o.strong_need_reversal = d.get("strong_need_reversal", None)
            o.strong_need_continuation = d.get("strong_need_continuation", None)
            o.burst_window_min_ms = d.get("burst_window_min_ms", None)
            o.burst_window_max_ms = d.get("burst_window_max_ms", None)
            o.atr_floor_t0_bps = d.get("atr_floor_t0_bps", None)
            o.atr_floor_t1_bps = d.get("atr_floor_t1_bps", None)
            o.atr_floor_t2_bps = d.get("atr_floor_t2_bps", None)
            rr = d.get("rollout", {}) if isinstance(d.get("rollout", {}), dict) else {}
            o.rollout = RolloutV1(
                mode=_s(rr.get("mode", "full"), "full"),
                canary_symbols=list(rr.get("canary_symbols", []) or []),
                canary_share=_f(rr.get("canary_share", 1.0), 1.0),
                promote_after_hours=_i(rr.get("promote_after_hours", 0), 0),
            )
            ok, reason = o.validate()
            return (o if ok else None), reason
        except Exception:
            return None, "json_error"

    def validate(self) -> Tuple[bool, str]:
        if int(self.v or 0) != 1:
            return False, "v"
        self.enabled = 1 if int(self.enabled or 0) == 1 else 0
        self.updated_ts_ms = max(0, int(self.updated_ts_ms or 0))

        for k in ("abs_lvl_tier_trend","abs_lvl_tier_range","abs_lvl_tier_thin"):
            v = getattr(self, k)
            if v is None:
                continue
            vv = _i(v, -1)
            if vv not in (0, 1, 2):
                return False, k
            setattr(self, k, vv)

        for k in ("strong_need_reversal","strong_need_continuation"):
            v = getattr(self, k)
            if v is None:
                continue
            vv = max(1, min(3, _i(v, 2)))
            setattr(self, k, vv)

        for k in ("burst_window_min_ms","burst_window_max_ms"):
            v = getattr(self, k)
            if v is None:
                continue
            vv = max(200, min(5000, _i(v, 2500)))
            setattr(self, k, vv)

        for k in ("atr_floor_t0_bps","atr_floor_t1_bps","atr_floor_t2_bps"):
            v = getattr(self, k)
            if v is None:
                continue
            vv = max(0.0, min(500.0, _f(v, 0.0)))
            setattr(self, k, float(vv))

        ok, reason = self.rollout.validate()
        if not ok:
            return False, f"rollout:{reason}"
        return True, "ok"

    def apply_to_cfg(self, cfg: Dict[str, Any]) -> Dict[str, Any]:
        """
        Apply overrides into runtime.config dict (copy-on-write).
        Only whitelisted keys are overridden.
        """
        out = dict(cfg or {})
        if int(self.enabled) != 1:
            return out

        # tier policy
        if self.abs_lvl_tier_trend is not None:
            out["abs_lvl_tier_trend"] = int(self.abs_lvl_tier_trend)
        if self.abs_lvl_tier_range is not None:
            out["abs_lvl_tier_range"] = int(self.abs_lvl_tier_range)
        if self.abs_lvl_tier_thin is not None:
            out["abs_lvl_tier_thin"] = int(self.abs_lvl_tier_thin)

        if self.strong_need_reversal is not None:
            out["strong_need_reversal"] = int(self.strong_need_reversal)
        if self.strong_need_continuation is not None:
            out["strong_need_continuation"] = int(self.strong_need_continuation)

        if self.burst_window_min_ms is not None:
            out["burst_window_min_ms"] = int(self.burst_window_min_ms)
        if self.burst_window_max_ms is not None:
            out["burst_window_max_ms"] = int(self.burst_window_max_ms)

        if self.atr_floor_t0_bps is not None:
            out["atr_floor_t0_bps"] = float(self.atr_floor_t0_bps)
        if self.atr_floor_t1_bps is not None:
            out["atr_floor_t1_bps"] = float(self.atr_floor_t1_bps)
        if self.atr_floor_t2_bps is not None:
            out["atr_floor_t2_bps"] = float(self.atr_floor_t2_bps)

        return out
