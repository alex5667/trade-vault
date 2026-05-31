from __future__ import annotations

# -*- coding: utf-8 -*-
"""
EntryPolicyOverridesV1
Strict schema for runtime overrides consumed by smt_entry_policy_service.

Design goals:
  - deterministic & reproducible (updated_ts_ms monotonic)
  - scope-aware (global / group / symbol+regime+scenario)
  - safe rollout (enabled=0 => ignore)
  - validation with fail-closed (invalid JSON => ignore)
  - supports hold-down & hysteresis in consumer service
"""

import json
from dataclasses import asdict, dataclass, field
from typing import Any

from utils.time_utils import get_ny_time_millis


def _s(x: Any, d: str = "") -> str:
    try:
        return str(x or d)
    except Exception:
        return d

def _i(x: Any, d: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return d

def _f(x: Any, d: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return d

def _arm(x: Any) -> str:
    v = _s(x, "").strip().upper()
    return v if v in ("A", "B", "C") else ""

def _scn(x: Any) -> str:
    v = _s(x, "").strip().lower()
    # strict taxonomy: only continuation/reversal are valid for scoped overrides
    return v if v in ("continuation", "reversal") else ""

def _rg(x: Any) -> str:
    return _s(x, "na").strip().lower() or "na"

def _grp(x: Any) -> str:
    return _s(x, "default").strip().lower() or "default"

def _sym(x: Any) -> str:
    return _s(x, "").strip().upper()

@dataclass
class EntryPolicyOverridesV1:
    # versioning
    v: int = 1
    kind: str = "overrides_v1"
    updated_ts_ms: int = 0
    enabled: int = 1

    # ---- scope (optional; empty => broader scope) ----
    symbol=""          # BTCUSDT; empty => any symbol
    regime: str = "na"        # thin/range/trend/...
    scenario: str = ""        # continuation/reversal; empty => any
    group: str = "default"    # default/thin/... (AB group)

    # ---- operational safety ----
    overrides_hold_down_ms: int = 60_000   # consumer should not re-apply more often

    # ---- hard controls ----
    force_active_arm: str = ""             # "A"/"B"/"C" (hard override)
    freeze_active: int = 0                 # 1 => freeze mode active
    freeze_mode: str = "shadow"            # shadow|hard
    freeze_reason: str = "override"
    freeze_until_ts_ms: int = 0

    # ---- policy knobs (optional) ----
    # these map to EntryPolicyCoreConfig in services/entry_policy_core.py (or can be applied as extra gates)
    coh_thr: float = 0.0                   # if >0 overrides base coh threshold
    leader_conf_score_min: float = 0.0     # if >0 require leader_conf_score >= this
    min_of_score: float = 0.0              # if >0 require of_confirm_score >= this
    max_spread_bp: float = 0.0             # if >0 require spread_bp <= this
    max_book_age_ms: int = 0               # if >0 require book_age_ms <= this
    require_book_health_ok: int = 0        # if 1 require book_health_ok==1

    # ------------------------------------------------------------
    # NEW: Tier-policy (autopilot can recommend; consumer enforces)
    # ------------------------------------------------------------
    abs_lvl_tier_trend: int = -1
    abs_lvl_tier_range: int = -1
    abs_lvl_tier_thin: int = -1
    abs_lvl_tier_mode: str = "min"  # min|exact

    # ------------------------------------------------------------
    # NEW: AB routing splits (canary/promote without ENV)
    # Meaning:
    #   choose_arm_abc uses split_b/split_c in [0..100], A gets remaining.
    #   Keep A >= 1% by enforcing split_b + split_c <= 99.
    #   salt allows de-biasing / deterministic reshuffles without changing keys.
    # ------------------------------------------------------------
    ab_split_b: int = 10
    ab_split_c: int = 10
    ab_salt: str = "v1"

    # extensibility (forward-compatible)
    extra: dict[str, Any] = field(default_factory=dict)

    # ---------------- Parsing ----------------
    @staticmethod
    def from_json(raw: str) -> tuple[EntryPolicyOverridesV1 | None, str]:
        try:
            d = json.loads(raw or "")
            return EntryPolicyOverridesV1.from_dict(d)
        except Exception as e:
            return None, f"bad_json:{e}"

    @staticmethod
    def from_dict(d: dict[str, Any]) -> tuple[EntryPolicyOverridesV1 | None, str]:
        if not isinstance(d, dict):
            return None, "not_dict"
        try:
            o = EntryPolicyOverridesV1(
                v=_i(d.get("v", 1), 1),
                kind=_s(d.get("kind", "overrides_v1"), "overrides_v1"),
                updated_ts_ms=_i(d.get("updated_ts_ms", 0), 0),
                enabled=_i(d.get("enabled", 1), 1),
                symbol=_sym(d.get("symbol", "")),  # type: ignore
                regime=_rg(d.get("regime", "na")),
                scenario=_scn(d.get("scenario", "")),
                group=_grp(d.get("group", "default")),
                overrides_hold_down_ms=_i(d.get("overrides_hold_down_ms", 60_000), 60_000),
                force_active_arm=_arm(d.get("force_active_arm", "")),
                freeze_active=_i(d.get("freeze_active", 0), 0),
                freeze_mode=_s(d.get("freeze_mode", "shadow"), "shadow").strip().lower(),
                freeze_reason=_s(d.get("freeze_reason", "override"), "override"),
                freeze_until_ts_ms=_i(d.get("freeze_until_ts_ms", 0), 0),
                coh_thr=_f(d.get("coh_thr", 0.0), 0.0),
                leader_conf_score_min=_f(d.get("leader_conf_score_min", 0.0), 0.0),
                min_of_score=_f(d.get("min_of_score", 0.0), 0.0),
                max_spread_bp=_f(d.get("max_spread_bp", 0.0), 0.0),
                max_book_age_ms=_i(d.get("max_book_age_ms", 0), 0),
                require_book_health_ok=_i(d.get("require_book_health_ok", 0), 0),

                # tier-policy (strict fields)
                abs_lvl_tier_trend=_i(d.get("abs_lvl_tier_trend", -1), -1),
                abs_lvl_tier_range=_i(d.get("abs_lvl_tier_range", -1), -1),
                abs_lvl_tier_thin=_i(d.get("abs_lvl_tier_thin", -1), -1),
                abs_lvl_tier_mode=_s(d.get("abs_lvl_tier_mode", "min"), "min").strip().lower(),

                # AB routing (NEW)
                ab_split_b=_i(d.get("ab_split_b", 10), 10),
                ab_split_c=_i(d.get("ab_split_c", 10), 10),
                ab_salt=_s(d.get("ab_salt", "v1"), "v1"),

                extra=dict(d.get("extra", {}) or {}),
            )
            # auto-fill updated_ts_ms if missing (producer bug tolerance)
            if o.updated_ts_ms <= 0:
                o.updated_ts_ms = get_ny_time_millis()
            return o, "ok"
        except Exception as e:
            return None, f"bad_fields:{e}"

    # ---------------- Validation ----------------
    def validate(self) -> tuple[bool, str]:
        if int(self.v) != 1:
            return False, "bad_v"
        if str(self.kind).strip().lower() != "overrides_v1":
            return False, "bad_kind"
        if int(self.enabled) not in (0, 1):
            return False, "enabled_not_0_1"
        if self.freeze_mode not in ("shadow", "hard"):
            return False, "bad_freeze_mode"
        if self.force_active_arm and self.force_active_arm not in ("A", "B", "C"):
            return False, "bad_force_active_arm"
        if self.scenario and self.scenario not in ("continuation", "reversal"):
            return False, "bad_scenario"
        if self.overrides_hold_down_ms < 0 or self.overrides_hold_down_ms > 24 * 3600 * 1000:
            return False, "bad_hold_down"

        # AB split validation
        sb = int(self.ab_split_b or 0)
        sc = int(self.ab_split_c or 0)
        if sb < 0 or sb > 100 or sc < 0 or sc > 100:
            return False, "ab_split_range"
        if (sb + sc) >= 100:
            return False, "ab_split_sum_ge_100"
        if not isinstance(self.ab_salt, str):
            return False, "ab_salt"

        # bounds (safe defaults)
        if self.coh_thr < 0 or self.coh_thr > 1.0:
            return False, "bad_coh_thr"
        if self.leader_conf_score_min < 0 or self.leader_conf_score_min > 5.0:
            return False, "bad_leader_conf_score_min"
        if self.min_of_score < 0 or self.min_of_score > 5.0:
            return False, "bad_min_of_score"
        if self.max_spread_bp < 0 or self.max_spread_bp > 10_000:
            return False, "bad_max_spread_bp"
        if self.max_book_age_ms < 0 or self.max_book_age_ms > 10_000_000:
            return False, "bad_max_book_age_ms"
        if int(self.require_book_health_ok) not in (0, 1):
            return False, "bad_require_book_health_ok"

        # tier-policy validation
        if self.abs_lvl_tier_mode not in ("min", "exact"):
            return False, "bad_abs_lvl_tier_mode"
        for k, v in (("trend", self.abs_lvl_tier_trend), ("range", self.abs_lvl_tier_range), ("thin", self.abs_lvl_tier_thin)):
            tv = int(v)
            if tv not in (-1, 0, 1, 2):
                return False, f"bad_abs_lvl_tier_{k}"
        return True, "ok"

    # ---------------- Keying ----------------
    def target_key(self, *, prefix: str = "cfg:entry_policy:overrides:v1") -> str:
        """
        Key precedence design (consumer should read most-specific first):
          1) cfg:entry_policy:overrides:v1:{symbol}:{regime}:{scenario}:{group}
          2) cfg:entry_policy:overrides:v1:{symbol}:{regime}:{group}
          3) cfg:entry_policy:overrides:v1:{group}
          4) cfg:entry_policy:overrides:v1
        This method returns the MOST specific key that this object represents.
        """
        sym = _sym(self.symbol)
        rg = _rg(self.regime)
        grp = _grp(self.group)
        scn = _scn(self.scenario)
        if sym and rg and scn:
            return f"{prefix}:{sym}:{rg}:{scn}:{grp}"
        if sym and rg:
            return f"{prefix}:{sym}:{rg}:{grp}"
        if grp and grp != "default":
            return f"{prefix}:{grp}"
        return prefix

    def to_json(self) -> str:
        # keep stable field set; extra stays nested
        return json.dumps(asdict(self), ensure_ascii=False, separators=(",", ":"))
