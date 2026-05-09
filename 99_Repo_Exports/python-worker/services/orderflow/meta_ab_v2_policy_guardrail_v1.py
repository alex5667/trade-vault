from __future__ import annotations

"""
Stage4: Policy guardrail for Meta AB v2 ramp/apply.

Design goals:
- Fail-closed: if uncertain -> HOLD (no automatic share change)
- Low cardinal reasons for observability
- Independent from evaluator internals (reads report + cfg thresholds)

Returned decision is embedded into report["policy"] and used to override report["ramp"].
"""


import json
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any
import contextlib


def _read_json_file(path: str) -> dict[str, Any] | None:
    if not path:
        return None
    try:
        p = Path(path)
        if not p.exists():
            return None
        with p.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _conf_coverage_check(action_raw: str, share_current: float, share_next_raw: float) -> list[str]:
    """Return reason codes if confirmations coverage report indicates drift."""
    if action_raw == "hold" or share_next_raw == share_current:
        return []

    if os.getenv("META_AB_POLICY_REQUIRE_CONF_COVERAGE_OK", "1") != "1":
        return []

    report_path = os.getenv("CONFIRMATIONS_COVERAGE_OUT_JSON", "/var/lib/trade/of_reports/confirmations_coverage_report.json")
    max_age_sec = int(os.getenv("META_AB_POLICY_CONF_REPORT_MAX_AGE_SEC", "172800"))  # 48h
    min_nonzero = float(os.getenv("META_AB_POLICY_MIN_CONF_NONZERO_RATE", "0.005"))

    rep = _read_json_file(report_path)
    if rep is None:
        return ["conf_coverage_missing"]

    ts_ms = int(rep.get("ts_ms") or 0)
    if ts_ms <= 0:
        return ["conf_coverage_missing"]

    age = max(0, int(time.time() - (ts_ms / 1000.0)))
    if max_age_sec > 0 and age > max_age_sec:
        return ["conf_coverage_stale"]

    reasons = set([str(r) for r in (rep.get("reasons") or []) if r])
    if "conf_cols_missing" in reasons:
        return ["conf_coverage_missing"]
    if "conf_all_zero" in reasons:
        return ["conf_coverage_low"]

    summ = rep.get("summary") or {}
    conf_min = summ.get("conf_min_nonzero_rate")
    try:
        conf_min_f = float(conf_min) if conf_min is not None else None
    except Exception:
        conf_min_f = None

    if conf_min_f is not None and conf_min_f < min_nonzero:
        return ["conf_coverage_low"]

    # As fallback compute from per-feature rates if present
    feats = rep.get("features") or {}
    conf_rates = []
    for k, st in feats.items():
        if isinstance(k, str) and k.startswith("conf_") and isinstance(st, dict) and st.get("present") == 1:
            with contextlib.suppress(Exception):
                conf_rates.append(float(st.get("nonzero_rate") or 0.0))
    if conf_rates and min(conf_rates) < min_nonzero:
        return ["conf_coverage_low"]

    return []


def _to_float(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default


def _to_int(x: Any, default: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return default


def _norm_action(action: str) -> str:
    a = (action or "").strip().lower()
    if a in ("increase_share", "inc", "increase", "up", "+"):
        return "increase_share"
    if a in ("decrease_share", "dec", "decrease", "down", "-"):
        return "decrease_share"
    if a in ("hold", "noop", "no_change", "none"):
        return "hold"
    return a or "hold"


def _ci_lo(rep: dict[str, Any]) -> float | None:
    ci = rep.get("ci") or {}
    if isinstance(ci, dict):
        for k in (
            "delta_exp_r_lo",
            "exp_r_per_candidate_lo",
            "delta_exp_r_per_candidate_lo",
            "delta_exp_r_ci_lo",
            "lo",
        ):
            if k in ci:
                return _to_float(ci.get(k))
    return None


@dataclass(frozen=True)
class PolicyConfig:
    enabled: bool = True
    fail_closed: bool = True
    allow_decrease: bool = True

    require_winner_challenger_for_increase: bool = True
    require_ci_positive_for_increase: bool = True

    min_n_eligible: int = 1000
    min_delta_exp_r: float = 0.002
    max_delta_tail: float = 0.01

    max_step: float = 0.05
    max_share: float = 0.50


@dataclass(frozen=True)
class PolicyDecision:
    blocked: bool
    allow_apply: bool
    share_next_final: float
    action_final: str
    reasons: tuple[str, ...]


def decide_meta_ab_v2_policy(
    rep: dict[str, Any],
    cfg: Any,
    share_current: float,
    share_next_raw: float,
    action_raw: str,
    freeze_max_share: float | None,
    env_overrides: dict[str, Any] | None = None,
) -> PolicyDecision:
    """
    Apply guardrails and return final ramp decision.

    Notes:
    - Blocking is only meaningful when share would change.
    - If blocked and fail_closed -> action_final=hold and share_next_final=share_current
    """
    env_overrides = env_overrides or {}

    # derive config from evaluator cfg, then allow env overrides
    pcfg = PolicyConfig(
        enabled=bool(env_overrides.get("enabled", True)),
        fail_closed=bool(env_overrides.get("fail_closed", True)),
        allow_decrease=bool(env_overrides.get("allow_decrease", True)),
        require_winner_challenger_for_increase=bool(env_overrides.get("require_winner_challenger_for_increase", True)),
        require_ci_positive_for_increase=bool(env_overrides.get("require_ci_positive_for_increase", True)),
        min_n_eligible=_to_int(env_overrides.get("min_n_eligible", getattr(cfg, "min_n", 1000)), 1000),
        min_delta_exp_r=_to_float(env_overrides.get("min_delta_exp_r", getattr(cfg, "min_delta_exp_r", 0.002)), 0.002),
        max_delta_tail=_to_float(env_overrides.get("max_delta_tail", getattr(cfg, "tail_slack", 0.01)), 0.01),
        max_step=_to_float(env_overrides.get("max_step", getattr(cfg, "ramp_step", 0.05)), 0.05),
        max_share=_to_float(env_overrides.get("max_share", getattr(cfg, "max_share", 0.50)), 0.50),
    )

    action = _norm_action(action_raw)
    share_next = float(share_next_raw)

    # normalize action vs delta
    d = share_next - float(share_current)
    if abs(d) < 1e-12:
        action = "hold"
        share_next = float(share_current)
    elif d > 0 and action == "hold":
        action = "increase_share"
    elif d < 0 and action == "hold":
        action = "decrease_share"

    # policy disabled -> pass-through
    if not pcfg.enabled:
        allow_apply = action != "hold"
        return PolicyDecision(blocked=False, allow_apply=allow_apply, share_next_final=share_next, action_final=action, reasons=())

    reasons: list[str] = []

    # Drift guard: confirmations coverage report (offline)
    reasons.extend(_conf_coverage_check(action_raw, share_current, share_next_raw))

    # hard validity checks
    if not (share_next == share_next):  # NaN
        reasons.append("share_nan")
    if share_next < 0.0 or share_next > 1.0:
        reasons.append("share_out_of_bounds")

    # evaluator error reason should block apply
    reason_txt = (rep.get("reason") or "").strip()
    if reason_txt:
        reasons.append("eval_reason_present")

    counts = rep.get("counts") or {}
    n_eligible = _to_int(counts.get("n_eligible") or counts.get("eligible") or 0)
    if n_eligible < pcfg.min_n_eligible:
        reasons.append("n_eligible_low")

    # step size guard
    if abs(share_next - float(share_current)) > (pcfg.max_step + 1e-9):
        reasons.append("share_step_too_large")

    # caps guard
    if share_next > pcfg.max_share + 1e-9:
        reasons.append("share_above_max_share")
    if freeze_max_share is not None and share_next > float(freeze_max_share) + 1e-9:
        reasons.append("share_above_freeze_max")

    # action-specific guards
    winner = (rep.get("winner") or "tie").strip().lower()
    delta = rep.get("delta") or {}
    delta_exp_r = _to_float(delta.get("exp_r_per_candidate") or 0.0)
    delta_tail = _to_float(delta.get("tail_rate_per_candidate") or 0.0)

    if action == "increase_share":
        if pcfg.require_winner_challenger_for_increase and winner != "challenger":
            reasons.append("winner_not_challenger")
        if delta_exp_r < pcfg.min_delta_exp_r:
            reasons.append("delta_exp_r_low")
        if delta_tail > pcfg.max_delta_tail:
            reasons.append("tail_worse")

        if pcfg.require_ci_positive_for_increase:
            lo = _ci_lo(rep)
            if lo is None:
                reasons.append("ci_missing")
            elif lo <= 0.0:
                reasons.append("ci_not_positive")

    if action == "decrease_share" and not pcfg.allow_decrease:
        reasons.append("decrease_disallowed")

    # decide block
    changing = action != "hold" and abs(share_next - float(share_current)) >= 1e-12
    blocked = changing and len(reasons) > 0

    if blocked and pcfg.fail_closed:
        return PolicyDecision(
            blocked=True,
            allow_apply=False,
            share_next_final=float(share_current),
            action_final="hold",
            reasons=tuple(reasons),
        )

    # if blocked but not fail_closed -> allow apply (not default)
    allow_apply = (not blocked) and (action != "hold")
    return PolicyDecision(blocked=blocked, allow_apply=allow_apply, share_next_final=share_next, action_final=action, reasons=tuple(reasons))
