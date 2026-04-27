from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

def norm_sym(sym: str) -> str:
    return (sym or "").strip().upper()

def norm_rg(rg: str) -> str:
    return (rg or "na").strip().lower()

def norm_grp(g: str) -> str:
    return (g or "default").strip().lower()

def norm_arm(a: str) -> str:
    a = (a or "").strip().upper()
    return a if a in ("A","B","C") else "A"

def active_arm_key(*, symbol: str, regime: str, group: str) -> str:
    return f"cfg:entry_policy:active_arm:{norm_sym(symbol)}:{norm_rg(regime)}:{norm_grp(group)}"

def lock_key(*, symbol: str, regime: str, group: str) -> str:
    return f"cfg:entry_policy:active_arm_lock:{norm_sym(symbol)}:{norm_rg(regime)}:{norm_grp(group)}"

def sugg_key(*, symbol: str, regime: str, group: str) -> str:
    return f"cfg:suggestions:entry_policy:ab_winner:v2:{norm_sym(symbol)}:{norm_rg(regime)}:{norm_grp(group)}"

@dataclass
class ApproveDecision:
    ok: bool
    winner: str
    reason: str
    have_r: bool
    edge: float
    n: int

def _get_arm_row(d: Dict[str, Any], arm: str) -> Optional[Dict[str, Any]]:
    arms = d.get("arms")
    if not isinstance(arms, dict):
        return None
    row = arms.get(arm)
    return row if isinstance(row, dict) else None

def decide_approve(
    sugg: Dict[str, Any],
    *,
    min_samples: int,
    min_edge_r: float,
) -> ApproveDecision:
    """
    Approve policy:
    - winner must exist in ABC
    - winner n >= min_samples
    - If R metrics exist (mean_r present for >=2 arms), require edge >= min_edge_r vs runner-up
    Fail-closed (ok=False) on malformed payload.
    """
    try:
        w = norm_arm(str(sugg.get("winner_arm","") or ""))
        if w not in ("A","B","C"):
            return ApproveDecision(False, "A", "bad_winner_arm", False, 0.0, 0)
        row_w = _get_arm_row(sugg, w)
        if row_w is None:
            return ApproveDecision(False, w, "missing_winner_stats", False, 0.0, 0)
        n = int(row_w.get("n", 0) or 0)
        if n < int(min_samples):
            return ApproveDecision(False, w, f"min_samples_not_met(n={n}<{min_samples})", False, 0.0, n)

        # detect if we have mean_r for at least 2 arms
        arms = sugg.get("arms") if isinstance(sugg.get("arms"), dict) else {}
        mean_r = {}
        for a in ("A","B","C"):
            r = _get_arm_row(sugg, a)
            if r is None:
                continue
            if "mean_r" in r and r.get("mean_r") is not None:
                try:
                    mean_r[a] = float(r.get("mean_r"))
                except Exception:
                    pass
        have_r = len(mean_r) >= 2
        if have_r:
            # compute edge vs best runner-up
            wv = float(mean_r.get(w, 0.0))
            runners = [v for a, v in mean_r.items() if a != w]
            rv = max(runners) if runners else 0.0
            edge = wv - rv
            if edge < float(min_edge_r):
                return ApproveDecision(False, w, f"edge_too_small(edge={edge:.4f}<{min_edge_r})", True, edge, n)
            return ApproveDecision(True, w, f"ok(mean_r_edge={edge:.4f})", True, edge, n)

        # fallback: accept if winner meets min_samples (USD-only world)
        return ApproveDecision(True, w, "ok(usd_only)", False, 0.0, n)
    except Exception:
        return ApproveDecision(False, "A", "exception_in_decide_approve", False, 0.0, 0)
