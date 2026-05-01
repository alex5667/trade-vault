# -*- coding: utf-8 -*-
from __future__ import annotations
"""
AB Winner Evaluator (core, unit-testable)
========================================

Задача:
  - Из потока POSITION_CLOSED (events:trades) собрать статистику R-multiple по arms (A/B/C)
  - Выбрать победителя по LCB (Lower Confidence Bound) с режимно-зависимыми порогами
  - "Ещё выше": агрегировать winners по сценариям (continuation/reversal) + hysteresis/hold-down
  - Сформировать meta payload для cfg:suggestions:entry_policy:meta:{sid}

Важно:
  - В core нет Redis / IO: только чистые функции для тестов и воспроизводимости.
  - Robustness: winsorize R (ограничение хвостов), защитные проверки n/NaN.
"""


import hashlib
import json
import math
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple


ARMS = ("A", "B", "C")


def _sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def norm_arm(v: str) -> str:
    a = (v or "").strip().upper()
    return a if a in ARMS else "A"


def norm_regime(v: str) -> str:
    r = (v or "na").strip().lower()
    # У вас встречаются: trend, range, mixed, thin, news, illiquid, trending_bull/bear
    return r or "na"


def regime_bucket(regime: str) -> str:
    r = norm_regime(regime)
    if r in ("thin", "news", "illiquid"):
        return "thin"
    if r in ("trend", "trending_bull", "trending_bear"):
        return "trend"
    if r in ("range",):
        return "range"
    return "mixed"


def norm_scenario(v: str) -> str:
    s = (v or "na").strip().lower()
    if s in ("cont", "continuation"):
        return "continuation"
    if s in ("rev", "reversal"):
        return "reversal"
    return "na"


def is_non_a(a: str) -> bool:
    return str(a or "").upper() in ("B", "C")


# --- Normal quantile Z (avoid scipy) ---
_Z_BY_ALPHA = {
    0.10: 1.281551565545,  # ~90% one-sided
    0.05: 1.644853626951,  # ~95% one-sided
    0.02: 2.053748910631,  # ~98% one-sided
    0.01: 2.326347874041,  # ~99% one-sided
}


def z_for_alpha(alpha: float) -> float:
    # keep deterministic, snap to common values
    a = float(alpha)
    # Choose nearest defined alpha
    best = min(_Z_BY_ALPHA.keys(), key=lambda x: abs(x - a))
    return float(_Z_BY_ALPHA[best])


def winsorize(xs: Iterable[float], lo: float, hi: float) -> List[float]:
    out: List[float] = []
    for x in xs:
        try:
            v = float(x)
        except Exception:
            continue
        if not math.isfinite(v):
            continue
        if v < lo:
            v = lo
        if v > hi:
            v = hi
        out.append(v)
    return out


@dataclass
class ArmStats:
    arm: str
    n: int
    mean: float
    std: float
    lcb: float


@dataclass
class WinnerDecision:
    winner: str
    reason: str
    # raw stats
    stats: Dict[str, ArmStats]

    # optional: for multi-scenario aggregation/debug
    meta: Dict[str, Any] = None


def _mean(xs: List[float]) -> float:
    return float(sum(xs) / max(1, len(xs)))


def _std_sample(xs: List[float], mu: float) -> float:
    n = len(xs)
    if n <= 1:
        return 0.0
    s2 = 0.0
    for x in xs:
        d = x - mu
        s2 += d * d
    s2 /= float(n - 1)
    return float(math.sqrt(max(0.0, s2)))


def compute_arm_stats(
    *,
    arm_to_r: Mapping[str, List[float]],
    alpha: float,
    winsor_lo: float = -5.0,
    winsor_hi: float = +5.0,
) -> Dict[str, ArmStats]:
    z = z_for_alpha(alpha)
    out: Dict[str, ArmStats] = {}
    for arm in ARMS:
        xs0 = list(arm_to_r.get(arm) or [])
        xs = winsorize(xs0, winsor_lo, winsor_hi)
        n = len(xs)
        if n == 0:
            out[arm] = ArmStats(arm=arm, n=0, mean=0.0, std=0.0, lcb=float("-inf"))
            continue
        mu = _mean(xs)
        sd = _std_sample(xs, mu)
        se = sd / math.sqrt(float(n)) if n > 0 else float("inf")
        lcb = mu - z * se
        out[arm] = ArmStats(arm=arm, n=n, mean=float(mu), std=float(sd), lcb=float(lcb))
    return out


def choose_winner_lcb(
    *,
    regime: str,
    arm_to_r: Mapping[str, List[float]],
    min_n: int,
    # regime-bucket thresholds (LCB gate)
    min_edge_by_bucket: Mapping[str, float],
    alpha_by_bucket: Mapping[str, float],
    # safety
    require_lcb_gt0_for_non_a: bool = True,
) -> WinnerDecision:
    rb = regime_bucket(regime)
    alpha = float(alpha_by_bucket.get(rb, 0.10))
    min_edge = float(min_edge_by_bucket.get(rb, 0.05))

    st = compute_arm_stats(arm_to_r=arm_to_r, alpha=alpha)

    # baseline always A unless доказано обратное
    best_arm = "A"
    best_lcb = st["A"].lcb

    # Candidates: B/C if enough samples
    for arm in ("B", "C"):
        s = st[arm]
        if s.n < int(min_n):
            continue
        if require_lcb_gt0_for_non_a and (not (s.lcb > 0.0)):
            continue
        if s.lcb > best_lcb:
            best_lcb = s.lcb
            best_arm = arm

    if best_arm == "A":
        # Still, ensure A has any data; if not, keep A for safety.
        if st["A"].n < int(min_n):
            return WinnerDecision(winner="A", reason="insufficient_samples_all", stats=st)
        return WinnerDecision(winner="A", reason="baseline_or_no_lcb_advantage", stats=st)

    # Enforce edge threshold for switching away from A
    if best_lcb < min_edge:
        return WinnerDecision(winner="A", reason=f"lcb_below_min_edge:{rb}", stats=st)

    return WinnerDecision(winner=best_arm, reason=f"winner_by_lcb:{rb}", stats=st)


def aggregate_scenario_winners(
    *,
    regime: str,
    pooled: WinnerDecision,
    per_scn: Mapping[str, WinnerDecision],
    # "ещё выше" safety knobs
    require_same_winner_when_non_a: bool = True,
    # if scenarios disagree, allow non-A only when pooled LCB margin is very strong
    disagree_allow_margin_r: float = 0.18,
) -> WinnerDecision:
    """
    Политика объединения continuation/reversal:
      1) База: pooled winner (по всем событиям)
      2) Если pooled winner = A -> OK
      3) Если pooled winner = B/C:
         - если оба сценария доступны и согласны -> OK
         - если расходятся -> по умолчанию откат в A
           (кроме случая, когда pooled LCB настолько силён, что можно рискнуть)
      4) Доп. safety: победитель должен иметь non-negative LCB в каждом доступном сценарии.
    """
    w = pooled.winner
    if not is_non_a(w):
        pooled.meta = pooled.meta or {}
        pooled.meta["agg"] = {"mode": "pooled", "result": "A_or_none"}
        return pooled

    # Scenario winners
    cont = per_scn.get("continuation")
    rev = per_scn.get("reversal")

    # If no scenario splits -> accept pooled
    if cont is None and rev is None:
        pooled.meta = pooled.meta or {}
        pooled.meta["agg"] = {"mode": "pooled", "result": "no_scenarios"}
        return pooled

    # non-negative LCB in each scenario where we have enough stats
    for tag, dec in (("continuation", cont), ("reversal", rev)):
        if dec is None:
            continue
        st = dec.stats.get(w)
        if st is None:
            continue
        if not (st.lcb > 0.0):
            out = WinnerDecision(winner="A", reason=f"scenario_lcb_nonpos:{tag}", stats=pooled.stats, meta={"pooled": pooled.reason})
            return out

    # Agreement check
    winners = []
    if cont is not None:
        winners.append(cont.winner)
    if rev is not None:
        winners.append(rev.winner)
    agree = (len(winners) >= 2 and all(x == winners[0] for x in winners))

    if agree:
        pooled.meta = pooled.meta or {}
        pooled.meta["agg"] = {"mode": "scenario_agree", "winners": winners}
        return pooled

    if require_same_winner_when_non_a:
        # allow only if pooled LCB is huge
        a_lcb = float(pooled.stats["A"].lcb)
        w_lcb = float(pooled.stats[w].lcb)
        margin = w_lcb - a_lcb
        if margin >= float(disagree_allow_margin_r):
            pooled.meta = pooled.meta or {}
            pooled.meta["agg"] = {"mode": "scenario_disagree_but_margin_ok", "margin_r": margin, "winners": winners}
            return pooled
        return WinnerDecision(winner="A", reason="scenario_disagree_fallback_A", stats=pooled.stats, meta={"winners": winners})

    pooled.meta = pooled.meta or {}
    pooled.meta["agg"] = {"mode": "scenario_disagree_allowed", "winners": winners}
    return pooled


def hysteresis_should_publish(
    *,
    now_ms: int,
    prev_meta: Optional[Mapping[str, Any]],
    new_winner: WinnerDecision,
    hold_down_ms: int,
    # stronger requirement when switching A->(B/C)
    switch_min_margin_r: float,
) -> Tuple[bool, str]:
    """
    "Ещё выше": hold-down + hysteresis чтобы latest pointer не дёргался.

    Правило:
      - Если нет prev -> publish
      - Если winner не изменился -> publish (idempotent) OK
      - Если switch B/C -> A: разрешаем сразу, если новый=A (без hold-down) (safety)
      - Если switch A -> B/C: требуем:
          - now - prev.ts_ms >= hold_down_ms
          - margin_lcb = LCB(new) - LCB(A) >= switch_min_margin_r
    """
    if prev_meta is None:
        return True, "no_prev"
    prev_w = str(prev_meta.get("winner_arm") or prev_meta.get("winner") or "A").upper()
    if prev_w not in ARMS:
        prev_w = "A"
    cur_w = str(new_winner.winner).upper()

    if cur_w == prev_w:
        return True, "same_winner"

    # Safety-first: allow reverting to A immediately
    if cur_w == "A" and prev_w in ("B", "C"):
        return True, "revert_to_A"

    # For A -> B/C require hold-down and margin
    try:
        prev_ts = int(prev_meta.get("ts_ms") or 0)
    except Exception:
        prev_ts = 0
    if prev_ts > 0 and int(hold_down_ms) > 0 and (now_ms - prev_ts) < int(hold_down_ms):
        return False, "hold_down"

    a_lcb = float(new_winner.stats["A"].lcb)
    w_lcb = float(new_winner.stats[cur_w].lcb)
    if (w_lcb - a_lcb) < float(switch_min_margin_r):
        return False, "switch_margin_low"
    return True, "switch_ok"


def build_suggestion_sid(meta: Mapping[str, Any]) -> str:
    """
    Stable SID (content-based):
      - do NOT include ts_ms to avoid churn if winner unchanged
      - include key axes: symbol/regime/group + winner_arm + thresholds bucket
    """
    key = {
        "symbol": str(meta.get("symbol") or ""),
        "regime": str(meta.get("regime") or ""),
        "group": str(meta.get("group") or ""),
        "winner_arm": str(meta.get("winner_arm") or ""),
        "arm_ver": int(meta.get("arm_ver") or 0),
        "bucket": str(meta.get("bucket") or ""),
        "min_n": int(meta.get("min_n") or 0),
    }
    return _sha1(json.dumps(key, separators=(",", ":"), ensure_ascii=False))


def make_meta_payload(
    *,
    now_ms: int,
    symbol: str,
    regime: str,
    group: str,
    arm_ver: int,
    window_sec: int,
    min_n: int,
    decision: WinnerDecision,
    rbucket: str,
    min_edge: float,
    alpha: float,
) -> Dict[str, Any]:
    st = decision.stats
    meta = {
        "ts_ms": int(now_ms),
        "symbol": str(symbol).upper(),
        "regime": norm_regime(regime),
        "group": str(group).strip().lower(),
        "bucket": str(rbucket),
        "winner_arm": str(decision.winner),
        "arm_ver": int(arm_ver),
        "reason": str(decision.reason),
        "window_sec": int(window_sec),
        "min_n": int(min_n),
        "alpha": float(alpha),
        "min_edge_lcb": float(min_edge),
        "stats": {
            a: {"n": int(st[a].n), "mean": float(st[a].mean), "std": float(st[a].std), "lcb": float(st[a].lcb)}
            for a in ARMS
        },
        "schema": "entry_policy_ab_winner_meta_v1",
        "evaluator": "lcb_v1",
    }
    meta["sid"] = build_suggestion_sid(meta)
    return meta
