# -*- coding: utf-8 -*-
"""
ATR Sanity Selector
==================
Selects the best ATR candidate among multiple Redis sources by:
  1) freshness (age_ms)
  2) TF consistency (candidate.tf == desired_tf when available)
  3) value consistency vs expected_atr (jump penalty)

Design goals:
  - deterministic: uses now_ms passed from caller (tick/signal time)
  - fail-open: if anything fails, caller can fall back to previous behavior
  - explainable: returns selection reason and cost components
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
import math
import os


def _clamp(x: float, lo: float, hi: float) -> float:
    return lo if x < lo else hi if x > hi else x


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        x = float(v)
        if not math.isfinite(x):
            return default
        return x
    except Exception:
        return default


def _safe_int(v: Any, default: int = 0) -> int:
    try:
        return int(float(v))
    except Exception:
        return default


@dataclass
class AtrCandidate:
    atr: float
    source: str
    key: str
    tf: str
    ts_ms: int = 0
    age_ms: int = 0
    extra: Dict[str, Any] = None

    def __post_init__(self) -> None:
        if self.extra is None:
            self.extra = {}


@dataclass
class AtrSelectResult:
    chosen: Optional[AtrCandidate]
    reason: str
    debug: Dict[str, Any]


class AtrSanitySelector:
    """
    Cost-based selector:
      cost = w_age * age_norm
           + w_tf  * tf_mismatch
           + w_jump * jump_norm
           + w_nots * no_ts_penalty
           + w_stale * stale_penalty

    Lower cost is better.
    """

    def __init__(self) -> None:
        # Tunables (env override; safe defaults)
        self.age_ref_ms = _safe_int(os.getenv("ATR_SANITY_AGE_REF_MS", "60000"), 60000)   # 60s
        self.max_age_ms = _safe_int(os.getenv("ATR_SANITY_MAX_AGE_MS", "900000"), 900000) # 15m
        self.jump_cap = _safe_float(os.getenv("ATR_SANITY_JUMP_CAP", "10.0"), 10.0)      # 10x jump cap

        self.w_age = _safe_float(os.getenv("ATR_SANITY_W_AGE", "1.0"), 1.0)
        self.w_tf = _safe_float(os.getenv("ATR_SANITY_W_TF", "3.0"), 3.0)
        self.w_jump = _safe_float(os.getenv("ATR_SANITY_W_JUMP", "2.0"), 2.0)
        self.w_nots = _safe_float(os.getenv("ATR_SANITY_W_NO_TS", "2.0"), 2.0)
        self.w_stale = _safe_float(os.getenv("ATR_SANITY_W_STALE", "4.0"), 4.0)

        # Slight preference ordering if costs are close (smaller is better)
        self.source_bias = {
            "tracker": -0.15,     # ATR:{sym}:{TF}
            "ta_last": -0.05,     # ta:last:atr:{sym}
            "atr_json": 0.00,     # atr:json:{sym}:{tf}
            "atr_str":  0.10,     # atr:{sym}:{tf} string (no ts)
            "atr_val":  0.12,     # atr:val:* mirror
            "unknown":  0.20,
        }

    def _cost(
        self,
        c: AtrCandidate,
        *,
        desired_tf: str,
        expected_atr: float,
    ) -> Tuple[float, Dict[str, Any]]:
        atr = float(c.atr or 0.0)
        if atr <= 0 or not math.isfinite(atr):
            return 1e9, {"bad": 1}

        age_ms = int(c.age_ms or 0)
        has_ts = 1 if int(c.ts_ms or 0) > 0 else 0
        no_ts = 0 if has_ts == 1 else 1

        age_norm = 0.0
        stale_pen = 0.0
        if has_ts == 1:
            age_norm = float(age_ms) / float(max(1, self.age_ref_ms))
            age_norm = _clamp(age_norm, 0.0, 5.0)
            if age_ms > self.max_age_ms:
                stale_pen = 1.0
        else:
            # If no timestamp -> treat as "old-ish" but not fatal
            age_norm = 1.0

        tf_mismatch = 0.0
        if desired_tf and c.tf:
            tf_mismatch = 0.0 if str(c.tf).upper() == str(desired_tf).upper() else 1.0

        jump_norm = 0.0
        if expected_atr and expected_atr > 0:
            jump_norm = abs(atr - expected_atr) / float(max(1e-12, expected_atr))
            jump_norm = _clamp(jump_norm, 0.0, float(self.jump_cap))
        # else: no expected -> no jump penalty

        base = (
            self.w_age * age_norm
            + self.w_tf * tf_mismatch
            + self.w_jump * jump_norm
            + self.w_nots * float(no_ts)
            + self.w_stale * float(stale_pen)
        )
        base += float(self.source_bias.get(str(c.source or "unknown"), 0.2))

        dbg = {
            "age_ms": age_ms,
            "age_norm": age_norm,
            "tf_mismatch": tf_mismatch,
            "jump_norm": jump_norm,
            "no_ts": no_ts,
            "stale": int(stale_pen),
            "bias": float(self.source_bias.get(str(c.source or "unknown"), 0.2)),
        }
        return float(base), dbg

    def choose(
        self,
        candidates: List[AtrCandidate],
        *,
        desired_tf: str,
        now_ms: int,
        expected_atr: float = 0.0,
    ) -> AtrSelectResult:
        if not candidates:
            return AtrSelectResult(chosen=None, reason="no_candidates", debug={})

        # Ensure age_ms is computed deterministically from now_ms.
        for c in candidates:
            try:
                ts = int(c.ts_ms or 0)
                if ts > 0 and now_ms > 0:
                    c.age_ms = int(max(0, now_ms - ts))
                else:
                    c.age_ms = int(c.age_ms or 0)
            except Exception:
                c.age_ms = int(c.age_ms or 0)

        best: Optional[AtrCandidate] = None
        best_cost = 1e18
        debug_rows: List[Dict[str, Any]] = []

        for c in candidates:
            cost, dbg = self._cost(c, desired_tf=desired_tf, expected_atr=float(expected_atr or 0.0))
            row = {
                "source": c.source,
                "key": c.key,
                "tf": c.tf,
                "atr": float(c.atr),
                "ts_ms": int(c.ts_ms or 0),
                "age_ms": int(c.age_ms or 0),
                "cost": float(cost),
                **dbg,
            }
            debug_rows.append(row)
            if cost < best_cost:
                best_cost = cost
                best = c

        debug_rows.sort(key=lambda x: float(x.get("cost", 1e18)))
        reason = "selected"
        if best is None:
            reason = "no_valid_candidate"

        return AtrSelectResult(
            chosen=best,
            reason=reason,
            debug={"best_cost": float(best_cost), "candidates": debug_rows},
        )
