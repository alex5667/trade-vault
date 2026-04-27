from __future__ import annotations

from dataclasses import dataclass
from typing import Any
import os
import math


def _f(x: Any, default: float = 0.0) -> float:
    try:
        v = float(x)
        if not math.isfinite(v):
            return float(default)
        return v
    except Exception:
        return float(default)


@dataclass(frozen=True)
class L3Assessment:
    score01: float
    flags: list[str]
    reason: str


class L3QualityPolicy:
    """
    Политика 4.1:
      - L3 недоступен => не veto, score01=0.5 + флаг l3_missing + метрика l3_missing_rate (метрика в handler'е)
      - L3 stale => score01=0.35
    """

    def __init__(self) -> None:
        self.max_stale_ms = int(os.getenv("L3_MAX_STALE_MS", "700"))
        self.neutral_score = float(os.getenv("L3_MISSING_SCORE01", "0.50"))
        self.stale_score = float(os.getenv("L3_STALE_SCORE01", "0.35"))
        # анти-спуф: высокий cancel_to_trade => штраф
        self.cancel_to_trade_bad = float(os.getenv("L3_CANCEL_TO_TRADE_BAD", "4.0"))

    def assess(self, *, ctx: Any, l3: Any | None) -> L3Assessment:
        flags: list[str] = []
        ts = _f(getattr(ctx, "ts", None), 0.0)
        if l3 is None:
            flags.append("l3_missing")
            return L3Assessment(self.neutral_score, flags, "l3_missing_neutral")

        ts_l3 = _f(getattr(l3, "ts_ms", None), 0.0)
        if not ts_l3 or not ts:
            flags.append("l3_no_ts")
            return L3Assessment(self.neutral_score, flags, "l3_no_ts_neutral")

        if abs(ts - ts_l3) > float(self.max_stale_ms):
            flags.append("l3_stale")
            return L3Assessment(self.stale_score, flags, "l3_stale_penalty")

        # используем уже проставленные в ctx агрегаты (сам l3 объект может отсутствовать в legacy)
        c2t = max(
            _f(getattr(ctx, "cancel_to_trade_bid_5s", None), 0.0),
            _f(getattr(ctx, "cancel_to_trade_ask_5s", None), 0.0),
            _f(getattr(ctx, "cancel_to_trade_bid_20s", None), 0.0),
            _f(getattr(ctx, "cancel_to_trade_ask_20s", None), 0.0),
        )
        sp = _f(getattr(ctx, "spread_bps", None), 0.0)

        score = 1.0
        if c2t >= self.cancel_to_trade_bad:
            flags.append("l3_high_cancel_to_trade")
            # жёстче: быстро уводим score к 0.35..0.7
            score *= max(0.35, 1.0 - min(1.0, (c2t - self.cancel_to_trade_bad) / max(self.cancel_to_trade_bad, 1e-9)) * 0.55)

        if sp > 0:
            # чуть-чуть штрафуем за спред (не veto)
            score *= max(0.55, 1.0 - min(1.0, sp / 30.0) * 0.25)

        return L3Assessment(float(max(0.0, min(1.0, score))), flags, "l3_ok")


def apply_l3_policy_to_ctx(*, ctx: Any, assessment: L3Assessment) -> None:
    """
    Применяет флаги оценки L3 к ctx.data_quality_flags.
    """
    try:
        arr = getattr(ctx, "data_quality_flags", None)
        if arr is None:
            setattr(ctx, "data_quality_flags", [])
            arr = getattr(ctx, "data_quality_flags")
        if isinstance(arr, list):
            for f in assessment.flags:
                if f not in arr:
                    arr.append(f)
    except Exception:
        return