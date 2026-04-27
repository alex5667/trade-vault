from __future__ import annotations

from dataclasses import dataclass
from typing import Any
import os
import math

from handlers.confirmations.l2_common import sanitize_book, best_bid_ask, spread_bps, is_crossed, top_wall_notional


def _f(x: Any, default: float = 0.0) -> float:
    try:
        v = float(x)
        if not math.isfinite(v):
            return float(default)
        return v
    except Exception:
        return float(default)


@dataclass(frozen=True)
class L2Assessment:
    score01: float
    veto: bool
    flags: list[str]
    reason: str

    @property
    def l2_score01(self) -> float:
        return self.score01


class L2QualityPolicy:
    """
    "Совсем жёстко":
      - breakout/absorption: fail-closed (missing/stale/wide/crossed/тонкая книга => veto)
      - extreme: fail-open (missing/stale => не veto, но штраф)
    """

    def __init__(self, max_stale_ms: int | None = None) -> None:
        self.max_levels = int(os.getenv("L2_MAX_LEVELS", "32"))
        self.min_notional = float(os.getenv("L2_MIN_NOTIONAL", "250.0"))

        if max_stale_ms is not None:
            self.max_stale_ms_breakout = max_stale_ms
            self.max_stale_ms_absorption = max_stale_ms
            self.max_stale_ms_extreme = max_stale_ms
        else:
            self.max_stale_ms_breakout = int(os.getenv("L2_MAX_STALE_MS_BREAKOUT", "250"))
            self.max_stale_ms_absorption = int(os.getenv("L2_MAX_STALE_MS_ABSORPTION", "400"))
            self.max_stale_ms_extreme = int(os.getenv("L2_MAX_STALE_MS_EXTREME", "900"))

        self.max_spread_bps_breakout = float(os.getenv("L2_MAX_SPREAD_BPS_BREAKOUT", "8.0"))
        self.max_spread_bps_absorption = float(os.getenv("L2_MAX_SPREAD_BPS_ABSORPTION", "12.0"))
        self.max_spread_bps_extreme = float(os.getenv("L2_MAX_SPREAD_BPS_EXTREME", "25.0"))

        self.min_top_wall_notional = float(os.getenv("L2_MIN_TOP_WALL_NOTIONAL", "5000.0"))

        self.min_levels_breakout = int(os.getenv("L2_MIN_LEVELS_BREAKOUT", "3"))
        self.min_levels_absorption = int(os.getenv("L2_MIN_LEVELS_ABSORPTION", "4"))
        self.min_levels_extreme = int(os.getenv("L2_MIN_LEVELS_EXTREME", "2"))

        # базовые штрафы fail-open (extreme)
        self.extreme_missing_score = float(os.getenv("L2_EXTREME_MISSING_SCORE01", "0.30"))
        self.extreme_stale_score = float(os.getenv("L2_EXTREME_STALE_SCORE01", "0.35"))

        # Fail-open penalty scores (extracted from hardcoded values in assess())
        self.thin_book_score = float(os.getenv("L2_THIN_BOOK_SCORE01", "0.45"))
        self.crossed_book_score = float(os.getenv("L2_CROSSED_BOOK_SCORE01", "0.35"))
        self.no_spread_score = float(os.getenv("L2_NO_SPREAD_SCORE01", "0.40"))
        self.no_wall_score = float(os.getenv("L2_NO_WALL_SCORE01", "0.55"))
        self.wide_spread_floor = float(os.getenv("L2_WIDE_SPREAD_FLOOR01", "0.15"))
        self.spread_penalty_fraction = float(os.getenv("L2_SPREAD_PENALTY_FRACTION", "0.35"))

    def _limits(self, kind: str) -> tuple[int, float, float, int]:
        k = (kind or "").lower()
        if k == "absorption":
            return self.max_stale_ms_absorption, self.max_spread_bps_absorption, self.min_levels_absorption, 0
        if k == "extreme":
            return self.max_stale_ms_extreme, self.max_spread_bps_extreme, self.min_levels_extreme, 1
        # дефолт breakout
        return self.max_stale_ms_breakout, self.max_spread_bps_breakout, self.min_levels_breakout, 0

    def assess(self, *, kind: str, ctx: Any, l2: Any | None) -> L2Assessment:
        flags: list[str] = []
        k = (kind or "").lower()
        max_stale_ms, max_spread_bps, min_levels, fail_open = self._limits(k)

        ts_event = _f(getattr(ctx, "ts", None), 0.0)
        if not ts_event:
            # без ts — считаем, что не можем оценить стейл (для breakout/abs => fail-closed)
            flags.append("ctx_no_ts")
            if fail_open:
                return L2Assessment(self.extreme_missing_score, False, flags, "ctx_no_ts_fail_open")
            return L2Assessment(0.0, True, flags, "ctx_no_ts_fail_closed")

        if l2 is None:
            flags.append("l2_missing")
            if fail_open:
                return L2Assessment(self.extreme_missing_score, False, flags, "l2_missing_fail_open")
            return L2Assessment(0.0, True, flags, "l2_missing_fail_closed")

        ts_l2_raw = getattr(l2, "ts_ms", None)
        ts_l2 = _f(ts_l2_raw, 0.0)
        if not ts_l2:
            if ts_l2_raw is not None:
                flags.append("l2_bad_ts")
            else:
                flags.append("l2_no_ts")
            if fail_open:
                return L2Assessment(self.extreme_missing_score, False, flags, "l2_no_ts_fail_open")
            return L2Assessment(0.0, True, flags, "l2_no_ts_fail_closed")

        stale = abs(ts_event - ts_l2)
        if stale > float(max_stale_ms):
            flags.append("l2_stale")
            if fail_open:
                return L2Assessment(self.extreme_stale_score, False, flags, "l2_stale_fail_open")
            return L2Assessment(0.0, True, flags, "l2_stale_fail_closed")

        bids = sanitize_book(getattr(l2, "bids", None) or [], max_levels=self.max_levels, min_notional=self.min_notional)
        asks = sanitize_book(getattr(l2, "asks", None) or [], max_levels=self.max_levels, min_notional=self.min_notional)

        if len(bids) < min_levels or len(asks) < min_levels:
            flags.append("l2_thin")
            if fail_open:
                return L2Assessment(self.thin_book_score, False, flags, "l2_thin_fail_open")
            return L2Assessment(0.0, True, flags, "l2_thin_fail_closed")

        bb, ba = best_bid_ask(bids, asks)
        if is_crossed(bb, ba):
            flags.append("l2_crossed")
            if fail_open:
                return L2Assessment(self.crossed_book_score, False, flags, "l2_crossed_fail_open")
            return L2Assessment(0.0, True, flags, "l2_crossed_fail_closed")

        sp = spread_bps(bb, ba)
        if sp is None:
            flags.append("l2_no_spread")
            if fail_open:
                return L2Assessment(self.no_spread_score, False, flags, "l2_no_spread_fail_open")
            return L2Assessment(0.0, True, flags, "l2_no_spread_fail_closed")

        if sp > float(max_spread_bps):
            flags.append("l2_wide_spread")
            if fail_open:
                # не veto, но сильный штраф
                score = max(self.wide_spread_floor, 1.0 - float(sp) / max(float(max_spread_bps) * 3.0, 1e-9))
                return L2Assessment(float(score), False, flags, "l2_wide_spread_fail_open")
            return L2Assessment(0.0, True, flags, "l2_wide_spread_fail_closed")

        # "есть ли жизнь в книге": топовая "стена" должна быть хотя бы какая-то
        wall_n = max(top_wall_notional(bids, max_scan=self.max_levels), top_wall_notional(asks, max_scan=self.max_levels))
        if wall_n < self.min_top_wall_notional:
            flags.append("l2_no_wall")
            if fail_open:
                return L2Assessment(self.no_wall_score, False, flags, "l2_no_wall_fail_open")
            return L2Assessment(0.0, True, flags, "l2_no_wall_fail_closed")

        soft = max(0.0, min(1.0, 1.0 - (float(sp) / max(float(max_spread_bps), 1e-9)) * self.spread_penalty_fraction))
        return L2Assessment(float(soft), False, flags, "l2_ok")

    def missing_rate(self) -> float:
        """Stub for tests."""
        return 0.0


def apply_l2_policy_to_ctx(ctx: Any, kind: str, a: L2Assessment, missing_rate: float = 0.0) -> None:
    """
    Apply L2Assessment results to a generic Context object.
    Matches the signature expected by test_l2_quality_policy_4_1.py.
    """
    ctx.data_quality_flags = a.flags
    ctx.l2_score01 = a.score01
    ctx.l2_missing_rate = missing_rate
    if a.veto:
        if hasattr(ctx, "veto_reasons"):
            ctx.veto_reasons.append(a.reason)
        elif hasattr(ctx, "veto_reason"):
            ctx.veto_reason = a.reason
