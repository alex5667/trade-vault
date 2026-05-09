from __future__ import annotations

import logging
import os
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)

from ..types.crypto_orderflow_pipeline_types import Candidate, QualityState
from .crypto_orderflow_confirmations import L2ConfirmAbsorption, L2ConfirmBreakout


class Validator:
    def validate(self, ctx: Any, cand: Candidate, q: QualityState) -> None:  # pragma: no cover
        raise NotImplementedError


@dataclass(frozen=True)
class CompositeValidator:
    validators: list[Validator]

    def validate(self, ctx: Any, cand: Candidate) -> QualityState:
        q = QualityState()
        for v in self.validators:
            if q.veto:
                break
            v.validate(ctx, cand, q)
        return q


@dataclass(frozen=True)
class SpreadValidator(Validator):
    spread_max_bps: float

    def validate(self, ctx: Any, cand: Candidate, q: QualityState) -> None:
        mx = float(self.spread_max_bps or 0.0)
        if mx <= 0.0:
            q.add_flag("spread_filter", False)
            return
        sp = float(getattr(ctx, "spread_bps", 0.0) or 0.0)
        q.add_flag("spread_bps", sp)
        if sp > mx:
            q.veto_with(f"spread>{mx:.2f}bps")


@dataclass(frozen=True)
class MinIntervalValidator(Validator):
    min_interval_ms: int

    def validate(self, ctx: Any, cand: Candidate, q: QualityState) -> None:
        if int(self.min_interval_ms) <= 0:
            return
        last_ts = int(getattr(ctx, "_last_signal_ts_ms", getattr(ctx, "last_signal_ts", 0)) or 0)
        ts = int(getattr(ctx, "ts", 0) or 0)
        if ts > 0 and last_ts > 0 and (ts - last_ts) < int(self.min_interval_ms):
            q.veto_with("min_interval")


class PivotsPresentValidator(Validator):
    def validate(self, ctx: Any, cand: Candidate, q: QualityState) -> None:
        piv = getattr(ctx, "pivots", None)
        if not piv:
            q.veto_with("no_pivots")


class ModeValidator(Validator):
    """
    Режим — это качество/контекст рынка, а не событие.
    """

    def validate(self, ctx: Any, cand: Candidate, q: QualityState) -> None:
        mode = str(getattr(ctx, "market_mode", "mixed") or "mixed").lower()
        q.add_flag("market_mode", mode)

        from common.market_mode import is_range_regime
        _is_range = is_range_regime(mode)

        z_abs = abs(float(getattr(ctx, "z_delta", 0.0) or 0.0))
        # breakout: в range требуем сильнее (минимальная защита)
        if cand.kind == "breakout" and _is_range:
            thr = float(getattr(ctx, "_breakout_thr", 0.0) or 0.0)
            if thr > 0 and z_abs < (thr * 1.2):
                q.veto_with("breakout_in_range_requires_stronger_z")

        # absorption: в momentum часто плохо (veto как качество)
        if cand.kind == "absorption" and mode == "momentum":
            q.veto_with("absorption_in_momentum")

        # extreme: в range требуем чуть сильнее (как было раньше)
        if cand.kind == "extreme" and _is_range:
            thr = float(getattr(ctx, "_extreme_thr", 0.0) or 0.0)
            if thr > 0 and z_abs < (thr * 1.15):
                q.veto_with("extreme_in_range_requires_stronger_z")


@dataclass(frozen=True)
class RegimeGateValidator(Validator):
    regime_gate: Any
    regime_allows_fn: Callable[[str, float, Any], bool]

    def validate(self, ctx: Any, cand: Candidate, q: QualityState) -> None:
        rscore = float(getattr(ctx, "regime_score", 0.0) or 0.0)
        q.add_flag("regime_score", rscore)
        try:
            ok = bool(self.regime_allows_fn(str(cand.kind), rscore, self.regime_gate))
        except Exception:
            ok = True  # fail-open (открыто при ошибке) на этом слое; ниже скоринг всё равно отсечет
        if not ok:
            q.veto_with("regime_gate")


@dataclass(frozen=True)
class OBIBreakoutValidator(Validator):
    require_obi: bool
    require_obi20: bool

    def validate(self, ctx: Any, cand: Candidate, q: QualityState) -> None:
        if cand.kind != "breakout":
            return
        z = float(getattr(ctx, "z_delta", 0.0) or 0.0)
        obi_sustained = bool(getattr(ctx, "obi_sustained", False))
        obi_avg = float(getattr(ctx, "obi_avg", 0.0) or 0.0)
        obi_confirms = bool(obi_sustained) and (obi_avg * z > 0.0)
        q.add_flag("obi_confirms", obi_confirms)

        # базовая проверка: require_obi
        if bool(self.require_obi) and not obi_confirms:
            q.veto_with("breakout_requires_obi")
            return

        # require_obi20 (если включено) подтверждаем 20с sustained + знак
        if bool(self.require_obi20):
            if not bool(getattr(ctx, "obi_sustained_20", False)):
                q.veto_with("breakout_requires_obi20_sustained")
                return
            s = float(getattr(ctx, "obi_avg_20", 0.0) or 0.0)
            if s * (1.0 if z > 0 else -1.0) <= 0.0:
                q.veto_with("breakout_requires_obi20_sign")
                return


class OBIFadeValidator(Validator):
    """
    Absorption — это fade (затухание): veto если OBI подтверждает импульс (как раньше).
    """

    def validate(self, ctx: Any, cand: Candidate, q: QualityState) -> None:
        if cand.kind != "absorption":
            return
        z = float(getattr(ctx, "z_delta", 0.0) or 0.0)
        obi_sustained = bool(getattr(ctx, "obi_sustained", False))
        obi_avg = float(getattr(ctx, "obi_avg", 0.0) or 0.0)
        obi_confirms = bool(obi_sustained) and (obi_avg * z > 0.0)
        q.add_flag("obi_confirms", obi_confirms)
        if obi_confirms:
            q.veto_with("absorption_fade_but_obi_confirms_impulse")


@dataclass(frozen=True)
class L2BreakoutValidator(Validator):
    confirmer: L2ConfirmBreakout

    def validate(self, ctx: Any, cand: Candidate, q: QualityState) -> None:
        if cand.kind != "breakout":
            return
        dir_up = bool(float(getattr(ctx, "z_delta", 0.0) or 0.0) > 0.0)
        ok, details = self.confirmer.check(ctx, dir_up=dir_up)
        q.add_flag("l2_breakout", details)
        if not ok:
            q.veto_with("l2_breakout")


@dataclass(frozen=True)
class L2AbsorptionValidator(Validator):
    confirmer: L2ConfirmAbsorption

    def validate(self, ctx: Any, cand: Candidate, q: QualityState) -> None:
        if cand.kind != "absorption":
            return
        dir_up = bool(float(getattr(ctx, "z_delta", 0.0) or 0.0) > 0.0)
        ok, details = self.confirmer.check(ctx, dir_up=dir_up)
        q.add_flag("l2_absorption", details)
        if not ok:
            q.veto_with("l2_absorption")


class ExtremeOptionalFiltersValidator(Validator):
    """
    Перенос "опциональных" фильтров extreme (spread/impact/wall/L3) в quality-слой.
    """

    def validate(self, ctx: Any, cand: Candidate, q: QualityState) -> None:
        if cand.kind != "extreme":
            return

        if os.getenv("EXTREME_USE_L2_FILTERS", "false").lower() != "true":
            return

        spread_max = float(os.getenv("EXTREME_MAX_SPREAD_BPS", "15.0"))
        impact_max = float(os.getenv("EXTREME_MAX_IMPACT_PROXY", "0.5"))

        sp = float(getattr(ctx, "spread_bps", 0.0) or 0.0)
        imp = float(getattr(ctx, "impact_proxy", 0.0) or 0.0)
        q.add_flag("extreme_spread_bps", sp)
        q.add_flag("extreme_impact_proxy", imp)
        if sp > spread_max:
            q.veto_with("extreme_spread")
            return
        if imp > impact_max:
            q.veto_with("extreme_impact")
            return

        if os.getenv("EXTREME_CHECK_WALL", "false").lower() == "true":
            dir_up = bool(float(getattr(ctx, "z_delta", 0.0) or 0.0) > 0.0)
            wall_max = float(os.getenv("EXTREME_WALL_MAX_DIST_BPS", "15.0"))
            if dir_up and bool(getattr(ctx, "wall_ask", False)) and float(getattr(ctx, "wall_ask_dist_bps", 0.0) or 0.0) <= wall_max:
                q.veto_with("extreme_wall_ask")
                return
            if (not dir_up) and bool(getattr(ctx, "wall_bid", False)) and float(getattr(ctx, "wall_bid_dist_bps", 0.0) or 0.0) <= wall_max:
                q.veto_with("extreme_wall_bid")
                return


class L3OptionalValidator(Validator):
    """
    Обобщенный облегченный L3-veto (если включено через ENV для конкретных видов).
    """

    def validate(self, ctx: Any, cand: Candidate, q: QualityState) -> None:
        if cand.kind not in ("breakout", "extreme", "absorption"):
            return
        # breakout/extreme используют разные ENV, сохраняем совместимость:
        if cand.kind == "breakout" and os.getenv("BREAKOUT_USE_L3_FILTERS", "false").lower() != "true":
            return
        if cand.kind == "extreme" and os.getenv("EXTREME_USE_L3_FILTERS", "false").lower() != "true":
            return
        if cand.kind == "absorption" and os.getenv("ABSORPTION_USE_L3_FILTERS", "true").lower() != "true":
            return

        dir_up = bool(float(getattr(ctx, "z_delta", 0.0) or 0.0) > 0.0)
        if dir_up:
            ctr = float(getattr(ctx, "cancel_to_trade_ask", 0.0) or 0.0)
            rate = float(getattr(ctx, "taker_buy_rate_ema", 0.0) or 0.0)
        else:
            ctr = float(getattr(ctx, "cancel_to_trade_bid", 0.0) or 0.0)
            rate = float(getattr(ctx, "taker_sell_rate_ema", 0.0) or 0.0)

        # значения берем по виду сигнала (совместимо с текущими ENV)
        if cand.kind == "breakout":
            ctr_max = float(os.getenv("BREAKOUT_L3_MAX_CANCEL_TO_TRADE", "3.0"))
            rate_min = float(os.getenv("BREAKOUT_L3_MIN_TAKER_RATE", "0.0"))
        elif cand.kind == "extreme":
            ctr_max = float(os.getenv("EXTREME_L3_MAX_CANCEL_TO_TRADE", "6.0"))
            rate_min = float(os.getenv("EXTREME_L3_MIN_TAKER_RATE", "0.0"))
        else:
            ctr_max = float(os.getenv("ABSORPTION_L3_MAX_CANCEL_TO_TRADE", "0.0"))
            rate_min = float(os.getenv("ABSORPTION_L3_MIN_TAKER_RATE", "0.0"))

        q.add_flag("l3_ctr", ctr)
        q.add_flag("l3_rate", rate)
        if ctr_max > 0 and ctr >= ctr_max and (rate_min <= 0 or rate < rate_min):
            q.veto_with("l3_cancel_to_trade")
            return
        if rate_min > 0 and rate < rate_min:
            q.veto_with("l3_rate")
            return


@dataclass(frozen=True)
class TouchVetoValidator(Validator):
    """
    Перенос части touch-filter в quality-слой:
    - если очевидный veto (refill на стороне пробоя) — не вызывать publish вообще.
    - счётчики suppressed сохраняем через handler.
    """
    handler: Any

    def validate(self, ctx: Any, cand: Candidate, q: QualityState) -> None:
        h = self.handler
        if not getattr(h, "touch_filter_enabled", False):
            return
        kinds = getattr(h, "touch_filter_kinds", set()) or set()
        if str(cand.kind) not in kinds:
            return
        if bool(getattr(ctx, "touch_is_stale", True)):
            return

        side = cand.side  # LONG/SHORT по знаку raw_score (уровень события)

        ask_tag = str(getattr(ctx, "touch_ask_tag", "none"))
        bid_tag = str(getattr(ctx, "touch_bid_tag", "none"))

        if side == "LONG" and ask_tag == "refill":
            # статистика подавлений
            try:
                h._touch_suppressed_total += 1
                h._touch_suppressed_by_kind[str(cand.kind)] = h._touch_suppressed_by_kind.get(str(cand.kind), 0) + 1
            except Exception:
                logger.debug("touch stats update failed", exc_info=True)
            q.veto_with("touch_refill_ask")
            return

        if side == "SHORT" and bid_tag == "refill":
            try:
                h._touch_suppressed_total += 1
                h._touch_suppressed_by_kind[str(cand.kind)] = h._touch_suppressed_by_kind.get(str(cand.kind), 0) + 1
            except Exception:
                logger.debug("touch stats update failed", exc_info=True)
            q.veto_with("touch_refill_bid")
            return
