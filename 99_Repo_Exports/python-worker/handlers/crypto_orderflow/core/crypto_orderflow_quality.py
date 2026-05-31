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


def _validate_breakout_in_range(ctx: Any, z: float, z_abs: float, q: "QualityState") -> None:
    """
    breakout в range/squeeze: allow_if_squeeze_expansion_confirmed.

    REGIME_SQUEEZE_BREAKOUT_ENABLED=0  → legacy z*1.2 guard (default/shadow).
    REGIME_SQUEEZE_BREAKOUT_ENABLED=1  → multi-condition squeeze-expansion gate:
      1. z_abs >= _breakout_thr + Z_BOOST   (сигнально сильнее обычного порога)
      2. OBI stable (obi_sustained OR lob_dw_obi_stable)
      3. microprice_shift подтверждает направление
      4. spread_bps <= MAX_SPREAD_BPS
      5. нет stale book / DQ-флага
      6. (опционально) touch_tag ∈ {depletion, strong_ofi, absorption, depletion_ofi}
    """
    thr = float(getattr(ctx, "_breakout_thr", 0.0) or 0.0)
    enabled = os.getenv("REGIME_SQUEEZE_BREAKOUT_ENABLED", "0").strip() == "1"

    if not enabled:
        if thr > 0 and z_abs < thr * 1.2:
            q.veto_with("breakout_in_range_requires_stronger_z")
        return

    # ── 1. z-threshold: base_thr + absolute boost ─────────────────────
    z_boost = float(os.getenv("REGIME_SQUEEZE_BREAKOUT_Z_BOOST", "0.5"))
    z_min = (thr + z_boost) if thr > 0 else z_boost
    if z_abs < z_min:
        q.veto_with("squeeze_breakout_z_weak")
        return

    # ── 2. OBI stable ─────────────────────────────────────────────────
    obi_stable = bool(getattr(ctx, "obi_sustained", False)) or bool(
        getattr(ctx, "lob_dw_obi_stable", False)
    )
    q.add_flag("squeeze_obi_stable", obi_stable)
    if not obi_stable:
        q.veto_with("squeeze_breakout_obi_not_stable")
        return

    # ── 3. microprice_shift подтверждает направление ───────────────────
    direction = 1.0 if z > 0 else -1.0
    mps = float(getattr(ctx, "microprice_shift", 0.0) or 0.0)
    q.add_flag("squeeze_microprice_shift", mps)
    if mps * direction <= 0:
        q.veto_with("squeeze_breakout_microprice_no_confirm")
        return

    # ── 4. spread <= нормальная полоса символа ────────────────────────
    max_spread = float(os.getenv("REGIME_SQUEEZE_BREAKOUT_MAX_SPREAD_BPS", "10.0"))
    sp = float(getattr(ctx, "spread_bps", 0.0) or 0.0)
    q.add_flag("squeeze_spread_bps", sp)
    if sp > max_spread:
        q.veto_with("squeeze_breakout_spread_wide")
        return

    # ── 5. нет stale book / DQ-флага ──────────────────────────────────
    if bool(getattr(ctx, "book_is_stale", False)):
        q.veto_with("squeeze_breakout_stale_book")
        return
    if bool(getattr(ctx, "dq_flag_stale", False)) or bool(
        getattr(ctx, "data_quality_flag", False)
    ):
        q.veto_with("squeeze_breakout_dq_flag")
        return

    # ── 6. (опционально) touch_tag = depletion / strong OFI ───────────
    if os.getenv("REGIME_SQUEEZE_BREAKOUT_REQUIRE_TOUCH_DEPLETION", "0").strip() == "1":
        tag_attr = "touch_ask_tag" if z > 0 else "touch_bid_tag"
        tag = str(getattr(ctx, tag_attr, "none") or "none").lower()
        _GOOD_TAGS = {"depletion", "strong_ofi", "absorption", "depletion_ofi"}
        q.add_flag("squeeze_touch_tag", tag)
        if tag not in _GOOD_TAGS:
            q.veto_with("squeeze_breakout_touch_not_depletion")
            return

    q.add_flag("squeeze_expansion_breakout", True)


class ModeValidator(Validator):
    """
    Режим — это качество/контекст рынка, а не событие.
    """

    def validate(self, ctx: Any, cand: Candidate, q: QualityState) -> None:
        mode = str(getattr(ctx, "market_mode", "mixed") or "mixed").lower()
        q.add_flag("market_mode", mode)

        from common.market_mode import is_range_regime
        _is_range = is_range_regime(mode)

        z = float(getattr(ctx, "z_delta", 0.0) or 0.0)
        z_abs = abs(z)

        # breakout: в range/squeeze — squeeze expansion gate (или legacy z*1.2)
        if cand.kind == "breakout" and _is_range:
            _validate_breakout_in_range(ctx, z, z_abs, q)

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
    # OR-gate thresholds (used when require_obi20=True)
    obi20_alt_stable_secs: float = 2.0   # obi_stable_secs arm
    obi20_alt_ofi_ml_min: float = 0.3    # ofi_ml_norm arm

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

        # require_obi20 — OR-gate: pass if any arm confirms
        if bool(self.require_obi20):
            obi20_sustained = bool(getattr(ctx, "obi_sustained_20", False))
            obi_avg_20 = float(getattr(ctx, "obi_avg_20", 0.0) or 0.0)
            arm_obi20 = obi20_sustained and obi_avg_20 * (1.0 if z > 0 else -1.0) > 0.0

            stable_secs = float(
                getattr(ctx, "obi_stable_secs", None)
                or getattr(ctx, "lob_dw_obi_stable_secs", 0.0)
                or 0.0
            )
            arm_stable = stable_secs >= self.obi20_alt_stable_secs

            ofi_ml = float(getattr(ctx, "ofi_ml_norm", 0.0) or 0.0)
            arm_ofi_ml = ofi_ml * (1.0 if z > 0 else -1.0) >= self.obi20_alt_ofi_ml_min

            if not (arm_obi20 or arm_stable or arm_ofi_ml):
                q.veto_with("breakout_requires_obi20_no_alternative")
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

        side = cand.side  # LONG/SHORT по знаку raw_score (уровень события)  # type: ignore

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
