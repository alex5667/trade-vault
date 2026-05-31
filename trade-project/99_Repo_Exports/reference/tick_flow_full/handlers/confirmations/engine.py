from __future__ import annotations

import math
import os
from dataclasses import dataclass, field
from typing import Any, Optional

import inspect
from common.qf_codes import QF
from signal_scoring.reason_registry import normalize_reason, reason_code_to_u16
from common.u16_pack import pack_u16_list
from common.math_safe import safe_float, clamp01, finite_or
from common.feature_flags import FeatureFlagsManager, FeatureFlagsSnapshot
from common.metrics import METRICS


@dataclass(frozen=True)
class Validation:
    veto: bool
    conf_factor01: float      # 0..1
    flags: list[int] = field(default_factory=list)
    reason: str = ""          # human/debug string (читаемая строка)
    parts: dict[str, float] = field(default_factory=dict)
    reason_code: str = ""     # structured enum string (структурированный код)
    reason_u16: int = 0       # compact stable wire id (компактный стабильный ID)
    # -------------------------------------------------------------------------
    # outcome / коды решения
    #
    # "reason_code/reason_u16" остаются SEMANTICALLY "veto reason" (если veto=True),
    # чтобы не ломать существующие ожидания downstream.
    #
    # Для статистики/аналитики всегда заполняем decision_code/decision_u16:
    #   - veto=True  => decision_code == reason_code (обычно VETO_*)
    #   - veto=False => decision_code == "OK" или "SOFT_*"
    # -------------------------------------------------------------------------
    decision_code: str = ""
    decision_u16: int = 0
    # NEW: multiple soft penalties (non-veto). Keep wire compact via u16 list + packed form.
    # НОВОЕ: множественные мягкие штрафы (не вето). Компактный wire формат через список u16 + упаковка.
    soft_codes: list[str] = field(default_factory=list)   # debug/readable (читаемые)
    soft_u16s: list[int] = field(default_factory=list)    # stable compact ids (стабильные ID)
    soft16: str = ""                                     # packed base64 (u16 list) (упакованный список)


def _finalize_reason(reason: str, reason_code: str) -> tuple[str, int]:
    """
    Единая точка нормализации reason -> (reason_code, reason_u16).

    Важные свойства:
    - reason_code всегда structured (или UNKNOWN_VETO)
    - reason_u16 стабилен и пригоден для wire-format/агрегаций
    - STRICT_REASON_CODES=1 включает fail-closed для неизвестных кодов (CI/канарейка)
    """
    # Если reason_code не передали — попробуем вывести его из legacy reason.
    code = (reason_code or "").strip()
    if not code:
        code = legacy_reason_to_code(reason)
    u16 = reason_code_to_u16(code)
    return code, int(u16)


def _finalize_decision(*, veto: bool, veto_reason: str, veto_reason_code: str, soft_code: str = "") -> tuple[str, int, str, int]:
    """
    Единая точка нормализации outcome:
      returns (reason_code, reason_u16, decision_code, decision_u16)

    - Если veto=True:
        decision_code == reason_code (обычно VETO_*)
    - Если veto=False:
        decision_code == soft_code (если задан) иначе "OK"

    soft_code должен быть structured ("SOFT_*") и зарегистрирован в reason_registry.
    Если soft_code неизвестен:
      - STRICT_REASON_CODES=1 -> ValueError
      - иначе fail-open -> decision_u16=0, decision_code остаётся строкой (для логов)
    """
    if veto:
        rc, ru16 = _finalize_reason(veto_reason, veto_reason_code)
        return rc, ru16, rc, ru16

    # non-veto path: OK/soft
    dc = (soft_code or "").strip() or "OK"
    du16 = reason_code_to_u16(dc)  # тот же реестр, что и veto
    return "", 0, dc, int(du16)


class ConfirmationsEngine:
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        self._min_conf = float(kwargs.get("min_conf", 0.10))
        self._metrics = kwargs.get("metrics")  # optional; expected inc/gauge contract (опционально)
        self._strict_reason_codes = bool(kwargs.get("strict_reason_codes", False))
        # валидаторы breakout/absorption внедряются/создаются извне
        self._breakout = kwargs.get("breakout_validator")
        self._absorption = kwargs.get("absorption_validator")

    def _emit_veto_metrics(self, *, kind: str, ctx: Any, reason_code: str) -> None:
        """
        9.6 Minimal:
          signals_veto_total{reason,kind,symbol}
        """
        m = self._metrics
        if not m:
            return
        try:
            sym = str(getattr(ctx, "symbol", "") or "")
            m.inc("signals_veto_total", 1, tags={"reason": reason_code, "kind": kind, "symbol": sym})
        except Exception:
            return

    def _emit_veto_metric(self, *, kind: str, reason_code: str, ff_mask: int) -> None:
        """
        Централизованная отправка метрик вето:
          - сохраняет ключи меток стабильными
          - позволяет менять бэкенд метрик без изменения бизнес-логики
        """
        if METRICS is None:
            return
        try:
            METRICS.inc(
                "signals_veto_total",
                labels={"kind": str(kind or "unknown"), "reason_code": str(reason_code), "ff_mask": str(int(ff_mask))},
            )
        except Exception:
            pass

    @staticmethod
    def _clamp01(x: float) -> float:
        if x is None:
            return 0.0
        try:
            xf = float(x)
        except Exception:
            return 0.0
        # защита от NaN/Inf
        if not math.isfinite(xf):
            return 0.0
        if xf < 0.0:
            return 0.0
        if xf > 1.0:
            return 1.0
        return xf

    def _soft_penalties(
        self,
        *,
        kind: str,
        ctx: Any,
        l2: Any,
        l3: Any
    ) -> tuple[float, list[tuple[str, float]], dict[str, float]]:
        """
        Возвращает:
          conf_factor01 (после умножения всех мягких штрафов),
          soft_hits: list[(reason_code, weight)],
          parts: float features для логов/отладки.

        ВАЖНО:
          - мягкие штрафы ЗДЕСЬ НИКОГДА НЕ ЯВЛЯЮТСЯ ВЕТО (решения о вето отдельны и стабильны).
          - веса мультипликативны: conf *= (1 - w).
        """
        parts: dict[str, float] = {}
        soft_hits: list[tuple[str, float]] = []

        # --- L3 missing => нейтральное понижение (fail-open) ---
        l3_missing = bool(l3 is None or getattr(ctx, "l3_missing", None) is True)
        parts["l3_missing"] = 1.0 if l3_missing else 0.0
        if l3_missing:
            soft_hits.append(("SOFT_L3_MISSING", 0.10))

        # --- HTF/geometry missing => небольшое понижение (fail-open) ---
        geo_score = getattr(ctx, "geometry_score", None)
        missing_htf = (geo_score is None) or (isinstance(geo_score, float) and geo_score != geo_score)
        parts["missing_htf"] = 1.0 if missing_htf else 0.0
        if missing_htf:
            soft_hits.append(("SOFT_HTF_MISSING", 0.05))

        # --- Пример: stale L2 для типов, которые fail-open (extreme) ---
        l2_stale = bool(getattr(ctx, "l2_is_stale", None) is True or getattr(ctx, "l2_stale", None) is True)
        parts["l2_stale"] = 1.0 if l2_stale else 0.0
        if l2_stale and str(kind) == "extreme":
            soft_hits.append(("SOFT_L2_STALE_EXTREME", 0.15))

        # Умножение штрафов
        conf = 1.0
        for _, w in soft_hits:
            conf *= max(0.0, 1.0 - float(w))
        conf = self._clamp01(conf)

        # Порядок приоритета: больший вес первым (более "поясняющий" для верхних слотов).
        soft_hits.sort(key=lambda x: float(x[1]), reverse=True)
        return conf, soft_hits, parts

    def _veto(self, *, reason: str, reason_code: str, flags: list[int], parts: dict[str, float]) -> Validation:
        r, rc, u16 = normalize_reason(reason=reason, reason_code=reason_code)
        return Validation(True, 0.0, list(flags), r, dict(parts), rc, u16)

    def _ok(self, *, conf01: float, flags: list[int], reason: str, reason_code: str, parts: dict[str, float]) -> Validation:
        c = self._clamp01(conf01)
        r, rc, u16 = normalize_reason(reason=reason, reason_code=reason_code)
        return Validation(False, c, list(flags), r, dict(parts), rc, u16)

    def validate(
        self,
        kind: str,
        ctx: Any,
        l2: Any,
        l3: Any,
        level_price: Optional[float],
    ) -> Validation:
        k = (kind or "").lower()
        # ---- вывод стороны без изменения сигнатуры validate() ----
        # Приоритет:
        #  1) ctx.side / ctx.signal_side текстовый
        #  2) ctx.side числовой
        #  3) эвристика price vs level
        def _infer_side_str() -> str:
            s = getattr(ctx, "side", None) or getattr(ctx, "signal_side", None) or getattr(ctx, "dir", None)
            if isinstance(s, str) and s:
                ss = s.strip().lower()
                if ss in {"buy", "up", "long"}:
                    return "buy"
                if ss in {"sell", "down", "short"}:
                    return "sell"
            try:
                if isinstance(s, (int, float)) and float(s) != 0:
                    return "buy" if float(s) > 0 else "sell"
            except Exception:
                pass
            try:
                if level_price is not None:
                    px = float(getattr(ctx, "price", None) or getattr(ctx, "last_price", None) or 0.0)
                    lv = float(level_price)
                    if lv > 0 and px > 0:
                        return "buy" if px >= lv else "sell"
            except Exception:
                pass
            return "buy"

        side_str = _infer_side_str()

        flags: list[int] = []
        parts: dict[str, float] = {}

        k = str(kind or "").strip().lower()

        # total counters для rate-графиков (denominator)
        if self._metrics is not None:
            try:
                self._metrics.inc("confirm_validate_total", tags={"kind": k, "ff_mask": str(ff_mask)})
            except Exception:
                pass

        # ---- защита: NaN/Inf на критических входах ----
        # spread_bps частый вход для вето; если он не-finite, вето как bad numeric.
        spread_bps = getattr(ctx, "spread_bps", None)
        try:
            if spread_bps is not None and not math.isfinite(float(spread_bps)):
                flags.append(int(QF.BAD_NUMERIC))
                return self._veto(
                    reason="spread_bps_non_finite",
                    reason_code=ReasonCode.VETO_BAD_NUMERIC.value,
                    flags=flags,
                    parts=parts,
                )
        except Exception:
            flags.append(int(QF.BAD_NUMERIC))
            return self._veto(
                reason="spread_bps_bad_type",
                reason_code=ReasonCode.VETO_BAD_NUMERIC.value,
                flags=flags,
                parts=parts,
            )

        # ---- политика fail-open/fail-closed L2 для каждого типа ----
        l2_missing = l2 is None
        l2_stale = bool(getattr(ctx, "l2_is_stale", False))

        if k == "breakout":
            if l2_missing:
                rc = normalize_reason_code("VETO_L2_MISSING")
                self._emit_veto_metric(kind=k, reason_code=rc, ff_mask=ff_mask)
                code, u16, dcode, du16 = _finalize_decision(veto=True, veto_reason="bo_l2_missing", veto_reason_code=rc)
                return Validation(
                    veto=True,
                    conf_factor01=0.0,
                    flags=[int(QF.BO_L2_FAIL_CLOSED)],
                    reason="bo_l2_missing",
                    parts=parts,
                    reason_code=code,
                    reason_u16=u16,
                    decision_code=dcode,
                    decision_u16=du16,
                )
            if l2_stale:
                rc = normalize_reason_code("VETO_L2_STALE")
                self._emit_veto_metric(kind=k, reason_code=rc, ff_mask=ff_mask)
                code, u16, dcode, du16 = _finalize_decision(veto=True, veto_reason="bo_l2_stale", veto_reason_code=rc)
                return Validation(
                    veto=True,
                    conf_factor01=0.0,
                    flags=[int(QF.BO_L2_FAIL_CLOSED)],
                    reason="bo_l2_stale",
                    parts=parts,
                    reason_code=code,
                    reason_u16=u16,
                    decision_code=dcode,
                    decision_u16=du16,
                )
            r = self._breakout.confirm(ctx=ctx, l2=l2, side=side_str, level_price=level_price)
            flags.extend(list(getattr(r, "flags", []) or []))
            parts.update(dict(getattr(r, "parts", {}) or {}))
            # Если валидатор предоставляет доп. словарь флагов, вмерживаем в parts (для логов/отладки)
            if isinstance(getattr(r, "flags", None), dict):
                parts.update(dict(getattr(r, "flags") or {}))
            if bool(getattr(r, "veto", False)):
                # "super-hard" уровень:
                #   Всегда нормализуем в structured reason_code и вычисляем стабильный uint16.
                #   Это сохраняет wire ABI маленьким и дашборды консистентными.
                rc = normalize_reason_code(getattr(r, "reason_code", "") or "VETO_GENERIC")
                u16 = int(getattr(r, "reason_u16", 0) or 0) or reason_u16(rc)
                self._emit_veto_metric(kind=k, reason_code=rc, ff_mask=ff_mask)
                return Validation(
                    veto=True,
                    conf_factor01=0.0,
                    flags=flags,
                    reason=(r.reasons[0] if r.reasons else "bo_l2_veto"),
                    parts=parts,
                    reason_code=rc,
                    reason_u16=u16,
                )
            l2_score = self._clamp01(getattr(r, "score01", 0.0))
            parts["l2_score01"] = l2_score

            # Опционально: L3 spoof veto для breakout (rollout через флаг)
            if ff and ff.use_l3_veto_for_breakout:
                # Эвристика: высокий cancel_to_trade + низкий taker_rate => риск спуфинга
                # side-aware: для buy breakout следим за asks; для sell breakout следим за bids.
                side = str(getattr(ctx, "side", "") or "").lower()
                taker = getattr(ctx, "taker_rate_ema", None)
                if taker is None:
                    taker = getattr(ctx, "taker_rate", None)
                try:
                    taker_f = float(taker) if taker is not None else None
                except Exception:
                    taker_f = None
                c2t = None
                if side in {"buy", "up", "long"}:
                    c2t = getattr(ctx, "cancel_to_trade_ask_5s", None) or getattr(ctx, "cancel_to_trade_ask_20s", None)
                else:
                    c2t = getattr(ctx, "cancel_to_trade_bid_5s", None) or getattr(ctx, "cancel_to_trade_bid_20s", None)
                try:
                    c2t_f = float(c2t) if c2t is not None else None
                except Exception:
                    c2t_f = None
                if taker_f is not None:
                    parts["taker_rate"] = float(taker_f)
                if c2t_f is not None:
                    parts["cancel_to_trade"] = float(c2t_f)
                if (c2t_f is not None and taker_f is not None) and (c2t_f >= self._l3_spoof_cancel_thr) and (taker_f <= self._l3_spoof_taker_thr):
                    rc = normalize_reason_code("VETO_L3_SPOOF_RISK")
                    self._emit_veto_metric(kind=k, reason_code=rc, ff_mask=ff_mask)
                    code, u16, dcode, du16 = _finalize_decision(veto=True, veto_reason="bo_l3_spoof_risk", veto_reason_code=rc)
                    return Validation(
                        veto=True,
                        conf_factor01=0.0,
                        flags=flags,
                        reason="bo_l3_spoof_risk",
                        parts=parts,
                        reason_code=code,
                        reason_u16=u16,
                        decision_code=dcode,
                        decision_u16=du16,
                    )

            # Опционально: RegimeDetectorV2 veto (rollout через флаг)
            if ff and ff.regime_detector_v2:
                # пример regime score: trend_score - range_score в ctx.market_regime_score
                try:
                    regime_score = float(getattr(ctx, "market_regime_score", 0.0) or 0.0)
                except Exception:
                    regime_score = 0.0
                parts["regime_score"] = float(regime_score)
                # В range-подобном режиме (score ~ 0), breakout часто ложный -> вето (rollout gated).
                if abs(regime_score) <= self._bo_range_veto_regime_score_thr:
                    rc = normalize_reason_code("VETO_REGIME_RANGE_BREAKOUT")
                    self._emit_veto_metric(kind=k, reason_code=rc, ff_mask=ff_mask)
                    code, u16, dcode, du16 = _finalize_decision(veto=True, veto_reason="bo_regime_range_v2", veto_reason_code=rc)
                    return Validation(
                        veto=True,
                        conf_factor01=0.0,
                        flags=flags,
                        reason="bo_regime_range_v2",
                        parts=parts,
                        reason_code=code,
                        reason_u16=u16,
                        decision_code=dcode,
                        decision_u16=du16,
                    )

        # --- absorption ---
        if k == "absorption":
            # внедрение: строгие 2-из-N подтверждений для absorption
            require_2ofn = True
            if ff is not None:
                require_2ofn = bool(ff.absorption_require_2ofn_confirmations)
            # policy: absorption без книги -> fail-closed? (обычно нет, но оставляем как есть)
            l2_missing = (l2 is None)
            l2_stale = bool(getattr(ctx, "l2_is_stale", False) or getattr(ctx, "l2_stale", False))
            if l2_missing or l2_stale:
                rc = normalize_reason_code("VETO_L2_STALE")
                self._emit_veto_metric(kind=k, reason_code=rc, ff_mask=ff_mask)
                code, u16, dcode, du16 = _finalize_decision(veto=True, veto_reason="ab_l2_missing_or_stale", veto_reason_code=rc)
                return Validation(
                    veto=True,
                    conf_factor01=0.0,
                    flags=[int(QF.AB_L2_FAIL_CLOSED)],
                    reason="ab_l2_missing_or_stale",
                    parts=parts,
                    reason_code=code,
                    reason_u16=u16,
                    decision_code=dcode,
                    decision_u16=du16,
                )

            # NOTE: absorption confirm может поддерживать строгий режим через kwarg (добавлено в этом патче).
            r = self._absorption.confirm(
                ctx=ctx,
                l2=l2,
                level_price=float(level_price or 0.0),
                side=str(getattr(ctx, "side", "buy")),
                require_2ofn=require_2ofn,
            )
            if getattr(r, "veto", False):
                rc = normalize_reason_code(getattr(r, "reason_code", "") or "VETO_GENERIC")
                self._emit_veto_metric(kind=k, reason_code=rc, ff_mask=ff_mask)
                code, u16, dcode, du16 = _finalize_decision(veto=True, veto_reason=(r.reasons[0] if r.reasons else "ab_l2_veto"), veto_reason_code=rc)
                return Validation(
                    veto=True,
                    conf_factor01=0.0,
                    flags=flags,
                    reason=(r.reasons[0] if r.reasons else "ab_l2_veto"),
                    parts=parts,
                    reason_code=code,
                    reason_u16=u16,
                    decision_code=dcode,
                    decision_u16=du16,
                )

            conf = float(getattr(r, "score01", 0.5) or 0.5)
            conf = max(0.0, min(1.0, conf))
            if conf < self._min_conf:
                rc = normalize_reason_code("VETO_CONF_BELOW_MIN")
                self._emit_veto_metric(kind=k, reason_code=rc, ff_mask=ff_mask)
                code, u16, dcode, du16 = _finalize_decision(veto=True, veto_reason="conf_below_min_veto", veto_reason_code=rc)
                return Validation(
                    veto=True,
                    conf_factor01=0.0,
                    flags=flags,
                    reason="conf_below_min_veto",
                    parts=parts,
                    reason_code=code,
                    reason_u16=u16,
                    decision_code=dcode,
                    decision_u16=du16,
                )

            if METRICS is not None:
                try:
                    METRICS.inc("candidates_total", labels={"kind": k, "ff_mask": str(ff_mask)})
                except Exception:
                    pass
            ok_rc = normalize_reason_code("OK")
            _, _, dcode, du16 = _finalize_decision(veto=False, veto_reason="", veto_reason_code="", soft_code="")
            return Validation(
                veto=False,
                conf_factor01=float(conf),
                flags=flags,
                reason="ok",
                parts=parts,
                reason_code=ok_rc,
                reason_u16=reason_u16(ok_rc),
                decision_code=dcode,
                decision_u16=du16,
            )

        # ---------------------------------------------------------------------
        # МИКРО-ШАГ: применяем fail-open штрафы (L3/geo/L2-missing)
        # decision_code фиксируем для downstream аналитики (одна ось).
        # ---------------------------------------------------------------------
        soft_conf, soft_hits, soft_parts = self._soft_penalties(kind=k, ctx=ctx, l2=l2, l3=l3)
        parts.update(soft_parts)

        # Создаем ограниченные списки (стабильные для дашбордов).
        soft_codes = [c for (c, _) in soft_hits][: max(0, self._soft_max)]
        soft_u16s = reason_codes_to_u16s(soft_codes)
        soft16 = ""
        if soft_u16s and self._pack_soft_u16:
            try:
                soft16 = pack_u16_list(soft_u16s)
            except Exception:
                # fail-open: никогда не блокируем emit из-за упаковки
                soft16 = ""

        # Если нужен один "decision soft code" для легаси логов, берем первый.
        # Но НЕ схлопываем его обратно в wire формат.
        if soft_hits:
            parts["soft_penalties_n"] = float(len(soft_hits))
            parts["soft_top_weight"] = float(soft_hits[0][1])
        else:
            parts["soft_penalties_n"] = 0.0
            parts["soft_top_weight"] = 0.0

        # ---- минимальная уверенность вето (глобальный гард) ----
        conf = safe_float(self._compute_conf_factor(ctx=ctx, kind=k, parts=parts), 0.0) or 0.0
        # Строго держим в [0..1] (wire-stable и защищает downstream модели).
        conf = clamp01(conf)
        if conf < float(self._min_conf):
            flags.append(int(QF.CONF_BELOW_MIN_VETO))
            rc = normalize_reason_code("VETO_CONF_BELOW_MIN")
            self._emit_veto_metric(kind=k, reason_code=rc, ff_mask=ff_mask)
            code, u16, dcode, du16 = _finalize_decision(veto=True, veto_reason="conf_below_min_veto", veto_reason_code=rc)
            return Validation(
                veto=True,
                conf_factor01=0.0,
                flags=flags,
                reason="conf_below_min_veto",
                parts=parts,
                reason_code=code,
                reason_u16=u16,
                decision_code=dcode,
                decision_u16=du16,
                soft_codes=(soft_codes if self._debug_soft_codes else []),
                soft_u16s=soft_u16s,
                soft16=soft16,
            )

        # OK / SOFT результат
        if METRICS is not None:
            try:
                METRICS.inc("candidates_total", labels={"kind": k, "ff_mask": str(ff_mask)})
            except Exception:
                pass
        ok_rc = normalize_reason_code("OK")
        _, _, dcode, du16 = _finalize_decision(veto=False, veto_reason="", veto_reason_code="", soft_code="")
        return Validation(
            veto=False,
            conf_factor01=float(conf),
            flags=flags,
            reason="ok",
            parts=parts,
            reason_code=ok_rc,
            reason_u16=reason_u16(ok_rc),
            decision_code=dcode,
            decision_u16=du16,
            soft_codes=(soft_codes if self._debug_soft_codes else []),
            soft_u16s=soft_u16s,
            soft16=soft16,
        )