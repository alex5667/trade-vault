from __future__ import annotations

import math
import os
from dataclasses import dataclass
from typing import Any

from handlers.confirmations.l2_quality import L2Assessment, L2QualityPolicy
from handlers.confirmations.l3_quality import L3QualityPolicy, apply_l3_policy_to_ctx
from handlers.geometry.geometry_quality import GeometryQualityPolicy, apply_geometry_policy_to_ctx
from handlers.metrics.quality_metrics import QualityMetrics
import contextlib


def clamp01(x: float) -> float:
    return 0.0 if x <= 0.0 else (1.0 if x >= 1.0 else float(x))


@dataclass(frozen=True)
class QualityAssessment:
    veto: bool
    quality_score01: float
    flags: list[str]
    parts: dict[str, float]
    reason: str


class QualityGate:
    """
    "Совсем жёстко": единый QualityGate для обоих путей.

    - global assess (без kind): L3 + HTF/geometry + ATR/HLC fallback flags
    - kind assess: добавляет L2 fail-open/fail-closed и может veto
    """

    def __init__(self) -> None:
        self._l2 = L2QualityPolicy()
        self._l3 = L3QualityPolicy()
        self._geo = GeometryQualityPolicy()
        self._m = QualityMetrics()
        self.atr_fallback_penalty01 = float(os.getenv("ATR_FALLBACK_PENALTY01", "0.85"))
        self.hlc_fallback_penalty01 = float(os.getenv("HLC_FALLBACK_PENALTY01", "0.90"))
        self.invalid_price_veto_kinds = {"breakout", "absorption"}
        self.invalid_price_penalty01 = float(os.getenv("INVALID_PRICE_PENALTY01", "0.25"))

    def _ensure_flags_list(self, ctx: Any) -> list[str]:
        arr = getattr(ctx, "data_quality_flags", None)
        if arr is None or not isinstance(arr, list):
            arr = []
            with contextlib.suppress(Exception):
                ctx.data_quality_flags = arr
        return arr

    def _ctx_price_ok(self, ctx: Any) -> bool:
        p = getattr(ctx, "price", None) or getattr(ctx, "last_price", None)
        try:
            fp = float(p)
            return math.isfinite(fp) and fp > 0.0
        except Exception:
            return False

    def _ctx_ts_ok(self, ctx: Any) -> bool:
        try:
            ts = int(getattr(ctx, "ts", None))
            return ts > 0
        except Exception:
            return False

    def assess_global(self, *, ctx: Any) -> QualityAssessment:
        parts: dict[str, float] = {}
        flags = self._ensure_flags_list(ctx)

        # "жёстче": базовая валидность контекста (без veto в global; только флаги/штраф)
        if not self._ctx_ts_ok(ctx):
            if "bad_ctx_ts" not in flags:
                flags.append("bad_ctx_ts")
        if not self._ctx_price_ok(ctx):
            if "bad_ctx_price" not in flags:
                flags.append("bad_ctx_price")

        # L3
        l3 = getattr(ctx, "l3", None)
        l3a = self._l3.assess(ctx=ctx, l3=l3)
        apply_l3_policy_to_ctx(ctx=ctx, assessment=l3a)
        parts["l3_score01"] = float(l3a.score01)
        for f in l3a.flags:
            if f not in flags:
                flags.append(f)

        # Geometry/HTF
        geoa = self._geo.assess(ctx=ctx)
        apply_geometry_policy_to_ctx(ctx=ctx, assessment=geoa)
        parts["geo_score01"] = float(geoa.score01)
        for f in geoa.flags:
            if f not in flags:
                flags.append(f)

        # HLC/ATR fallback flags (fail-open, только штраф)
        # - "hlc_fallback": candles/HLC used as fallback
        # - "atr_fallback": ATR estimate/local fallback used
        q = 1.0
        if "hlc_fallback" in flags:
            q *= self.hlc_fallback_penalty01
            parts["hlc_penalty01"] = float(self.hlc_fallback_penalty01)
        else:
            parts["hlc_penalty01"] = 1.0

        if "atr_fallback" in flags:
            q *= self.atr_fallback_penalty01
            parts["atr_penalty01"] = float(self.atr_fallback_penalty01)
        else:
            parts["atr_penalty01"] = 1.0

        if "bad_ctx_price" in flags:
            q *= self.invalid_price_penalty01
            parts["bad_price_penalty01"] = float(self.invalid_price_penalty01)
        else:
            parts["bad_price_penalty01"] = 1.0

        # итоговая global quality
        q *= float(parts["l3_score01"])
        q *= float(parts["geo_score01"])
        q = clamp01(float(q))
        parts["global_quality01"] = q

        with contextlib.suppress(Exception):
            ctx.global_quality01 = q

        # метрики
        self._m.record_flags(ctx, list(flags))

        return QualityAssessment(veto=False, quality_score01=q, flags=list(flags), parts=parts, reason="global_ok")

    def assess_kind(self, *, kind: str, ctx: Any, l2: Any | None = None) -> QualityAssessment:
        # Начинаем с global, затем добавляем kind-специфичное
        g = self.assess_global(ctx=ctx)
        parts = dict(g.parts)
        flags = self._ensure_flags_list(ctx)
        veto = False
        k = (kind or "custom").lower()

        # "жёстче": invalid price => fail-closed для breakout/absorption
        if not self._ctx_price_ok(ctx):
            if "bad_ctx_price" not in flags:
                flags.append("bad_ctx_price")
            if k in self.invalid_price_veto_kinds:
                veto = True
                parts["ctx_veto"] = 1.0
                self._m.record_flags(ctx, list(flags))
                self._m.inc(ctx, "l2_veto", 0)  # no-op, just keeps naming parity
                return QualityAssessment(veto=True, quality_score01=0.0, flags=list(flags), parts=parts, reason="bad_ctx_price_fail_closed")

        # L2 kind-policy
        l2a: L2Assessment = self._l2.assess(kind=kind, ctx=ctx, l2=l2)
        parts["l2_score01"] = float(l2a.score01)
        for f in l2a.flags:
            if f not in flags:
                flags.append(f)
        veto = bool(l2a.veto)
        if veto:
            self._m.inc(ctx, "l2_veto", 1)

        # объединение
        q = float(parts.get("global_quality01", 1.0)) * float(parts["l2_score01"])
        q = clamp01(q)
        parts["quality_score01"] = q

        with contextlib.suppress(Exception):
            ctx.quality_score01 = q

        reason = l2a.reason if veto else "quality_ok"
        self._m.record_flags(ctx, list(flags))
        return QualityAssessment(veto=veto, quality_score01=q, flags=list(flags), parts=parts, reason=reason)
