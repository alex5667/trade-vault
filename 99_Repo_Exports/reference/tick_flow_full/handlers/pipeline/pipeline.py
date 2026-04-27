from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any

from common.decision_trace import trace_gate, trace_enabled


@dataclass(frozen=True)
class PipelineResult:
    veto: bool
    quality_codes: list[int] = field(default_factory=list)  # uint16
    parts: dict[str, float] = field(default_factory=dict)


class SignalPipeline:
    def __init__(self) -> None:
        self._conf = ConfirmationsEngine()
        self._score = ScoreModel()
        self._labeler = OutcomeLabeler()

    def _signal_id(self, ctx: Any, cand: Candidate) -> str:
        sym = str(getattr(ctx, "symbol", "") or "")
        ts = str(getattr(ctx, "ts", "") or "")
        lvl = str(cand.level_key or cand.level_price or "")
        base = f"{sym}|{cand.kind}|{cand.side}|{ts}|{lvl}"
        return hashlib.sha1(base.encode("utf-8")).hexdigest()[:20]

    def on_ctx(self, ctx: Any) -> list[dict[str, Any]]:
        # разметка эвентов созрела
        return self._labeler.on_ctx(ctx)

    def validate_and_score(self, *, ctx: Any, cand: Candidate) -> PipelineResult:
        l2 = getattr(ctx, "l2", None)
        l3 = getattr(ctx, "l3", None)
        # ------------------------------------------------------------
        # DecisionTrace timing: confirmations/validation stage
        # ------------------------------------------------------------
        # ЦЕЛЬ: Тайминги duration_ms по gate'ам
        # ЗАЧЕМ: Даёт минимум "сквозных" latency-метрик уже сейчас, без переписывания всех гейтов
        # ГДЕ: CandidatePipeline.validate_and_score() для confirmations_engine (validate)
        with Span() as sp_val:
            v: Validation = self._conf.validate(kind=cand.kind, ctx=ctx, l2=l2, l3=l3, level_price=cand.level_price)
        try:
            trace_gate(
                ctx,
                stage="gates",
                name="confirmations_engine",
                passed=bool(not v.veto),
                veto=bool(v.veto),
                reason_code=str(getattr(v, "reason_code", "") or ("VETO" if v.veto else "OK")),
                metrics=dict(getattr(v, "parts", {}) or {}),
                duration_ms=sp_val.ms,
            )
        except Exception:
            pass
        if v.veto:
            return PipelineResult(True, quality_codes=list(v.flags or []), parts=dict(v.parts))
        # ------------------------------------------------------------
        # DecisionTrace timing: scoring/calibration stage
        # ------------------------------------------------------------
        # ЦЕЛЬ: Тайминги duration_ms по gate'ам
        # ЗАЧЕМ: Даёт минимум "сквозных" latency-метрик уже сейчас, без переписывания всех гейтов
        # ГДЕ: CandidatePipeline.validate_and_score() для score_model (score)
        with Span() as sp_score:
            out = self._score.score(
                raw_score=float(cand.raw_score),
                conf_factor01=float(v.conf_factor01),
                kind=cand.kind,
                ctx=ctx,
                parts_in=dict(v.parts),
            )
        try:
            trace_gate(
                ctx,
                stage="scoring",
                name="score_model",
                passed=True,
                veto=False,
                reason_code="OK",
                metrics=dict(getattr(out, "parts", {}) or {}),
                duration_ms=sp_score.ms,
            )
        except Exception:
            pass
        parts = dict(out.parts)
        return PipelineResult(False, quality_codes=list(v.flags or []), parts=dict(out.parts))

    def register_emitted(self, *, ctx: Any, cand: Candidate, signal_id: str) -> None:
        if cand.kind == "breakout":
            self._labeler.register_breakout(signal_id=signal_id, ctx=ctx, side=int(cand.side), level_price=cand.level_price)
