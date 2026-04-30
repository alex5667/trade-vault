from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from typing import Any

from common.decision_trace import trace_gate, Span
from common.qf_codes import QF
from handlers.confirmations.engine import ConfirmationsEngine, Validation
from handlers.signal_scoring.score_model import ScoreModel
from handlers.labeling.outcome_labeler import OutcomeLabeler
from handlers.pipeline.candidate import Candidate

@dataclass(frozen=True)
class PipelineResult:
    veto: bool
    quality_codes: list[int] = field(default_factory=list)  # uint16
    parts: dict[str, float] = field(default_factory=dict)
from handlers.pipeline.validators import BreakoutValidator, AbsorptionValidator, OBISpikeValidator
from handlers.confirmations.l2_confirm_breakout import L2ConfirmBreakout
from handlers.confirmations.l2_confirm_absorption import L2ConfirmAbsorption

class SignalPipeline:
    def __init__(self) -> None:
        self._conf = ConfirmationsEngine(
            breakout_validator=L2ConfirmBreakout()
            absorption_validator=L2ConfirmAbsorption()
        )
        self._bo = BreakoutValidator()
        self._ab = AbsorptionValidator()
        self._obi = OBISpikeValidator()
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
        # 1) Base confirmations
        with Span() as sp_val:
            v: Validation = self._conf.validate(kind=cand.kind, ctx=ctx, l2=l2, l3=l3, level_price=cand.level_price)
            
        try:
            trace_gate(
                ctx
                stage="gates"
                name="confirmations_engine"
                passed=bool(not v.veto)
                veto=bool(v.veto)
                reason_code=str(getattr(v, "reason_code", "") or ("VETO" if v.veto else "OK"))
                metrics=dict(getattr(v, "parts", {}) or {})
                duration_ms=sp_val.ms
            )
        except Exception:
            pass

        flags_list = list(v.flags or [])
        parts_dict = dict(v.parts)

        if v.veto:
            return PipelineResult(True, quality_codes=flags_list, parts=parts_dict)

        conf01 = float(v.conf_factor01)

        # 2) Kind-specific high-level validators
        adj = None
        kind = str(cand.kind or "").strip().lower()
        if kind in ("bo", "breakout"):
            adj = self._bo.adjust(kind=kind, ctx=ctx, side=cand.side, level_price=cand.level_price)
        elif kind in ("abs", "absorption"):
            adj = self._ab.adjust(kind=kind, ctx=ctx, side=cand.side, level_price=cand.level_price)
        elif kind in ("obi", "obi_spike", "obi-spike"):
            adj = self._obi.adjust(kind=kind, ctx=ctx, side=cand.side, level_price=cand.level_price)

        if adj is not None:
            for f in adj.flags:
                if f.upper() in QF.__members__:
                    flags_list.append(int(QF[f.upper()]))
            for k_part, v_part in (adj.parts or {}).items():
                try:
                    parts_dict[k_part] = float(v_part)
                except Exception:
                    continue
            if adj.veto:
                return PipelineResult(True, quality_codes=flags_list, parts=parts_dict)
            conf01 *= float(adj.mult01)
            parts_dict["kind_mult01"] = float(adj.mult01)

        # 3) Score Model
        with Span() as sp_score:
            out = self._score.score(
                raw_score=float(cand.raw_score)
                conf_factor01=conf01
                kind=cand.kind
                ctx=ctx
                parts_in=parts_dict
            )
        try:
            trace_gate(
                ctx
                stage="scoring"
                name="score_model"
                passed=True
                veto=False
                reason_code="OK"
                metrics=dict(getattr(out, "parts", {}) or {})
                duration_ms=sp_score.ms
            )
        except Exception:
            pass

        return PipelineResult(False, quality_codes=flags_list, parts=dict(out.parts))

    def register_emitted(self, *, ctx: Any, cand: Candidate, signal_id: str) -> None:
        if cand.kind == "breakout":
            self._labeler.register_breakout(signal_id=signal_id, ctx=ctx, side=int(cand.side), level_price=cand.level_price)
