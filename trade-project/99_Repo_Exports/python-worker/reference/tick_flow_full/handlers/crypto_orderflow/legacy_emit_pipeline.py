from __future__ import annotations

"""
Legacy candidate emission pipeline (Step 2.1) — CONTRACTED VERSION

Контракт (из вашего описания):
  _emit_candidate_signal(ctx, scored: ScoredCandidate) -> bool
    scored.candidate: Candidate
    scored.conf_factor: float [0..1]
    scored.final_score: float
    scored.confidence_pct: float [0..100]
    scored.score_parts: Dict[str, Any]

Цели:
  - разрезать _emit_candidate_signal() на явный pipeline стадий
  - минимизировать риск регрессий: бизнес-логика остаётся в handler._legacy_*,
    в этом модуле только оркестрация и fail-open оболочка

Важно:
  - В legacy-contract режиме источник кандидата — scored.candidate (ОДИН кандидат на вызов).
  - Pipeline сохраняет гарантию: fail-open (никогда не бросает наружу), idempotent (dedup=True),
    and "signals_veto{reason}" исходит именно из ConfirmationsEngine.validate().
"""


from dataclasses import dataclass, field
from typing import Any


@dataclass
class StageTrace:
    stage: str
    ok: bool
    reason: str = ""
    meta: dict[str, Any] = field(default_factory=dict)


@dataclass
class CandidateFrame:
    """
    Переносимый контекст стадий.
    """
    ctx: Any
    scored: Any
    cand: Any
    kind_str: str
    kind_key: str
    side_int: int
    side_raw: str
    level_price: float | None = None
    # validate result
    res: Any = None
    # score meta (from ScoredCandidate)
    score: Any = None
    # payload + sidecar
    payload: dict[str, Any] | None = None
    meta: dict[str, Any] | None = None
    parts: dict[str, Any] | None = None


class CandidateExtractor:
    """
    Contracted extractor: returns exactly one candidate from scored.
    """

    def run(self, handler: Any, *, ctx: Any, scored: Any) -> list[Any]:
        if scored is None:
            # Contract says scored must be provided; keep fail-open semantics.
            return []
        cand = getattr(scored, "candidate", None)
        if cand is None:
            return []
        return [cand]


class ContextEnricher:
    """
    Parse kind/side/level_price + attach levels (tp/sl) + ensure invariants for gates.
    """

    def run(self, handler: Any, *, ctx: Any, scored: Any, cand: Any, pre: dict[str, Any]) -> CandidateFrame:
        info = handler._legacy_parse_candidate(cand=cand)
        frame = CandidateFrame(
            ctx=ctx,
            scored=scored,
            cand=cand,
            kind_str=info["kind_str"],
            kind_key=info["kind_key"],
            side_int=info["side_int"],
            side_raw=info["side_raw"],
        )
        frame.level_price = handler._legacy_parse_level_price(cand=cand)

        # IMPORTANT ORDER:
        #   levels MUST be attached before EV/cost gates
        handler._legacy_attach_levels(ctx=ctx, frame=frame, pre=pre)
        return frame


class GateRunner:
    """
    Единая последовательность гейтов + trace.
    """

    def run_pre_validate(self, handler: Any, *, frame: CandidateFrame, pre: dict[str, Any], trace: list[StageTrace]) -> bool:
        # candidates_total must be counted BEFORE validate/emit (includes veto)
        handler._legacy_metric_candidate(frame=frame, pre=pre)

        ok, reason = handler._legacy_gate_regime(frame=frame, pre=pre)
        trace.append(StageTrace(stage="regime_gate", ok=ok, reason=reason))
        if not ok:
            return False

        ok, reason = handler._legacy_gate_ev(frame=frame, pre=pre)
        trace.append(StageTrace(stage="ev_gate", ok=ok, reason=reason))
        if not ok:
            return False

        handler._legacy_log_candidate(frame=frame, pre=pre)
        return True

    def run_validate(self, handler: Any, *, frame: CandidateFrame, pre: dict[str, Any], trace: list[StageTrace]) -> bool:
        ok, reason, res = handler._legacy_gate_confirmations(frame=frame, pre=pre)
        frame.res = res
        trace.append(StageTrace(stage="confirmations_validate", ok=ok, reason=reason))
        return bool(ok)

    def run_post_validate(self, handler: Any, *, frame: CandidateFrame, pre: dict[str, Any], trace: list[StageTrace]) -> bool:
        # confidence/conf_factor gates use ScoredCandidate fields (contract)
        ok, reason = handler._legacy_gate_min_conf(frame=frame, pre=pre)
        trace.append(StageTrace(stage="min_conf", ok=ok, reason=reason))
        if not ok:
            return False

        ok, reason = handler._legacy_gate_min_conf_factor(frame=frame, pre=pre)
        trace.append(StageTrace(stage="min_conf_factor", ok=ok, reason=reason))
        if not ok:
            return False

        # cost/edge gate (expected_move vs costs) — after confidence gates (per legacy intent)
        ok, reason = handler._legacy_gate_cost_edge(frame=frame, pre=pre)
        trace.append(StageTrace(stage="cost_edge_gate", ok=ok, reason=reason))
        if not ok:
            return False

        return True

    def run_pre_emit(self, handler: Any, *, frame: CandidateFrame, pre: dict[str, Any], trace: list[StageTrace]) -> bool:
        ok, reason = handler._legacy_gate_entry_policy(frame=frame, pre=pre)
        trace.append(StageTrace(stage="entry_policy", ok=ok, reason=reason))
        return bool(ok)


class ScoringRunner:
    """
    Contracted scoring: take score axis from ScoredCandidate (no recompute here).
    """

    def run(self, handler: Any, *, frame: CandidateFrame, pre: dict[str, Any]) -> Any:
        return handler._legacy_score_from_scored(frame=frame, pre=pre)


class PayloadBuilder:
    def run(self, handler: Any, *, frame: CandidateFrame, pre: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any] | None, dict[str, Any]]:
        return handler._legacy_build_payload(frame=frame, pre=pre)


class OutboxWriter:
    def run(self, handler: Any, *, frame: CandidateFrame, pre: dict[str, Any]) -> bool:
        return bool(handler._legacy_emit_outbox(frame=frame, pre=pre))


class Observability:
    def on_emit(self, handler: Any, *, frame: CandidateFrame, pre: dict[str, Any], ok: bool) -> None:
        handler._legacy_observe_emit(frame=frame, pre=pre, ok=ok)


class LegacyCandidatePipeline:
    """
    Главная "склейка".
    """

    def __init__(self) -> None:
        self.extractor = CandidateExtractor()
        self.enricher = ContextEnricher()
        self.gates = GateRunner()
        self.scoring = ScoringRunner()
        self.payload = PayloadBuilder()
        self.outbox = OutboxWriter()
        self.obs = Observability()

    def run(self, handler: Any, *, ctx: Any, scored: Any) -> bool:
        pre = handler._legacy_prepare(ctx=ctx)

        candidates = self.extractor.run(handler, ctx=ctx, scored=scored)
        if not candidates:
            return False

        any_sent = False
        for cand in candidates:
            trace: list[StageTrace] = []
            try:
                frame = self.enricher.run(handler, ctx=ctx, scored=scored, cand=cand, pre=pre)
            except Exception as e:
                try:
                    handler._legacy_mark_dq(ctx=ctx, flag="ctx_enrich_failed", exc=e)
                except Exception:
                    pass
                continue

            if not self.gates.run_pre_validate(handler, frame=frame, pre=pre, trace=trace):
                continue

            if not self.gates.run_validate(handler, frame=frame, pre=pre, trace=trace):
                continue

            try:
                frame.score = self.scoring.run(handler, frame=frame, pre=pre)
            except Exception as e:
                try:
                    handler._legacy_mark_dq(ctx=ctx, flag="score_from_scored_failed", exc=e)
                except Exception:
                    pass
                continue

            if not self.gates.run_post_validate(handler, frame=frame, pre=pre, trace=trace):
                continue

            try:
                payload, meta, parts = self.payload.run(handler, frame=frame, pre=pre)
                frame.payload = payload
                frame.meta = meta
                frame.parts = parts
            except Exception as e:
                try:
                    handler._legacy_mark_dq(ctx=ctx, flag="payload_build_failed", exc=e)
                except Exception:
                    pass
                continue

            # IMPORTANT CONTRACT:
            #   conf_factor_hist/final_score_hist/confidence_pct_hist should be observed ONLY on ok branch.
            try:
                handler._legacy_observe_effect(frame=frame, pre=pre)
            except Exception:
                pass

            if not self.gates.run_pre_emit(handler, frame=frame, pre=pre, trace=trace):
                continue

            ok = False
            try:
                ok = self.outbox.run(handler, frame=frame, pre=pre)
            except Exception as e:
                try:
                    handler._legacy_mark_dq(ctx=ctx, flag="emit_failed", exc=e)
                except Exception:
                    pass
                ok = False

            self.obs.on_emit(handler, frame=frame, pre=pre, ok=ok)
            if ok:
                any_sent = True

        return any_sent
