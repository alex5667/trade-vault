"""
Candidate pipeline stages (step 2.1 scaffold).

Purpose:
  - Make _emit_candidate_signal() refactor low-risk by expressing it as an explicit pipeline.
  - Keep CryptoOrderFlowHandler as orchestration/glue.
  - Allow incremental migration: old megamethod stays, but logic can be moved stage-by-stage.

NOTE:
  This file is intentionally dependency-light and unit-testable.
  It does NOT assume concrete handler internals. Stages accept callables or a handler reference.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple


@dataclass
class StageTraceItem:
    stage: str
    ok: bool
    reason: str = ""
    meta: Dict[str, Any] = field(default_factory=dict)


@dataclass
class CandidatePipelineResult:
    sent: bool
    suppressed: bool = False
    veto_reason: str = ""
    trace: List[StageTraceItem] = field(default_factory=list)


class CandidateExtractor:
    """Stage A: extract candidate objects from current state (tick/bar context)."""
    def __init__(self, fn: Callable[..., List[Any]]):
        self._fn = fn

    def run(self, *args: Any, **kwargs: Any) -> List[Any]:
        return self._fn(*args, **kwargs)


class ContextEnricher:
    """Stage B: build/enrich ctx (levels/session/atr/htf/empirical)."""
    def __init__(self, fn: Callable[..., Any]):
        self._fn = fn

    def run(self, *args: Any, **kwargs: Any) -> Any:
        return self._fn(*args, **kwargs)


class GateRunner:
    """
    Stage C: run gates in a fixed order and collect trace.

    Gate callable contract:
      gate(ctx, **kwargs) -> (ok: bool, reason_code: str, meta: dict)
    """
    def __init__(self, gates: List[Tuple[str, Callable[..., Tuple[bool, str, Dict[str, Any]]]]]):
        self._gates = list(gates)

    def run(self, *, ctx: Any, **kwargs: Any) -> Tuple[bool, str, List[StageTraceItem]]:
        trace: List[StageTraceItem] = []
        for name, gate in self._gates:
            ok, reason, meta = gate(ctx, **kwargs)
            trace.append(StageTraceItem(stage=name, ok=bool(ok), reason=str(reason or ""), meta=dict(meta or {})))
            if not ok:
                return False, str(reason or name), trace
        return True, "", trace


class ScoringRunner:
    """Stage D: scoring and confidence calibration."""
    def __init__(self, fn: Callable[..., Dict[str, Any]]):
        self._fn = fn

    def run(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return dict(self._fn(*args, **kwargs) or {})


class PayloadBuilder:
    """Stage E: build JSON-safe payload (strict types, bounded strings)."""
    def __init__(self, fn: Callable[..., Dict[str, Any]]):
        self._fn = fn

    def run(self, *args: Any, **kwargs: Any) -> Dict[str, Any]:
        return dict(self._fn(*args, **kwargs) or {})


class OutboxWriter:
    """Stage F: emit to outbox + write sidecar/meta if needed."""
    def __init__(self, fn: Callable[..., bool]):
        self._fn = fn

    def run(self, *args: Any, **kwargs: Any) -> bool:
        return bool(self._fn(*args, **kwargs))


class Observability:
    """Stage G: logging/metrics/sempled debug (must be fail-open)."""
    def __init__(self, fn: Callable[..., None]):
        self._fn = fn

    def run(self, *args: Any, **kwargs: Any) -> None:
        try:
            self._fn(*args, **kwargs)
        except Exception:
            return


@dataclass
class CandidateEmitPipeline:
    """
    Orchestrator.
    This class is intended to be used from CryptoOrderFlowHandler, e.g.:
      pipeline = CandidateEmitPipeline(...)
      for cand in extractor.run(...):
          res = pipeline.process_candidate(cand=..., ...)
    """
    enricher: ContextEnricher
    gates: GateRunner
    scoring: ScoringRunner
    payload: PayloadBuilder
    outbox: OutboxWriter
    obs: Optional[Observability] = None

    def process_candidate(self, *, cand: Any, **kwargs: Any) -> CandidatePipelineResult:
        trace: List[StageTraceItem] = []
        try:
            ctx = self.enricher.run(cand=cand, **kwargs)
        except Exception as e:
            trace.append(StageTraceItem(stage="ContextEnricher", ok=False, reason="ctx_build_failed", meta={"err": repr(e)}))
            return CandidatePipelineResult(sent=False, veto_reason="CTX_BUILD_FAILED", trace=trace)

        ok, reason, gate_trace = self.gates.run(ctx=ctx, cand=cand, **kwargs)
        trace.extend(gate_trace)
        if not ok:
            return CandidatePipelineResult(sent=False, veto_reason=str(reason or "GATE_VETO"), trace=trace)

        score_meta = self.scoring.run(ctx=ctx, cand=cand, **kwargs)
        trace.append(StageTraceItem(stage="ScoringRunner", ok=True, meta=score_meta))

        payload = self.payload.run(ctx=ctx, cand=cand, score=score_meta, **kwargs)
        trace.append(StageTraceItem(stage="PayloadBuilder", ok=True, meta={"keys": list(payload.keys())[:20]}))

        sent = self.outbox.run(payload=payload, ctx=ctx, cand=cand, **kwargs)
        trace.append(StageTraceItem(stage="OutboxWriter", ok=bool(sent), reason="" if sent else "not_sent"))

        if self.obs is not None:
            self.obs.run(ctx=ctx, cand=cand, payload=payload, sent=sent, trace=trace, **kwargs)

        return CandidatePipelineResult(sent=bool(sent), trace=trace)
