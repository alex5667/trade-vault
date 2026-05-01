from __future__ import annotations

import logging
import os
from typing import Any, Optional, Tuple

from common.dq_flags import append_dq_flag
from common.ctx_cache import cached_on_ctx
from handlers.crypto_orderflow.utils.entry_policy_gate import EntryPolicyGate
from handlers.crypto_orderflow.utils.edge_cost_gate import EdgeCostGate
from handlers.crypto_orderflow.utils.pre_publish_gates import GateDecision

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Optional Prometheus metrics (fail-open if registry unavailable)
# ---------------------------------------------------------------------------
try:
    from prometheus_client import Counter

    _GATES_ERROR = Counter(
        "gates_error_total",
        "Number of gate errors (fail-open activations) per gate",
        ["gate", "reason"],
    )
    _GATES_METRICS = True
except Exception:  # pragma: no cover
    _GATES_METRICS = False


def _record_gate_error(gate: str, reason: str) -> None:
    """Increment gates_error_total counter, fail-open if metrics unavailable."""
    if _GATES_METRICS:
        try:
            _GATES_ERROR.labels(gate=gate, reason=reason).inc()
        except Exception:
            pass

class CryptoSignalGates:
    """
    Manages signal validation gates:
    - Consistency (cached)
    - Entry Policy (microstructure)
    - Cost / Edge (profitability)
    - EV Rule (expected value)
    """

    def __init__(
        self,
        entry_policy: Optional[EntryPolicyGate],
        cost_gate: Optional[EdgeCostGate],
        consistency_gate: Any = None,
        regime_liquidity_gate: Any = None,
        smt_gate: Any = None,
    ):
        self._entry_policy = entry_policy
        self._cost_gate = cost_gate
        self._consistency_gate = consistency_gate
        self._regime_liquidity_gate = regime_liquidity_gate
        self._smt_gate = smt_gate

        # Cache regime gate config from ENV
        self._regime_strict = (os.getenv("REGIME_GATE_STRICT", "0") or "0").strip().lower() in {"1", "true", "yes", "on"}
        def _csv(name: str) -> set[str]:
            v = (os.getenv(name, "") or "").strip().lower()
            return {x.strip() for x in v.split(",") if x.strip()}
        
        self._regime_breakout_block = _csv("REGIME_GATE_BREAKOUT_BLOCK")
        self._regime_extreme_block = _csv("REGIME_GATE_EXTREME_BLOCK")

    def check_quality(self, ctx: Any, kind: str, side: str = "") -> Any:
        """
        Check Quality Gate (Detector level).
        Delegates to RegimeSessionLiquidityGate.
        Fail-open: on any exception returns a pass-through decision + DQ flag.
        """
        if self._regime_liquidity_gate is None:
            return type("QA", (), {"veto": False, "reason": "", "flags": []})()
        try:
            sym = str(getattr(ctx, "symbol", "") or "")
            return self._regime_liquidity_gate.evaluate(ctx=ctx, symbol=sym, kind=kind, side=side)
        except Exception as exc:
            log.exception("check_quality failed, fail-open: %s", exc)
            self._mark_dq(ctx, "quality_error", exc=exc)
            _record_gate_error("check_quality", "quality_error")
            return type("QA", (), {"veto": False, "reason": "FAIL_OPEN_QUALITY", "flags": []})()


    def check_entry_policy(self, ctx: Any, payload: dict) -> Any:
        """Evaluates entry microstructure conditions with fail-open."""
        if self._entry_policy is None:
            return GateDecision(apply=True, veto=False, reason_code="OK", gate="EntryPolicyGate", notes="no_gate")
        try:
            # Extract args from ctx or payload
            sym = str(getattr(ctx, "symbol", "") or "").strip().upper()
            kind = str(payload.get("kind", "") or "custom")
            return self._entry_policy.evaluate(ctx=ctx, symbol=sym, kind=kind)
        except Exception as e:
            log.exception("check_entry_policy failed, fail-open: %s", e)
            return GateDecision(apply=True, veto=False, reason_code="ERROR", gate="EntryPolicyGate", notes=str(e))

    # check_ev_gate removed: dead code (body was pass-only, never called from active paths).
    # If EV-gate logic is needed, implement as a proper gate with real logic and tests.
    # Ref: audit P2.6 / 2026-04-15

    def consistency_once(self, *, ctx: Any, symbol: str, kind: str, side: str) -> Any:
        """
        Evaluate SignalConsistencyGate exactly once per (symbol, kind, side).
        """
        fn = getattr(self._consistency_gate, "evaluate", None) if self._consistency_gate is not None else None

        if not callable(fn):
            return type("QD", (), {"apply": False, "veto": False, "reason_code": "OK", "notes": "no_gate"})()

        key = (str(symbol), str(kind), str(side))

        def _compute():
            try:
                return fn(ctx=ctx, symbol=str(symbol), kind=str(kind), side=str(side))
            except Exception as exc:
                # fail-open: explicitly mark as FAIL_OPEN so downstream telemetry
                # can distinguish real OK from degraded pass-through (P1.3)
                log.exception("consistency_once failed, fail-open: %s", exc)
                self._mark_dq(ctx, "consistency_error", exc=exc)
                _record_gate_error("consistency_once", "FAIL_OPEN_CONSISTENCY")
                return type("QD", (), {"apply": True, "veto": False, "reason_code": "FAIL_OPEN_CONSISTENCY", "notes": "fail_open"})()

        return cached_on_ctx(ctx, slot="_cache_consistency_decision", key=key, compute=_compute)

    def edge_cost_cached(self, *, ctx: Any, kind: str, symbol: str, side: Any, cfg: Any = None) -> Any:
        """
        Evaluate cost/edge logic with caching on ctx.
        On exception returns a structured FAIL_OPEN_EDGE_COST decision + DQ flag
        so the orchestrator can detect edge-gate regressions via telemetry (P1.2).
        """
        gate = self._cost_gate
        if gate is None:
            return None

        key = (str(kind), str(symbol), str(side))

        def _compute():
            try:
                return gate.evaluate(ctx=ctx, kind=kind, symbol=symbol, side=side, cfg=cfg)
            except Exception as exc:
                log.exception("edge_cost_cached failed, fail-open: %s", exc)
                self._mark_dq(ctx, "edge_cost_error", exc=exc)
                _record_gate_error("edge_cost_cached", "FAIL_OPEN_EDGE_COST")
                # Return structured decision so orchestrator veto-check works correctly
                return type("ECD", (), {"veto": False, "reason_code": "FAIL_OPEN_EDGE_COST", "apply": True})()

        return cached_on_ctx(ctx, slot="_cache_edge_cost", key=key, compute=_compute)

    def _mark_dq(self, ctx: Any, flag: str, exc: Optional[Exception] = None) -> None:
        """Append a DQ flag to ctx and optionally log the originating exception."""
        if exc is not None:
            log.debug("_mark_dq %s: %s", flag, exc)
        append_dq_flag(ctx, flag)

    def check_regime_gate(self, ctx: Any, kind: str) -> Tuple[bool, str]:
        """
        Step 2: Strict regime gate (configured via ENV).

        Uses substring match (e.g. 'range' in 'wide_range') to preserve 
        production backward compatibility (P1.4).
        """
        if not self._regime_strict:
            return (True, "OK")

        def _sl(v: Any) -> str:
            try:
                if isinstance(v, str):
                    return v.lower()
                return str(v or "").lower()
            except Exception:
                return ""

        k = _sl(kind)
        regime = _sl(getattr(ctx, "market_regime", None) or getattr(ctx, "regime", None) or "")

        if regime:
            if k == "breakout" and any(b in regime for b in self._regime_breakout_block):
                return (False, "VETO_REGIME_BREAKOUT_BLOCK")
            if k == "extreme" and any(b in regime for b in self._regime_extreme_block):
                return (False, "VETO_REGIME_EXTREME_BLOCK")

        return (True, "OK")

    def check_smt(self, ctx: Any, kind: str, side: Any) -> Any:
        """
        Step 3: SMT Leader Coherence Gate (Observe/Veto).
        Fail-open: on any exception returns a pass-through decision + DQ flag (P1.1).
        """
        if self._smt_gate is None:
            return type("SMT", (), {"veto": False, "reason": "SMT_DISABLED"})()

        # Determine direction from side
        # Side can be int (1/-1) or str ("LONG"/"SHORT"/"BUY"/"SELL")
        try:
            s = str(side).upper()
            if s in {"1", "BUY", "LONG"}:
                direction = "UP"
            elif s in {"-1", "SELL", "SHORT"}:
                direction = "DOWN"
            else:
                direction = "NA"
        except Exception:
            direction = "NA"

        if direction == "NA":
            return type("SMT_NA", (), {"veto": False, "reason_code": "OK", "apply": False, "notes": "VETO_SMT_NA_SIDE (shadow)"})()

        try:
            return self._smt_gate.evaluate(
                ctx=ctx,
                symbol=str(getattr(ctx, "symbol", "") or ""),
                kind=str(kind),
                direction=direction,
            )
        except Exception as exc:
            log.exception("check_smt failed, fail-open: %s", exc)
            self._mark_dq(ctx, "smt_error", exc=exc)
            _record_gate_error("check_smt", "smt_error")
            return type("SMT", (), {"veto": False, "reason": "FAIL_OPEN_SMT", "reason_code": "FAIL_OPEN_SMT"})()
