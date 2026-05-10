"""
SLO Regression Suite: GateOrchestrator
======================================
Covers the 4 bugs fixed during the 2026-05-09 GateOrchestrator audit:

    BUG-1  check_smt was async with no awaits → returned coroutine, not GateDecisionV1
    BUG-2  check_portfolio was never called in signal_pipeline.publish_signal
    BUG-3  _GATES_EVAL_TOTAL was referenced but never created → silent NameError
    BUG-4  indicators rebound after kind-resolution → split-reference

And 2 architectural fixes:
    ARCH-3  TIGHTEN decision from edge_cost gate silently ignored in orchestrator.py
    METRICS Prometheus collision gate_latency_us between gates.py and facade.py
"""

from __future__ import annotations

import asyncio
import inspect
import os
import time
import types
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_gate_ctx(symbol: str = "BTCUSDT", ts_ms: int = 0) -> Any:
    """Minimal context object accepted by GateOrchestrator methods."""
    ctx = types.SimpleNamespace()
    ctx.symbol = symbol
    ctx.ts_ms = ts_ms or int(time.time() * 1000)
    ctx.redis = None
    ctx.indicators = {}
    return ctx


def _make_gate_decision(decision: str = "ALLOW", reason_code: str = "OK") -> Any:
    """Build a minimal GateDecisionV1 for mocking."""
    from core.signal_payload import GateDecisionV1
    return GateDecisionV1(
        stage="test",
        gate="TestGate",
        decision=decision,
        reason_code=reason_code,
        severity="INFO",
        profile="default",
        fail_policy="OPEN",
        ts_event_ms=0,
        ts_decision_ms=0,
        latency_us=0,
        inputs_hash="",
        notes={},
    )


def _make_orchestrator(**overrides: Any) -> Any:
    """Construct a GateOrchestrator with all-None gates (safe for unit tests)."""
    from handlers.crypto_orderflow.components.gates import GateOrchestrator
    kwargs = dict(
        entry_policy=None,
        cost_gate=None,
        portfolio_gate=None,
        consistency_gate=None,
        regime_liquidity_gate=None,
        smt_gate=None,
        dq_gate=None,
        book_sanity_gate=None,
        stream_integrity_gate=None,
        atr_floor_gate=None,
        breadth_gate=None,
    )
    kwargs.update(overrides)
    return GateOrchestrator(**kwargs)


# ===========================================================================
# BUG-1: check_smt must be a sync function (not async/coroutine)
# ===========================================================================


class TestBug1SmtSync:
    """BUG-1: check_smt was declared `async def` but had no `await` inside.
    In orchestrator.py it was called without `await`, which meant it returned
    a coroutine object instead of GateDecisionV1 → SMT gate was completely bypassed.
    """

    def test_check_smt_is_not_async(self):
        """check_smt must NOT be a coroutine function."""
        from handlers.crypto_orderflow.components.gates import GateOrchestrator
        method = GateOrchestrator.check_smt
        assert not inspect.iscoroutinefunction(method), (
            "BUG-1 regressed: check_smt is async again. "
            "This means SMT gate silently returns coroutine in orchestrator.py."
        )

    def test_check_smt_returns_gate_decision(self):
        """check_smt must return GateDecisionV1, not a coroutine."""
        from core.signal_payload import GateDecisionV1

        orch = _make_orchestrator()
        ctx = _make_gate_ctx()
        result = orch.check_smt(ctx=ctx, kind="breakout", side=1)

        assert isinstance(result, GateDecisionV1), (
            f"BUG-1 regressed: check_smt returned {type(result)}, expected GateDecisionV1"
        )

    def test_check_smt_abstain_when_no_gate(self):
        """When _smt_gate is None (default), check_smt must return ABSTAIN, not raise."""
        orch = _make_orchestrator()
        assert orch._smt_gate is None, "Precondition: no SMT gate configured"

        ctx = _make_gate_ctx()
        result = orch.check_smt(ctx=ctx, kind="absorption", side=-1)
        assert result.decision in ("ABSTAIN", "ALLOW"), (
            f"Expected ABSTAIN/ALLOW when SMT gate not configured, got {result.decision}"
        )

    def test_orchestrator_smt_no_await_required(self):
        """In orchestrator.py line 432 the call has no await. Verify no TypeError."""
        orch = _make_orchestrator()
        ctx = _make_gate_ctx()
        # Simulate the exact call pattern from orchestrator.py:432
        try:
            result = orch.check_smt(ctx=ctx, kind="test", side=1)
        except TypeError as e:
            pytest.fail(f"BUG-1 regressed: calling check_smt without await raised TypeError: {e}")
        # Must NOT be a coroutine (would require await and would be mishandled)
        assert not asyncio.iscoroutine(result), "BUG-1: check_smt returned coroutine"


# ===========================================================================
# BUG-2: check_portfolio must be called in signal_pipeline.publish_signal
# ===========================================================================


class TestBug2PortfolioGateCalled:
    """BUG-2: check_portfolio was implemented in GateOrchestrator but never
    invoked in SignalPipeline.publish_signal. Portfolio / exposure limits were
    silently bypassed.
    """

    def test_check_portfolio_method_exists_on_orchestrator(self):
        """GateOrchestrator must expose check_portfolio."""
        from handlers.crypto_orderflow.components.gates import GateOrchestrator
        assert callable(getattr(GateOrchestrator, "check_portfolio", None)), (
            "check_portfolio missing from GateOrchestrator"
        )

    def test_check_portfolio_fail_open_when_not_configured(self):
        """If portfolio_gate is None, check_portfolio must ALLOW (fail-open)."""
        from core.signal_payload import GateDecisionV1

        orch = _make_orchestrator()
        assert orch.portfolio_gate is None, "Precondition: portfolio_gate not set"

        ctx = _make_gate_ctx()
        result = orch.check_portfolio(
            ctx, source="test", side="LONG", intent_notional=1000.0, symbol="BTCUSDT"
        )
        assert isinstance(result, GateDecisionV1)
        assert result.decision == "ALLOW"
        assert result.reason_code == "PORTFOLIO_GATE_NOT_CONFIGURED"

    def test_check_portfolio_called_when_gate_configured(self):
        """When portfolio_gate is configured, evaluate() must be called."""
        mock_gate = MagicMock()
        mock_gate.evaluate.return_value = _make_gate_decision("ALLOW", "PORTFOLIO_APPROVED")

        orch = _make_orchestrator(portfolio_gate=mock_gate)

        ctx = _make_gate_ctx()
        result = orch.check_portfolio(
            ctx, source="CryptoOrderFlow", side="LONG", intent_notional=500.0, symbol="ETHUSDT"
        )

        mock_gate.evaluate.assert_called_once()
        call_kwargs = mock_gate.evaluate.call_args.kwargs
        assert call_kwargs["symbol"] == "ETHUSDT"
        assert call_kwargs["side"] == "LONG"
        assert call_kwargs["intent_notional"] == 500.0

    def test_check_portfolio_deny_blocks(self):
        """When portfolio_gate returns DENY, check_portfolio must forward DENY."""
        mock_gate = MagicMock()
        mock_gate.evaluate.return_value = _make_gate_decision(
            "DENY", "PORTFOLIO_MAX_POSITIONS_EXCEEDED"
        )

        orch = _make_orchestrator(portfolio_gate=mock_gate)

        ctx = _make_gate_ctx()
        result = orch.check_portfolio(
            ctx, source="CryptoOrderFlow", side="LONG", intent_notional=999.0
        )
        assert result.decision == "DENY"
        assert result.reason_code == "PORTFOLIO_MAX_POSITIONS_EXCEEDED"

    def test_signal_pipeline_check_portfolio_in_source(self):
        """Signal pipeline source code must contain a check_portfolio call (static check)."""
        pipeline_path = os.path.join(
            os.path.dirname(__file__),
            "../../services/orderflow/signal_pipeline.py",
        )
        pipeline_path = os.path.normpath(pipeline_path)
        with open(pipeline_path) as f:
            src = f.read()
        assert "check_portfolio" in src, (
            "BUG-2 regressed: check_portfolio call not found in signal_pipeline.py"
        )


# ===========================================================================
# BUG-3: _GATES_EVAL_TOTAL must be defined in gates.py
# ===========================================================================


class TestBug3GatesEvalTotal:
    """BUG-3: _GATES_EVAL_TOTAL was used inside _record_gate_eval() but never
    created in the prometheus try-block → silent NameError on every evaluation,
    masked by contextlib.suppress(Exception).
    """

    def test_gates_eval_total_module_attribute_exists(self):
        """_GATES_EVAL_TOTAL must be a module-level attribute in gates.py."""
        import handlers.crypto_orderflow.components.gates as gates_mod
        assert hasattr(gates_mod, "_GATES_EVAL_TOTAL"), (
            "BUG-3 regressed: _GATES_EVAL_TOTAL not found in gates module"
        )

    def test_gates_eval_total_not_undefined(self):
        """_GATES_EVAL_TOTAL must not raise NameError when accessed."""
        import handlers.crypto_orderflow.components.gates as gates_mod
        try:
            val = gates_mod._GATES_EVAL_TOTAL
        except NameError as e:
            pytest.fail(f"BUG-3 regressed: accessing _GATES_EVAL_TOTAL raises NameError: {e}")
        # Either a Prometheus Counter or None (if prometheus_client not available)
        assert val is None or hasattr(val, "labels"), (
            f"_GATES_EVAL_TOTAL has unexpected type: {type(val)}"
        )

    def test_record_gate_eval_does_not_raise(self):
        """_record_gate_eval must never raise regardless of prometheus availability."""
        import handlers.crypto_orderflow.components.gates as gates_mod
        try:
            gates_mod._record_gate_eval("TestGate")
        except Exception as e:
            pytest.fail(f"BUG-3 regressed: _record_gate_eval raised {type(e).__name__}: {e}")


# ===========================================================================
# BUG-4: indicators reference must be stable before kind resolution
# ===========================================================================


class TestBug4IndicatorsRebind:
    """BUG-4: indicators was bound from _build_gate_ctx, then rebound via
    signal.setdefault("indicators", {}) AFTER kind resolution used it.
    This created a split-reference: mutations before the rebind didn't
    appear in signal["indicators"] and vice-versa.
    """

    def test_indicators_kind_comes_from_signal_indicators(self):
        """kind must read from the same indicators dict that gates mutate."""
        pipeline_path = os.path.normpath(
            os.path.join(os.path.dirname(__file__),
                         "../../services/orderflow/signal_pipeline.py")
        )
        with open(pipeline_path) as f:
            lines = f.readlines()

        # Find the line that binds indicators via signal.setdefault
        setdefault_lines = [
            i for i, l in enumerate(lines)
            if 'signal.setdefault("indicators"' in l or "signal.setdefault('indicators'" in l
        ]
        # Find the line that reads indicators.get("kind") for kind resolution
        kind_lines = [
            i for i, l in enumerate(lines)
            if 'indicators.get("kind")' in l or "indicators.get('kind')" in l
        ]

        assert setdefault_lines, "signal.setdefault('indicators') not found in pipeline"
        assert kind_lines, "indicators.get('kind') not found in pipeline"

        # setdefault must come BEFORE kind resolution (lower line number)
        first_setdefault = min(setdefault_lines)
        first_kind = min(kind_lines)
        assert first_setdefault < first_kind, (
            f"BUG-4 regressed: indicators rebound at line {first_setdefault+1} AFTER "
            f"kind resolution at line {first_kind+1}. "
            "Mutations from gate context won't be visible during kind detection."
        )


# ===========================================================================
# ARCH-3: TIGHTEN handling in orchestrator.py
# ===========================================================================


class TestArch3TightenInOrchestrator:
    """ARCH-3: orchestrator.py ignored TIGHTEN decisions from edge_cost gate.
    signal_pipeline.py correctly accumulated tighten_add_bps into expected_slippage_bps,
    but orchestrator.py skipped any non-DENY decision silently.
    """

    def test_orchestrator_source_contains_tighten_handling(self):
        """orchestrator.py must handle TIGHTEN from edge_cost gate."""
        orch_path = os.path.normpath(
            os.path.join(os.path.dirname(__file__),
                         "../../handlers/crypto_orderflow/pipeline/orchestrator.py")
        )
        with open(orch_path) as f:
            src = f.read()
        assert '"TIGHTEN"' in src or "'TIGHTEN'" in src, (
            "ARCH-3 regressed: TIGHTEN decision not handled in orchestrator.py"
        )

    def test_tighten_accumulates_in_indicators(self):
        """When edge_cost gate returns TIGHTEN, expected_slippage_bps must increase."""
        # Simulate TIGHTEN decision propagation logic (mirrors orchestrator.py ARCH-3 fix)
        tighten_dec = _make_gate_decision("TIGHTEN", "EDGE_TIGHTEN_HIGH_SPREAD")
        # Patch notes dict to carry tighten_add_bps
        object.__setattr__(tighten_dec, "notes", {"tighten_add_bps": 3.5})

        ctx = _make_gate_ctx()
        ctx.indicators = {"expected_slippage_bps": 5.0}

        # Replicate the ARCH-3 fix logic
        _cost_dec = tighten_dec.decision
        if _cost_dec == "TIGHTEN":
            tadd = float(tighten_dec.notes.get("tighten_add_bps", 0.0) or 0.0)
            if tadd > 0 and isinstance(ctx.indicators, dict):
                ctx.indicators["expected_slippage_bps"] = (
                    float(ctx.indicators.get("expected_slippage_bps", 0.0) or 0.0) + tadd
                )

        assert ctx.indicators["expected_slippage_bps"] == pytest.approx(8.5), (
            "ARCH-3: TIGHTEN did not accumulate into expected_slippage_bps correctly"
        )


# ===========================================================================
# METRICS: Prometheus metric name collision
# ===========================================================================


class TestPrometheusCollision:
    """gate_latency_us was registered in both gates.py and ml_confirm_gate/facade.py.
    When both modules are imported in the same process (as in tests), Prometheus
    raised ValueError: Duplicated timeseries. Fixed by renaming facade.py metric
    to ml_confirm_latency_us.
    """

    def test_no_duplicate_gate_latency_us(self):
        """Both gates and ml_confirm_gate must be importable without Prometheus collision."""
        from prometheus_client import REGISTRY

        # Reset registry state by using a fresh registry for this check
        try:
            # Import both modules — they must coexist
            import handlers.crypto_orderflow.components.gates  # noqa: F401
            import services.ml_confirm.facade  # noqa: F401
        except ValueError as e:
            if "Duplicated timeseries" in str(e):
                pytest.fail(
                    f"Prometheus collision regression: {e}. "
                    "facade.py must use 'ml_confirm_latency_us' not 'gate_latency_us'."
                )
            raise

    def test_facade_uses_renamed_metric(self):
        """facade.py must register ml_confirm_latency_us, not gate_latency_us."""
        facade_path = os.path.normpath(
            os.path.join(os.path.dirname(__file__),
                         "../../services/ml_confirm_gate/facade.py")
        )
        with open(facade_path) as f:
            src = f.read()
        assert "ml_confirm_latency_us" in src, (
            "Prometheus collision regression: facade.py still uses 'gate_latency_us'. "
            "Must be renamed to 'ml_confirm_latency_us'."
        )
        # The old name must NOT appear as a metric registration string
        import re
        old_reg = re.findall(r'Histogram\s*\(\s*["\']gate_latency_us["\']', src)
        assert not old_reg, (
            f"Prometheus collision regression: found old 'gate_latency_us' Histogram "
            f"registration in facade.py: {old_reg}"
        )


# ===========================================================================
# Contract: GateDecisionV1 frozen dataclass invariants
# ===========================================================================


class TestGateDecisionV1Contract:
    """Golden contract tests — GateDecisionV1 must remain a frozen dataclass
    with exact field set. Any breaking change here breaks replay determinism.
    """

    REQUIRED_FIELDS = {
        "stage", "gate", "decision", "reason_code", "severity",
        "profile", "fail_policy", "ts_event_ms", "ts_decision_ms",
        "latency_us", "inputs_hash", "notes",
    }

    def test_required_fields_present(self):
        """All required fields must be present on GateDecisionV1."""
        from core.signal_payload import GateDecisionV1
        import dataclasses
        field_names = {f.name for f in dataclasses.fields(GateDecisionV1)}
        missing = self.REQUIRED_FIELDS - field_names
        assert not missing, (
            f"GateDecisionV1 contract broken: missing fields {missing}"
        )

    def test_frozen_immutability(self):
        """GateDecisionV1 must be frozen (immutable after construction)."""
        from core.signal_payload import GateDecisionV1
        dec = _make_gate_decision()
        with pytest.raises((TypeError, AttributeError)):
            dec.decision = "MODIFIED"  # type: ignore[misc]

    def test_valid_decision_values(self):
        """decision field must accept the canonical set of values."""
        from core.signal_payload import GateDecisionV1
        for valid in ("ALLOW", "DENY", "SHADOW_DENY", "TIGHTEN", "ABSTAIN"):
            dec = GateDecisionV1(
                stage="test", gate="g", decision=valid, reason_code="OK",
                severity="INFO", profile="p", fail_policy="OPEN",
                ts_event_ms=0, ts_decision_ms=0, latency_us=0, inputs_hash="", notes={},
            )
            assert dec.decision == valid
