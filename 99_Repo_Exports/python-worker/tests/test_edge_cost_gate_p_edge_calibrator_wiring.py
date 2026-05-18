from __future__ import annotations

"""Unit tests for the calibrator-reader wiring inside
`EdgeCostGate._p_min_for_kind`.

We don't exercise the full evaluate() path here — those integration paths are
covered by the existing edge_cost_gate test suites. We isolate the lookup
helper to assert the precedence:

  1. PEdgeThresholdReader (when get_reader() returns a live instance)
  2. ev_p_min_by_kind[kind]
  3. ev_p_min default
"""

from typing import Any

import pytest

from handlers.crypto_orderflow.utils.edge_cost_gate import EdgeCostGate


# ---------------------------------------------------------------------------
# stub reader & monkey-patched factory
# ---------------------------------------------------------------------------


class _StubReader:
    def __init__(self, returns: float) -> None:
        self.returns = returns
        self.calls: list[dict[str, Any]] = []

    def p_min_for(
        self,
        *,
        symbol: str,
        regime: str,
        kind: str,
        default: float,
    ) -> float:
        self.calls.append({"symbol": symbol, "regime": regime, "kind": kind, "default": default})
        return self.returns


@pytest.fixture
def reader_stub(monkeypatch: pytest.MonkeyPatch) -> _StubReader:
    stub = _StubReader(returns=0.62)
    # Patch the import target inside the lazy import in _p_min_for_kind.
    monkeypatch.setattr(
        "core.p_edge_threshold_reader.get_reader",
        lambda: stub,
    )
    return stub


@pytest.fixture
def reader_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        "core.p_edge_threshold_reader.get_reader",
        lambda: None,
    )


# ---------------------------------------------------------------------------
# behaviour
# ---------------------------------------------------------------------------


def _gate(*, ev_p_min: float = 0.55, by_kind: dict[str, float] | None = None) -> EdgeCostGate:
    return EdgeCostGate(
        enabled=True,
        mode="ev",
        strict_missing_levels=False,
        apply_kinds=set(),
        k_default=4.0,
        k_by_symbol={},
        fees_bps_default=4.0,
        slippage_bps_default=4.0,
        slippage_use_spread_half=True,
        min_expected_move_bps_default=0.0,
        min_expected_move_bps_by_symbol={},
        ev_p_min=ev_p_min,
        ev_p_min_by_kind=by_kind or {},
    )


def test_uses_reader_value_when_available(reader_stub: _StubReader) -> None:
    gate = _gate(ev_p_min=0.55)
    val = gate._p_min_for_kind("breakout", symbol="BTCUSDT", regime="trend")
    assert val == 0.62
    assert reader_stub.calls == [{
        "symbol": "BTCUSDT", "regime": "trend",
        "kind": "breakout", "default": 0.55,
    }]


def test_reader_default_is_per_kind_floor(reader_stub: _StubReader) -> None:
    """When the gate has a per-kind ENV override, that override is passed as
    `default` to the reader — preserves the static floor semantics."""
    gate = _gate(ev_p_min=0.55, by_kind={"breakout": 0.58})
    gate._p_min_for_kind("breakout", symbol="BTCUSDT", regime="trend")
    assert reader_stub.calls[-1]["default"] == 0.58


def test_falls_back_to_static_when_reader_disabled(reader_disabled: None) -> None:
    gate = _gate(ev_p_min=0.55, by_kind={"breakout": 0.58})
    assert gate._p_min_for_kind("breakout", symbol="BTCUSDT", regime="trend") == 0.58
    assert gate._p_min_for_kind("absorption", symbol="BTCUSDT", regime="trend") == 0.55


def test_reader_exception_falls_back_to_static(monkeypatch: pytest.MonkeyPatch) -> None:
    class _BoomReader:
        def p_min_for(self, **_: Any) -> float:
            raise RuntimeError("boom")

    monkeypatch.setattr(
        "core.p_edge_threshold_reader.get_reader",
        lambda: _BoomReader(),
    )
    gate = _gate(ev_p_min=0.55, by_kind={"breakout": 0.58})
    assert gate._p_min_for_kind("breakout", symbol="BTCUSDT", regime="trend") == 0.58


def test_no_arg_call_still_works(reader_stub: _StubReader) -> None:
    """Back-compat — call sites that haven't been upgraded to pass
    symbol/regime still get a valid float."""
    gate = _gate(ev_p_min=0.55)
    val = gate._p_min_for_kind("breakout")
    assert val == 0.62  # reader was queried with empty symbol/regime
    assert reader_stub.calls[-1]["symbol"] == ""
    assert reader_stub.calls[-1]["regime"] == ""
