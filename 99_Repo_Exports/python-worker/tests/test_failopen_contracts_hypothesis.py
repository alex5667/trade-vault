from __future__ import annotations
"""Fail-open contract tests (Hypothesis).

Goals (per request 5.2):
  - _safe_str / _safe_lower / _safe_reason_u16 / _sanitize_u16_list never raise
    and always return a predictable type/range.
  - EdgeCostGate: missing tp1/sl must be handled deterministically
    (no exception, meaningful reason).

Notes:
  - The exact module paths can vary between branches. Tests are written to be
    resilient:
      * use pytest.importorskip for optional modules
      * validate invariants instead of exact reason strings
"""


from dataclasses import dataclass
from typing import Any, Iterable, Tuple

import pytest


hypothesis = pytest.importorskip("hypothesis")
strategies = pytest.importorskip("hypothesis.strategies")


def _import_first(*paths: str):
    """Import helper: tries modules in order and returns the first that imports."""
    last = None
    for p in paths:
        try:
            mod = __import__(p, fromlist=["*"])
            return mod
        except Exception as e:  # pragma: no cover
            last = e
    raise last  # type: ignore[misc]


# ---------------------------------------------------------------------------
# 1) safe_* contract tests
# ---------------------------------------------------------------------------


@strategies.composite
def any_jsonish(draw):
    """Generate broadly 'messy' inputs for safe-casters."""
    scalars = strategies.one_of(
        strategies.none(),
        strategies.booleans(),
        strategies.integers(min_value=-10**9, max_value=10**9),
        strategies.floats(allow_nan=True, allow_infinity=True, width=32),
        strategies.text(max_size=128),
        strategies.binary(max_size=128),
    )
    # keep recursive structures shallow to avoid timeouts
    lists = strategies.lists(scalars, max_size=16)
    dicts = strategies.dictionaries(
        keys=strategies.text(min_size=0, max_size=32),
        values=scalars,
        max_size=16,
    )
    return draw(strategies.one_of(scalars, lists, dicts))


def _assert_u16(x: Any) -> None:
    assert isinstance(x, int)
    assert 0 <= x <= 65535


def _assert_u16_list(xs: Any) -> None:
    assert isinstance(xs, list)
    for v in xs:
        _assert_u16(v)


def _get_safe_funcs():
    """Find safe helpers in the codebase.

We expect them in crypto_orderflow utils (edge_cost_gate), but allow alternates.
"""
    # Most likely locations (add more if you move the helpers):
    mod = _import_first(
        "handlers.crypto_orderflow.utils.edge_cost_gate",
        "handlers.crypto_orderflow.utils.safe",
        "common.safe",
    )
    safe_str = getattr(mod, "_safe_str", None)
    safe_lower = getattr(mod, "_safe_lower", None)
    safe_reason_u16 = getattr(mod, "_safe_reason_u16", None)
    sanitize_u16_list = getattr(mod, "_sanitize_u16_list", None)
    if not callable(safe_str) or not callable(safe_lower) or not callable(safe_reason_u16) or not callable(sanitize_u16_list):
        pytest.skip(
            "safe_* helpers are not importable/callable from expected modules. "
            "Adjust _get_safe_funcs() paths to your project layout."
        )
    return safe_str, safe_lower, safe_reason_u16, sanitize_u16_list


@hypothesis.given(x=any_jsonish())
def test_safe_str_never_raises_and_returns_str(x: Any):
    safe_str, _, _, _ = _get_safe_funcs()
    out = safe_str(x)
    assert isinstance(out, str)


@hypothesis.given(x=any_jsonish())
def test_safe_lower_never_raises_and_returns_lower_str(x: Any):
    _, safe_lower, _, _ = _get_safe_funcs()
    out = safe_lower(x)
    assert isinstance(out, str)
    assert out == out.lower()


@hypothesis.given(x=any_jsonish())
def test_safe_reason_u16_returns_u16_for_any_input(x: Any):
    _, _, safe_reason_u16, _ = _get_safe_funcs()
    out1 = safe_reason_u16(x)
    out2 = safe_reason_u16(x)
    _assert_u16(out1)
    # determinism
    assert out1 == out2


@hypothesis.given(xs=strategies.lists(any_jsonish(), max_size=64))
def test_sanitize_u16_list_returns_list_of_u16(xs: list[Any]):
    _, _, _, sanitize_u16_list = _get_safe_funcs()
    out = sanitize_u16_list(xs)
    _assert_u16_list(out)
    # should not grow
    assert len(out) <= len(xs)


# ---------------------------------------------------------------------------
# 2) Cost gate: missing tp1/sl must be deterministic (no crash + clear reason)
# ---------------------------------------------------------------------------


@dataclass
class _Ctx:
    """Minimal ctx stub used by EdgeCostGate tests."""

    entry_price: float = 100.0
    # Missing tp1/sl by default
    tp1_price: Any = None
    sl_price: Any = None
    tp_levels: Any = None
    sl: Any = None
    symbol: str = "TEST"


def _normalize_gate_result(res: Any) -> Tuple[bool, str]:
    """Accept multiple gate return shapes and normalize to (ok, reason_code)."""
    # common pattern: (ok, rc)
    if isinstance(res, tuple) and len(res) >= 2:
        ok = bool(res[0])
        rc = str(res[1] or "")
        return ok, rc
    # object pattern: res.veto / res.reason_code
    if hasattr(res, "veto"):
        ok = not bool(getattr(res, "veto"))
        rc = str(getattr(res, "reason_code", "") or "")
        return ok, rc
    # bool only
    if isinstance(res, bool):
        return bool(res), ""
    return False, ""


def test_cost_gate_missing_tp1_and_sl_is_fail_open_but_predictable():
    mod = pytest.importorskip("handlers.crypto_orderflow.utils.edge_cost_gate")
    EdgeCostGate = getattr(mod, "EdgeCostGate", None)
    assert EdgeCostGate is not None, "EdgeCostGate not found in edge_cost_gate module"

    gate = EdgeCostGate()  # should be constructible with defaults
    ctx = _Ctx()

    # Must not raise
    res = gate.evaluate(ctx)  # type: ignore[attr-defined]
    ok, rc = _normalize_gate_result(res)

    # We cannot compute EV/cost without tp1/sl: gate must NOT crash.
    # Policy expectation: either veto with explicit reason, or fail-open with a clear marker.
    # We only assert that reason is meaningful when it is a veto.
    assert isinstance(ok, bool)
    assert isinstance(rc, str)
    if not ok:
        assert rc, "missing tp1/sl must produce a non-empty reason_code"
        low = rc.lower()
        assert ("tp" in low) or ("sl" in low) or ("missing" in low), rc
