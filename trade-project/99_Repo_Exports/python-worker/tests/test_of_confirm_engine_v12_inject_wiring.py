"""Regression test: of_confirm_engine.build() must call inject_v12_of_features.

Without this wiring 21 v12_of new keys (Group MA/MB/MC/MD/ME/MX) are absent in
the outbound `indicators` payload because tick_processor.py lives in
reference/ and its inject call is dead code in prod (audit 2026-05-19).

This is a static-import test: assert the engine imports
inject_v12_of_features in source. Cheap to run, hard to silently break.
"""
from __future__ import annotations

from pathlib import Path

ENGINE = Path(__file__).resolve().parents[1] / "core" / "of_confirm_engine.py"
V12_INJECT_LINE = "from core.v12_of_features import inject_v12_of_features"
V12_CALL_FRAG = "inject_v12_of_features(runtime=runtime"


def test_engine_imports_v12_inject():
    src = ENGINE.read_text()
    assert V12_INJECT_LINE in src, (
        f"of_confirm_engine.py must import inject_v12_of_features; "
        f"missing line: {V12_INJECT_LINE!r}. Without this, 21 v12_of base keys "
        f"(trade_arrival_rate_hz, large_trade_ratio, etc.) never reach the "
        f"outbound indicators dict — train/serve skew."
    )


def test_engine_calls_v12_inject():
    src = ENGINE.read_text()
    assert V12_CALL_FRAG in src, (
        f"of_confirm_engine.py must call inject_v12_of_features(runtime=runtime,...); "
        f"missing fragment: {V12_CALL_FRAG!r}."
    )


def test_v12_inject_populates_21_keys():
    """Sanity: inject_v12_of_features(empty_runtime, ...) writes 21 keys."""
    from core.v12_of_features import inject_v12_of_features

    class _FakeRuntime:
        pass

    ind: dict = {}
    inject_v12_of_features(runtime=_FakeRuntime(), now_ms=1779158002461, indicators=ind)
    assert len(ind) == 21, f"expected 21 v12_of new keys, got {len(ind)}: {sorted(ind)}"
    # Spot-check critical Group MA keys
    for k in (
        "trade_arrival_rate_hz",
        "large_trade_ratio",
        "tick_direction_run",
        "trade_size_entropy",
    ):
        assert k in ind, f"missing v12_of MA key: {k}"
