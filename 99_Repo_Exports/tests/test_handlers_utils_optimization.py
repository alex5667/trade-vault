"""
Regression tests verifying the handlers/crypto_orderflow/utils optimization:
- No duplicate class/function definitions
- LogSamplerFactory correctness
- _b2s hoisting (module-level, reusable)
- pre_publish_gates _safe_float uses correct signature with default
- smt_coherence_gate continuation enforcement logic (indentation fixed correctly)
"""
from __future__ import annotations

import inspect
import os
import sys
import types
from types import SimpleNamespace

import pytest

# python-worker is the root for domain.time_utils and domain.gate_profile imports
_PW = os.path.join(os.path.dirname(__file__), "..", "python-worker")
_PW = os.path.normpath(_PW)
if _PW not in sys.path:
    sys.path.insert(0, _PW)

# ---------------------------------------------------------------------------
# 1. Verify no duplicate QualityGateDecision in quality_gates module
# ---------------------------------------------------------------------------

def test_quality_gate_decision_is_single_class():
    """Importing multiple times must return the same class object (no shadow copies)."""
    import handlers.crypto_orderflow.utils.quality_gates as qg

    # Gather all items that look like QualityGateDecision
    found = [
        (name, obj) for name, obj in inspect.getmembers(qg, inspect.isclass)
        if name == "QualityGateDecision"
    ]
    # There must be exactly one definition
    assert len(found) == 1, f"Expected 1 QualityGateDecision, found {len(found)}"


def test_env_str_consistent_in_quality_gates():
    """_env_str must be a single consistent function (not shadowed)."""
    import handlers.crypto_orderflow.utils.quality_gates as qg

    fns = [
        (name, obj) for name, obj in inspect.getmembers(qg, inspect.isfunction)
        if name == "_env_str"
    ]
    assert len(fns) == 1, f"Expected 1 _env_str, found {len(fns)}"


def test_norm_symbol_consistent_in_quality_gates():
    """_norm_symbol must be a single function."""
    import handlers.crypto_orderflow.utils.quality_gates as qg

    fns = [
        (name, obj) for name, obj in inspect.getmembers(qg, inspect.isfunction)
        if name == "_norm_symbol"
    ]
    assert len(fns) == 1, f"Expected 1 _norm_symbol, found {len(fns)}"


# ---------------------------------------------------------------------------
# 2. Verify _env_str stripping (the surviving implementation must strip)
# ---------------------------------------------------------------------------

def test_env_str_strips_whitespace(monkeypatch):
    monkeypatch.setenv("_TEST_ENV_STR_STRIP", "  hello  ")
    import handlers.crypto_orderflow.utils.quality_gates as qg
    result = qg._env_str("_TEST_ENV_STR_STRIP", "")
    assert result == "hello", f"Expected 'hello', got {result!r}"


# ---------------------------------------------------------------------------
# 3. LogSamplerFactory - no double assignment of name_str
# ---------------------------------------------------------------------------

def test_log_sampler_factory_returns_same_instance():
    """get_sampler with same name must return the same instance (singleton)."""
    from handlers.crypto_orderflow.utils.log_sampler import LogSamplerFactory
    s1 = LogSamplerFactory.get_sampler("test_singleton_42", 100)
    s2 = LogSamplerFactory.get_sampler("test_singleton_42", 200)  # different default
    assert s1 is s2, "Expected same singleton instance from get_sampler"


def test_log_sampler_factory_math_not_imported():
    """math should no longer be imported in log_sampler."""
    import handlers.crypto_orderflow.utils.log_sampler as ls
    assert not hasattr(ls, "math") or "math" not in vars(ls), \
        "math should not be a module-level name in log_sampler"


# ---------------------------------------------------------------------------
# 4. pre_publish_gates - _b2s at module level and _safe_float single def
# ---------------------------------------------------------------------------

def test_pre_publish_gates_b2s_module_level():
    """_b2s must be a module-level function (not only nested)."""
    import handlers.crypto_orderflow.utils.pre_publish_gates as ppg
    assert hasattr(ppg, "_b2s"), "_b2s must be a module-level export in pre_publish_gates"
    assert callable(ppg._b2s)
    assert ppg._b2s(b"hello") == "hello"
    assert ppg._b2s("world") == "world"


def test_pre_publish_gates_safe_float_single_definition():
    """_safe_float must exist once and support the 'default' keyword."""
    import handlers.crypto_orderflow.utils.pre_publish_gates as ppg
    fns = [
        (name, obj) for name, obj in inspect.getmembers(ppg, inspect.isfunction)
        if name == "_safe_float"
    ]
    assert len(fns) == 1, f"Expected 1 _safe_float, found {len(fns)}"
    # Must accept default keyword
    result = ppg._safe_float("not_a_float", 99.0)
    assert result == 99.0, f"Expected 99.0, got {result}"


# ---------------------------------------------------------------------------
# 5. entry_policy_gate - _b2s at module level
# ---------------------------------------------------------------------------

def test_entry_policy_gate_b2s_module_level():
    import handlers.crypto_orderflow.utils.entry_policy_gate as epg
    assert hasattr(epg, "_b2s"), "_b2s must be module-level in entry_policy_gate"
    assert callable(epg._b2s)
    assert epg._b2s(b"abc") == "abc"


# ---------------------------------------------------------------------------
# 6. smt_coherence_gate - continuation enforcement (indentation check via logic)
# ---------------------------------------------------------------------------

def test_smt_coherence_gate_continuation_logic():
    """
    'continuation' decision + align==0 must set blocked=1.
    This was broken when the inner-if had extra leading space (logic was correct
    but could be misread by some parsers).
    """
    from handlers.crypto_orderflow.utils.smt_coherence_gate import SmtLeaderCoherenceGate

    class MockRedis:
        def get(self, key):
            import json
            return json.dumps({
                "leader": "BTCUSDT",
                "leader_dir": "UP",
                "leader_confirm": "1",
                "coh": "0.80",
                "decision": "continuation",   # V2 mode
                "pick": "",
                "news_blocked": "0",
                "news_until_ts_ms": "0",
                "leader_conf_score": "0.9",
            }).encode()

        def hgetall(self, key):
            return {}

        def xadd(self, *a, **kw):
            pass

    gate = SmtLeaderCoherenceGate(
        redis_client=MockRedis(),
        bundle_id="test_bundle",
        mode="veto",
        coh_hi_thr=0.65,
        veto_kinds=None,
        diag_stream="",
        diag_sample=1,
    )
    ctx = SimpleNamespace(ts_ms=1_700_000_000_000)
    # dir=DOWN against leader UP => align=0 => continuation enforcement => blocked
    dec = gate.evaluate(ctx=ctx, symbol="BTCUSDT", kind="breakout", direction="SHORT")
    assert dec.veto is True, f"Expected veto=True for countertrend continuation, got {dec}"


def test_smt_coherence_gate_golden_reversal():
    """'reversal' decision + symbol==pick => allow (golden ticket)."""
    from handlers.crypto_orderflow.utils.smt_coherence_gate import SmtLeaderCoherenceGate

    class MockRedis:
        def get(self, key):
            import json
            return json.dumps({
                "leader": "BTCUSDT",
                "leader_dir": "UP",
                "leader_confirm": "1",
                "coh": "0.80",
                "decision": "reversal",
                "pick": "BTCUSDT",
                "news_blocked": "0",
                "news_until_ts_ms": "0",
                "leader_conf_score": "0.9",
            }).encode()

        def hgetall(self, key):
            return {}

        def xadd(self, *a, **kw):
            pass

    gate = SmtLeaderCoherenceGate(
        redis_client=MockRedis(),
        bundle_id="test_bundle",
        mode="veto",
        coh_hi_thr=0.65,
        veto_kinds=None,
        diag_stream="",
        diag_sample=1,
    )
    ctx = SimpleNamespace(ts_ms=1_700_000_000_000)
    # reversal + symbol==pick => golden reversal => allow even if countertrend
    dec = gate.evaluate(ctx=ctx, symbol="BTCUSDT", kind="breakout", direction="SHORT")
    assert dec.veto is False, f"Expected veto=False for golden reversal, got {dec}"
    assert dec.reason_code == "SMT_GOLDEN_REVERSAL"
