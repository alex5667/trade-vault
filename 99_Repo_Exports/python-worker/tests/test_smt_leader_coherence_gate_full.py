from __future__ import annotations
"""
Comprehensive unit tests for SmtLeaderCoherenceGate (gate #8).

Covers all scenarios from the spec table:
  - Disabled gate (SMT_COH_BUNDLE empty)
  - No state / fail-open
  - Observe mode: never veto, always attach audit fields
  - Veto mode: only narrow rule (confirm + coh_hi + countertrend)
  - News veto (VETO_SMT_NEWS_GATE)
  - Golden Reversal (SMT_GOLDEN_REVERSAL)
  - Continuation Enforcement (VETO_SMT_COUNTERTREND via continuation)
  - Kind allowlist (SMT_LEADER_VETO_KINDS)
  - Coherence boundary (>= semantics)
  - Direction normalization (LONG/BUY/UP all → UP)
  - Hash / JSON state sources (dual Redis read path)
  - All 9 required ctx audit fields
"""

import json
import math
from types import SimpleNamespace
from typing import Any, Dict

import pytest

from tests.fake_redis import FakeRedis
from handlers.crypto_orderflow.utils.smt_coherence_gate import SmtLeaderCoherenceGate


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _mk_gate(
    r: Any,
    *,
    bundle: str = "btc_eth",
    mode: str = "veto",
    thr: float = 0.65,
    veto_kinds: str = "",
    monkeypatch=None,
) -> SmtLeaderCoherenceGate:
    if monkeypatch is not None:
        monkeypatch.setenv("SMT_COH_BUNDLE", bundle)
        monkeypatch.setenv("SMT_LEADER_MODE", mode)
        monkeypatch.setenv("SMT_COH_HI_THRESHOLD", str(thr))
        if veto_kinds:
            monkeypatch.setenv("SMT_LEADER_VETO_KINDS", veto_kinds)
        else:
            monkeypatch.delenv("SMT_LEADER_VETO_KINDS", raising=False)
    return SmtLeaderCoherenceGate.from_env(redis_client=r)


def _mk_state_json(r: FakeRedis, bundle: str, **kwargs) -> None:
    """Write bundle state as JSON string (GET path)."""
    key = f"smt:bundle:v1:{bundle}"
    data = {
        "leader": kwargs.get("leader", "BTCUSDT"),
        "leader_dir": kwargs.get("leader_dir", "UP"),
        "leader_confirm": kwargs.get("leader_confirm", 1),
        "coh": kwargs.get("coh", 0.80),
        **{k: v for k, v in kwargs.items()
           if k not in ("leader", "leader_dir", "leader_confirm", "coh")},
    }
    r.set(key, json.dumps(data))


def _mk_state_hash(r: FakeRedis, bundle: str, **kwargs) -> None:
    """Write bundle state as Redis hash (HGETALL path)."""
    key = f"smt:bundle:v1:{bundle}"
    mapping = {
        "leader": kwargs.get("leader", "BTCUSDT"),
        "leader_dir": kwargs.get("leader_dir", "UP"),
        "leader_confirm": str(kwargs.get("leader_confirm", 1)),
        "coh": str(kwargs.get("coh", 0.80)),
    }
    mapping.update({k: str(v) for k, v in kwargs.items()
                    if k not in ("leader", "leader_dir", "leader_confirm", "coh")})
    r.hset(key, mapping=mapping)


def _ctx() -> SimpleNamespace:
    return SimpleNamespace(ts_ms=1_700_000_000_000)


# ===========================================================================
# 1. Disabled gate
# ===========================================================================

def test_disabled_gate_no_bundle(monkeypatch):
    r = FakeRedis()
    _mk_state_json(r, "btc_eth")
    g = _mk_gate(r, bundle="", monkeypatch=monkeypatch)
    dec = g.evaluate(ctx=_ctx(), symbol="ETHUSDT", kind="absorption", direction="SHORT")
    assert dec.apply is False
    assert dec.veto is False
    assert dec.reason_code == "SMT_DISABLED"


# ===========================================================================
# 2. No state / fail-open
# ===========================================================================

def test_no_state_fail_open(monkeypatch):
    r = FakeRedis()  # empty Redis
    g = _mk_gate(r, monkeypatch=monkeypatch)
    dec = g.evaluate(ctx=_ctx(), symbol="ETHUSDT", kind="absorption", direction="SHORT")
    assert dec.apply is False
    assert dec.veto is False
    assert dec.reason_code == "SMT_NO_STATE"


def test_no_state_no_ctx_audit_fields(monkeypatch):
    """No ctx fields set when state is absent (correct fail-open)."""
    r = FakeRedis()
    g = _mk_gate(r, monkeypatch=monkeypatch)
    ctx = _ctx()
    g.evaluate(ctx=ctx, symbol="ETHUSDT", kind="absorption", direction="SHORT")
    # None of the smt_ fields should have been set
    assert not hasattr(ctx, "smt_leader")
    assert not hasattr(ctx, "smt_blocked")


# ===========================================================================
# 3. Observe mode — never veto
# ===========================================================================

def test_observe_countertrend_never_veto(monkeypatch):
    """Observe mode: countertrend + confirmed leader + coh_hi → still no veto."""
    r = FakeRedis()
    _mk_state_json(r, "btc_eth", leader_dir="UP", leader_confirm=1, coh=0.90)
    g = _mk_gate(r, mode="observe", monkeypatch=monkeypatch)
    dec = g.evaluate(ctx=_ctx(), symbol="ETHUSDT", kind="breakout", direction="SHORT")
    assert dec.apply is True
    assert dec.veto is False
    assert dec.reason_code == "SMT_OBSERVE"


def test_observe_mode_attaches_all_audit_fields(monkeypatch):
    """All 9 mandatory ctx audit fields are set in observe mode."""
    r = FakeRedis()
    _mk_state_json(r, "btc_eth", leader="BTCUSDT", leader_dir="UP",
                   leader_confirm=1, coh=0.80)
    g = _mk_gate(r, mode="observe", monkeypatch=monkeypatch)
    ctx = _ctx()
    g.evaluate(ctx=ctx, symbol="ETHUSDT", kind="breakout", direction="LONG")

    assert getattr(ctx, "smt_bundle") == "btc_eth"
    assert getattr(ctx, "smt_leader") == "BTCUSDT"
    assert getattr(ctx, "smt_leader_dir") == "UP"
    assert int(getattr(ctx, "smt_leader_confirm")) == 1
    assert math.isfinite(float(getattr(ctx, "smt_coh")))
    assert float(getattr(ctx, "smt_coh")) == pytest.approx(0.80)
    assert int(getattr(ctx, "smt_coh_hi")) == 1   # >= 0.65
    assert int(getattr(ctx, "smt_align")) == 1     # LONG vs UP → align
    assert int(getattr(ctx, "smt_blocked")) == 0
    assert getattr(ctx, "smt_block_reason") == ""


# ===========================================================================
# 4. Veto mode — narrow rule
# ===========================================================================

def test_veto_countertrend_with_confirm_and_coh_hi(monkeypatch):
    """Veto mode: SHORT vs confirmed UP leader with coh≥thr → VETO_SMT_COUNTERTREND."""
    r = FakeRedis()
    _mk_state_json(r, "btc_eth", leader_dir="UP", leader_confirm=1, coh=0.80)
    g = _mk_gate(r, mode="veto", monkeypatch=monkeypatch)
    ctx = _ctx()
    dec = g.evaluate(ctx=ctx, symbol="ETHUSDT", kind="breakout", direction="SHORT")
    assert dec.apply is True
    assert dec.veto is True
    assert dec.reason_code == "VETO_SMT_COUNTERTREND"
    assert int(getattr(ctx, "smt_blocked")) == 1
    assert getattr(ctx, "smt_block_reason") == "COUNTERTREND_VS_CONFIRMED_LEADER"
    assert int(getattr(ctx, "smt_align")) == 0


def test_veto_aligned_no_veto(monkeypatch):
    """Veto mode: LONG vs UP leader (aligned) → SMT_OK, no veto."""
    r = FakeRedis()
    _mk_state_json(r, "btc_eth", leader_dir="UP", leader_confirm=1, coh=0.90)
    g = _mk_gate(r, mode="veto", monkeypatch=monkeypatch)
    dec = g.evaluate(ctx=_ctx(), symbol="ETHUSDT", kind="breakout", direction="LONG")
    assert dec.veto is False
    assert dec.reason_code == "SMT_OK"


def test_veto_no_veto_when_coh_below_threshold(monkeypatch):
    """Veto mode: coh < thr even with confirm=1 + countertrend → no veto."""
    r = FakeRedis()
    _mk_state_json(r, "btc_eth", leader_dir="UP", leader_confirm=1, coh=0.50)
    g = _mk_gate(r, mode="veto", monkeypatch=monkeypatch)
    dec = g.evaluate(ctx=_ctx(), symbol="ETHUSDT", kind="breakout", direction="SHORT")
    assert dec.veto is False
    assert dec.reason_code == "SMT_OK"


def test_veto_no_veto_when_not_confirmed(monkeypatch):
    """Veto mode: leader_confirm=0 → narrow rule not triggered → no veto."""
    r = FakeRedis()
    _mk_state_json(r, "btc_eth", leader_dir="UP", leader_confirm=0, coh=0.90)
    g = _mk_gate(r, mode="veto", monkeypatch=monkeypatch)
    dec = g.evaluate(ctx=_ctx(), symbol="ETHUSDT", kind="breakout", direction="SHORT")
    assert dec.veto is False


# ===========================================================================
# 5. Coherence boundary (>= semantics)
# ===========================================================================

def test_coh_exactly_at_threshold_triggers_hi(monkeypatch):
    """coh == threshold → coh_hi = 1 (>= is inclusive)."""
    r = FakeRedis()
    _mk_state_json(r, "btc_eth", leader_dir="UP", leader_confirm=1, coh=0.65)
    g = _mk_gate(r, mode="veto", thr=0.65, monkeypatch=monkeypatch)
    ctx = _ctx()
    dec = g.evaluate(ctx=ctx, symbol="ETHUSDT", kind="breakout", direction="SHORT")
    assert int(getattr(ctx, "smt_coh_hi")) == 1
    assert dec.veto is True


def test_coh_just_below_threshold_not_hi(monkeypatch):
    """coh = 0.6499 < 0.65 → coh_hi = 0, no veto."""
    r = FakeRedis()
    _mk_state_json(r, "btc_eth", leader_dir="UP", leader_confirm=1, coh=0.6499)
    g = _mk_gate(r, mode="veto", thr=0.65, monkeypatch=monkeypatch)
    ctx = _ctx()
    dec = g.evaluate(ctx=ctx, symbol="ETHUSDT", kind="breakout", direction="SHORT")
    assert int(getattr(ctx, "smt_coh_hi")) == 0
    assert dec.veto is False


# ===========================================================================
# 6. News veto
# ===========================================================================

def test_news_veto_in_veto_mode(monkeypatch):
    """news_blocked=1 + veto mode → VETO_SMT_NEWS_GATE regardless of alignment."""
    r = FakeRedis()
    _mk_state_json(r, "btc_eth", leader_dir="UP", leader_confirm=1, coh=0.80,
                   news_blocked=1, news_until_ts_ms=1_700_000_999_000)
    g = _mk_gate(r, mode="veto", monkeypatch=monkeypatch)
    ctx = _ctx()
    dec = g.evaluate(ctx=ctx, symbol="BTCUSDT", kind="breakout", direction="LONG")
    assert dec.apply is True
    assert dec.veto is True
    assert dec.reason_code == "VETO_SMT_NEWS_GATE"


def test_news_veto_sets_audit_fields(monkeypatch):
    """On news veto, ctx.smt_blocked=1 and ctx.smt_block_reason='NEWS_GATE'."""
    r = FakeRedis()
    _mk_state_json(r, "btc_eth", leader_dir="UP", leader_confirm=1, coh=0.80,
                   news_blocked=1)
    g = _mk_gate(r, mode="veto", monkeypatch=monkeypatch)
    ctx = _ctx()
    g.evaluate(ctx=ctx, symbol="BTCUSDT", kind="breakout", direction="LONG")
    assert int(getattr(ctx, "smt_blocked")) == 1
    assert getattr(ctx, "smt_block_reason") == "NEWS_GATE"


def test_news_veto_not_triggered_in_observe_mode(monkeypatch):
    """News blocked but mode=observe → no veto."""
    r = FakeRedis()
    _mk_state_json(r, "btc_eth", leader_dir="UP", leader_confirm=1, coh=0.80,
                   news_blocked=1)
    g = _mk_gate(r, mode="observe", monkeypatch=monkeypatch)
    dec = g.evaluate(ctx=_ctx(), symbol="BTCUSDT", kind="breakout", direction="LONG")
    assert dec.veto is False


# ===========================================================================
# 7. Golden Reversal ticket
# ===========================================================================

def test_golden_reversal_allows_override(monkeypatch):
    """decision=reversal + pick=ETHUSDT + symbol=ETHUSDT → SMT_GOLDEN_REVERSAL, no veto."""
    r = FakeRedis()
    _mk_state_json(r, "btc_eth", leader_dir="UP", leader_confirm=1, coh=0.90,
                   decision="reversal", pick="ETHUSDT")
    g = _mk_gate(r, mode="veto", monkeypatch=monkeypatch)
    ctx = _ctx()
    dec = g.evaluate(ctx=ctx, symbol="ETHUSDT", kind="breakout", direction="SHORT")
    assert dec.apply is True
    assert dec.veto is False
    assert dec.reason_code == "SMT_GOLDEN_REVERSAL"


def test_golden_reversal_sets_ctx_fields(monkeypatch):
    """On golden reversal, ctx.smt_blocked=0, ctx.smt_block_reason='GOLDEN_REVERSAL', ctx.smt_golden=1."""
    r = FakeRedis()
    _mk_state_json(r, "btc_eth", leader_dir="UP", leader_confirm=1, coh=0.90,
                   decision="reversal", pick="ETHUSDT")
    g = _mk_gate(r, mode="veto", monkeypatch=monkeypatch)
    ctx = _ctx()
    g.evaluate(ctx=ctx, symbol="ETHUSDT", kind="breakout", direction="SHORT")
    assert int(getattr(ctx, "smt_blocked")) == 0
    assert getattr(ctx, "smt_block_reason") == "GOLDEN_REVERSAL"
    assert int(getattr(ctx, "smt_golden")) == 1


def test_golden_reversal_not_matched_for_other_symbol(monkeypatch):
    """decision=reversal + pick=ETHUSDT but symbol=SOLUSDT → no override, normal veto applies."""
    r = FakeRedis()
    _mk_state_json(r, "btc_eth", leader_dir="UP", leader_confirm=1, coh=0.90,
                   decision="reversal", pick="ETHUSDT")
    g = _mk_gate(r, mode="veto", monkeypatch=monkeypatch)
    dec = g.evaluate(ctx=_ctx(), symbol="SOLUSDT", kind="breakout", direction="SHORT")
    # SOLUSDT is countertrend, confirm=1, coh_hi=1 → should be vetoed
    assert dec.veto is True
    assert dec.reason_code == "VETO_SMT_COUNTERTREND"


# ===========================================================================
# 8. Continuation Enforcement
# ===========================================================================

def test_continuation_veto_when_countertrend(monkeypatch):
    """decision=continuation, align=0, veto mode → blocked → VETO_SMT_COUNTERTREND."""
    r = FakeRedis()
    # Note: even with leader_confirm=0 or coh < thr, continuation forces veto
    _mk_state_json(r, "btc_eth", leader_dir="UP", leader_confirm=0, coh=0.40,
                   decision="continuation")
    g = _mk_gate(r, mode="veto", monkeypatch=monkeypatch)
    ctx = _ctx()
    dec = g.evaluate(ctx=ctx, symbol="ETHUSDT", kind="breakout", direction="SHORT")
    assert dec.veto is True
    assert dec.reason_code == "VETO_SMT_COUNTERTREND"
    assert int(getattr(ctx, "smt_blocked")) == 1
    assert getattr(ctx, "smt_block_reason") == "COUNTER_CONTINUATION"


def test_continuation_no_veto_when_aligned(monkeypatch):
    """decision=continuation, align=1 → no block, no veto."""
    r = FakeRedis()
    _mk_state_json(r, "btc_eth", leader_dir="UP", leader_confirm=1, coh=0.80,
                   decision="continuation")
    g = _mk_gate(r, mode="veto", monkeypatch=monkeypatch)
    dec = g.evaluate(ctx=_ctx(), symbol="ETHUSDT", kind="breakout", direction="LONG")
    assert dec.veto is False
    assert dec.reason_code == "SMT_OK"


def test_continuation_observe_mode_no_veto(monkeypatch):
    """decision=continuation + align=0 + observe mode → no veto (observe never veto)."""
    r = FakeRedis()
    _mk_state_json(r, "btc_eth", leader_dir="UP", leader_confirm=1, coh=0.80,
                   decision="continuation")
    g = _mk_gate(r, mode="observe", monkeypatch=monkeypatch)
    dec = g.evaluate(ctx=_ctx(), symbol="ETHUSDT", kind="breakout", direction="SHORT")
    assert dec.veto is False
    assert dec.reason_code == "SMT_OBSERVE"


# ===========================================================================
# 9. Kind allowlist (SMT_LEADER_VETO_KINDS)
# ===========================================================================

def test_kind_allowlist_not_applicable_no_veto(monkeypatch):
    """Kind not in veto_kinds allowlist → no veto even if blocked."""
    r = FakeRedis()
    _mk_state_json(r, "btc_eth", leader_dir="UP", leader_confirm=1, coh=0.80)
    monkeypatch.setenv("SMT_COH_BUNDLE", "btc_eth")
    monkeypatch.setenv("SMT_LEADER_MODE", "veto")
    monkeypatch.setenv("SMT_COH_HI_THRESHOLD", "0.65")
    monkeypatch.setenv("SMT_LEADER_VETO_KINDS", "breakout,absorption")
    g = SmtLeaderCoherenceGate.from_env(redis_client=r)
    # "obi_spike" is NOT in the allowlist
    dec = g.evaluate(ctx=_ctx(), symbol="ETHUSDT", kind="obi_spike", direction="SHORT")
    assert dec.veto is False
    assert dec.reason_code == "SMT_BLOCKED_BUT_KIND_NOT_APPLICABLE"


def test_kind_allowlist_applicable_veto(monkeypatch):
    """Kind in veto_kinds → veto applies normally."""
    r = FakeRedis()
    _mk_state_json(r, "btc_eth", leader_dir="UP", leader_confirm=1, coh=0.80)
    monkeypatch.setenv("SMT_COH_BUNDLE", "btc_eth")
    monkeypatch.setenv("SMT_LEADER_MODE", "veto")
    monkeypatch.setenv("SMT_COH_HI_THRESHOLD", "0.65")
    monkeypatch.setenv("SMT_LEADER_VETO_KINDS", "breakout,absorption")
    g = SmtLeaderCoherenceGate.from_env(redis_client=r)
    dec = g.evaluate(ctx=_ctx(), symbol="ETHUSDT", kind="breakout", direction="SHORT")
    assert dec.veto is True
    assert dec.reason_code == "VETO_SMT_COUNTERTREND"


# ===========================================================================
# 10. Direction normalization
# ===========================================================================

@pytest.mark.parametrize("direction,expected_align", [
    ("LONG", 1),
    ("BUY", 1),
    ("UP", 1),
    ("SHORT", 0),
    ("SELL", 0),
    ("DOWN", 0),
])
def test_direction_normalization(monkeypatch, direction: str, expected_align: int):
    """LONG/BUY/UP all map to UP; SHORT/SELL/DOWN all map to DOWN."""
    r = FakeRedis()
    _mk_state_json(r, "btc_eth", leader_dir="UP", leader_confirm=1, coh=0.80)
    g = _mk_gate(r, mode="observe", monkeypatch=monkeypatch)
    ctx = _ctx()
    g.evaluate(ctx=ctx, symbol="ETHUSDT", kind="breakout", direction=direction)
    assert int(getattr(ctx, "smt_align")) == expected_align


# ===========================================================================
# 11. Hash Redis state (HGETALL path)
# ===========================================================================

def test_hash_state_read_path(monkeypatch):
    """State stored as Redis hash (not JSON string) is correctly parsed."""
    r = FakeRedis()
    _mk_state_hash(r, "btc_eth", leader="BTCUSDT", leader_dir="UP",
                   leader_confirm=1, coh=0.80)
    g = _mk_gate(r, mode="veto", monkeypatch=monkeypatch)
    ctx = _ctx()
    dec = g.evaluate(ctx=ctx, symbol="ETHUSDT", kind="breakout", direction="SHORT")
    assert dec.apply is True
    assert dec.veto is True
    assert getattr(ctx, "smt_leader") == "BTCUSDT"
    assert float(getattr(ctx, "smt_coh")) == pytest.approx(0.80)


# ===========================================================================
# 12. All spec audit fields present simultaneously
# ===========================================================================

def test_all_spec_audit_fields_present(monkeypatch):
    """All 9 audit fields from spec must be present on ctx after evaluate()."""
    r = FakeRedis()
    _mk_state_json(r, "btc_eth", leader="BTCUSDT", leader_dir="DOWN",
                   leader_confirm=1, coh=0.70)
    g = _mk_gate(r, mode="veto", monkeypatch=monkeypatch)
    ctx = _ctx()
    g.evaluate(ctx=ctx, symbol="ETHUSDT", kind="breakout", direction="SHORT")

    # All spec-mandated ctx fields
    for field in (
        "smt_bundle", "smt_leader", "smt_leader_dir", "smt_leader_confirm",
        "smt_coh", "smt_coh_hi", "smt_align", "smt_blocked", "smt_block_reason",
    ):
        assert hasattr(ctx, field), f"Missing ctx field: {field}"


# ===========================================================================
# 13. from_env threshold fallback chain
# ===========================================================================

def test_threshold_fallback_to_reliability_thr(monkeypatch):
    """SMT_COH_HI_THRESHOLD not set → should fall back to RELIABILITY_SMT_COH_THR."""
    r = FakeRedis()
    _mk_state_json(r, "btc_eth", leader_dir="UP", leader_confirm=1, coh=0.70)
    monkeypatch.setenv("SMT_COH_BUNDLE", "btc_eth")
    monkeypatch.setenv("SMT_LEADER_MODE", "veto")
    monkeypatch.delenv("SMT_COH_HI_THRESHOLD", raising=False)
    monkeypatch.setenv("RELIABILITY_SMT_COH_THR", "0.65")
    g = SmtLeaderCoherenceGate.from_env(redis_client=r)
    assert g.coh_hi_thr == pytest.approx(0.65)
    ctx = _ctx()
    dec = g.evaluate(ctx=ctx, symbol="ETHUSDT", kind="breakout", direction="SHORT")
    # 0.70 >= 0.65 → coh_hi=1
    assert int(getattr(ctx, "smt_coh_hi")) == 1
    assert dec.veto is True
