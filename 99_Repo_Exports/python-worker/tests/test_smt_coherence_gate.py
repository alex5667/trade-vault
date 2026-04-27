import json
from types import SimpleNamespace

import pytest

from tests.fake_redis import FakeRedis
from handlers.crypto_orderflow.utils.smt_coherence_gate import SmtLeaderCoherenceGate


def _mk_state(*, leader="BTCUSDT", leader_dir="UP", leader_confirm=1, coh=0.72):
    return {"leader": leader, "leader_dir": leader_dir, "leader_confirm": int(leader_confirm), "coh": float(coh)}


def test_smt_gate_observe_attaches_ctx_fields(monkeypatch):
    r = FakeRedis()
    monkeypatch.setenv("SMT_COH_BUNDLE", "btc_eth_sol")
    monkeypatch.setenv("SMT_LEADER_MODE", "observe")
    monkeypatch.setenv("SMT_COH_HI_THRESHOLD", "0.65")

    r.set("smt:bundle:v1:btc_eth_sol", json.dumps(_mk_state(), ensure_ascii=False))
    g = SmtLeaderCoherenceGate.from_env(redis_client=r)

    ctx = SimpleNamespace(ts_ms=1_700_000_000_000)
    dec = g.evaluate(ctx=ctx, symbol="ETHUSDT", kind="absorption", direction="LONG")

    assert dec.apply is True
    assert dec.veto is False

    # Required audit fields
    assert getattr(ctx, "smt_bundle_id") == "btc_eth_sol"
    assert getattr(ctx, "smt_leader") == "BTCUSDT"
    assert getattr(ctx, "smt_leader_dir") == "UP"
    assert int(getattr(ctx, "smt_leader_confirm")) == 1
    assert float(getattr(ctx, "smt_coh")) == pytest.approx(0.72)
    assert int(getattr(ctx, "smt_coh_hi")) == 1
    assert int(getattr(ctx, "smt_align")) == 1
    assert int(getattr(ctx, "smt_blocked")) == 0


def test_smt_gate_veto_only_countertrend_when_confirm_and_coh_hi(monkeypatch):
    r = FakeRedis()
    monkeypatch.setenv("SMT_COH_BUNDLE", "btc_eth_sol")
    monkeypatch.setenv("SMT_LEADER_MODE", "veto")
    monkeypatch.setenv("SMT_COH_HI_THRESHOLD", "0.65")

    r.set("smt:bundle:v1:btc_eth_sol", json.dumps(_mk_state(leader_dir="UP", leader_confirm=1, coh=0.80), ensure_ascii=False))
    g = SmtLeaderCoherenceGate.from_env(redis_client=r)

    # Countertrend: SHORT while leader_dir=UP
    ctx = SimpleNamespace(ts_ms=1_700_000_000_000)
    dec = g.evaluate(ctx=ctx, symbol="ETHUSDT", kind="absorption", direction="SHORT")

    assert dec.apply is True
    assert dec.veto is True
    assert getattr(dec, "reason_code") == "VETO_SMT_COUNTERTREND"
    assert int(getattr(ctx, "smt_blocked")) == 1
    assert str(getattr(ctx, "smt_block_reason")) == "COUNTERTREND_VS_CONFIRMED_LEADER"


def test_smt_gate_veto_mode_does_not_veto_when_align(monkeypatch):
    r = FakeRedis()
    monkeypatch.setenv("SMT_COH_BUNDLE", "btc_eth_sol")
    monkeypatch.setenv("SMT_LEADER_MODE", "veto")
    monkeypatch.setenv("SMT_COH_HI_THRESHOLD", "0.65")

    r.set("smt:bundle:v1:btc_eth_sol", json.dumps(_mk_state(leader_dir="DOWN", leader_confirm=1, coh=0.90), ensure_ascii=False))
    g = SmtLeaderCoherenceGate.from_env(redis_client=r)

    # Align: SHORT while leader_dir=DOWN
    ctx = SimpleNamespace(ts_ms=1_700_000_000_000)
    dec = g.evaluate(ctx=ctx, symbol="ETHUSDT", kind="absorption", direction="SHORT")
    assert dec.apply is True
    assert dec.veto is False
    assert int(getattr(ctx, "smt_align")) == 1


def test_smt_gate_fail_open_when_no_state(monkeypatch):
    r = FakeRedis()
    monkeypatch.setenv("SMT_COH_BUNDLE", "btc_eth_sol")
    monkeypatch.setenv("SMT_LEADER_MODE", "veto")
    monkeypatch.setenv("SMT_COH_HI_THRESHOLD", "0.65")

    g = SmtLeaderCoherenceGate.from_env(redis_client=r)
    ctx = SimpleNamespace(ts_ms=1_700_000_000_000)
    dec = g.evaluate(ctx=ctx, symbol="ETHUSDT", kind="absorption", direction="SHORT")

    # Must not veto without state
    assert dec.veto is False
    assert dec.apply is False