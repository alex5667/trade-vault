from __future__ import annotations

from types import SimpleNamespace

from handlers.crypto_orderflow.utils.smt_coherence_gate import SmtLeaderCoherenceGate
from tests.fake_redis import FakeRedis


def _mk_ctx() -> SimpleNamespace:
    return SimpleNamespace()


def test_smt_gate_observe_never_veto_even_countertrend(monkeypatch):
    r = FakeRedis()
    # bundle state: leader UP confirmed, coherence high
    r.hset(
        "smt:bundle:v1:btc_eth_sol",
        mapping={
            "leader": "BTCUSDT",
            "leader_dir": "UP",
            "leader_confirm": "1",
            "coh": "0.800000",
            "ts_ms": "1700000000000",
        },
    )

    monkeypatch.setenv("SMT_COH_BUNDLE", "btc_eth_sol")
    monkeypatch.setenv("SMT_LEADER_MODE", "observe")
    monkeypatch.setenv("SMT_COH_HI_THRESHOLD", "0.65")

    g = SmtLeaderCoherenceGate.from_env(redis_client=r)
    ctx = _mk_ctx()
    dec = g.evaluate(ctx=ctx, symbol="ETHUSDT", kind="breakout", direction="SHORT")  # countertrend vs UP
    assert dec.apply is True
    assert dec.veto is False
    # audit fields must exist
    assert ctx.smt_bundle_id == "btc_eth_sol"
    assert getattr(ctx, "smt_leader_confirm", 0) == 1
    assert float(getattr(ctx, "smt_coh", 0.0)) >= 0.79


def test_smt_gate_veto_countertrend_only_when_confirmed_and_coh_high(monkeypatch):
    r = FakeRedis()
    r.hset(
        "smt:bundle:v1:btc_eth_sol",
        mapping={
            "leader": "BTCUSDT",
            "leader_dir": "UP",
            "leader_confirm": "1",
            "coh": "0.800000",
            "ts_ms": "1700000000000",
        },
    )

    monkeypatch.setenv("SMT_COH_BUNDLE", "btc_eth_sol")
    monkeypatch.setenv("SMT_LEADER_MODE", "veto")
    monkeypatch.setenv("SMT_COH_HI_THRESHOLD", "0.65")

    g = SmtLeaderCoherenceGate.from_env(redis_client=r)
    ctx = _mk_ctx()
    dec = g.evaluate(ctx=ctx, symbol="ETHUSDT", kind="breakout", direction="SHORT")
    assert dec.apply is True
    assert dec.veto is True
    assert dec.reason_code == "VETO_SMT_COUNTERTREND"
    assert getattr(ctx, "smt_blocked", False) == 1


def test_smt_gate_fail_open_on_missing_state(monkeypatch):
    r = FakeRedis()  # empty redis, no bundle state

    monkeypatch.setenv("SMT_COH_BUNDLE", "btc_eth_sol")
    monkeypatch.setenv("SMT_LEADER_MODE", "veto")
    monkeypatch.setenv("SMT_COH_HI_THRESHOLD", "0.65")

    g = SmtLeaderCoherenceGate.from_env(redis_client=r)
    ctx = _mk_ctx()
    dec = g.evaluate(ctx=ctx, symbol="ETHUSDT", kind="breakout", direction="SHORT")
    assert dec.apply is False  # no state -> apply=False
    assert dec.veto is False  # fail-open
    assert dec.reason_code == "SMT_NO_STATE"


def test_smt_gate_disabled_noop(monkeypatch):
    r = FakeRedis()
    r.hset(
        "smt:bundle:v1:btc_eth_sol",
        mapping={
            "leader": "BTCUSDT",
            "leader_dir": "UP",
            "leader_confirm": "1",
            "coh": "0.900000",
            "ts_ms": "1700000000000",
        },
    )

    monkeypatch.setenv("SMT_COH_BUNDLE", "")  # empty bundle_id disables gate
    monkeypatch.setenv("SMT_LEADER_MODE", "veto")

    g = SmtLeaderCoherenceGate.from_env(redis_client=r)
    ctx = _mk_ctx()
    dec = g.evaluate(ctx=ctx, symbol="ETHUSDT", kind="breakout", direction="SHORT")
    assert dec.apply is False  # disabled gate
    assert dec.veto is False
    assert dec.reason_code == "SMT_DISABLED"
