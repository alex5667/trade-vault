import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tests.fake_redis import FakeRedis


def _hset(r, key, mapping):
    try:
        r.hset(key, mapping=mapping)
    except TypeError:
        r.hset(name=key, mapping=mapping)


def test_adjuster_increases_or_decreases_confidence_by_context_delta():
    os.environ["RELIABILITY_ADJUST_ENABLED"] = "1"
    os.environ["RELIABILITY_ADJUST_TARGET"] = "tp1"
    os.environ["RELIABILITY_BUCKET_STEP"] = "5"
    os.environ["RELIABILITY_SMT_COH_THR"] = "0.65"
    os.environ["RELIABILITY_ADJ_ALPHA"] = "0.5"
    os.environ["RELIABILITY_ADJ_MIN_BUCKET_SAMPLES"] = "50"

    from services.reliability_curves import make_reliability_key_v3, make_reliability_key_v4
    from services.reliability_adjuster import maybe_adjust_confidence

    r = FakeRedis()

    strategy, symbol, tf = "CryptoOrderFlow", "BTCUSDT", "1m"
    venue = "binance_futures"
    kind = "absorption"
    regime = "trending_bull"
    bucket = 70

    k_glob = make_reliability_key_v4(target="tp1", strategy=strategy, symbol=symbol, tf=tf, venue=venue, kind=kind, regime=regime, ctx_key="na")
    k_al = make_reliability_key_v4(target="tp1", strategy=strategy, symbol=symbol, tf=tf, venue=venue, kind=kind, regime=regime, ctx_key="smtc1_coh1_al1")
    k_ct = make_reliability_key_v4(target="tp1", strategy=strategy, symbol=symbol, tf=tf, venue=venue, kind=kind, regime=regime, ctx_key="smtc1_coh1_al0")

    # global: rate=0.50
    _hset(r, k_glob, {"n:70": "100", "h:70": "50"})
    # aligned context: rate=0.70 => delta=+0.20 => adj=0.70 + 0.5*0.20 = 0.80
    _hset(r, k_al, {"n:70": "100", "h:70": "70"})
    # counter context: rate=0.30 => delta=-0.20 => adj=0.70 - 0.10 = 0.60
    _hset(r, k_ct, {"n:70": "100", "h:70": "30"})

    env_al = {"venue": venue, "kind": kind, "entry_regime": regime, "ctx": {"final_score": 0.70, "smt_leader_confirm": 1, "smt_coh": 0.80, "smt_leader_dir": "UP"}}
    res1 = maybe_adjust_confidence(r, envelope=env_al, strategy=strategy, symbol=symbol, tf=tf, direction="LONG")
    assert res1 is not None
    assert abs(res1.adjusted - 0.80) < 1e-9

    env_ct = {"venue": venue, "kind": kind, "entry_regime": regime, "ctx": {"final_score": 0.70, "smt_leader_confirm": 1, "smt_coh": 0.80, "smt_leader_dir": "UP"}}
    res2 = maybe_adjust_confidence(r, envelope=env_ct, strategy=strategy, symbol=symbol, tf=tf, direction="SHORT")
    assert res2 is not None
    assert abs(res2.adjusted - 0.60) < 1e-9
