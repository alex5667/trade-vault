import os

from tests.fake_redis import FakeRedis


def _hset(r: FakeRedis, key: str, mapping: dict) -> None:
    # FakeRedis supports hset(mapping=...)
    r.hset(key, mapping=mapping)


def test_hard_profile_skips_when_samples_insufficient():
    os.environ["RELIABILITY_ADJUST_ENABLED"] = "1"
    os.environ["RELIABILITY_ADJUST_TARGET"] = "tp1"
    os.environ["RELIABILITY_ADJ_PROFILE"] = "hard"
    os.environ["RELIABILITY_ADJ_MIN_BUCKET_SAMPLES"] = "50"
    os.environ["RELIABILITY_ADJ_MIN_SAMPLES_GLOBAL"] = "200"
    os.environ["RELIABILITY_ADJ_MIN_SAMPLES_CTX"] = "200"
    os.environ["RELIABILITY_ADJ_MIN_DELTA"] = "0.03"
    os.environ["RELIABILITY_ADJ_MIN_Z"] = "1.96"
    os.environ["RELIABILITY_ADJ_PRIOR_N"] = "50"

    from services.reliability_adjuster import maybe_adjust_confidence
    from services.reliability_curves import make_reliability_key_v4

    r = FakeRedis()
    strategy, symbol, tf = "CryptoOrderFlow", "BTCUSDT", "1m"
    venue, kind, regime = "binance_futures", "absorption", "trending_bull"
    bucket = 70

    k_glob = make_reliability_key_v4(target="tp1", strategy=strategy, symbol=symbol, tf=tf, venue=venue, kind=kind, regime=regime, ctx_key="na")
    k_ctx = make_reliability_key_v4(target="tp1", strategy=strategy, symbol=symbol, tf=tf, venue=venue, kind=kind, regime=regime, ctx_key="smtc1_coh1_al1")
    # global ok (>=200), ctx insufficient (199)
    _hset(r, k_glob, {"n:70": "250", "h:70": "125"})
    _hset(r, k_ctx, {"n:70": "199", "h:70": "160"})

    env = {"venue": venue, "kind": kind, "entry_regime": regime, "ctx": {"final_score": 0.70, "smt_leader_confirm": 1, "smt_coh": 0.80, "smt_leader_dir": "UP"}}
    res = maybe_adjust_confidence(r, envelope=env, strategy=strategy, symbol=symbol, tf=tf, direction="LONG")
    assert res is None, "hard profile must skip adjustment if ctx/global sample thresholds are not met"


def test_hard_profile_applies_only_when_significant():
    os.environ["RELIABILITY_ADJUST_ENABLED"] = "1"
    os.environ["RELIABILITY_ADJUST_TARGET"] = "tp1"
    os.environ["RELIABILITY_ADJ_PROFILE"] = "hard"
    os.environ["RELIABILITY_ADJ_ALPHA"] = "0.5"
    os.environ["RELIABILITY_ADJ_MIN_BUCKET_SAMPLES"] = "50"
    os.environ["RELIABILITY_ADJ_MIN_SAMPLES_GLOBAL"] = "200"
    os.environ["RELIABILITY_ADJ_MIN_SAMPLES_CTX"] = "200"
    os.environ["RELIABILITY_ADJ_MIN_DELTA"] = "0.03"
    os.environ["RELIABILITY_ADJ_MIN_Z"] = "1.96"
    os.environ["RELIABILITY_ADJ_PRIOR_N"] = "50"
    os.environ["RELIABILITY_ADJ_MAX_ABS"] = "0.20"

    from services.reliability_adjuster import maybe_adjust_confidence
    from services.reliability_curves import make_reliability_key_v4

    r = FakeRedis()
    strategy, symbol, tf = "CryptoOrderFlow", "BTCUSDT", "1m"
    venue, kind, regime = "binance_futures", "absorption", "trending_bull"
    bucket = 70

    k_glob = make_reliability_key_v4(target="tp1", strategy=strategy, symbol=symbol, tf=tf, venue=venue, kind=kind, regime=regime, ctx_key="na")
    k_ctx = make_reliability_key_v4(target="tp1", strategy=strategy, symbol=symbol, tf=tf, venue=venue, kind=kind, regime=regime, ctx_key="smtc1_coh1_al1")

    # global p=0.50 (n=400), ctx p=0.80 (n=300) => should be significant
    _hset(r, k_glob, {"n:70": "400", "h:70": "200"})
    _hset(r, k_ctx, {"n:70": "300", "h:70": "240"})

    env = {"venue": venue, "kind": kind, "entry_regime": regime, "ctx": {"final_score": 0.70, "smt_leader_confirm": 1, "smt_coh": 0.80, "smt_leader_dir": "UP"}}
    res = maybe_adjust_confidence(r, envelope=env, strategy=strategy, symbol=symbol, tf=tf, direction="LONG")
    assert res is not None
    assert res.adjusted > 0.70
    assert 0.0 <= res.adjusted <= 1.0
