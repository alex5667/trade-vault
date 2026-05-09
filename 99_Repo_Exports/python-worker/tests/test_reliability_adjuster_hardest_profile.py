import os

from tests.fake_redis import FakeRedis


def _hset(r: FakeRedis, key: str, mapping: dict) -> None:
    r.hset(key, mapping=mapping)


def test_hardest_skips_when_smt_context_absent_ctx_key_na():
    """
    Hardest profile MUST be no-op when SMT context is absent (ctx_key == 'na').
    This ensures maximum stability: no "mysterious" confidence shifts when
    SMT fields are missing/unknown.
    """
    os.environ["RELIABILITY_ADJUST_ENABLED"] = "1"
    os.environ["RELIABILITY_ADJUST_TARGET"] = "tp1"
    os.environ["RELIABILITY_ADJ_PROFILE"] = "hardest"
    os.environ["RELIABILITY_ADJ_ALPHA"] = "0.5"
    os.environ["RELIABILITY_ADJ_MIN_BUCKET_SAMPLES"] = "50"

    from services.reliability_adjuster import maybe_adjust_confidence
    from services.reliability_curves import make_reliability_key_v4

    r = FakeRedis()
    strategy, symbol, tf = "CryptoOrderFlow", "BTCUSDT", "1m"
    venue, kind, regime = "binance_futures", "absorption", "trending_bull"
    bucket = 70

    k_glob = make_reliability_key_v4(target="tp1", strategy=strategy, symbol=symbol, tf=tf, venue=venue, kind=kind, regime=regime, ctx_key="na")
    _hset(r, k_glob, {"n:70": "2000", "h:70": "1000"})

    env = {"venue": venue, "kind": kind, "entry_regime": regime, "ctx": {"final_score": 0.70, "smt_leader_confirm": 1, "smt_coh": 0.80, "smt_leader_dir": "UP"}}
    # even if redis has enough stats, hardest must NOT adjust ctx_key="na"
    res = maybe_adjust_confidence(r, envelope=env, strategy=strategy, symbol=symbol, tf=tf, direction="LONG")
    assert res is None


def test_hardest_applies_only_with_large_samples_and_strong_signal():
    """
    Hardest profile should adjust ONLY when:
      - SMT ctx exists (ctx_key != 'na')
      - global and ctx have large N
      - effect size and significance are strong
    """
    os.environ["RELIABILITY_ADJUST_ENABLED"] = "1"
    os.environ["RELIABILITY_ADJUST_TARGET"] = "tp1"
    os.environ["RELIABILITY_ADJ_PROFILE"] = "hardest"
    os.environ["RELIABILITY_ADJ_ALPHA"] = "0.5"
    os.environ["RELIABILITY_ADJ_MIN_BUCKET_SAMPLES"] = "50"
    os.environ["RELIABILITY_SMT_COH_THR"] = "0.65"

    # lock hardest thresholds for determinism
    os.environ["RELIABILITY_ADJ_MIN_SAMPLES_GLOBAL"] = "1000"
    os.environ["RELIABILITY_ADJ_MIN_SAMPLES_CTX"] = "500"
    os.environ["RELIABILITY_ADJ_MIN_DELTA"] = "0.04"
    os.environ["RELIABILITY_ADJ_MIN_Z"] = "2.58"
    os.environ["RELIABILITY_ADJ_PRIOR_N"] = "150"
    os.environ["RELIABILITY_ADJ_MAX_ABS"] = "0.10"

    from services.reliability_adjuster import maybe_adjust_confidence
    from services.reliability_curves import make_reliability_key_v4

    r = FakeRedis()
    strategy, symbol, tf = "CryptoOrderFlow", "BTCUSDT", "1m"
    venue, kind, regime = "binance_futures", "absorption", "trending_bull"
    bucket = 70

    ctx_key = "smtc1_coh1_al1"
    k_glob = make_reliability_key_v4(target="tp1", strategy=strategy, symbol=symbol, tf=tf, venue=venue, kind=kind, regime=regime, ctx_key="na")
    k_ctx = make_reliability_key_v4(target="tp1", strategy=strategy, symbol=symbol, tf=tf, venue=venue, kind=kind, regime=regime, ctx_key=ctx_key)

    # Global p=0.50 (n=4000)
    _hset(r, k_glob, {"n:70": "4000", "h:70": "2000"})
    # Context p=0.80 (n=1200) -> very strong signal even after shrinkage
    _hset(r, k_ctx, {"n:70": "1200", "h:70": "960"})

    env = {
        "venue": venue,
        "kind": kind,
        "entry_regime": regime,
        "ctx": {"final_score": 0.70, "smt_leader_confirm": 1, "smt_coh": 0.80, "smt_leader_dir": "UP"},
    }

    # SMT fields ensure ctx_key == "smtc1_coh1_al1":
    # smt_leader_confirm=1, smt_coh=0.80 >= 0.65 (coh_hi=1), direction="LONG" -> "UP" matches leader_dir="UP" (align=1)
    res = maybe_adjust_confidence(r, envelope=env, strategy=strategy, symbol=symbol, tf=tf, direction="LONG")
    assert res is not None
    assert 0.70 <= res.adjusted <= 0.80  # capped by max_abs=0.10 and alpha=0.5
    assert res.adjusted > 0.70
