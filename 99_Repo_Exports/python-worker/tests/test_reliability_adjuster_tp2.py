from __future__ import annotations

import pytest

from services.reliability_adjuster import maybe_adjust_confidence
from services.reliability_curves import make_reliability_key_v4
from tests.fake_redis import FakeRedis


def _set_bucket(r: FakeRedis, *, key: str, bucket: int, n: int, h: int) -> None:
    # Writer stores strings; FakeRedis is permissive, but keep it consistent.
    r.hset(key, mapping={
        "samples": str(n),
        "hits": str(h),
        "last_ts_ms": "1",
        f"n:{bucket}": str(n),
        f"h:{bucket}": str(h),
    })


def test_adjuster_soft_tp2_applies(monkeypatch: pytest.MonkeyPatch) -> None:
    import os
    os.environ["RELIABILITY_ADJUST_ENABLED"] = "1"
    os.environ["RELIABILITY_ADJ_PROFILE"] = "soft"
    os.environ["RELIABILITY_TARGETS"] = "tp2"
    os.environ["RELIABILITY_BUCKET_STEP"] = "5"
    os.environ["RELIABILITY_ADJ_ALPHA"] = "0.5"
    os.environ["RELIABILITY_ADJ_MIN_BUCKET_SAMPLES"] = "10"

    r = FakeRedis()

    # Envelope with SMT context (align=1)
    env = {
        "symbol": "BTCUSDT",
        "tf": "1m",
        "venue": "binance_futures",
        "kind": "absorption",
        "entry_regime": "trending_bull",
        "strategy": "S",
        "confidence": 0.50,  # -> bucket 50
        "ctx": {"smt_leader_confirm": 1, "smt_coh": 0.8, "smt_leader_dir": "UP"},
    }

    bucket = 50
    # Global: 0.50
    k_g = make_reliability_key_v4(
        target="tp2", strategy="S", symbol="BTCUSDT", tf="1m",
        venue="binance_futures", kind="absorption", regime="trending_bull", ctx_key="na",
    )
    _set_bucket(r, key=k_g, bucket=bucket, n=100, h=50)

    # Ctx: 0.70
    k_c = make_reliability_key_v4(
        target="tp2", strategy="S", symbol="BTCUSDT", tf="1m",
        venue="binance_futures", kind="absorption", regime="trending_bull", ctx_key="smtc1_coh1_al1",
    )
    _set_bucket(r, key=k_c, bucket=bucket, n=100, h=70)

    res = maybe_adjust_confidence(
        r,
        envelope=env,
        strategy="S",
        symbol="BTCUSDT",
        tf="1m",
        direction="LONG",
    )
    assert res is not None
    # delta = alpha*(0.70-0.50)=0.10 => 0.60
    assert abs(res.adjusted - 0.60) < 1e-9


def test_adjuster_hard_requires_samples(monkeypatch: pytest.MonkeyPatch) -> None:
    r = FakeRedis()

    monkeypatch.setenv("RELIABILITY_ADJUST_ENABLED", "1")
    monkeypatch.setenv("RELIABILITY_ADJ_PROFILE", "hard")
    monkeypatch.setenv("RELIABILITY_TARGETS", "tp2")
    monkeypatch.setenv("RELIABILITY_BUCKET_STEP", "5")
    monkeypatch.setenv("RELIABILITY_ADJ_MIN_BUCKET_SAMPLES", "10")
    monkeypatch.setenv("RELIABILITY_ADJ_MIN_SAMPLES_GLOBAL", "200")
    monkeypatch.setenv("RELIABILITY_ADJ_MIN_SAMPLES_CTX", "200")

    env = {
        "venue": "binance_futures",
        "kind": "absorption",
        "entry_regime": "trending_bull",
        "confidence": 0.50,
        "ctx": {"smt_leader_confirm": 1, "smt_coh": 0.8, "smt_leader_dir": "UP"},
    }
    bucket = 50
    k_g = make_reliability_key_v4(
        target="tp2", strategy="S", symbol="BTCUSDT", tf="1m",
        venue="binance_futures", kind="absorption", regime="trending_bull", ctx_key="na",
    )
    k_c = make_reliability_key_v4(
        target="tp2", strategy="S", symbol="BTCUSDT", tf="1m",
        venue="binance_futures", kind="absorption", regime="trending_bull", ctx_key="smtc1_coh1_al1",
    )
    # Not enough for hard profile
    _set_bucket(r, key=k_g, bucket=bucket, n=100, h=50)
    _set_bucket(r, key=k_c, bucket=bucket, n=100, h=90)

    res = maybe_adjust_confidence(r, envelope=env, strategy="S", symbol="BTCUSDT", tf="1m", direction="LONG")
    assert res is None


def test_adjuster_hardest_skips_without_smt(monkeypatch: pytest.MonkeyPatch) -> None:
    r = FakeRedis()

    monkeypatch.setenv("RELIABILITY_ADJUST_ENABLED", "1")
    monkeypatch.setenv("RELIABILITY_ADJ_PROFILE", "hardest")
    monkeypatch.setenv("RELIABILITY_TARGETS", "tp2")
    monkeypatch.setenv("RELIABILITY_BUCKET_STEP", "5")
    monkeypatch.setenv("RELIABILITY_ADJ_MIN_BUCKET_SAMPLES", "1")

    env = {
        "venue": "binance_futures",
        "kind": "absorption",
        "entry_regime": "trending_bull",
        "confidence": 0.50,
        "ctx": {},  # <- no SMT fields => ctx_key becomes "na"
    }

    # Even if curves exist, hardest must refuse without SMT context
    res = maybe_adjust_confidence(r, envelope=env, strategy="S", symbol="BTCUSDT", tf="1m", direction="LONG")
    assert res is None
