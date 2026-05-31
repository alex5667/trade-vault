from __future__ import annotations

import pytest

from services.reliability_adjuster import maybe_apply_confidence_adjustment
from services.reliability_curves import make_reliability_key_v4
from tests.fake_redis import FakeRedis


def _set_bucket(r: FakeRedis, *, key: str, bucket: int, n: int, h: int) -> None:
    r.hset(
        key,
        mapping={
            "samples": str(n),
            "hits": str(h),
            "last_ts_ms": "1",
            f"n:{bucket}": str(n),
            f"h:{bucket}": str(h),
        },
    )


def test_apply_adjustment_attaches_fields_soft(monkeypatch: pytest.MonkeyPatch) -> None:
    r = FakeRedis()
    monkeypatch.setenv("RELIABILITY_ADJUST_ENABLED", "1")
    monkeypatch.setenv("RELIABILITY_ADJ_PROFILE", "soft")
    monkeypatch.setenv("RELIABILITY_ADJUST_TARGET", "tp2")
    monkeypatch.setenv("RELIABILITY_BUCKET_STEP", "5")
    monkeypatch.setenv("RELIABILITY_ADJ_ALPHA", "0.5")
    monkeypatch.setenv("RELIABILITY_ADJ_MIN_BUCKET_SAMPLES", "10")
    monkeypatch.setenv("RELIABILITY_SMT_COH_THR", "0.65")

    r = FakeRedis()

    env = {
        "symbol": "BTCUSDT",
        "tf": "1m",
        "kind": "absorption",
        "venue": "binance_futures",
        "entry_regime": "trending_bull",
        "strategy": "S",
        "confidence": 0.50,  # bucket 50
        "ctx": {
            "smt_leader_confirm": 1,
            "smt_coh": 0.80,
            "smt_leader_dir": "UP",
        },
    }

    bucket = 50
    k_g = make_reliability_key_v4(
        target="tp2",
        strategy="S",
        symbol="BTCUSDT",
        tf="1m",
        venue="binance_futures",
        kind="absorption",
        regime="trending_bull",
        ctx_key="na",
    )
    _set_bucket(r, key=k_g, bucket=bucket, n=100, h=50)  # pG=0.50

    k_c = make_reliability_key_v4(
        target="tp2",
        strategy="S",
        symbol="BTCUSDT",
        tf="1m",
        venue="binance_futures",
        kind="absorption",
        regime="trending_bull",
        ctx_key="smtc1_coh1_al1",
    )
    _set_bucket(r, key=k_c, bucket=bucket, n=100, h=70)  # pC=0.70

    maybe_apply_confidence_adjustment(
        r,
        envelope=env,
        strategy="S",
        symbol="BTCUSDT",
        tf="1m",
        direction="LONG",
    )

    assert "confidence_adjusted" in env
    assert abs(float(env["confidence_adjusted"]) - 0.60) < 1e-9  # 0.5 + 0.5*(0.7-0.5)
    assert env["confidence_adjust_ctx"] == "smtc1_coh1_al1"
    assert int(env["confidence_adjust_bucket"]) == 50
    assert int(env["confidence_adjust_n_glob"]) == 100
    assert int(env["confidence_adjust_n_ctx"]) == 100
    assert env["confidence_adjust_target"] == "tp2"
    assert env["confidence_adjust_profile"] == "soft"


def test_maybe_apply_confidence_adjustment_fail_open_when_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    r = FakeRedis()
    monkeypatch.setenv("RELIABILITY_ADJUST_ENABLED", "0")
    env = {"confidence": 0.5, "ctx": {}}
    maybe_apply_confidence_adjustment(r, envelope=env, strategy="S", symbol="X", tf="1m", direction="LONG")
    assert "confidence_adjusted" not in env


def test_apply_adjustment_fail_open_no_redis(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test fail-open when Redis is None (unit test scenario)"""
    monkeypatch.setenv("RELIABILITY_ADJUST_ENABLED", "1")
    env = {"confidence": 0.5, "ctx": {}}
    maybe_apply_confidence_adjustment(None, envelope=env, strategy="S", symbol="X", tf="1m", direction="LONG")
    assert "confidence_adjusted" not in env


def test_apply_adjustment_fail_open_missing_data(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test fail-open when reliability curves don't exist in Redis"""
    r = FakeRedis()
    monkeypatch.setenv("RELIABILITY_ADJUST_ENABLED", "1")
    monkeypatch.setenv("RELIABILITY_ADJ_PROFILE", "soft")
    monkeypatch.setenv("RELIABILITY_ADJUST_TARGET", "tp2")

    env = {
        "symbol": "BTCUSDT",
        "tf": "1m",
        "kind": "absorption",
        "venue": "binance_futures",
        "entry_regime": "trending_bull",
        "confidence": 0.50,
        "ctx": {"smt_leader_confirm": 1, "smt_coh": 0.80, "smt_leader_dir": "UP"},
    }

    # Redis is empty - no curves written yet
    maybe_apply_confidence_adjustment(
        r, envelope=env, strategy="S", symbol="BTCUSDT", tf="1m", direction="LONG"
    )

    # Should fail-open gracefully without adding adjustment fields
    assert "confidence_adjusted" not in env


def test_apply_adjustment_hardest_profile_requires_smt_context(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that hardest profile requires SMT context to be present"""
    r = FakeRedis()
    monkeypatch.setenv("RELIABILITY_ADJUST_ENABLED", "1")
    monkeypatch.setenv("RELIABILITY_ADJ_PROFILE", "hardest")
    monkeypatch.setenv("RELIABILITY_ADJUST_TARGET", "tp2")
    monkeypatch.setenv("RELIABILITY_BUCKET_STEP", "5")
    monkeypatch.setenv("RELIABILITY_ADJ_MIN_BUCKET_SAMPLES", "10")

    env = {
        "symbol": "BTCUSDT",
        "tf": "1m",
        "kind": "absorption",
        "venue": "binance_futures",
        "entry_regime": "trending_bull",
        "confidence": 0.50,
        "ctx": {},  # Empty context - no SMT fields
    }

    bucket = 50
    k_g = make_reliability_key_v4(
        target="tp2", strategy="S", symbol="BTCUSDT", tf="1m",
        venue="binance_futures", kind="absorption", regime="trending_bull", ctx_key="na",
    )
    _set_bucket(r, key=k_g, bucket=bucket, n=1000, h=500)  # Large global sample

    k_c = make_reliability_key_v4(
        target="tp2", strategy="S", symbol="BTCUSDT", tf="1m",
        venue="binance_futures", kind="absorption", regime="trending_bull", ctx_key="na",  # na context
    )
    _set_bucket(r, key=k_c, bucket=bucket, n=1000, h=700)  # Large context sample

    maybe_apply_confidence_adjustment(
        r, envelope=env, strategy="S", symbol="BTCUSDT", tf="1m", direction="LONG"
    )

    # Hardest profile should skip adjustment when SMT context is absent (ctx_key="na")
    assert "confidence_adjusted" not in env


def test_apply_adjustment_hardest_profile_with_smt_context(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test hardest profile works when SMT context is present"""
    r = FakeRedis()
    monkeypatch.setenv("RELIABILITY_ADJUST_ENABLED", "1")
    monkeypatch.setenv("RELIABILITY_ADJ_PROFILE", "hardest")
    monkeypatch.setenv("RELIABILITY_ADJUST_TARGET", "tp2")
    monkeypatch.setenv("RELIABILITY_BUCKET_STEP", "5")
    monkeypatch.setenv("RELIABILITY_ADJ_MIN_BUCKET_SAMPLES", "10")
    monkeypatch.setenv("RELIABILITY_ADJ_MIN_SAMPLES_GLOBAL", "100")
    monkeypatch.setenv("RELIABILITY_ADJ_MIN_SAMPLES_CTX", "50")
    monkeypatch.setenv("RELIABILITY_ADJ_MIN_DELTA", "0.01")
    monkeypatch.setenv("RELIABILITY_ADJ_MIN_Z", "1.0")

    env = {
        "symbol": "BTCUSDT",
        "tf": "1m",
        "kind": "absorption",
        "venue": "binance_futures",
        "entry_regime": "trending_bull",
        "confidence": 0.50,
        "ctx": {"smt_leader_confirm": 1, "smt_coh": 0.80, "smt_leader_dir": "UP"},
    }

    bucket = 50
    k_g = make_reliability_key_v4(
        target="tp2", strategy="S", symbol="BTCUSDT", tf="1m",
        venue="binance_futures", kind="absorption", regime="trending_bull", ctx_key="na",
    )
    _set_bucket(r, key=k_g, bucket=bucket, n=1000, h=400)  # pG=0.40

    k_c = make_reliability_key_v4(
        target="tp2", strategy="S", symbol="BTCUSDT", tf="1m",
        venue="binance_futures", kind="absorption", regime="trending_bull", ctx_key="smtc1_coh1_al1",
    )
    _set_bucket(r, key=k_c, bucket=bucket, n=1000, h=600)  # pC=0.60 (higher reliability)

    maybe_apply_confidence_adjustment(
        r, envelope=env, strategy="S", symbol="BTCUSDT", tf="1m", direction="LONG"
    )

    # Should apply adjustment with hardest profile
    assert "confidence_adjusted" in env
    assert env["confidence_adjust_profile"] == "hardest"
    assert env["confidence_adjust_target"] == "tp2"
    assert float(env["confidence_adjusted"]) > 0.5  # Should increase confidence
