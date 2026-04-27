from __future__ import annotations

from core.exec_regime_bucket_v1 import compute_exec_regime_bucket


def test_exec_regime_bucket_normal() -> None:
    r = compute_exec_regime_bucket(liq_regime_label="normal", vol_regime_label="normal")
    assert r.bucket == "NORMAL"
    assert r.low_liq == 0
    assert r.high_vol == 0


def test_exec_regime_bucket_low_liq() -> None:
    r = compute_exec_regime_bucket(liq_regime_label="very_low", vol_regime_label="calm")
    assert r.bucket == "LOW_LIQ"
    assert r.low_liq == 1
    assert r.high_vol == 0


def test_exec_regime_bucket_high_vol() -> None:
    r = compute_exec_regime_bucket(liq_regime_label="normal", vol_regime_label="shock")
    assert r.bucket == "HIGH_VOL"
    assert r.low_liq == 0
    assert r.high_vol == 1


def test_exec_regime_bucket_high_vol_low_liq() -> None:
    r = compute_exec_regime_bucket(liq_regime_label="stressed", vol_regime_label="shock")
    assert r.bucket == "HIGH_VOL_LOW_LIQ"
    assert r.low_liq == 1
    assert r.high_vol == 1
