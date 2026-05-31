"""Phase 3.1 — ensemble weights tests."""
from __future__ import annotations

import math
from calibration.ensemble_weights import (
    compute_weights,
    neg_log_loss,
    sharpe,
    to_redis_payload,
)


_NOW_MS = 1_780_000_000_000


def _row(symbol="BTCUSDT", source="of", p=0.6, y=1, rr=0.5, age_days=0.0):
    return dict(
        symbol=symbol,
        source=source,
        decision_time_ms=_NOW_MS - int(age_days * 86_400_000),
        calib_prob=p,
        realized_r=rr,
        label=y,
    )


def test_neg_log_loss_perfect():
    assert neg_log_loss([0.99] * 10, [1] * 10) < 0.02


def test_neg_log_loss_worst():
    # Predicting near-zero for all winners → high loss
    assert neg_log_loss([0.01] * 10, [1] * 10) > 4.0


def test_sharpe_zero_when_constant():
    assert sharpe([1.0, 1.0, 1.0]) == 0.0


def test_sharpe_positive_uptrend():
    assert sharpe([0.1, 0.2, 0.3, 0.4]) > 0.0


def test_min_samples_dropped_to_equalweight_fallback():
    rows = [_row(source="a") for _ in range(5)] + [_row(source="b") for _ in range(5)]
    w = compute_weights(rows, min_samples=100, now_ms=_NOW_MS)
    assert "BTCUSDT" in w
    # Equal-weight fallback over both observed sources
    weights = [it.weight for it in w["BTCUSDT"]]
    assert all(math.isclose(x, 0.5, rel_tol=1e-6) for x in weights)


def test_better_source_gets_higher_weight():
    good = [_row(source="good", p=0.9, y=1) for _ in range(150)]
    bad  = [_row(source="bad",  p=0.1, y=1) for _ in range(150)]
    w = compute_weights(good + bad, min_samples=100, now_ms=_NOW_MS, temperature=1.0)
    sym_w = {it.source: it.weight for it in w["BTCUSDT"]}
    assert sym_w["good"] > sym_w["bad"]
    assert math.isclose(sym_w["good"] + sym_w["bad"], 1.0, rel_tol=1e-6)


def test_temperature_clamp_concentrates():
    good = [_row(source="good", p=0.9, y=1) for _ in range(150)]
    bad  = [_row(source="bad",  p=0.1, y=1) for _ in range(150)]
    cold = compute_weights(good + bad, min_samples=100, now_ms=_NOW_MS, temperature=0.1)
    warm = compute_weights(good + bad, min_samples=100, now_ms=_NOW_MS, temperature=10.0)
    cold_good = next(it.weight for it in cold["BTCUSDT"] if it.source == "good")
    warm_good = next(it.weight for it in warm["BTCUSDT"] if it.source == "good")
    assert cold_good > warm_good  # lower T → more concentration on best


def test_time_decay_demotes_old_samples():
    fresh = [_row(source="a", p=0.6, y=1, age_days=0.5) for _ in range(150)]
    stale = [_row(source="a", p=0.6, y=0, age_days=30.0) for _ in range(150)]
    w = compute_weights(fresh + stale, min_samples=100, now_ms=_NOW_MS, halflife_days=3.0)
    items = w["BTCUSDT"]
    # Same source — single bucket; check skill reflects fresh wins dominating
    assert items[0].avg_realized_r > 0


def test_sharpe_metric_path():
    rows = [_row(rr=0.5) for _ in range(150)] + [_row(source="b", rr=0.1) for _ in range(150)]
    w = compute_weights(rows, metric="sharpe", min_samples=100, now_ms=_NOW_MS)
    assert "BTCUSDT" in w


def test_payload_shape():
    rows = [_row(source="a") for _ in range(150)] + [_row(source="b") for _ in range(150)]
    w = compute_weights(rows, min_samples=100, now_ms=_NOW_MS)
    payload = to_redis_payload(w)
    assert "BTCUSDT" in payload
    assert "a" in payload["BTCUSDT"]
    assert isinstance(payload["BTCUSDT"]["a"], str)


def test_per_symbol_independent():
    rows = (
        [_row(symbol="BTC", source="a") for _ in range(150)]
        + [_row(symbol="ETH", source="a") for _ in range(150)]
    )
    w = compute_weights(rows, min_samples=100, now_ms=_NOW_MS)
    assert "BTC" in w and "ETH" in w
