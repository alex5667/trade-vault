"""Phase 2.3 — adaptive TTL recommendation tests."""
from __future__ import annotations

import math
from calibration.adaptive_ttl import recommend, to_redis_payload


def _row(symbol="BTCUSDT", regime="momentum", side=1, label=1, mfe=1.5, mae=-0.5):
    return dict(symbol=symbol, regime=regime, side=side, label=label, mfe_r=mfe, mae_r=mae)


def test_insufficient_samples_drops_group():
    rows = [_row() for _ in range(10)]
    assert recommend(rows, min_samples=50) == []


def test_basic_recommendation():
    rows = [_row(mfe=1.0, mae=-0.5) for _ in range(60)]
    recs = recommend(rows, min_samples=50)
    assert len(recs) == 1
    r = recs[0]
    assert r.symbol == "BTCUSDT"
    assert r.regime == "momentum"
    assert r.direction == 1
    assert r.n == 60
    assert r.win_rate == 1.0
    assert math.isclose(r.tp_r, 1.0, rel_tol=1e-6)
    assert math.isclose(r.sl_r, 0.5, rel_tol=1e-6)


def test_min_sl_r_floor():
    rows = [_row(mae=-0.1) for _ in range(60)]
    recs = recommend(rows, min_samples=50, min_sl_r=0.5)
    assert recs[0].sl_r == 0.5


def test_tp_clamped_to_max():
    rows = [_row(mfe=10.0) for _ in range(60)]
    recs = recommend(rows, min_samples=50)
    assert recs[0].tp_r == 3.0  # _MAX_TP_R


def test_losers_only_uses_fallback_tp():
    # All losers — no winners — falls back to 1.0 → clamped to [_MIN_TP_R, _MAX_TP_R]
    rows = [_row(label=-1, mfe=0.0, mae=-2.0) for _ in range(60)]
    recs = recommend(rows, min_samples=50)
    assert recs[0].win_rate == 0.0
    assert recs[0].tp_r == 1.0


def test_groupby_symbol_regime_side():
    rows = (
        [_row(symbol="BTCUSDT", side=1) for _ in range(60)]
        + [_row(symbol="BTCUSDT", side=-1) for _ in range(60)]
        + [_row(symbol="ETHUSDT", side=1) for _ in range(60)]
    )
    recs = recommend(rows, min_samples=50)
    keys = {(r.symbol, r.direction) for r in recs}
    assert keys == {("BTCUSDT", 1), ("BTCUSDT", -1), ("ETHUSDT", 1)}


def test_zero_side_skipped():
    rows = [_row(side=0) for _ in range(60)]
    assert recommend(rows, min_samples=50) == []


def test_missing_symbol_skipped():
    rows = [_row(symbol="") for _ in range(60)]
    assert recommend(rows, min_samples=50) == []


def test_payload_shape():
    rows = [_row() for _ in range(60)]
    recs = recommend(rows, min_samples=50)
    payload = to_redis_payload(recs, generated_at_ms=1_780_000_000_000)
    assert payload["v"] == 1
    assert payload["n"] == 1
    assert payload["generated_at_ms"] == 1_780_000_000_000
    assert payload["recs"][0]["symbol"] == "BTCUSDT"


def test_mixed_win_loss_uses_median_mfe_of_winners_only():
    # 50 winners with mfe=2.0, 50 losers with mfe=0.1 → median of winners = 2.0
    rows = (
        [_row(label=1, mfe=2.0, mae=-0.3) for _ in range(50)]
        + [_row(label=-1, mfe=0.1, mae=-1.0) for _ in range(50)]
    )
    recs = recommend(rows, min_samples=50)
    assert recs[0].win_rate == 0.5
    assert math.isclose(recs[0].tp_r, 2.0, rel_tol=1e-6)
    # p10 of all mae: tail is -1.0 (losers), so sl_r ≈ 1.0
    assert recs[0].sl_r > 0.5
