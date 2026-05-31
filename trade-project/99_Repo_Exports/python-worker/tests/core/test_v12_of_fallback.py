"""Audit 2026-05-19: fallback to indicators when runtime attrs unwired."""
from core.v12_of_features import compute_group_md, compute_group_mb, compute_group_mx

class _R: pass

def test_md_basis_bps_fallback():
    r = _R()  # no runtime attr
    ind = {"basis_bps": -5.7}
    out = compute_group_md(r, 0, ind)
    assert out["perp_spot_basis_bps"] == -5.7, out

def test_mb_depth_imbalance_fallback():
    r = _R()
    ind = {"depth_imbalance_5": 0.55}
    out = compute_group_mb(r, 0, ind)
    assert out["bid_ask_queue_imbalance"] == 0.55, out

def test_mb_level2_wap_div_fallback():
    r = _R()
    ind = {"micro_mid_shift_vel_bps_s": 1.2}
    out = compute_group_mb(r, 0, ind)
    assert out["level2_wap_divergence"] == 1.2, out

def test_mx_cvd_divergence_via_ema_and_deltaz():
    r = _R()
    # opposite signs → divergence=1
    ind = {"cvd_ema": 100.0, "delta_z": -2.0}
    out = compute_group_mx(r, 0, ind)
    assert out["cvd_divergence_from_price"] == 1.0, out
    # same signs → divergence=0
    ind2 = {"cvd_ema": 100.0, "delta_z": 2.0}
    out2 = compute_group_mx(r, 0, ind2)
    assert out2["cvd_divergence_from_price"] == 0.0, out2

def test_mx_ofi_momentum_uses_ml_norm():
    r = _R()
    ind = {"ofi_ml_norm": 0.3}
    out = compute_group_mx(r, 0, ind)
    # prev cached 0 → delta = 0.3
    assert abs(out["order_imbalance_momentum"] - 0.3) < 1e-9, out

def test_runtime_attr_wins_over_indicator_fallback():
    r = _R(); r.perp_spot_basis_bps = -9.9
    out = compute_group_md(r, 0, {"basis_bps": -1.0})
    assert out["perp_spot_basis_bps"] == -9.9, out


# ---- Phase 3 fallbacks (audit 2026-05-19) ----

def test_mc_time_since_last_liq_fallback_clamps_sentinel():
    """liq_book_stale_ms ~1e9 is producer's 'never seen' sentinel — should clip to 0."""
    from core.v12_of_features import compute_group_mc
    r = _R()
    out_sent = compute_group_mc(r, 1000, {"liq_book_stale_ms": 1_000_000_000})
    assert out_sent["time_since_last_liq_ms"] == 0.0, out_sent
    out_real = compute_group_mc(r, 1000, {"liq_book_stale_ms": 5000})
    assert out_real["time_since_last_liq_ms"] == 5000.0, out_real


def test_me_signal_frequency_1h_fallback_from_indicators():
    """Counter is populated by signal_pipeline; ME picks it up."""
    from core.v12_of_features import compute_group_me
    r = _R()
    out = compute_group_me(r, 0, {"signal_frequency_1h": 7})
    assert out["signal_frequency_1h"] == 7.0, out


def test_me_calibration_age_falls_back_to_atr_age_ms():
    from core.v12_of_features import compute_group_me
    r = _R()
    out = compute_group_me(r, 0, {"atr_age_ms": 1234.0})
    assert out["calibration_age_ms"] == 1234.0, out


def test_mx_spread_rank_via_p95_threshold():
    from core.v12_of_features import compute_group_mx
    r = _R()
    out = compute_group_mx(r, 0, {"spread_bps": 10.0, "spread_p95_bps_symbol_kind_session": 10.0})
    assert abs(out["spread_percentile_rank_1d"] - 0.95) < 1e-9, out
    out2 = compute_group_mx(r, 0, {"spread_bps": 20.0, "spread_p95_bps_symbol_kind_session": 10.0})
    assert out2["spread_percentile_rank_1d"] == 1.0, out2
    out3 = compute_group_mx(r, 0, {"spread_bps": 10.0})
    assert out3["spread_percentile_rank_1d"] == 0.0, out3


def test_mx_atr_rank_via_threshold():
    from core.v12_of_features import compute_group_mx
    r = _R()
    out_th = compute_group_mx(r, 0, {"atr_bps": 10.0, "atr_bps_th": 10.0})
    assert abs(out_th["atr_percentile_rank_30d"] - 0.5) < 1e-9, out_th
    out_hi = compute_group_mx(r, 0, {"atr_bps": 25.0, "atr_bps_th": 10.0})
    assert out_hi["atr_percentile_rank_30d"] == 1.0, out_hi


def test_ma_trade_arrival_rate_fallback_to_book_rate_hz():
    from core.v12_of_features import compute_group_ma
    r = _R()
    out = compute_group_ma(r, 0, {"book_rate_hz": 8.5})
    assert out["trade_arrival_rate_hz"] == 8.5, out


def test_ma_tick_direction_run_no_obi_fallback():
    """Phase 7: obi_stable_secs×book_rate_hz proxy removed (OOD 25-1200 vs 1-20).
    tick_direction_run now requires either runtime attr or pre-populated indicator
    (from signal_pipeline per-symbol direction-run counter)."""
    from core.v12_of_features import compute_group_ma
    r = _R()
    out = compute_group_ma(r, 0, {"obi_stable_secs": 6.0, "book_rate_hz": 10.0})
    assert out["tick_direction_run"] == 0.0, out


def test_me_last_trade_outcome_uses_indicators_when_runtime_unset():
    from core.v12_of_features import compute_group_me
    r = _R()
    # Indicators-pre-populated (by signal_pipeline trades:closed reader)
    out = compute_group_me(r, 0, {"last_trade_outcome_raw": -42.5})
    assert out["last_trade_outcome_raw"] == -42.5, out


def test_me_runtime_wins_over_indicators_for_last_trade():
    from core.v12_of_features import compute_group_me
    r = _R(); r.last_trade_pnl_bps = 17.0  # type: ignore[attr-defined]
    out = compute_group_me(r, 0, {"last_trade_outcome_raw": -42.5})
    assert out["last_trade_outcome_raw"] == 17.0, out


def test_signal_frequency_counter_logic():
    """Audit-trail logic mirror of publish_signal counter; isolated unit."""
    from collections import deque
    state: dict[str, deque] = {}
    def emit(symbol: str, ts_ms: int) -> float:
        if symbol not in state:
            state[symbol] = deque(maxlen=10000)
        q = state[symbol]
        cutoff = ts_ms - 3_600_000
        while q and q[0] < cutoff:
            q.popleft()
        q.append(ts_ms)
        return float(len(q))
    assert emit("BTC", 1_000_000) == 1.0
    assert emit("BTC", 1_001_000) == 2.0
    assert emit("BTC", 1_000_000 + 3_600_001) == 2.0  # 1st pruned
    assert emit("ETH", 1_001_500) == 1.0


def test_ma_tick_direction_run_uses_indicators_when_runtime_unset():
    """Phase 7: tick_direction_run reads from indicators when runtime attr missing."""
    from core.v12_of_features import compute_group_ma
    r = _R()
    out = compute_group_ma(r, 0, {"tick_direction_run": 7.0})
    assert out["tick_direction_run"] == 7.0, out


def test_ma_tick_direction_run_runtime_wins():
    """Phase 7: runtime attr precedes indicator fallback."""
    from core.v12_of_features import compute_group_ma
    r = _R(); r.tick_direction_run = 15  # type: ignore[attr-defined]
    out = compute_group_ma(r, 0, {"tick_direction_run": 99.0})
    assert out["tick_direction_run"] == 15.0, out
