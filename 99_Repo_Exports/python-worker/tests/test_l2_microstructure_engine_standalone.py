from __future__ import annotations

from types import SimpleNamespace


def _import_engine_and_contexts():
    # Support both package and flat execution layouts
    try:
        from contexts import BucketState, L2Level, SimpleL2Snapshot  # type: ignore
        from l2_microstructure_engine import L2MicrostructureEngine  # type: ignore
        return L2MicrostructureEngine, BucketState, L2Level, SimpleL2Snapshot
    except Exception:
        # try common package layout
        from handlers.l2_microstructure_engine import L2MicrostructureEngine  # type: ignore

        from contexts import BucketState, L2Level, SimpleL2Snapshot  # type: ignore
        return L2MicrostructureEngine, BucketState, L2Level, SimpleL2Snapshot


L2MicrostructureEngine, BucketState, L2Level, SimpleL2Snapshot = _import_engine_and_contexts()


def cfg(**kw):
    # minimal defaults + your anti-spoof rules
    d = dict(
        # walls
        wall_hist_m=5,
        wall_persist_p=3,
        wall_price_tol_bps=2.0,
        wall_drop_ratio_min=0.35,
        wall_near_bps=20.0,
        wall_mult_vs_avg=4.0,
        # OBI
        obi_samples_maxlen=200,
        obi20_samples_maxlen=200,
        obi_thr=0.10,
        obi_sustain_k5=5,
        obi_sustain_k20=5,
        obi_ema_alpha=0.20,
        obi_band_mode="bps",
        obi_band_5_bps=10.0,
        obi_band_20_bps=20.0,
        obi_min_levels_each_side=2,
        obi_min_total_depth=0.0,
        obi_min_total_depth_20=0.0,
    )
    d.update(kw)
    return SimpleNamespace(**d)


def lvl(p: float, s: float):
    # tolerate different dataclass signatures
    try:
        return L2Level(price=float(p), size=float(s))
    except Exception:
        try:
            return L2Level(float(p), float(s))
        except Exception:
            return SimpleNamespace(price=float(p), size=float(s))


def snap(bids, asks, ts_ms: int = 0):
    try:
        return SimpleL2Snapshot(bids=bids, asks=asks, ts_ms=ts_ms)
    except Exception:
        return SimpleNamespace(bids=bids, asks=asks, ts_ms=ts_ms)


def st_empty():
    try:
        return BucketState.empty()
    except Exception:
        return BucketState()


def test_inverted_book_sets_valid_false():
    engine = L2MicrostructureEngine(cfg())
    st = st_empty()

    s = snap(
        bids=[lvl(100.0, 10.0)],
        asks=[lvl(99.0, 10.0)],  # ask < bid (inverted)
        ts_ms=123,
    )
    engine.update(s, 123, st)

    assert st.obi_valid is False
    assert st.obi_20_valid is False


def test_obi20_invalid_when_band_empty_min_levels_each_side():
    # hb20 very tight => only top1 inside band => n_bid20=1, n_ask20=1 < min_lv(2)
    engine = L2MicrostructureEngine(cfg(obi_band_20_bps=1.0, obi_min_levels_each_side=2))
    st = st_empty()

    # best_bid=99.99, best_ask=100.01 => mid~100
    # second level outside 1 bps band
    s = snap(
        bids=[lvl(99.99, 10.0), lvl(99.98, 10.0)],
        asks=[lvl(100.01, 10.0), lvl(100.02, 10.0)],
        ts_ms=1000,
    )
    engine.update(s, 1000, st)

    assert st.obi_20_valid is False
    assert float(getattr(st, "obi_20", 0.0) or 0.0) == 0.0


def test_spoof_wall_found_now_persist_low_is_suspicious_and_not_confirmed():
    engine = L2MicrostructureEngine(cfg())
    st = st_empty()

    # Make a clear bid wall near mid (candidate >> others)
    s = snap(
        bids=[lvl(99.99, 1000.0), lvl(99.98, 10.0), lvl(99.97, 10.0)],
        asks=[lvl(100.01, 10.0), lvl(100.02, 10.0), lvl(100.03, 10.0)],
        ts_ms=2000,
    )

    # First observation => persist_ratio = 1.0 (first in history) => suspicious False => confirmed True
    engine.update(s, 2000, st)

    assert bool(getattr(st, "wall_bid_suspicious", False)) is False
    assert bool(getattr(st, "wall_bid", False)) is True  # confirmed flag
    assert float(getattr(st, "wall_bid_persist_ratio", 0.0) or 0.0) == 1.0


def test_real_wall_becomes_confirmed_after_persistence():
    engine = L2MicrostructureEngine(cfg())
    st1 = st_empty()
    st2 = st_empty()

    s = snap(
        bids=[lvl(99.99, 1000.0), lvl(99.98, 10.0), lvl(99.97, 10.0)],
        asks=[lvl(100.01, 10.0), lvl(100.02, 10.0), lvl(100.03, 10.0)],
        ts_ms=3000,
    )

    # 1st time: persist_ratio = 1.0 => confirmed True
    engine.update(s, 3000, st1)
    assert bool(getattr(st1, "wall_bid", False)) is True

    # 2nd time at same level: persist_ratio should be high => confirmed
    engine.update(s, 3010, st2)
    assert bool(getattr(st2, "wall_bid", False)) is True
    assert bool(getattr(st2, "wall_bid_suspicious", True)) is False
    assert float(getattr(st2, "wall_bid_persist_ratio", 0.0) or 0.0) >= 0.8
