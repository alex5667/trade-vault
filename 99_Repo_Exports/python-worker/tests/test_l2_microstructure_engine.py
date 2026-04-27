import pytest
from contexts import SimpleL2Snapshot, L2Level, BucketState
from l2_microstructure_engine import L2MicrostructureEngine


class Cfg:
    # OBI
    obi_band_mode = "bps"
    obi_band_5_bps = 10.0
    obi_band_20_bps = 20.0
    obi_min_levels_each_side = 2
    obi_min_total_depth = 0.0
    obi_min_total_depth_20 = 0.0
    obi_ema_alpha = 0.2
    obi_thr = 0.10
    obi_sustain_k5 = 3
    obi_sustain_k20 = 3
    obi_samples_maxlen = 50
    obi20_samples_maxlen = 50
    # WALL
    wall_hist_m = 5
    wall_persist_p = 3
    wall_drop_ratio_min = 0.35
    wall_near_bps = 10.0
    wall_mult_vs_avg = 4.0
    wall_price_tol_bps = 2.0


def mk_snap(bids, asks, ts_ms=0):
    """Helper to create SimpleL2Snapshot from bid/ask lists"""
    return SimpleL2Snapshot(
        bids=[L2Level(p, s) for p, s in bids],
        asks=[L2Level(p, s) for p, s in asks],
        ts_ms=ts_ms,
    )


class TestL2MicrostructureEngine:
    # Use global Cfg class for all tests
    cfg_class = Cfg

    def test_spoof_wall_bid_single_shot(self):
        """Тест спуф-стены: появилась 1 раз из 5 → suspicious"""
        eng = L2MicrostructureEngine(self.cfg_class())
        st = BucketState.empty()

        # 4 снапа без стены
        for i in range(4):
            snap = mk_snap(
                bids=[(100.0, 1.0), (99.95, 1.0)],
                asks=[(100.1, 1.0), (100.15, 1.0)],
                ts_ms=1000*i
            )
            eng.update(snap, 1000*i, st)

        # 5-й снап со стеной (огромный bid size)
        snap = mk_snap(
            bids=[(100.0, 50.0), (99.95, 1.0)],  # "стена"
            asks=[(100.1, 1.0), (100.15, 1.0)],
            ts_ms=4000
        )
        eng.update(snap, 4000, st)

        # Raw wall detected (price > 0), but not confirmed (persist_ratio < threshold)
        assert st.wall_bid_price > 0  # wall detected
        assert st.wall_bid is False  # but not confirmed (single shot)
        assert st.wall_bid_suspicious is True
        assert st.wall_bid_persist_ratio == 0.2  # 1 wall out of 5 observations

    def test_real_wall_bid_persistent(self):
        """Тест реальной стены: есть 4/5 → не suspicious"""
        eng = L2MicrostructureEngine(self.cfg_class())
        st = BucketState.empty()

        for i in range(5):
            snap = mk_snap(
                bids=[(100.0, 30.0), (99.95, 1.0)],
                asks=[(100.1, 1.0), (100.15, 1.0)],
                ts_ms=1000*i
            )
            eng.update(snap, 1000*i, st)

        assert st.wall_bid is True  # confirmed wall (persistent)
        assert st.wall_bid_persist_ratio >= 0.6
        assert st.wall_bid_suspicious is False

    def test_wall_drop_ratio_suspicious(self):
        """Тест drop ratio: стенка схлопнулась → suspicious"""
        eng = L2MicrostructureEngine(self.cfg_class())
        st = BucketState.empty()

        # Сначала создаем persistent стену (несколько снэпов)
        for i in range(4):
            snap = mk_snap(bids=[(100.0, 40.0), (99.95, 1.0)], asks=[(100.1, 1.0)], ts_ms=i*1000)
            eng.update(snap, i*1000, st)

        # Теперь стена confirmed, проверяем drop ratio
        snap_drop = mk_snap(bids=[(100.0, 10.0), (99.95, 1.0)], asks=[(100.1, 1.0)], ts_ms=4000)  # 10/40=0.25 < 0.35
        eng.update(snap_drop, 4000, st)

        assert st.wall_bid_drop_ratio < 0.35
        assert st.wall_bid_suspicious is True

    def test_inverted_book_no_walls(self):
        """Тест inverted book (best_ask < best_bid) → engine не пишет wall/obi"""
        eng = L2MicrostructureEngine(self.cfg_class())
        st = BucketState.empty()

        # Создаем inverted book: best_ask < best_bid
        snap = mk_snap(
            bids=[(100.0, 50.0), (99.95, 1.0)],
            asks=[(99.5, 1.0), (99.4, 1.0)],  # best_ask = 99.5 < best_bid = 100.0
            ts_ms=1000
        )
        eng.update(snap, 1000, st)

        # Engine не должен обновлять wall поля при inverted book
        assert st.wall_bid is False
        assert st.wall_ask is False
        assert st.best_bid is None  # или не обновляется
        assert st.best_ask is None

    def test_narrow_band_empty_no_wall(self):
        """Тест narrow band empty (нет уровней в near) → wall_found False"""
        eng = L2MicrostructureEngine(self.cfg_class())
        st = BucketState.empty()

        # Создаем стакан где в near band (1bps) нет уровней
        snap = mk_snap(
            bids=[(99.0, 50.0), (98.9, 1.0)],  # далеко от mid (~100)
            asks=[(101.0, 50.0), (101.1, 1.0)],  # далеко от mid
            ts_ms=1000
        )
        eng.update(snap, 1000, st)

        assert st.wall_bid is False
        assert st.wall_ask is False

    def test_price_shift_no_persistence(self):
        """Тест price shift (стена "уехала" далеко) → persistence не засчитывается"""
        eng = L2MicrostructureEngine(self.cfg_class())
        st = BucketState.empty()

        # Сначала стена на одном уровне
        snap1 = mk_snap(
            bids=[(100.0, 30.0), (99.95, 1.0)],
            asks=[(100.1, 1.0)],
            ts_ms=0
        )
        eng.update(snap1, 0, st)

        # Затем стена "уезжает" далеко (цена изменилась > wall_price_tol_bps)
        snap2 = mk_snap(
            bids=[(98.0, 30.0), (97.95, 1.0)],  # цена изменилась на ~2%, что > 2.0 bps
            asks=[(98.1, 1.0)],
            ts_ms=1000
        )
        eng.update(snap2, 1000, st)

        # Persistence не должен засчитываться из-за price shift
        assert st.wall_bid_persist_ratio < 0.6  # меньше порога

    def test_ask_wall_spoof(self):
        """Тест спуф ask стены"""
        eng = L2MicrostructureEngine(self.cfg_class())
        st = BucketState.empty()

        # 4 снапа без ask стены
        for i in range(4):
            snap = mk_snap(
                bids=[(100.0, 1.0)],
                asks=[(100.1, 1.0), (100.15, 1.0)],
                ts_ms=1000*i
            )
            eng.update(snap, 1000*i, st)

        # 5-й снап с ask стеной
        snap = mk_snap(
            bids=[(100.0, 1.0)],
            asks=[(100.1, 50.0), (100.15, 1.0)],  # ask "стена"
            ts_ms=4000
        )
        eng.update(snap, 4000, st)

        # Raw wall detected, but not confirmed
        assert st.wall_ask_price > 0  # wall detected
        assert st.wall_ask is False  # but not confirmed (single shot)
        assert st.wall_ask_suspicious is True
        assert st.wall_ask_persist_ratio == 0.2  # 1 wall out of 5 observations

    def test_ask_wall_drop_ratio(self):
        """Тест ask wall drop ratio"""
        eng = L2MicrostructureEngine(self.cfg_class())
        st = BucketState.empty()

        # Сначала создаем persistent ask стену
        for i in range(4):
            snap = mk_snap(bids=[(100.0, 1.0)], asks=[(100.1, 40.0), (100.15, 1.0)], ts_ms=i*1000)
            eng.update(snap, i*1000, st)

        # Теперь стена confirmed, проверяем drop ratio
        snap_drop = mk_snap(bids=[(100.0, 1.0)], asks=[(100.1, 10.0), (100.15, 1.0)], ts_ms=4000)
        eng.update(snap_drop, 4000, st)

        assert st.wall_ask_drop_ratio < 0.35
        assert st.wall_ask_suspicious is True

    def test_inverted_book_no_update(self):
        """Test inverted book (ask < bid) → engine should not update state"""
        eng = L2MicrostructureEngine(self.cfg_class())
        st = BucketState.empty()

        # Save original l2_ts
        original_l2_ts = st.l2_ts

        # Create inverted book: best_ask < best_bid
        snap = mk_snap(
            bids=[(100.0, 50.0), (99.95, 1.0)],
            asks=[(99.5, 1.0), (99.4, 1.0)],  # best_ask = 99.5 < best_bid = 100.0
            ts_ms=1000
        )

        eng.update(snap, 1000, st)

        # Engine should not update L2 data for inverted book
        assert st.l2_ts == original_l2_ts  # unchanged
        assert st.obi_20_valid is False   # should be invalid

    def test_stale_l2_tick_age(self):
        """Test stale L2 detection based on tick timing"""
        eng = L2MicrostructureEngine(self.cfg_class())
        st = BucketState.empty()

        # Update with fresh book
        snap = mk_snap(
            bids=[(100.0, 10.0), (99.95, 1.0)],
            asks=[(100.1, 10.0), (100.15, 1.0)],
            ts_ms=100000  # book timestamp
        )
        eng.update(snap, 100000, st)

        # Simulate tick 1 second later - should be fresh
        from contexts import Tick
        tick = Tick(ts=101000, bid=100.0, ask=100.1, last=100.05, volume=1.0, flags=0, is_buyer_maker=True)
        st.update_from_tick_inplace(tick, 101000)

        assert st.l2_age_ms_tick == 1000
        assert st.l2_is_stale_now is False
        assert st.l2_skew_tick_flag is False

        # Simulate tick 5 seconds later - should be stale and skewed
        tick_late = Tick(ts=105000, bid=100.0, ask=100.1, last=100.05, volume=1.0, flags=0, is_buyer_maker=True)
        st.update_from_tick_inplace(tick_late, 105000)

        assert st.l2_age_ms_tick == 5000
        assert st.l2_is_stale_now is True  # > 2000ms
        assert st.l2_skew_tick_flag is True  # > 3000ms

    def test_narrow_band_empty_invalid_obi(self):
        """Test narrow band with insufficient levels → obi_20_valid False"""
        eng = L2MicrostructureEngine(self.cfg_class())
        st = BucketState.empty()

        # Create book where band20 captures very few levels
        # mid = 100.05, band20 = 100.05 ± 0.2% = [99.85, 100.25]
        # But levels are at 100.0, 101.0 - only one level per side in band
        snap = mk_snap(
            bids=[(100.0, 10.0), (99.0, 100.0)],  # 99.0 is outside band [99.85, 100.25]
            asks=[(100.1, 10.0), (101.0, 100.0)],  # 101.0 is outside band
            ts_ms=1000
        )

        eng.update(snap, 1000, st)

        # Should be invalid due to insufficient levels in band
        assert st.obi_20_valid is False
        assert st.depth_bid_20 >= 10.0  # depth is there
        assert st.depth_ask_20 >= 10.0

    def test_spoof_wall_not_confirmed(self):
        """Test spoof wall: detected but not confirmed (low persistence)"""
        eng = L2MicrostructureEngine(self.cfg_class())
        st = BucketState.empty()

        # 4 snapshots without wall
        for i in range(4):
            snap = mk_snap(
                bids=[(100.0, 1.0), (99.95, 1.0)],
                asks=[(100.1, 1.0), (100.15, 1.0)],
                ts_ms=1000*i
            )
            eng.update(snap, 1000*i, st)

        # 5th snapshot with sudden large wall (spoof)
        snap_spoof = mk_snap(
            bids=[(100.0, 50.0), (99.95, 1.0)],  # sudden wall
            asks=[(100.1, 1.0), (100.15, 1.0)],
            ts_ms=4000
        )
        eng.update(snap_spoof, 4000, st)

        # Wall detected but not confirmed
        assert st.wall_bid_price > 0  # raw detection
        assert st.wall_bid is False   # not confirmed (low persistence)
        assert st.wall_bid_suspicious is True  # suspicious due to low persistence
        assert st.wall_bid_persist_ratio < 0.6  # low persistence ratio

    def test_real_wall_confirmed(self):
        """Test real persistent wall: detected and confirmed"""
        eng = L2MicrostructureEngine(self.cfg_class())
        st = BucketState.empty()

        # 5 snapshots with consistent wall
        for i in range(5):
            snap = mk_snap(
                bids=[(100.0, 30.0), (99.95, 1.0)],  # consistent wall
                asks=[(100.1, 1.0), (100.15, 1.0)],
                ts_ms=1000*i
            )
            eng.update(snap, 1000*i, st)

        # Wall should be confirmed
        assert st.wall_bid is True   # confirmed (high persistence)
        assert st.wall_bid_suspicious is False  # not suspicious
        assert st.wall_bid_persist_ratio >= 0.8  # high persistence ratio

    def test_contradiction_microprice_obi(self):
        """Test contradiction between OBI and microprice"""
        eng = L2MicrostructureEngine(self.cfg_class())
        st = BucketState.empty()

        # Create setup where OBI shows bid pressure but microprice shows ask pressure
        # This happens when there are large bids deeper in the book but large asks at top
        # OBI: looks at band depth, sees more bids
        # Microprice: looks at top-of-book sizes, sees more asks
        snap = mk_snap(
            bids=[(100.0, 1.0), (99.95, 100.0)],    # small top bid, large deeper bids
            asks=[(100.1, 50.0), (100.15, 1.0)],     # large top ask, small deeper asks
            ts_ms=1000
        )

        eng.update(snap, 1000, st)

        # OBI should be positive due to large bid depth in band (99.95 level)
        assert st.obi_20 > 0.0

        # Microprice should be pulled toward bid side due to small top bid vs large top ask
        # bid_sz1=1.0, ask_sz1=50.0, so microprice < mid (toward bids)
        assert st.microprice_shift_bps_20 < 0.0  # negative shift = toward bids


    def test_inverted_book_sets_invalid(self):
        eng = L2MicrostructureEngine(self.cfg_class())
        st = BucketState.empty()
        snap = mk_snap(bids=[(100, 1)], asks=[(99, 1)])  # ask < bid
        eng.update(snap, 1000, st)
        assert st.obi_valid is False
        assert st.obi_20_valid is False

    def test_narrow_band_empty_makes_obi20_invalid_when_min_levels_required(self):
        eng = L2MicrostructureEngine(self.cfg_class())
        st = BucketState.empty()
        # Только 1 уровень с каждой стороны внутри бэнда -> min_levels_each_side=2 => invalid
        snap = mk_snap(
            bids=[(100.00, 1.0), (90.0, 1.0)],
            asks=[(100.01, 1.0), (110.0, 1.0)]
        )
        eng.update(snap, 1000, st)
        assert st.obi_valid is False or st.obi_20_valid is False  # зависит от band bps/цен

    def test_spoof_wall_suspicious_when_low_persistence(self):
        eng = L2MicrostructureEngine(self.cfg_class())
        st = BucketState.empty()

        # 4 снапа без стены - используем уровни точно в near band
        for i in range(4):
            snap = mk_snap(bids=[(100, 1), (99.95, 1)], asks=[(100.1, 1), (100.15, 1)], ts_ms=1000+i)
            eng.update(snap, 1000+i, st)

        # 5-й снап: внезапно огромный уровень внутри near band => found, но persistence низкий
        snap = mk_snap(bids=[(100, 50), (99.95, 1)], asks=[(100.1, 1), (100.15, 1)], ts_ms=1005)
        eng.update(snap, 1005, st)

        assert st.wall_bid_suspicious is True
        assert st.wall_bid is False  # confirmed должен быть False

    def test_real_wall_confirmed_when_persistent(self):
        eng = L2MicrostructureEngine(self.cfg_class())
        st = BucketState.empty()

        # 5 снапов подряд: стена на одном уровне
        for i in range(5):
            snap = mk_snap(bids=[(100, 50), (99.95, 1)], asks=[(100.1, 1), (100.15, 1)], ts_ms=1000+i)
            eng.update(snap, 1000+i, st)

        assert st.wall_bid_persist_ratio >= 0.6  # при p=3/m=5
        assert st.wall_bid_suspicious is False
        assert st.wall_bid is True

    def test_microprice_contradiction_example(self):
        eng = L2MicrostructureEngine(self.cfg_class())
        st = BucketState.empty()
        # Сильный bid size на best_bid => microprice уйдёт вверх (положительный shift)
        snap = mk_snap(bids=[(100.0, 100.0), (99.95, 1.0)], asks=[(100.1, 1.0), (100.15, 1.0)])
        eng.update(snap, 1000, st)
        assert st.microprice_shift_bps_20 >= 0.0

        # This creates contradiction: OBI shows bid pressure (large depth at 99.95), microprice shows bid pressure too (small top bid vs large top ask pulls toward bids)
