"""
Tests for spread_bps reliability fixes:
  Fix 1: Crossed BBO preservation — book_processor must NOT zero last_spread_bps_l2
           when the book is crossed; it should keep the last good value.
  Fix 2: Stale-book invalidation — if book_ts_gap_ms > threshold, last_spread_bps_l2
           is skipped in the fallback chain (annotated as spread_bps_stale_book).
  Fix 3: Cold-start grace period — spread_missing must NOT degrade data_health
           during the first SPREAD_MISSING_COLD_START_MS after worker restart.
"""
import types
import pytest

SENTINEL_NEVER_SEEN = int(10**9)   # book_ts_gap sentinel when no book arrived yet
SPREAD_STALE_MS     = 30_000
COLD_START_MS       = 10_000
DATA_HEALTH_ON_SPREAD_MISSING = 0.60


# ─── helpers ──────────────────────────────────────────────────────────────────

def _runtime(**kw):
    r = types.SimpleNamespace()
    r.last_spread_bps_l2 = 0.0
    r.last_spread_bps    = 0.0
    r.first_book_ts_ms   = 0
    r.book_crossed       = 0
    for k, v in kw.items():
        setattr(r, k, v)
    return r


def _indicators(**kw):
    return dict(kw)


def _cfg2(**kw):
    d = dict(
        spread_bps_missing_default=15.0,
        data_health_on_spread_missing=DATA_HEALTH_ON_SPREAD_MISSING,
        spread_stale_book_gap_ms=SPREAD_STALE_MS,
        spread_missing_cold_start_ms=COLD_START_MS,
    )
    d.update(kw)
    return d


# ─── mirror of book_processor crossed-book logic ───────────────────────────

def _book_proc_update_spread(runtime, bb_px: float, ba_px: float, book_ts_ms: int):
    """Mirrors book_processor._update_ofi spread logic (Fix 1)."""
    if bb_px > 0 and ba_px > 0:
        mid = 0.5 * (bb_px + ba_px)
        spr = ba_px - bb_px
        if spr > 0 and mid > 0:
            runtime.last_spread_bps_l2 = float((spr / mid) * 10_000.0)
            runtime.last_spread_bps_l2_ts_ms = book_ts_ms
            runtime.book_crossed = 0
            if not getattr(runtime, "first_book_ts_ms", 0):
                runtime.first_book_ts_ms = book_ts_ms
        else:
            runtime.book_crossed = 1  # preserve last good value


# ─── mirror of strategy fallback chain ─────────────────────────────────────

def _resolve_spread_strategy(indicators: dict, runtime, cfg2: dict, tick_ts: int):
    """Mirrors strategy.py spread fallback chain (Fix 2 + Fix 3)."""
    _stale_ms      = int(cfg2.get("spread_stale_book_gap_ms", SPREAD_STALE_MS))
    _cold_start_ms = int(cfg2.get("spread_missing_cold_start_ms", COLD_START_MS))

    _book_ts_gap   = int(indicators.get("book_ts_gap_ms", 0) or 0)
    _book_never_seen = _book_ts_gap >= int(10**8)
    _book_stale      = (not _book_never_seen) and (_book_ts_gap > _stale_ms)

    _first_book_ts = int(getattr(runtime, "first_book_ts_ms", 0) or 0)
    _in_cold_start = _book_never_seen and (
        _first_book_ts <= 0 or (tick_ts - _first_book_ts) < _cold_start_ms
    )

    spr = float(indicators.get("spread_bps", 0.0) or 0.0)
    if spr <= 0:
        if not _book_stale and not _book_never_seen:
            spr = float(getattr(runtime, "last_spread_bps_l2", 0.0) or 0.0)
        else:
            indicators["spread_bps_stale_book"] = 1
    if spr <= 0:
        spr = float(getattr(runtime, "last_spread_bps", 0.0) or 0.0)
    if spr <= 0:
        spr = float(indicators.get("liq_spread_bps", 0.0) or 0.0)
    if spr <= 0:
        spr = float(cfg2.get("spread_bps_missing_default", 15.0))
        indicators["spread_bps_missing"] = 1
        if not _in_cold_start:
            dh = float(indicators.get("data_health", 1.0) or 1.0)
            indicators["data_health"] = min(dh, float(cfg2.get("data_health_on_spread_missing", DATA_HEALTH_ON_SPREAD_MISSING)))
            r_str = str(indicators.get("data_health_reasons", ""))
            indicators["data_health_reasons"] = (r_str + ",spread_missing") if r_str else "spread_missing"
            indicators["book_health_ok"] = 0
        else:
            r_str = str(indicators.get("data_health_reasons", ""))
            indicators["data_health_reasons"] = (r_str + ",spread_cold_start") if r_str else "spread_cold_start"
            indicators["spread_bps_cold_start"] = 1

    indicators["spread_bps"] = float(spr)


# ════════════════════════════════════════════════════════════════════════════
# Fix 1: Crossed-book guard
# ════════════════════════════════════════════════════════════════════════════

class TestCrossedBookGuard:

    def test_normal_book_updates_spread(self):
        """Normal BBO (ask > bid) → last_spread_bps_l2 is updated."""
        rt = _runtime()
        _book_proc_update_spread(rt, bb_px=50_000.0, ba_px=50_000.50, book_ts_ms=1_000)
        assert rt.last_spread_bps_l2 > 0, "Should compute positive spread"
        assert rt.book_crossed == 0

    def test_crossed_book_preserves_last_good_spread(self):
        """Crossed BBO (ask <= bid) → last_spread_bps_l2 is NOT zeroed."""
        rt = _runtime(last_spread_bps_l2=4.5)
        _book_proc_update_spread(rt, bb_px=50_001.0, ba_px=49_999.0, book_ts_ms=2_000)
        assert rt.last_spread_bps_l2 == pytest.approx(4.5), "Must preserve last good value"
        assert rt.book_crossed == 1

    def test_first_book_ts_ms_set_on_first_good_book(self):
        """first_book_ts_ms should be set exactly once, on the first valid book."""
        rt = _runtime()
        assert rt.first_book_ts_ms == 0
        _book_proc_update_spread(rt, bb_px=100.0, ba_px=100.05, book_ts_ms=9_000)
        assert rt.first_book_ts_ms == 9_000, "Must record first good book ts"

    def test_first_book_ts_ms_not_overwritten(self):
        """Subsequent valid books must NOT overwrite first_book_ts_ms."""
        rt = _runtime(first_book_ts_ms=5_000)
        _book_proc_update_spread(rt, bb_px=100.0, ba_px=100.05, book_ts_ms=99_000)
        assert rt.first_book_ts_ms == 5_000, "Must not overwrite first_book_ts_ms"


# ════════════════════════════════════════════════════════════════════════════
# Fix 2: Stale-book invalidation
# ════════════════════════════════════════════════════════════════════════════

class TestStaleBookInvalidation:

    def test_live_book_uses_l2_spread(self):
        """book_ts_gap_ms well below threshold → last_spread_bps_l2 is used."""
        rt     = _runtime(last_spread_bps_l2=3.5, first_book_ts_ms=1_000)
        ind    = _indicators(book_ts_gap_ms=500, data_health=1.0)
        cfg    = _cfg2()
        _resolve_spread_strategy(ind, rt, cfg, tick_ts=1_600)
        assert ind["spread_bps"] == pytest.approx(3.5)
        assert ind.get("spread_bps_stale_book") is None

    def test_stale_book_skips_l2_spread(self):
        """book_ts_gap_ms > SPREAD_STALE_MS → last_spread_bps_l2 is skipped."""
        rt  = _runtime(last_spread_bps_l2=3.5, first_book_ts_ms=1_000)
        ind = _indicators(book_ts_gap_ms=35_000, data_health=1.0)
        cfg = _cfg2()
        _resolve_spread_strategy(ind, rt, cfg, tick_ts=40_000)
        # l2 must be skipped; no other fallback → spread_bps_missing
        assert ind.get("spread_bps_stale_book") == 1
        assert ind.get("spread_bps_missing") == 1
        assert ind["spread_bps"] == pytest.approx(15.0)   # missing-default

    def test_stale_book_degrades_data_health(self):
        """With stale book → data_health must be penalised (not cold-start)."""
        rt  = _runtime(last_spread_bps_l2=3.5, first_book_ts_ms=1_000)
        ind = _indicators(book_ts_gap_ms=35_000, data_health=1.0)
        cfg = _cfg2()
        _resolve_spread_strategy(ind, rt, cfg, tick_ts=40_000)
        assert ind["data_health"] == pytest.approx(DATA_HEALTH_ON_SPREAD_MISSING)
        assert "spread_missing" in ind.get("data_health_reasons", "")


# ════════════════════════════════════════════════════════════════════════════
# Fix 3: Cold-start grace period
# ════════════════════════════════════════════════════════════════════════════

class TestColdStartGracePeriod:

    def test_cold_start_no_data_health_penalty(self):
        """Within cold-start window (book never seen) → data_health must NOT be degraded."""
        rt  = _runtime(last_spread_bps_l2=0.0, first_book_ts_ms=0)  # no book yet
        now = 5_000
        ind = _indicators(book_ts_gap_ms=SENTINEL_NEVER_SEEN, data_health=1.0)
        cfg = _cfg2()
        _resolve_spread_strategy(ind, rt, cfg, tick_ts=now)
        # data_health must remain 1.0 during cold start
        assert ind["data_health"] == pytest.approx(1.0), "data_health must NOT be penalised during cold-start"
        assert "spread_cold_start" in ind.get("data_health_reasons", "")
        assert ind.get("spread_bps_cold_start") == 1
        assert ind.get("book_health_ok") is None or int(ind.get("book_health_ok", 1)) == 1

    def test_cold_start_expired_applies_penalty(self):
        """After cold-start window expires (first_book_ts_ms + cold_start_ms ago) → penalty applied."""
        first_ts = 0   # Worker started but first book arrived long ago
        now_ts   = 50_000  # 50s since first book — way beyond 10s grace
        rt  = _runtime(last_spread_bps_l2=0.0, first_book_ts_ms=first_ts)
        ind = _indicators(book_ts_gap_ms=SENTINEL_NEVER_SEEN, data_health=1.0)
        cfg = _cfg2()
        # first_book_ts_ms=0 means never seen, so _in_cold_start true only if delta < cold_start_ms
        # When first_book_ts_ms == 0 (book never seen): _in_cold_start = True by design (first_book_ts <= 0)
        # To simulate "cold-start expired": set first_book_ts_ms to a past time, but keep book_ts_gap sentinel
        rt.first_book_ts_ms = now_ts - 20_000  # set 20s ago → beyond 10s window
        _resolve_spread_strategy(ind, rt, cfg, tick_ts=now_ts)
        # Now _in_cold_start should be False → penalty applied
        assert ind["data_health"] == pytest.approx(DATA_HEALTH_ON_SPREAD_MISSING)
        assert "spread_missing" in ind.get("data_health_reasons", "")

    def test_normal_missing_book_one_hour_after_start_degraded(self):
        """If worker has been running 1h and book suddenly vanishes → full penalty applies."""
        first_ts = 1_000
        now_ts   = 3_601_000   # 1 hour later
        rt  = _runtime(last_spread_bps_l2=0.0, first_book_ts_ms=first_ts)
        # book_ts_gap huge (>30s stale) but book was seen; now it's gone for SENTINEL ms
        ind = _indicators(book_ts_gap_ms=SENTINEL_NEVER_SEEN, data_health=1.0)
        cfg = _cfg2()
        _resolve_spread_strategy(ind, rt, cfg, tick_ts=now_ts)
        # _in_cold_start = False (first_book_ts set and delta >> cold_start_ms)
        assert ind["data_health"] == pytest.approx(DATA_HEALTH_ON_SPREAD_MISSING)
