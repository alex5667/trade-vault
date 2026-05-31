

from core.atr_sanity_guard import (
    AtrCandidate,
    RangeTfAggregator,
    pick_best_atr,
)
from utils.time_utils import get_ny_time_millis


def _feed_range_1m(agg: RangeTfAggregator, *, start_ts_ms: int, n_micro: int = 120, px: float = 100.0) -> None:
    """
    Feed 1s microbars to create at least 2 buckets of 1m range stats.
    """
    ts = int(start_ts_ms)
    for i in range(n_micro):
        # synthetic: small oscillation
        o = px
        h = px * (1.0005 + (i % 3) * 0.0001)
        l = px * (0.9995 - (i % 3) * 0.0001)
        c = px * (1.0000 + ((i % 5) - 2) * 0.00005)
        agg.push_microbar(end_ts_ms=ts + (i + 1) * 1000, o=o, h=h, l=l, c=c)


def test_pick_best_atr_prefers_sane_and_fresh():
    now = get_ny_time_millis()
    agg = RangeTfAggregator(tf_ms=60_000, min_samples=10)
    # Feed enough data (1000s > 16m) to satisfy min_samples=10
    _feed_range_1m(agg, start_ts_ms=now - 2_000_000, n_micro=1000, px=100.0)
    assert agg.is_ready()

    entry = 100.0
    # Range bps should be around ~10 bps order (synthetic), so sane ATR_bps roughly in that band.
    # Candidate A: very fresh, sane
    c1 = AtrCandidate(atr=0.12, key="ta:last:atr:SYM", src="ta_last", tf="M1", ts_ms=now - 10_000, age_ms=10_000)  # atr_bps=12
    # Candidate B: fresh but too low (atr_bps=0.5)
    c2 = AtrCandidate(atr=0.005, key="atr:SYM:1m", src="atr_str", tf="1m", ts_ms=0, age_ms=0)
    pick = pick_best_atr(
        candidates=[c2, c1],
        entry_px=entry,
        now_ms=now,
        range_agg=agg,
        max_age_ms=180_000,
        min_mult=0.6,
        max_mult=3.0,
    )
    assert pick.atr == c1.atr
    assert pick.sane == 1


def test_pick_best_atr_fail_open_returns_freshest_when_none_sane():
    now = get_ny_time_millis()
    agg = RangeTfAggregator(tf_ms=60_000, min_samples=10)
    # Feed enough data
    _feed_range_1m(agg, start_ts_ms=now - 2_000_000, n_micro=1000, px=100.0)
    assert agg.is_ready()

    entry = 100.0
    # both too low vs range
    c1 = AtrCandidate(atr=0.001, key="k1", src="s1", tf="1m", ts_ms=now - 5_000, age_ms=5_000)
    c2 = AtrCandidate(atr=0.002, key="k2", src="s2", tf="1m", ts_ms=now - 10_000, age_ms=10_000)
    pick = pick_best_atr(
        candidates=[c2, c1],
        entry_px=entry,
        now_ms=now,
        range_agg=agg,
        max_age_ms=180_000,
        min_mult=0.6,
        max_mult=3.0,
    )
    # fail-open -> freshest by age_ms
    assert pick.key == "k1"
    assert pick.sane == 0


def test_pick_best_atr_no_range_ready_accepts_fresh():
    now = get_ny_time_millis()
    agg = RangeTfAggregator(tf_ms=60_000, min_samples=9999)  # never ready
    entry = 100.0
    c1 = AtrCandidate(atr=0.05, key="k1", src="s1", tf="1m", ts_ms=now - 2_000, age_ms=2_000)
    pick = pick_best_atr(
        candidates=[c1],
        entry_px=entry,
        now_ms=now,
        range_agg=agg,
        max_age_ms=180_000,
        min_mult=0.6,
        max_mult=3.0,
    )
    assert pick.sane == 1
    assert pick.reason == "no_range_ref_ready"
