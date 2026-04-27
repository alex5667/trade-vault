from __future__ import annotations

from common.zone_store import Zone, ZonePack


def test_nearest_inside_band_dist0():
    zp = ZonePack(
        v=1,
        symbol="BTCUSDT",
        ts_ms=1,
        zones=[Zone(id="FVG1", type="FVG", src="daily", side="SUP", px_lo=99.0, px_hi=101.0, ts_ms=1, weight=1.0)],
    )
    z, d, inside = zp.nearest(100.0)
    assert z is not None
    assert inside is True
    assert d == 0.0


def test_nearest_outside_band_bp_positive():
    zp = ZonePack(
        v=1,
        symbol="BTCUSDT",
        ts_ms=1,
        zones=[Zone(id="L1", type="LEVEL", src="weekly", side="RES", px_lo=110.0, px_hi=110.0, ts_ms=1, weight=1.0)],
    )
    z, d, inside = zp.nearest(100.0)
    assert z is not None
    assert inside is False
    assert d > 0.0


def test_tiebreak_by_weight():
    zp = ZonePack(
        v=1,
        symbol="BTCUSDT",
        ts_ms=1,
        zones=[
            Zone(id="A", type="LEVEL", src="daily", side="RES", px_lo=101.0, px_hi=101.0, ts_ms=1, weight=0.1),
            Zone(id="B", type="LEVEL", src="weekly", side="RES", px_lo=101.0, px_hi=101.0, ts_ms=1, weight=1.0),
        ],
    )
    z, d, inside = zp.nearest(100.0)
    assert z is not None
    assert z.id == "B"
