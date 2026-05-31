from core.delta_volume_fallback import signed_qty_from_tick, volume_delta_z_from_tick


class DummyRuntime:
    pass


def test_signed_qty_from_tick_side_buy_sell():
    assert signed_qty_from_tick({"side": "buy", "qty": 2.5}) == 2.5
    assert signed_qty_from_tick({"side": "sell", "qty": 2.5}) == -2.5


def test_signed_qty_from_tick_is_buyer_maker_binance():
    # isBuyerMaker=True => aggressor SELL => negative
    assert signed_qty_from_tick({"m": True, "q": 3.0}) == -3.0
    assert signed_qty_from_tick({"is_buyer_maker": True, "qty": 3.0}) == -3.0
    assert signed_qty_from_tick({"m": False, "q": 3.0}) == 3.0


def test_volume_delta_z_state_is_deterministic():
    rt = DummyRuntime()
    z1, d1 = volume_delta_z_from_tick(rt, {"side": "buy", "qty": 1.0}, window=10)
    z2, d2 = volume_delta_z_from_tick(rt, {"side": "buy", "qty": 1.0}, window=10)
    assert d1 == 1.0 and d2 == 1.0
    # With identical stream, z should be finite and stable (often 0 due to MAD=0 at start)
    assert abs(float(z1)) <= 20.0
    assert abs(float(z2)) <= 20.0

