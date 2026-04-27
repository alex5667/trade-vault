from dataclasses import dataclass

from handlers.data_processor import OrderFlowDataProcessor


@dataclass
class FakeTick:
    ts: int = 0
    volume: float = 1.0
    is_buyer_maker: object = None
    last: float = 100.0


@dataclass
class FakeCfg:
    delta_window_ticks = 100
    delta_bucket_ms = 1000
    l2_stale_ms = 2000
    l2_skew_tick_thr_ms = 5000


def test_data_processor_delta_sign():
    dp = OrderFlowDataProcessor("BTCUSDT", specs=None, config=FakeCfg())

    t = FakeTick(ts=1, volume=2.5, is_buyer_maker=True)
    assert dp._classify_delta(t) == -2.5

    t = FakeTick(ts=2, volume=3.0, is_buyer_maker=False)
    assert dp._classify_delta(t) == 3.0

    t = FakeTick(ts=3, volume=5.0, is_buyer_maker=None)
    assert dp._classify_delta(t) == 0.0
