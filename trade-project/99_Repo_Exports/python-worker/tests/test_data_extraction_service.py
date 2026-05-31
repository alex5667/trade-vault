from dataclasses import dataclass

from handlers.data_extraction_service import DataExtractionService


@dataclass
class FakeTick:
    ts: int = 0
    volume: float = 1.0
    flags: int = 0
    is_buyer_maker: object = None


def test_delta_sign_binance_semantics():
    s = DataExtractionService("BTCUSDT")

    # buyer is maker -> taker sell -> delta negative
    t = FakeTick(volume=2.5, is_buyer_maker=True)
    assert s._classify_delta(t) == -2.5
    assert s._taker_side(t) == -1

    # buyer is taker -> delta positive
    t = FakeTick(volume=3.0, is_buyer_maker=False)
    assert s._classify_delta(t) == 3.0
    assert s._taker_side(t) == 1


def test_delta_unknown_returns_zero():
    s = DataExtractionService("BTCUSDT")
    t = FakeTick(volume=5.0, is_buyer_maker=None)
    assert s._classify_delta(t) == 0.0
    assert s._taker_side(t) == 0


def test_is_trade_tick_safe():
    s = DataExtractionService("BTCUSDT")
    assert s._is_trade_tick(FakeTick(flags=1, volume=0.0)) is True
    assert s._is_trade_tick(FakeTick(flags=0, volume=1.0)) is True
    assert s._is_trade_tick(FakeTick(flags=0, volume=0.0)) is False
