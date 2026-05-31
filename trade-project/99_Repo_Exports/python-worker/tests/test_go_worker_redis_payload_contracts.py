import json
from collections import defaultdict

from services.candles_archiver import parse_candle
from services.crypto_htf_aggregator import HTFAggregator
from services.liquidation_map_core import normalize_liq_event


def _candle_stream_fields(payload: dict) -> dict[bytes, bytes]:
    return {
        b"symbol": b"BTCUSDT",
        b"tf": b"1d",
        b"ts": b"1700000000000",
        b"payload": json.dumps(payload).encode("utf-8"),
    }


def test_liquidation_accepts_go_worker_legacy_alias_payload() -> None:
    ev, reason = normalize_liq_event(
        {
            "src": "binance_usdm",
            "symbol": "BTCUSDT",
            "ts_ms": 1700000000000,
            "recv_ts_ms": 1700000000100,
            "price": "50000",
            "qty": "0.01",
            "notional_usd": "500",
            "liq_side": "long",
            "raw_side": "SELL",
            "schema_version": "1",
            "event_time_ms": 1700000000000,
            "ingest_time_ms": 1700000000100,
        }
    )

    assert reason is None
    assert ev is not None
    assert ev.venue == "binance_usdtm"
    assert ev.ts_event_ms == 1700000000000
    assert ev.ts_ingest_ms == 1700000000100
    assert ev.order_side == "SELL"


def test_liquidation_accepts_canonical_alias_payload() -> None:
    ev, reason = normalize_liq_event(
        {
            "venue": "bybit_linear",
            "symbol": "ETHUSDT",
            "ts_event_ms": 1700000000001,
            "ts_ingest_ms": 1700000000101,
            "price": "3500",
            "qty": "2",
            "notional_usd": "7000",
            "liq_side": "short",
            "order_side": "Sell",
            "schema_version": "1",
        }
    )

    assert reason is None
    assert ev is not None
    assert ev.venue == "bybit_linear"
    assert ev.order_side == "SELL"


def test_candles_archiver_accepts_go_rest_short_payload_without_taker_fields() -> None:
    candle = parse_candle(
        _candle_stream_fields(
            {
                "t": 1699913600000,
                "T": 1700000000000,
                "o": "1",
                "h": "2",
                "l": "0.5",
                "c": "1.5",
                "v": "10",
                "q": "15",
                "n": 3,
                "x": True,
            }
        )
    )

    assert candle is not None
    assert candle["symbol"] == "BTCUSDT"
    assert candle["timeframe"] == "1d"
    assert candle["open"] == 1.0
    assert candle["high"] == 2.0
    assert candle["low"] == 0.5
    assert candle["close"] == 1.5
    assert candle["taker_buy_base"] == 0.0
    assert candle["taker_buy_quote"] == 0.0


def test_candles_archiver_accepts_go_rest_full_name_payload() -> None:
    candle = parse_candle(
        _candle_stream_fields(
            {
                "openTime": 1699913600000,
                "closeTime": 1700000000000,
                "open": "1",
                "high": "2",
                "low": "0.5",
                "close": "1.5",
                "volume": "10",
                "quoteVolume": "15",
                "numberOfTrades": 3,
                "takerBuyVolume": "4",
                "takerBuyQuoteVolume": "6",
            }
        )
    )

    assert candle is not None
    assert candle["trades"] == 3
    assert candle["taker_buy_base"] == 4.0
    assert candle["taker_buy_quote"] == 6.0


def test_htf_aggregator_accepts_go_rest_short_payload() -> None:
    agg = object.__new__(HTFAggregator)
    agg.history = defaultdict(lambda: defaultdict(list))

    agg._process_candle(
        {
            "symbol": "BTCUSDT",
            "tf": "1d",
            "ts": "1700000000000",
            "payload": json.dumps(
                {
                    "t": 1699913600000,
                    "T": 1700000000000,
                    "o": "1",
                    "h": "2",
                    "l": "0.5",
                    "c": "1.5",
                    "v": "10",
                    "q": "15",
                    "n": 3,
                    "x": True,
                }
            ),
        }
    )

    bar = agg.history["BTCUSDT"]["1d"][-1]
    assert bar == {
        "timestamp": 1700000000000,
        "open": 1.0,
        "high": 2.0,
        "low": 0.5,
        "close": 1.5,
        "volume": 10.0,
    }
