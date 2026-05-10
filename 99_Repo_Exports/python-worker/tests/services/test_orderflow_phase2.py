from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from services.orderflow.runtime import SymbolRuntime
from services.orderflow.signal_pipeline import SignalPipeline
from services.orderflow.utils import _parse_book_payload, _parse_tick_payload


def test_robust_tick_timestamp_parsing():
    """Verify that multiple timestamp keys are supported in _parse_tick_payload."""
    # Test ts_ms
    tick = _parse_tick_payload({"ts_ms": 1700000000000, "price": 100, "qty": 1, "side": "BUY"})
    assert tick["ts_ms"] == 1700000000000

    # Test E (Binance event time)
    tick = _parse_tick_payload({"E": 1700000000001, "price": 100, "qty": 1, "side": "BUY"})
    assert tick["ts_ms"] == 1700000000001

    # Test T (Binance trade time)
    tick = _parse_tick_payload({"T": 1700000000002, "price": 100, "qty": 1, "side": "BUY"})
    assert tick["ts_ms"] == 1700000000002

    # Test written_at
    tick = _parse_tick_payload({"written_at": 1700000000003, "price": 100, "qty": 1, "side": "BUY"})
    assert tick["ts_ms"] == 1700000000003

def test_robust_book_timestamp_parsing():
    """Verify that multiple timestamp keys are supported in _parse_book_payload."""
    book = _parse_book_payload({"ts_ms": 1700000000000, "bids": [], "asks": []}, "BTCUSDT")
    assert book["ts_ms"] == 1700000000000

    book = _parse_book_payload({"E": 1700000000001, "bids": [], "asks": []}, "BTCUSDT")
    assert book["ts_ms"] == 1700000000001

@pytest.mark.asyncio
async def test_publish_signal_bookkeeping_deferred():
    """Verify that bookkeeping is NOT updated if a veto occurs (direction check)."""
    # Note: Bookkeeping is now at the end. Direction check is at the top.
    publisher = MagicMock()
    atr_cache = MagicMock()
    pipeline = SignalPipeline(publisher, atr_cache)

    runtime = MagicMock(spec=SymbolRuntime)
    runtime.symbol = "BTCUSDT"
    runtime.last_signal_ts = 0
    runtime.pressure = MagicMock()

    # Invalid direction should trigger early return before any bookkeeping
    signal = {"direction": "INVALID", "tick_ts": 1700000000000}

    await pipeline.publish_signal(runtime, signal)

    # Check that bookkeeping didn't happen
    assert runtime.last_signal_ts == 0
    runtime.pressure.record_emit.assert_not_called()

@pytest.mark.asyncio
async def test_publish_signal_bookkeeping_happens_at_end():
    """Verify that bookkeeping is updated after successful logic (mocking publishing)."""
    publisher = MagicMock()
    publisher.r = AsyncMock()
    publisher.xadd_json = AsyncMock() # Fix: must be AsyncMock
    atr_cache = MagicMock()
    pipeline = SignalPipeline(publisher, atr_cache)

    runtime = MagicMock(spec=SymbolRuntime)
    runtime.symbol = "BTCUSDT"
    runtime.last_signal_ts = 0
    runtime.config = {"tp_ratio": "0.5,0.3,0.2", "require_strong_confirmation": False}
    runtime.dynamic_cfg = {}
    runtime.pressure = MagicMock()
    runtime.get_atr_tf_selected.return_value = "15m"

    # Use real dict for indicators to avoid JSON serialization errors
    signal = {
        "direction": "LONG",
        "entry": 100,
        "tick_ts": 1700000000000,
        "signal_id": "test_sig",
        "confidence": 0.8,
        "indicators": {"of_confirm_ok": 1, "delta_z": 3.0} # Real dict
    }

    # Mocking external calls in SignalPipeline
    with patch("services.orderflow.signal_pipeline.preprocess_signal_for_publish"), \
         patch("services.orderflow.signal_pipeline.calculate_position_size", return_value=(1.0, 100.0, 1000.0, 1.0)), \
         patch("services.orderflow.signal_pipeline.CryptoSignal"), \
         patch("services.orderflow.signal_pipeline.CryptoSignalFormatter"), \
         patch("services.orderflow.signal_pipeline.build_outbox_envelope", return_value={"meta":{}, "targets":{}}), \
         patch("services.orderflow.signal_pipeline.atomic_xadd_async"), \
         patch.object(pipeline, "_calculate_levels", return_value=(90.0, [110, 120, 130], 1.0, 5.0)):

        await pipeline.publish_signal(runtime, signal)

        # Verify bookkeeping happened
        assert runtime.last_signal_ts == 1700000000000
        runtime.pressure.record_emit.assert_called_with(1700000000000)

@pytest.mark.asyncio
async def test_send_telegram_report_success():
    """Verify that send_telegram_report correctly sends to notify stream."""
    publisher = MagicMock()
    publisher.r = AsyncMock()
    publisher.r.xadd = AsyncMock()
    atr_cache = MagicMock()
    pipeline = SignalPipeline(publisher, atr_cache)

    # Test parameters
    text = "Test report message"
    source = "test_source"
    symbol = "BTCUSDT"

    # Mock time.time() to return predictable value
    with patch("services.orderflow.signal_pipeline.time.time", return_value=1700000000.123):
        await pipeline.send_telegram_report(text, source, symbol)

    # Verify xadd was called with correct parameters
    publisher.r.xadd.assert_called_once_with(
        pipeline.notify_stream,
        fields={
            "type": "report",
            "text": text,
            "source": source,
            "symbol": symbol,
            "ts_ms": "1700000000123"  # get_ny_time_millis() as string
        },
        maxlen=pipeline.notify_maxlen,
        approximate=True,
    )

@pytest.mark.asyncio
async def test_send_telegram_report_failure():
    """Verify that send_telegram_report handles exceptions gracefully."""
    publisher = MagicMock()
    publisher.r = AsyncMock()
    publisher.r.xadd = AsyncMock(side_effect=Exception("Redis connection failed"))
    atr_cache = MagicMock()
    pipeline = SignalPipeline(publisher, atr_cache)

    # Test parameters
    text = "Test report message"
    source = "test_source"
    symbol = "BTCUSDT"

    # Should not raise exception despite Redis failure
    with patch("services.orderflow.signal_pipeline.time.time", return_value=1700000000.123):
        await pipeline.send_telegram_report(text, source, symbol)

    # Verify xadd was still called
    publisher.r.xadd.assert_called_once()
