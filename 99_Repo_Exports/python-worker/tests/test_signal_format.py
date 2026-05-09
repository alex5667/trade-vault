from core.crypto_signal_formatter import CryptoSignal, CryptoSignalFormatter


def test_crypto_signal_formatter_with_atr_bps():
    signal = CryptoSignal(
        sid="test-id",
        symbol="BTCUSDT",
        side="LONG",
        entry=50000.0,
        sl=49500.0,
        tp_levels=[51000.0, 52000.0],
        lot=0.1,
        atr=500.0,
        confidence=0.85,
        ts=1700000000000,
        source="CryptoOrderFlow",
        indicators={
            "atr_bps": 100.5,
            "atr_bps_th": 80.0,
            "atr_floor_tier": 1,
            "atr_floor_rg": "range"
        }
    )

    msg = CryptoSignalFormatter.format_telegram_message(signal)

    # Check if new ATR info is present
    assert "ATR=500.00" in msg
    assert "(100.5 bps)" in msg
    assert "Th=80.0" in msg
    assert "(T1, range)" in msg
    assert "Conf=85%" in msg

def test_crypto_signal_formatter_fallback_rg():
    signal = CryptoSignal(
        sid="test-id",
        symbol="BTCUSDT",
        side="LONG",
        entry=50000.0,
        sl=49500.0,
        tp_levels=[51000.0],
        lot=0.1,
        atr=500.0,
        confidence=0.85,
        ts=1700000000000,
        source="CryptoOrderFlow",
        indicators={
            "atr_bps": 100.5,
            "atr_bps_th": 80.0,
            "atr_floor_tier": 0,
            "atr_gate_rg": "trend" # Testing fallback
        }
    )

    msg = CryptoSignalFormatter.format_telegram_message(signal)
    assert "(T0, trend)" in msg

def test_crypto_signal_formatter_missing_bps():
    signal = CryptoSignal(
        sid="test-id",
        symbol="BTCUSDT",
        side="LONG",
        entry=50000.0,
        sl=49500.0,
        tp_levels=[51000.0],
        lot=0.1,
        atr=500.0,
        confidence=0.85,
        ts=1700000000000,
        source="CryptoOrderFlow",
        indicators={}
    )

    msg = CryptoSignalFormatter.format_telegram_message(signal)
    # Should fallback to basic ATR line
    assert "📊 ATR=500.00 | Conf=85%" in msg
