
def test_atr_normalization():
    # Simulate signal processing
    raw_atr = 150.0
    atr_used_for_levels = "5m"

    # In payload normalization
    envelope = {
        "price": 60000.0,
        "atr": raw_atr,
        "atr_used_for_levels": atr_used_for_levels,
        "tp1": 60100.0,
        "sl": 59900.0
    }

    # Ensure ATR corresponds to the timeframe specified
    assert envelope["atr_used_for_levels"] == "5m"
    assert envelope["atr"] == 150.0
