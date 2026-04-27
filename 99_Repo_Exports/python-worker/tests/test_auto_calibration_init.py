from services.auto_calibration_service import init_auto_calibration, get_auto_calibration_service


def test_init_auto_calibration_filters_and_applies_threshold():
    init_auto_calibration(trades_threshold=200, enabled_symbols=["ETHUSDT"], source="CryptoOrderFlow")
    svc = get_auto_calibration_service()
    assert svc is not None
    assert any(c.symbol == "ETHUSDT" for c in svc._symbols)
    assert all(c.source == "CryptoOrderFlow" for c in svc._symbols)
    assert all(c.min_total_trades >= 200 for c in svc._symbols)