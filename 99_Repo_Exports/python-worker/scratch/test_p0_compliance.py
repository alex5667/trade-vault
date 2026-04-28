from common.contracts.registry import SignalV1
from common.normalization import normalize_side_3, generate_signal_id
from common.enums.trading import Direction, Side

def test_p0_compliance():
    # 1. Side Normalization
    s1 = normalize_side_3("LONG")
    assert s1.direction == Direction.LONG
    assert s1.side == Side.BUY
    assert s1.side_int == 1

    s2 = normalize_side_3("sell")
    assert s2.direction == Direction.SHORT
    assert s2.side == Side.SELL
    assert s2.side_int == -1

    # 2. Signal ID Generation
    sid = generate_signal_id(kind="iceberg", symbol="BTCUSDT", ts_ms=1714238000000, direction=Direction.LONG)
    assert sid == "iceberg:BTCUSDT:1714238000000:L"
    
    # 3. SignalV1 Validation
    sig = SignalV1(
        signal_id=sid,
        symbol="BTCUSDT",
        ts_event_ms=1714238000000,
        ts_publish_ms=1714238000100,
        direction=Direction.LONG,
        side=Side.BUY,
        side_int=1,
        entry_price=64000.0,
        sl_price=63500.0,
        tp_levels=[65000.0],
        ok=1,
        reason="spike"
    )
    dump = sig.model_dump()
    assert dump["signal_id"] == sid
    assert dump["side_int"] == 1
    assert dump["direction"] == "LONG"
    
    print("✅ P0 Compliance Test Passed!")

if __name__ == "__main__":
    test_p0_compliance()
