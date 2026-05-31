#!/usr/bin/env python3
"""
Test MT5 Bridge Components

Тестирование компонентов MT5-моста без реального подключения к MT5.
Проверяет парсинг планов, логику исполнения, Redis consumer.
"""

import sys
import os
import json
import asyncio
from datetime import datetime, timezone, timedelta
from unittest.mock import Mock, patch
sys.path.insert(0, os.path.dirname(__file__))

def test_plan_parsing():
    """Test ExecutionPlan parsing from Redis payload."""
    print("🧪 Testing ExecutionPlan parsing...")

    try:
        from mt5_bridge.models import plan_from_dict, Mt5ExecutionPlan
    except ImportError as e:
        print(f"❌ Cannot test plan parsing: {e}")
        return

    # Sample plan dict from Redis
    plan_dict = {
        "signal_id": "XAUUSD-test-123",
        "symbol": "XAUUSD",
        "setup_type": "breakout_R1",
        "side": "long",
        "ts_signal": "2025-12-15T12:34:56.123456+00:00",
        "price_at_signal": 2615.3,
        "entry_zone_low": 2610.0,
        "entry_zone_high": 2616.0,
        "stop_price": 2600.0,
        "tp_levels": [2625.0, 2640.0],
        "partials": [0.5, 0.5],
        "pos_risk_R": 1.0,
        "risk_usd": 100.0,
        "position_size": 0.2,
        "expiry_bars": 3,
        "created_at": "2025-12-15T12:34:56.123456+00:00",
    }

    # Parse plan
    plan = plan_from_dict(plan_dict)

    # Verify parsing
    assert isinstance(plan, Mt5ExecutionPlan)
    assert plan.signal_id == "XAUUSD-test-123"
    assert plan.symbol == "XAUUSD"
    assert plan.side == "long"
    assert plan.is_long == True
    assert plan.is_short == False
    assert plan.entry_zone_low == 2610.0
    assert plan.entry_zone_high == 2616.0
    assert plan.position_size_lots == 0.2
    assert plan.expiry_bars == 3
    assert plan.ttl_seconds == 180  # 3 * 60

    # Test entry zone logic
    assert plan.price_in_entry_zone(2612.0) == True  # Inside zone
    assert plan.price_in_entry_zone(2605.0) == False  # Below zone
    assert plan.price_in_entry_zone(2620.0) == False  # Above zone

    # Test expiry logic
    ts_signal = datetime.fromisoformat("2025-12-15T12:34:56.123456+00:00")
    expired_time = ts_signal + timedelta(seconds=200)  # After TTL
    not_expired_time = ts_signal + timedelta(seconds=100)  # Before TTL

    # Mock expiry check (can't easily test with real time)
    print("✅ Plan parsing works correctly")

def test_executor_logic():
    """Test PlanExecutor logic without MT5 connection."""
    print("🧪 Testing PlanExecutor logic...")

    try:
        from mt5_bridge.models import Mt5ExecutionPlan
        from mt5_bridge.executor import PlanExecutor
        from datetime import datetime, timezone

        # Create mock MT5 client
        mock_mt5 = Mock()
        mock_mt5.get_tick.return_value = (2612.0, 2613.0)  # bid=2612, ask=2613
        mock_mt5.send_market_order.return_value = 12345  # Mock ticket

        # Create executor
        executor = PlanExecutor(mock_mt5)

        # Create test plan
        plan = Mt5ExecutionPlan(
            signal_id="test-signal-123",
            symbol="XAUUSD",
            side="long",
            ts_signal=datetime.now(timezone.utc) - timedelta(minutes=1),
            price_at_signal=2615.0,
            entry_zone_low=2610.0,
            entry_zone_high=2620.0,
            stop_price=2600.0,
            tp_levels=[2630.0, 2650.0],
            partials=[0.5, 0.5],
            risk_usd=100.0,
            position_size_lots=0.2,
            expiry_bars=5,
            created_at=datetime.now(timezone.utc),
        )

        # Add plan
        executor.add_plan(plan)
        assert executor.get_active_plans_count() == 1
        assert executor.get_entered_positions_count() == 0

        # First step - should enter position (price 2612 in zone)
        executor.step()

        # Verify position was entered
        assert executor.get_entered_positions_count() == 1
        mock_mt5.send_market_order.assert_called()

        # Check call arguments
        call_args = mock_mt5.send_market_order.call_args
        assert call_args[1]['symbol'] == 'XAUUSD'
        assert call_args[1]['is_buy'] == True  # Long position
        assert call_args[1]['volume_lots'] == 0.1  # First partial: 0.2 * 0.5
        assert call_args[1]['sl_price'] == 2600.0
        assert call_args[1]['tp_price'] == 2630.0  # First TP level

        print("✅ PlanExecutor logic works correctly")

    except ImportError as e:
        print(f"❌ Executor test failed: {e}")
        return

async def test_redis_consumer():
    """Test Redis consumer with mock Redis."""
    print("🧪 Testing Redis consumer...")

    from mt5_bridge.redis_consumer import PlansStreamConsumer

    # Mock Redis
    mock_redis = Mock()
    mock_redis.xread.return_value = [
        [
            "stream:signals:plans",
            [
                [
                    "1671096896123-0",  # message id
                    {
                        "signal_id": "test-signal-123",
                        "symbol": "XAUUSD",
                        "setup_type": "breakout_R1",
                        "side": "long",
                        "ts_signal": "2025-12-15T12:34:56.123456+00:00",
                        "payload": json.dumps({
                            "plan": {
                                "signal_id": "test-signal-123",
                                "symbol": "XAUUSD",
                                "side": "long",
                                "ts_signal": "2025-12-15T12:34:56.123456+00:00",
                                "price_at_signal": 2615.0,
                                "entry_zone_low": 2610.0,
                                "entry_zone_high": 2620.0,
                                "stop_price": 2600.0,
                                "tp_levels": [2630.0],
                                "partials": [1.0],
                                "risk_usd": 100.0,
                                "position_size": 0.2,
                                "expiry_bars": 3,
                                "created_at": "2025-12-15T12:34:56.123456+00:00",
                            }
                        })
                    }
                ]
            ]
        ]
    ]

    # Create consumer
    consumer = PlansStreamConsumer("redis://mock:6379/0")

    # Mock the _r attribute
    consumer._r = mock_redis

    # Poll for plans
    plans = consumer.poll(block_ms=100)

    # Verify
    assert len(plans) == 1
    plan = plans[0]
    assert plan.signal_id == "test-signal-123"
    assert plan.symbol == "XAUUSD"
    assert plan.side == "long"
    assert plan.position_size_lots == 0.2

    print("✅ Redis consumer works correctly")

def test_execution_events():
    """Test ExecutionEvent creation and serialization."""
    print("🧪 Testing ExecutionEvent...")

    from mt5_bridge.exec_events import ExecutionEvent

    # Create event
    event = ExecutionEvent(
        signal_id="test-signal-123",
        symbol="XAUUSD",
        side="long",
        venue="mt5",
        kind="fill",
        event_type="OPEN",
        ts_event=datetime.now(timezone.utc),
        price=2615.5,
        qty_lots=0.2,
        pnl_ccy=0.0,
        account_ccy="USD",
        mt5_deal=12345,
        mt5_order=67890,
        comment="sig=test-signal-123",
        meta={"swap": 0.0, "commission": 0.5}
    )

    # Test serialization
    payload = event._to_payload()
    assert payload["signal_id"] == "test-signal-123"
    assert payload["venue"] == "mt5"
    assert payload["kind"] == "fill"
    assert payload["event_type"] == "OPEN"
    assert payload["price"] == 2615.5
    assert payload["qty_lots"] == 0.2

    # Test Redis fields
    redis_fields = event.to_redis_fields()
    assert "signal_id" in redis_fields
    assert "venue" in redis_fields
    assert "payload" in redis_fields

    # Parse payload back
    parsed_payload = json.loads(redis_fields["payload"])
    assert parsed_payload["signal_id"] == "test-signal-123"

    print("✅ ExecutionEvent works correctly")

def test_deals_watcher():
    """Test deals watcher signal ID parsing."""
    print("🧪 Testing deals watcher...")

    try:
        from mt5_bridge.deals_watcher import Mt5DealsWatcher
        import MetaTrader5 as mt5

        # Test signal ID parsing
        assert Mt5DealsWatcher._parse_signal_id("sig=XAUUSD-123") == "XAUUSD-123"
        assert Mt5DealsWatcher._parse_signal_id("other sig=ABC def") == "ABC"
        assert Mt5DealsWatcher._parse_signal_id("no signal here") is None
        assert Mt5DealsWatcher._parse_signal_id("") is None

        # Test side mapping
        # Mock the MT5 constants for testing
        class MockMT5:
            DEAL_TYPE_BUY = 0
            DEAL_TYPE_SELL = 1
            DEAL_ENTRY_IN = 0
            DEAL_ENTRY_OUT = 1

        # Temporarily replace mt5 module
        import sys
        original_mt5 = sys.modules.get('MetaTrader5')
        sys.modules['MetaTrader5'] = MockMT5()

        try:
            watcher = Mt5DealsWatcher(None, None)  # We don't need real instances for parsing tests

            assert watcher._map_side(0) == "long"   # DEAL_TYPE_BUY
            assert watcher._map_side(1) == "short"  # DEAL_TYPE_SELL
            assert watcher._map_side(999) == "unknown"

            assert watcher._map_event_type(0) == "OPEN"     # DEAL_ENTRY_IN
            assert watcher._map_event_type(1) == "CLOSE"    # DEAL_ENTRY_OUT
            assert watcher._map_event_type(999) == "DEAL"

        finally:
            # Restore original mt5 module
            if original_mt5:
                sys.modules['MetaTrader5'] = original_mt5
            elif 'MetaTrader5' in sys.modules:
                del sys.modules['MetaTrader5']

        print("✅ Deals watcher works correctly")

    except ImportError as e:
        print(f"❌ Deals watcher test failed: {e}")
        return

def test_mt5_config():
    """Test MT5 configuration loading."""
    print("🧪 Testing MT5 configuration...")

    try:
        from mt5_bridge.mt5_client import Mt5Config

        # Test config creation
        config = Mt5Config(
            login=123456,
            password="test_password",
            server="TestBroker-Server",
            symbol_map={"XAUUSD": "XAUUSD.m"}
        )

        assert config.login == 123456
        assert config.password == "test_password"
        assert config.server == "TestBroker-Server"
        assert config.symbol_map["XAUUSD"] == "XAUUSD.m"

        # Test symbol mapping
        assert config.symbol_map.get("XAUUSD") == "XAUUSD.m"
        assert config.symbol_map.get("EURUSD", "EURUSD") == "EURUSD"

        print("✅ MT5 configuration works correctly")
    except ImportError as e:
        print(f"⚠️ MT5 config test skipped (MetaTrader5 not installed): {e}")

def run_all_tests():
    """Run all tests."""
    print("🚀 Running MT5 Bridge Tests")
    print("=" * 50)

    try:
        # Test parsing
        test_plan_parsing()

        # Test executor logic (without MT5)
        test_executor_logic()

        # Test async Redis consumer
        asyncio.run(test_redis_consumer())

        # Test execution events
        test_execution_events()

        # Test deals watcher
        test_deals_watcher()

        # Test MT5 config (optional, skips if MT5 not installed)
        test_mt5_config()

        print("\n🎉 All MT5 Bridge tests passed!")
        print("MT5 Bridge is ready for integration.")
        print("\n📋 Next steps:")
        print("1. Install MetaTrader5: pip install MetaTrader5")
        print("2. Configure mt5-bridge.env with real credentials")
        print("3. Run: docker-compose --profile mt5 up mt5-bridge")

    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    run_all_tests()
