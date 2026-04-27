from utils.time_utils import get_ny_time_millis
import json
import os
import time
import pytest
import redis
from services.trade_events_logger import TradeEventsLogger, TradeEvent

def test_trade_events_payload_expansion():
    """
    Verify that TradeEventsLogger.log_event expands the 'payload' field 
    into the root of the Redis stream message.
    """
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    r = redis.from_url(redis_url, decode_responses=True)
    logger = TradeEventsLogger(redis_url)
    
    test_stream = f"test:stream:expansion:{int(time.time())}"
    logger.events_stream = test_stream
    
    try:
        # 1. Create event with a payload
        payload = {
            "risk_usd": 100.0,
            "ab_arm": "A",
            "ab_ver": 2,
            "extra_info": "hello"
        }
        
        event = TradeEvent(
            event_type="TEST_EXPANSION",
            sid="test-sid-123",
            symbol="BTCUSD",
            ts=get_ny_time_millis(),
            source="test_unit",
            payload=payload
        )
        
        # 2. Log event
        event_id = logger.log_event(event)
        assert event_id != ""
        
        # 3. Read from stream
        msgs = r.xread({test_stream: "0"}, count=1)
        assert len(msgs) > 0
        
        # Structure: [[stream_name, [(id, fields)]]]
        _, entries = msgs[0]
        _, fields = entries[0]
        
        # 4. Assertions: fields should contain expanded payload keys
        assert fields["event_type"] == "TEST_EXPANSION"
        assert fields["sid"] == "test-sid-123"
        assert fields["risk_usd"] == "100.0" # cast to string by Redis xadd
        assert fields["ab_arm"] == "A"
        assert fields["ab_ver"] == "2"
        assert fields["extra_info"] == "hello"
        
        # Payload itself is NOT in fields (popped in log_event)
        assert "payload" not in fields
        
        print("\n✅ Verification successful: payload expanded into root fields.")
        
    finally:
        # Cleanup
        r.delete(test_stream)

if __name__ == "__main__":
    test_trade_events_payload_expansion()
