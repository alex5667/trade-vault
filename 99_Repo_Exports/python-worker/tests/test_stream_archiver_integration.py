from utils.time_utils import get_ny_time_millis
"""
Integration tests for stream_archiver.py

Tests the complete flow: Redis Streams -> Archiver -> PostgreSQL

Prerequisites:
- Redis must be running and accessible
- PostgreSQL must be running with migrations applied
- Set environment variables:
  - REDIS_URL (default: redis://localhost:6379/0)
  - ANALYTICS_DB_DSN or PG_DSN (PostgreSQL connection string)

Run with:
    python -m pytest python-worker/tests/test_stream_archiver_integration.py -v

Or directly:
    python python-worker/tests/test_stream_archiver_integration.py
"""
import asyncio
import json
import os
import time
from typing import Dict, Any

import psycopg2
import pytest

# Import the archiver components
import sys
# [AUTOGRAVITY CLEANUP] sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from services.archivers.stream_archiver import StreamArchiver, PgWriter, PgCfg


# Test configuration
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
PG_DSN = os.getenv("ANALYTICS_DB_DSN") or (os.getenv("ANALYTICS_DB_DSN") or os.getenv("PG_DSN")) or "postgresql://trading:trading_password@localhost:5432/scanner_analytics"

TEST_ENTRY_STREAM = "test:stream:entry_audit"
TEST_EVENTS_STREAM = "test:stream:position_events"
TEST_ENTRY_DLQ = "test:stream:dlq:entry_audit"
TEST_EVENTS_DLQ = "test:stream:dlq:position_events"


@pytest.fixture
async def redis_client():
    """Create Redis client for testing"""
    from redis.asyncio import Redis
    r = Redis.from_url(REDIS_URL, decode_responses=True)
    yield r
    await r.close()


@pytest.fixture
def pg_connection():
    """Create PostgreSQL connection for testing"""
    conn = psycopg2.connect(PG_DSN)
    yield conn
    conn.close()


@pytest.fixture
async def cleanup_redis(redis_client):
    """Clean up test Redis streams before and after tests"""
    r = redis_client
    # Cleanup before test
    await r.delete(TEST_ENTRY_STREAM, TEST_EVENTS_STREAM, TEST_ENTRY_DLQ, TEST_EVENTS_DLQ)
    yield
    # Cleanup after test
    await r.delete(TEST_ENTRY_STREAM, TEST_EVENTS_STREAM, TEST_ENTRY_DLQ, TEST_EVENTS_DLQ)


@pytest.fixture
def cleanup_postgres(pg_connection):
    """Clean up test data from PostgreSQL before and after tests"""
    conn = pg_connection
    with conn.cursor() as cur:
        # Cleanup before test
        cur.execute("DELETE FROM entry_policy_audit WHERE stream_id LIKE 'test:%'")
        cur.execute("DELETE FROM position_events WHERE stream_id LIKE 'test:%'")
        conn.commit()
    yield
    with conn.cursor() as cur:
        # Cleanup after test
        cur.execute("DELETE FROM entry_policy_audit WHERE stream_id LIKE 'test:%'")
        cur.execute("DELETE FROM position_events WHERE stream_id LIKE 'test:%'")
        conn.commit()


async def test_entry_audit_flow_minimal(redis_client, pg_connection, cleanup_redis, cleanup_postgres):
    """Test basic entry_audit archival flow"""
    r = redis_client
    
    # Add test messages to Redis stream
    test_messages = [
        {
            "symbol": "BTCUSDT",
            "decision": "ALLOW",
            "sid": "BTCUSDT_1m_rev_001",
            "ts_ms": get_ny_time_millis(),
        },
        {
            "symbol": "ETHUSDT",
            "decision": "DENY",
            "sid": "ETHUSDT_5m_cont_002",
            "of_confirm_score": 0.45,
            "ts_ms": get_ny_time_millis(),
        },
    ]
    
    stream_ids = []
    for msg in test_messages:
        sid = await r.xadd(TEST_ENTRY_STREAM, {"data": json.dumps(msg)})
        stream_ids.append(sid)
    
    # Create archiver with test configuration
    os.environ["TRADE_ENTRY_AUDIT_STREAM"] = TEST_ENTRY_STREAM
    os.environ["ENTRY_AUDIT_CG"] = "test_cg_entry"
    os.environ["ENTRY_AUDIT_CONSUMER"] = "test_consumer"
    os.environ["ENTRY_AUDIT_BATCH"] = "10"
    os.environ["ENTRY_AUDIT_BLOCK_MS"] = "1000"
    os.environ["ENTRY_AUDIT_DLQ_STREAM"] = TEST_ENTRY_DLQ
    os.environ["ENTRY_AUDIT_ARCHIVE_ENABLED"] = "1"
    os.environ["POSITION_EVENTS_ARCHIVE_ENABLED"] = "0"
    
    pg = PgWriter(PgCfg(dsn=PG_DSN))
    archiver = StreamArchiver(r, pg)
    
    # Create consumer group
    await archiver._ensure_group(TEST_ENTRY_STREAM, "test_cg_entry")
    
    # Process one batch
    resp = await r.xreadgroup(
        groupname="test_cg_entry",
        consumername="test_consumer",
        streams={TEST_ENTRY_STREAM: ">"},
        count=10,
        block=1000
    )
    
    assert resp, "Should have received messages from stream"
    _, msgs = resp[0]
    assert len(msgs) == 2, "Should have 2 messages"
    
    # Parse and insert
    rows = []
    ack_ids = []
    for mid, fields in msgs:
        payload = json.loads(fields.get("data"))
        rows.append(archiver._entry_row(mid, payload))
        ack_ids.append(mid)
    
    # Insert into PostgreSQL
    pg.insert_entry_audit_batch(rows)
    
    # Acknowledge messages
    await r.xack(TEST_ENTRY_STREAM, "test_cg_entry", *ack_ids)
    
    # Verify PostgreSQL data
    with pg_connection.cursor() as cur:
        cur.execute("SELECT stream_id, symbol, decision FROM entry_policy_audit WHERE stream_id = ANY(%s)", (ack_ids,))
        results = cur.fetchall()
        
        assert len(results) == 2, "Should have 2 rows in PostgreSQL"
        
        symbols = {row[1] for row in results}
        assert "BTCUSDT" in symbols
        assert "ETHUSDT" in symbols
        
        decisions = {row[2] for row in results}
        assert "ALLOW" in decisions
        assert "DENY" in decisions
    
    # Verify pending list is empty
    pending = await r.xpending(TEST_ENTRY_STREAM, "test_cg_entry")
    assert pending["pending"] == 0, "Pending list should be empty after ack"


async def test_position_events_flow(redis_client, pg_connection, cleanup_redis, cleanup_postgres):
    """Test position_events archival flow with event type filtering"""
    r = redis_client
    
    # Add test messages to Redis stream
    test_messages = [
        {
            "type": "TP_HIT",
            "order_id": "order_001",
            "symbol": "BTCUSDT",
            "ts_ms": get_ny_time_millis(),
        },
        {
            "type": "SL_ADJUST",
            "order_id": "order_002",
            "symbol": "ETHUSDT",
            "old_sl": 2400.0,
            "new_sl": 2420.0,
            "ts_ms": get_ny_time_millis(),
        },
        {
            "type": "POSITION_CLOSED",  # Should be filtered out
            "order_id": "order_003",
            "symbol": "SOLUSDT",
            "ts_ms": get_ny_time_millis(),
        },
        {
            "type": "TRAILING_MOVE",
            "order_id": "order_004",
            "symbol": "XRPUSDT",
            "ts_ms": get_ny_time_millis(),
        },
    ]
    
    stream_ids = []
    for msg in test_messages:
        sid = await r.xadd(TEST_EVENTS_STREAM, {"data": json.dumps(msg)})
        stream_ids.append(sid)
    
    # Create archiver with test configuration
    os.environ["TRADE_EVENTS_STREAM"] = TEST_EVENTS_STREAM
    os.environ["POSITION_EVENTS_CG"] = "test_cg_events"
    os.environ["POSITION_EVENTS_CONSUMER"] = "test_consumer"
    os.environ["POSITION_EVENTS_BATCH"] = "10"
    os.environ["POSITION_EVENTS_BLOCK_MS"] = "1000"
    os.environ["POSITION_EVENTS_DLQ_STREAM"] = TEST_EVENTS_DLQ
    os.environ["POSITION_EVENTS_TYPES"] = "TP_HIT,TRAILING_MOVE,SL_ADJUST"
    os.environ["ENTRY_AUDIT_ARCHIVE_ENABLED"] = "0"
    os.environ["POSITION_EVENTS_ARCHIVE_ENABLED"] = "1"
    
    pg = PgWriter(PgCfg(dsn=PG_DSN))
    archiver = StreamArchiver(r, pg)
    
    # Create consumer group
    await archiver._ensure_group(TEST_EVENTS_STREAM, "test_cg_events")
    
    # Process one batch
    resp = await r.xreadgroup(
        groupname="test_cg_events",
        consumername="test_consumer",
        streams={TEST_EVENTS_STREAM: ">"},
        count=10,
        block=1000
    )
    
    assert resp, "Should have received messages from stream"
    _, msgs = resp[0]
    assert len(msgs) == 4, "Should have 4 messages"
    
    # Parse and insert (filter by type)
    rows = []
    ack_ids = []
    for mid, fields in msgs:
        payload = json.loads(fields.get("data"))
        et = payload.get("type")
        
        if et in archiver.events_types:
            rows.append(archiver._event_row(mid, payload, et))
            ack_ids.append(mid)
        else:
            # Still ack filtered messages
            await r.xack(TEST_EVENTS_STREAM, "test_cg_events", mid)
    
    # Should have filtered out POSITION_CLOSED
    assert len(rows) == 3, "Should have 3 rows after filtering"
    
    # Insert into PostgreSQL
    pg.insert_position_events_batch(rows)
    
    # Acknowledge messages
    await r.xack(TEST_EVENTS_STREAM, "test_cg_events", *ack_ids)
    
    # Verify PostgreSQL data
    with pg_connection.cursor() as cur:
        cur.execute("SELECT stream_id, event_type, order_id FROM position_events WHERE stream_id = ANY(%s)", (ack_ids,))
        results = cur.fetchall()
        
        assert len(results) == 3, "Should have 3 rows in PostgreSQL"
        
        event_types = {row[1] for row in results}
        assert "TP_HIT" in event_types
        assert "SL_ADJUST" in event_types
        assert "TRAILING_MOVE" in event_types
        assert "POSITION_CLOSED" not in event_types  # Filtered out


async def test_dlq_on_parse_error(redis_client, pg_connection, cleanup_redis, cleanup_postgres):
    """Test that parse errors are sent to DLQ"""
    r = redis_client
    
    # Add invalid message to Redis stream
    invalid_msg_id = await r.xadd(TEST_ENTRY_STREAM, {"data": "not-valid-json"})
    
    # Create archiver
    os.environ["TRADE_ENTRY_AUDIT_STREAM"] = TEST_ENTRY_STREAM
    os.environ["ENTRY_AUDIT_CG"] = "test_cg_dlq"
    os.environ["ENTRY_AUDIT_CONSUMER"] = "test_consumer"
    os.environ["ENTRY_AUDIT_DLQ_STREAM"] = TEST_ENTRY_DLQ
    os.environ["ENTRY_AUDIT_ARCHIVE_ENABLED"] = "1"
    os.environ["POSITION_EVENTS_ARCHIVE_ENABLED"] = "0"
    
    pg = PgWriter(PgCfg(dsn=PG_DSN))
    archiver = StreamArchiver(r, pg)
    
    # Create consumer group
    await archiver._ensure_group(TEST_ENTRY_STREAM, "test_cg_dlq")
    
    # Process message
    resp = await r.xreadgroup(
        groupname="test_cg_dlq",
        consumername="test_consumer",
        streams={TEST_ENTRY_STREAM: ">"},
        count=1,
        block=1000
    )
    
    assert resp, "Should have received message"
    _, msgs = resp[0]
    mid, fields = msgs[0]
    
    # Try to parse (should fail and go to DLQ)
    try:
        payload = json.loads(fields.get("data"))
        # If it somehow succeeds, continue
    except Exception as e:
        # Send to DLQ
        await archiver._dlq(TEST_ENTRY_DLQ, TEST_ENTRY_STREAM, mid, f"parse_error:{e}", {"fields": fields})
        # Ack the message
        await r.xack(TEST_ENTRY_STREAM, "test_cg_dlq", mid)
    
    # Verify DLQ has the message
    dlq_msgs = await r.xrange(TEST_ENTRY_DLQ, "-", "+")
    assert len(dlq_msgs) > 0, "DLQ should have messages"
    
    dlq_msg_id, dlq_fields = dlq_msgs[0]
    assert dlq_fields["stream"] == TEST_ENTRY_STREAM
    assert dlq_fields["stream_id"] == mid
    assert "parse_error" in dlq_fields["err"]


async def test_idempotency(redis_client, pg_connection, cleanup_redis, cleanup_postgres):
    """Test that duplicate inserts are handled via ON CONFLICT DO NOTHING"""
    r = redis_client
    
    # Add test message
    msg = {
        "symbol": "BTCUSDT",
        "decision": "ALLOW",
        "ts_ms": get_ny_time_millis(),
    }
    msg_id = await r.xadd(TEST_ENTRY_STREAM, {"data": json.dumps(msg)})
    
    # Create archiver
    pg = PgWriter(PgCfg(dsn=PG_DSN))
    archiver = StreamArchiver(r, pg)
    
    # Parse row
    row = archiver._entry_row(msg_id, msg)
    
    # Insert once
    pg.insert_entry_audit_batch([row])
    
    # Insert again (should be ignored due to ON CONFLICT)
    pg.insert_entry_audit_batch([row])
    
    # Verify only one row exists
    with pg_connection.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM entry_policy_audit WHERE stream_id = %s", (msg_id,))
        count = cur.fetchone()[0]
        assert count == 1, "Should have exactly 1 row (idempotent)"


# Main test runner (for running without pytest)
async def run_all_tests():
    """Run all tests manually (without pytest)"""
    from redis.asyncio import Redis
    
    print("Starting integration tests...")
    
    redis_client = Redis.from_url(REDIS_URL, decode_responses=True)
    pg_connection = psycopg2.connect(PG_DSN)
    
    try:
        print("\n1. Testing entry_audit flow...")
        await test_entry_audit_flow_minimal(redis_client, pg_connection, None, None)
        print("   ✅ PASSED")
        
        print("\n2. Testing position_events flow with filtering...")
        await test_position_events_flow(redis_client, pg_connection, None, None)
        print("   ✅ PASSED")
        
        print("\n3. Testing DLQ on parse error...")
        await test_dlq_on_parse_error(redis_client, pg_connection, None, None)
        print("   ✅ PASSED")
        
        print("\n4. Testing idempotency...")
        await test_idempotency(redis_client, pg_connection, None, None)
        print("   ✅ PASSED")
        
        print("\n✅ All integration tests passed!")
        
    finally:
        await redis_client.close()
        pg_connection.close()


if __name__ == "__main__":
    asyncio.run(run_all_tests())

