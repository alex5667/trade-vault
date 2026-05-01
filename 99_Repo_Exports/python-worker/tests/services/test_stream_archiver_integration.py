"""
Integration тесты для stream_archiver.py

Тестируем полный цикл:
1. Запись событий в Redis Stream
2. Чтение через archiver (consumer group)
3. Вставка в PostgreSQL
4. Проверка idempotency (ON CONFLICT DO NOTHING)
5. Проверка DLQ при ошибках

ВАЖНО: Эти тесты требуют запущенных Redis и PostgreSQL.
Можно запускать через pytest с соответствующими fixtures или skip.
"""
import asyncio
import json
import os
import pytest
from typing import Any, Dict

# Skip если нет тестового окружения
pytestmark = pytest.mark.skipif(
    os.getenv("TEST_INTEGRATION") != "1",
    reason="Integration tests требуют TEST_INTEGRATION=1 и запущенных Redis/PostgreSQL"
)

try:
    import redis.asyncio as aioredis
    import psycopg2
    from services.archivers.stream_archiver import (
        StreamArchiver,
        PgWriter,
        PgCfg,
    )
except ImportError:
    pytest.skip("Missing dependencies for integration tests", allow_module_level=True)


@pytest.fixture
async def redis_client():
    """Redis client для тестов"""
    redis_url = os.getenv("TEST_REDIS_URL", "redis://redis:6379/0")
    r = aioredis.from_url(redis_url, decode_responses=True)
    yield r
    await r.close()


@pytest.fixture
def pg_writer():
    """PostgreSQL writer для тестов"""
    dsn = os.getenv("TEST_PG_DSN", "postgresql://trading:trading_password@postgres:5432/scanner_analytics")
    pg = PgWriter(PgCfg(dsn=dsn))
    yield pg


@pytest.mark.asyncio
async def test_entry_audit_full_cycle(redis_client, pg_writer):
    """
    Полный цикл: запись в stream -> archiver -> PostgreSQL
    """
    stream = "test:entry_audit"
    test_payload = {
        "ts": 1706380000000,
        "sid": ":test:v1",
        "symbol": "",
        "decision": "ALLOW",
        "arm": "B",
        "ab_group": "test_group",
        "scenario": "continuation",
        "regime": "trend",
        "of_confirm_score": 0.85,
        "coh": 0.75,
        "leader_conf": 0.90,
        "spread_z": 1.2,
        "pressure_sps": 0.3,
        "obi_age_ms": 2000,
    }

    # 1. Запись в stream
    stream_id = await redis_client.xadd(stream, {"data": json.dumps(test_payload)})
    assert stream_id is not None

    # 2. Создать archiver и обработать (1 iteration)
    os.environ["TRADE_ENTRY_AUDIT_STREAM"] = stream
    os.environ["ENTRY_AUDIT_ARCHIVE_ENABLED"] = "1"
    os.environ["ENTRY_AUDIT_CG"] = "test_cg"
    os.environ["ENTRY_AUDIT_BATCH"] = "1"
    os.environ["POSITION_EVENTS_ARCHIVE_ENABLED"] = "0"
    
    svc = StreamArchiver(redis_client, pg_writer)
    
    # Ensure consumer group
    await svc.ensure_group(stream, "test_cg")
    
    # Read and process one batch
    resp = await svc._read_new(stream, "test_cg", "test_consumer", 1, 100)
    assert len(resp) > 0
    _, msgs = resp[0]
    assert len(msgs) == 1
    
    mid, fields = msgs[0]
    payload = json.loads(fields["data"])
    row = svc.entry_row(mid, payload)
    
    # 3. Insert в PostgreSQL
    pg_writer.insert_entry_audit([row])
    
    # 4. Проверка: запись должна быть в БД
    with psycopg2.connect(pg_writer.cfg.dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT symbol, decision, arm FROM entry_policy_audit WHERE stream_id = %s",
                (mid,)
            )
            result = cur.fetchone()
            assert result is not None
            assert result[0] == ""
            assert result[1] == "ALLOW"
            assert result[2] == "B"
    
    # 5. Ack message
    await redis_client.xack(stream, "test_cg", mid)
    
    # Cleanup
    await redis_client.delete(stream)


@pytest.mark.asyncio
async def test_position_events_full_cycle(redis_client, pg_writer):
    """
    Полный цикл для position_events: stream -> archiver -> PostgreSQL
    """
    stream = "test:position_events"
    test_payload = {
        "event_type": "POSITION_CLOSED",
        "position_id": "12345678",
        "ts": 1706380000000,
        "sid": "BTCUSD:trend:v1",
        "symbol": "BTCUSD",
        "meta": json.dumps({"close_reason": "trailing_stop", "pnl": 150.0}),
        "price": 42000.0,
    }

    # 1. Запись в stream
    stream_id = await redis_client.xadd(stream, {"data": json.dumps(test_payload)})
    assert stream_id is not None

    # 2. Создать archiver
    os.environ["TRADE_EVENTS_STREAM"] = stream
    os.environ["POSITION_EVENTS_ARCHIVE_ENABLED"] = "1"
    os.environ["POSITION_EVENTS_CG"] = "test_events_cg"
    os.environ["POSITION_EVENTS_BATCH"] = "1"
    os.environ["ENTRY_AUDIT_ARCHIVE_ENABLED"] = "0"
    
    svc = StreamArchiver(redis_client, pg_writer)
    
    await svc.ensure_group(stream, "test_events_cg")
    
    resp = await svc._read_new(stream, "test_events_cg", "test_consumer", 1, 100)
    assert len(resp) > 0
    _, msgs = resp[0]
    assert len(msgs) == 1
    
    mid, fields = msgs[0]
    payload = json.loads(fields["data"])
    row = svc.event_row(mid, payload)
    
    # 3. Insert в PostgreSQL
    pg_writer.insert_position_events([row])
    
    # 4. Проверка
    with psycopg2.connect(pg_writer.cfg.dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT position_id, event_type, meta_json::text FROM position_events WHERE stream_id = %s",
                (mid,)
            )
            result = cur.fetchone()
            assert result is not None
            assert result[0] == "12345678"
            assert result[1] == "POSITION_CLOSED"
            
            # meta должен быть JSONB
            meta = json.loads(result[2])
            assert meta["close_reason"] == "trailing_stop"
    
    await redis_client.xack(stream, "test_events_cg", mid)
    await redis_client.delete(stream)


@pytest.mark.asyncio
async def test_idempotency(redis_client, pg_writer):
    """
    Проверка idempotency: повторная вставка того же stream_id не создает дублей
    """
    stream = "test:idempotency"
    test_payload = {
        "ts": 1706380000000,
        "symbol": "",
        "decision": "ALLOW",
    }

    stream_id = await redis_client.xadd(stream, {"data": json.dumps(test_payload)})
    
    os.environ["TRADE_ENTRY_AUDIT_STREAM"] = stream
    os.environ["ENTRY_AUDIT_ARCHIVE_ENABLED"] = "1"
    os.environ["POSITION_EVENTS_ARCHIVE_ENABLED"] = "0"
    
    svc = StreamArchiver(redis_client, pg_writer)
    row = svc.entry_row(stream_id, test_payload)
    
    # Вставка 1
    pg_writer.insert_entry_audit([row])
    
    # Вставка 2 (должна быть пропущена через ON CONFLICT DO NOTHING)
    pg_writer.insert_entry_audit([row])
    
    # Проверка: только 1 запись
    with psycopg2.connect(pg_writer.cfg.dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM entry_policy_audit WHERE stream_id = %s",
                (stream_id,)
            )
            count = cur.fetchone()[0]
            assert count == 1, "должна быть только 1 запись (idempotency)"
    
    await redis_client.delete(stream)


@pytest.mark.asyncio
async def test_dlq_on_parse_error(redis_client, pg_writer):
    """
    Проверка DLQ: invalid JSON должен попасть в DLQ stream
    """
    stream = "test:dlq_test"
    dlq_stream = "test:dlq:entry_audit"
    
    # Invalid JSON
    await redis_client.xadd(stream, {"data": "invalid json {{"})
    
    os.environ["TRADE_ENTRY_AUDIT_STREAM"] = stream
    os.environ["ENTRY_AUDIT_DLQ_STREAM"] = dlq_stream
    os.environ["ENTRY_AUDIT_ARCHIVE_ENABLED"] = "1"
    os.environ["POSITION_EVENTS_ARCHIVE_ENABLED"] = "0"
    
    svc = StreamArchiver(redis_client, pg_writer)
    await svc.ensure_group(stream, "test_dlq_cg")
    
    resp = await svc._read_new(stream, "test_dlq_cg", "test_consumer", 1, 100)
    _, msgs = resp[0]
    mid, fields = msgs[0]
    
    try:
        payload = json.loads(fields["data"])
        svc.entry_row(mid, payload)
    except Exception as e:
        # Должен попасть в DLQ
        await svc.dlq(dlq_stream, stream, mid, f"parse_error:{e}", {"fields": fields})
    
    # Проверка: сообщение в DLQ
    dlq_msgs = await redis_client.xrange(dlq_stream, "-", "+")
    assert len(dlq_msgs) > 0
    
    # Cleanup
    await redis_client.delete(stream)
    await redis_client.delete(dlq_stream)


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main([__file__, "-v", "-s"]))

