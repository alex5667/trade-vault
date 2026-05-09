"""
Тесты для парсинга событий в stream_archiver.py

Проверяем:
1. event_row парсит position_id (не order_id) правильно
2. meta_json парсится из JSON string в dict
3. event_type извлекается из корня payload
4. timestamp coalescing работает корректно
"""
import json

from services.archivers.stream_archiver import (
    PgCfg,
    PgWriter,
    StreamArchiver,
    coalesce_ts_ms,
    parse_meta_json,
    ts_ms_from_stream_id,
)


class DummyRedis:
    """Dummy Redis для unit-тестов"""
    pass


def test_event_row_position_id_and_meta_json():
    """
    ВАЖНО: MT5 использует position_id вместо order_id.
    meta должен парситься из JSON string в dict.
    """
    pg = PgWriter(PgCfg(dsn="postgresql://invalid"))
    svc = StreamArchiver(DummyRedis(), pg)  # type: ignore

    payload = {
        "event_type": "POSITION_CLOSED",
        "position_id": "12345678",
        "ts": 1706380000000,
        "meta": '{"close_reason": "trailing_stop"}',
        "sid": ":absorption:v1",
        "symbol": "",
    }
    row = svc.event_row("1706380000000-0", payload)

    # row: (stream_id, ts_ms, ts, position_id, sid, symbol, event_type, meta_json, payload_json)
    assert row[3] == "12345678", "position_id должен быть извлечен"
    assert row[6] == "POSITION_CLOSED", "event_type должен быть в корне"
    assert row[7] is not None, "meta_json не должен быть None"

    # meta_json должен быть JSON string (для PostgreSQL JSONB)
    meta_dict = json.loads(row[7])
    assert meta_dict["close_reason"] == "trailing_stop", "meta должен быть распарсен из JSON string"


def test_event_row_meta_dict():
    """meta может быть уже dict (не string)"""
    pg = PgWriter(PgCfg(dsn="postgresql://invalid"))
    svc = StreamArchiver(DummyRedis(), pg)  # type: ignore

    payload = {
        "event_type": "TP1_HIT",
        "position_id": "87654321",
        "ts": 1706380000000,
        "meta": {"tp_level": 1, "price": 2765.5},
        "sid": "BTCUSD:trend:v2",
        "symbol": "BTCUSD",
    }
    row = svc.event_row("1706380000000-0", payload)

    assert row[7] is not None
    meta_dict = json.loads(row[7])
    assert meta_dict["tp_level"] == 1


def test_event_row_meta_none():
    """meta может отсутствовать"""
    pg = PgWriter(PgCfg(dsn="postgresql://invalid"))
    svc = StreamArchiver(DummyRedis(), pg)  # type: ignore

    payload = {
        "event_type": "TRAILING_MOVE",
        "position_id": "11111111",
        "ts": 1706380000000,
        "sid": ":test",
        "symbol": "",
    }
    row = svc.event_row("1706380000000-0", payload)

    assert row[7] is None, "meta_json должен быть None если meta отсутствует"


def test_parse_meta_json_string():
    """parse_meta_json парсит JSON string"""
    meta_str = '{"close_reason": "tp2_hit", "pnl": 150.0}'
    result = parse_meta_json(meta_str)
    assert result is not None
    assert result["close_reason"] == "tp2_hit"
    assert result["pnl"] == 150.0


def test_parse_meta_json_dict():
    """parse_meta_json возвращает dict as-is"""
    meta_dict = {"close_reason": "sl_hit"}
    result = parse_meta_json(meta_dict)
    assert result == meta_dict


def test_parse_meta_json_invalid():
    """parse_meta_json обрабатывает invalid JSON gracefully"""
    meta_str = "invalid json {{"
    result = parse_meta_json(meta_str)
    assert result is not None
    assert "_raw" in result
    assert "invalid json" in result["_raw"]


def test_parse_meta_json_none():
    """parse_meta_json возвращает None для None"""
    result = parse_meta_json(None)
    assert result is None


def test_coalesce_ts_ms_from_payload():
    """timestamp должен извлекаться из payload.ts (события используют 'ts' в ms)"""
    payload = {"ts": 1706380000000, "other": "data"}
    stream_id = "1706370000000-0"  # другой timestamp в stream_id

    result = coalesce_ts_ms(payload, stream_id)
    assert result == 1706380000000, "должен использовать payload.ts"


def test_coalesce_ts_ms_from_stream_id():
    """timestamp fallback на stream_id если нет в payload"""
    payload = {"other": "data"}
    stream_id = "1706380000000-0"

    result = coalesce_ts_ms(payload, stream_id)
    assert result == 1706380000000, "должен извлечь из stream_id"


def test_coalesce_ts_ms_priority():
    """проверка приоритета: ts_ms > ts > timestamp_ms > stream_id"""
    payload = {
        "ts_ms": 1000,
        "ts": 2000,
        "timestamp_ms": 3000,
    }
    stream_id = "4000-0"

    # Должен использовать ts_ms (highest priority)
    result = coalesce_ts_ms(payload, stream_id)
    assert result == 1000


def test_ts_ms_from_stream_id():
    """Извлечение timestamp из Redis stream ID"""
    stream_id = "1706380123456-0"
    result = ts_ms_from_stream_id(stream_id)
    assert result == 1706380123456


def test_entry_row_decision_normalization():
    """decision может быть в разных полях (normalize)"""
    pg = PgWriter(PgCfg(dsn="postgresql://invalid"))
    svc = StreamArchiver(DummyRedis(), pg)  # type: ignore

    # decision в поле "result"
    payload1 = {
        "result": "ALLOW",
        "symbol": "",
        "ts": 1706380000000,
    }
    row1 = svc.entry_row("1706380000000-0", payload1)
    assert row1[8] == "ALLOW"

    # decision в поле "policy_decision"
    payload2 = {
        "policy_decision": "DENY",
        "symbol": "BTCUSD",
        "ts": 1706380000000,
    }
    row2 = svc.entry_row("1706380000000-0", payload2)
    assert row2[8] == "DENY"

    # decision в поле "decision"
    payload3 = {
        "decision": "SHADOW",
        "symbol": "ETHUSD",
        "ts": 1706380000000,
    }
    row3 = svc.entry_row("1706380000000-0", payload3)
    assert row3[8] == "SHADOW"

    # Отсутствует decision -> UNKNOWN
    payload4 = {
        "symbol": "SOLUSD",
        "ts": 1706380000000,
    }
    row4 = svc.entry_row("1706380000000-0", payload4)
    assert row4[8] == "UNKNOWN"


def test_entry_row_arm_normalization():
    """arm может быть в ab_arm или arm"""
    pg = PgWriter(PgCfg(dsn="postgresql://invalid"))
    svc = StreamArchiver(DummyRedis(), pg)  # type: ignore

    # ab_arm
    payload1 = {
        "ab_arm": "B",
        "symbol": "",
        "ts": 1706380000000,
    }
    row1 = svc.entry_row("1706380000000-0", payload1)
    assert row1[9] == "B"

    # arm
    payload2 = {
        "arm": "C",
        "symbol": "BTCUSD",
        "ts": 1706380000000,
    }
    row2 = svc.entry_row("1706380000000-0", payload2)
    assert row2[9] == "C"


def test_entry_row_ab_group_normalization():
    """ab_group может быть в group или ab_group"""
    pg = PgWriter(PgCfg(dsn="postgresql://invalid"))
    svc = StreamArchiver(DummyRedis(), pg)  # type: ignore

    # ab_group
    payload1 = {
        "ab_group": "gold",
        "symbol": "",
        "ts": 1706380000000,
    }
    row1 = svc.entry_row("1706380000000-0", payload1)
    assert row1[10] == "gold"

    # group
    payload2 = {
        "group": "crypto",
        "symbol": "BTCUSD",
        "ts": 1706380000000,
    }
    row2 = svc.entry_row("1706380000000-0", payload2)
    assert row2[10] == "crypto"


if __name__ == "__main__":
    # Run tests
    import sys

    import pytest

    sys.exit(pytest.main([__file__, "-v"]))

