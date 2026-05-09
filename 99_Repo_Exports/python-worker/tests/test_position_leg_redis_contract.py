import json

from services.position_leg_policy import PositionLeg


def test_position_leg_redis_hash_contract() -> None:
    # Simulates what gets packed into redis
    leg = PositionLeg(entry=50000.0, qty=1.0, side="SHORT", signal_id="sig_2", ts_ms=100)

    # Normally lists of legs are json-encoded into a redis hash field
    # (e.g., hset(pos_key, "legs", json.dumps([leg.to_dict()])))
    raw_json = json.dumps([leg.to_dict()])

    # Deserialization from redis
    loaded = json.loads(raw_json)

    # Contract validation
    first = loaded[0]
    assert isinstance(first["entry"], float)
    assert isinstance(first["qty"], float)
    assert first["side"] == "SHORT"
    assert first["signal_id"] == "sig_2"

    # Can it parse back safely?
    restored = PositionLeg.from_dict(first)
    assert restored.entry == 50000.0
