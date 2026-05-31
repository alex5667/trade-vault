import json
import os


# Assuming a simple parsing logic is usually standalone or we test the property
def parse_is_virtual(redis_hash: dict) -> bool:
    """Helper mirroring the production logic for parsing is_virtual."""
    val = redis_hash.get("is_virtual")
    if not val:
        return False
    # Handle string cases from Redis MGET/HGETAll
    if isinstance(val, str):
        return val.lower() in ("1", "true", "yes")
    return bool(val)

def test_is_virtual_redis_hash_contract():
    fixture_path = os.path.join(
        os.path.dirname(__file__), "fixtures", "orders_closed_hash_virtual_v1.json"
    )
    with open(fixture_path, encoding="utf-8") as f:
        payload = json.load(f)

    assert parse_is_virtual(payload) is True
    assert payload["orderId"].endswith("_virtual")

def test_is_virtual_falsy_cases():
    assert parse_is_virtual({"is_virtual": "0"}) is False
    assert parse_is_virtual({"is_virtual": "False"}) is False
    assert parse_is_virtual({"is_virtual": ""}) is False
    assert parse_is_virtual({}) is False

def test_is_virtual_truthy_cases():
    assert parse_is_virtual({"is_virtual": "1"}) is True
    assert parse_is_virtual({"is_virtual": "True"}) is True
    assert parse_is_virtual({"is_virtual": "true"}) is True
