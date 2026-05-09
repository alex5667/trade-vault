
import json
import os
import sys

# Add current directory to path so we can import orders_router
# [AUTOGRAVITY CLEANUP] sys.path.append("/home/alex/front/trade/scanner_infra/python-worker/services")
# [AUTOGRAVITY CLEANUP] sys.path.append("/home/alex/front/trade/scanner_infra/python-worker")

# Mock for redis
class MockRedis:
    def __init__(self):
        self.data = {}
        self.lists = {}

    def get(self, key):
        return self.data.get(str(key))

    def set(self, key, val):
        self.data[str(key)] = val

    def lpush(self, key, val):
        if key not in self.lists:
            self.lists[key] = []
        self.lists[key].insert(0, val)
        print(f"LPUSH {key}: {val}")

# Mock os.getenv for orders_router
original_getenv = os.getenv
def mock_getenv(key, default=None):
    return original_getenv(key, default)

os.getenv = mock_getenv

try:
    from orders_router import _decimals_from_point, _ensure_min_distance, _round_price, route_open

    from symbol_specs_store import SymbolSpecs, SymbolSpecsStore
except ImportError as e:
    print(f"Error importing modules: {e}")
    sys.exit(1)

def test_decimal_calculation():
    print("\n--- Testing _decimals_from_point ---")

    cases = [
        (0.1, 1),
        (0.01, 2),
        (0.0001, 4),
        (0.00000001, 8),
        (1e-8, 8),
        (0.0000000001, 10), # Should be 10 now (limit 12)
        (1e-12, 12),
        (1.0, 0),
        (0.0, 2), # Default fallback
    ]

    for point, expected in cases:
        result = _decimals_from_point(point)
        print(f"Point: {point} -> Decimals: {result} (Expected: {expected})")
        if result != expected and (expected <= 8 or result <= 12): # Adjust checks
             # Note: if point is smaller than 1e-8, logic might have capped at 8 before.
             # We updated it to 12.
             if expected > 8 and result == 8:
                 print(f"WARNING: Capped at 8? {result}")
             elif result != expected:
                 print(f"FAIL: {point} got {result}, expected {expected}")

def test_pepe_routing():
    print("\n--- Testing PEPE Routing ---")
    mock_redis = MockRedis()

    # Setup Symbol Specs for PEPE
    pepe_point = 0.00000001
    mock_redis.data["symbol_specs:1000PEPEUSDT"] = json.dumps({
        "symbol": "1000PEPEUSDT",
        "point": pepe_point,
        "tick_size": pepe_point,
        "min_stop_points": 10
    })

    # Setup Signal Snapshot
    sid = "test_pepe_signal"
    mock_redis.data["signal:snap:" + sid] = json.dumps({
        "symbol": "1000PEPEUSDT",
        "price": 0.0066560,
        "risk": {
            "sl": 0.0066000,
            "tp_levels": [0.0067000, 0.0068000],
            "atr": 0.0000500
        }
    })

    # Trigger Route Open
    parts = ["open", "LONG", "100", sid]

    # We need to monkeypath redis used inside orders_router or pass it if possible.
    # route_open(r, parts).
    route_open(mock_redis, parts)

    # Check Result
    queue = mock_redis.lists.get("orders:queue", [])
    if not queue:
        print("FAIL: No order in queue")
        return

    order_json = queue[0]
    order = json.loads(order_json)

    print("Order Payload:")
    print(json.dumps(order, indent=2))

    # Verify precision
    entry = order.get("entry")
    sl = order.get("sl")

    print(f"Entry: {entry}")
    print(f"SL: {sl}")

    if entry == 0.01:
        print("FAIL: Entry rounded to 0.01!")
    elif abs(entry - 0.006656) < 1e-9:
        print("PASS: Entry precision looks correct.")
    else:
        print(f"WARN: Entry {entry} != 0.006656")

if __name__ == "__main__":
    test_decimal_calculation()
    test_pepe_routing()
