import sys
import os

# Add the project directory to sys.path
sys.path.insert(0, '/home/alex/front/trade/scanner_infra/python-worker')

from domain.normalizers import bucket_close_reason

test_cases = {
    "TP1": "TP",
    "TP2": "TP",
    "TP3": "TP",
    "TP": "TP",
    "TAKE_PROFIT": "TP",
    "MANUAL_TP": "TP",
    "TRAIL": "TRAIL_SL",
    "TRAILING": "TRAIL_SL",
    "TRAILING_STOP": "TRAIL_SL",
    "SL_AFTER_TP1": "TRAIL_SL",
    "SL_AFTER_TP2": "TRAIL_SL",
    "MOVED_SL": "TRAIL_SL",
    "LOCK_PROFIT": "TRAIL_SL",
    "SL": "INITIAL_SL",
    "STOP_LOSS": "INITIAL_SL",
    "INITIAL_SL": "INITIAL_SL",
    "LIQUIDATION": "INITIAL_SL",
    "ADL": "INITIAL_SL",
    "TIMEOUT": "TIMEOUT",
    "ORPHAN_TIMEOUT": "TIMEOUT",
    "EXPIRED": "TIMEOUT",
    "MANUAL": "MANUAL",
    "MANUAL_CLOSE": "MANUAL",
    "ERROR": "ERROR",
    "UNKNOWN": "ERROR",
    "": "ERROR",
    None: "ERROR",
    "RANDOM_STRING": "ERROR"
}

print(f"{'Raw Reason':<20} | {'Expected Bucket':<15} | {'Actual Bucket':<15} | {'Status'}")
print("-" * 65)

all_passed = True
for raw, expected in test_cases.items():
    actual = bucket_close_reason(raw)
    status = "✅ PASS" if actual == expected else "❌ FAIL"
    if actual != expected:
        all_passed = False
    print(f"{str(raw):<20} | {expected:<15} | {actual:<15} | {status}")

if all_passed:
    print("\n✅ All bucketing tests passed!")
    sys.exit(0)
else:
    print("\n❌ Some bucketing tests failed!")
    sys.exit(1)
