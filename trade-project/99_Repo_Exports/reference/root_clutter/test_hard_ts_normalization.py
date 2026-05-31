#!/usr/bin/env python3
"""
Demo script for testing HARD timestamp normalization.
Shows the difference between strict and hard normalization.
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'python-worker'))

from domain.time_utils import normalize_ts_ms_strict, normalize_ts_ms_hard
import time

def test_cases():
    """Test cases for timestamp normalization."""
    # Mock now as 2023-11-15 12:00:00 UTC
    now_ms = 1_700_000_000_000
    future_ms = now_ms + 15 * 60 * 1000  # 15 minutes in future
    past_ms = now_ms - 11 * 365 * 24 * 3600 * 1000  # 11 years in past

    test_cases = [
        # (input, description)
        (0, "zero"),
        (600, "minutes-of-day (600 minutes = 10 hours)"),
        (1439, "end of day minutes (23:59)"),
        (1_000_000_000, "epoch seconds (2001-09-09)"),
        (1_600_000_000, "epoch seconds (2020-09-13)"),
        (1_700_000_000, "epoch seconds (2023-11-15)"),
        (1_000_000_000_000, "epoch ms (2001-09-09)"),
        (now_ms, "current time epoch ms"),
        (future_ms, "15 minutes in future"),
        (past_ms, "11 years in past"),
        ("1700000000", "string epoch seconds"),
        ("1700000000000", "string epoch ms"),
        ("invalid", "invalid string"),
        (None, "None value"),
    ]

    print("🔍 HARD Timestamp Normalization Demo")
    print("=" * 90)
    print(f"{'Input':<25} {'Description':<35} {'Strict':<10} {'Hard':<10} {'Status'}")
    print("-" * 90)

    for ts_input, description in test_cases:
        try:
            strict = normalize_ts_ms_strict(ts_input)
            hard = normalize_ts_ms_hard(ts_input, now_ms=now_ms)

            if strict == hard:
                status = "✅ SAME"
            elif hard == 0:
                status = "⚠️ REJECTED"
            else:
                status = "✅ CORRECTED"

            print(f"{str(ts_input):<25} {description:<35} {strict:<10} {hard:<10} {status}")
        except Exception as e:
            print(f"{str(ts_input):<25} {description:<35} ERROR: {str(e)}")

    print("\n📋 Summary:")
    print("- Strict normalization rejects non-epoch timestamps (< 2001-09-09 in ms)")
    print("- Hard normalization additionally rejects far-future/far-past timestamps")
    print("- Far-future: > now + 10 minutes (clock skew protection)")
    print("- Far-past: < now - 10 years (stale data protection)")
    print("- This prevents poisoning stats with data from wrong clock domains")

if __name__ == "__main__":
    test_cases()
