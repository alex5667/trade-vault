#!/usr/bin/env python3
"""
Demo script for testing strict timestamp normalization.
Shows the difference between regular and strict normalization.
"""

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'python-worker'))

from domain.time_utils import normalize_ts_ms, normalize_ts_ms_strict

def test_cases():
    """Test cases for timestamp normalization."""
    test_cases = [
        # (input, description)
        (0, "zero"),
        (600, "minutes-of-day (600 minutes = 10 hours)"),
        (1439, "end of day minutes (23:59)"),
        (1_000_000_000, "epoch seconds (2001-09-09)"),
        (1_600_000_000, "epoch seconds (2020-09-13)"),
        (1_700_000_000, "epoch seconds (2023-11-15)"),
        (1_000_000_000_000, "epoch ms (2001-09-09)"),
        (1_700_000_000_000, "epoch ms (2023-11-15)"),
        ("1700000000", "string epoch seconds"),
        ("1700000000000", "string epoch ms"),
        ("invalid", "invalid string"),
        (None, "None value"),
    ]

    print("🔍 Strict Timestamp Normalization Demo")
    print("=" * 80)
    print(f"{'Input':<20} {'Description':<35} {'Regular':<15} {'Strict':<15}")
    print("-" * 80)

    for ts_input, description in test_cases:
        try:
            regular = normalize_ts_ms(ts_input)
            strict = normalize_ts_ms_strict(ts_input)

            status = "✅" if regular == strict else "⚠️ REJECTED"

            print(f"{str(ts_input):<20} {description:<35} {regular:<15} {strict:<15} {status}")
        except Exception as e:
            print(f"{str(ts_input):<20} {description:<35} ERROR: {str(e)}")

    print("\n📋 Summary:")
    print("- Regular normalization converts small numbers to ms (assumes seconds)")
    print("- Strict normalization rejects non-epoch timestamps (< 2001-09-09 in ms)")
    print("- This prevents 'minutes-of-day' clocks from being misinterpreted as epoch")

if __name__ == "__main__":
    test_cases()
