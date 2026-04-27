#!/usr/bin/env python3
"""
Test script for calibration fix in BaseOrderFlowHandler.
"""

def test_parse_rr_levels():
    """Test _parse_rr_levels method logic."""

    def _parse_rr_levels(rr_str: str):
        """Парсит строку RR уровней в список float."""
        if not rr_str:
            return [1.0, 2.0, 3.0]
        try:
            result = []
            for x in rr_str.split(","):
                x = x.strip()
                if x:
                    result.append(float(x))
            return result if result else [1.0, 2.0, 3.0]
        except Exception:
            return [1.0, 2.0, 3.0]

    # Test cases
    test_cases = [
        ("1.0,2.0,3.0", [1.0, 2.0, 3.0]),
        ("1,2,3", [1.0, 2.0, 3.0]),
        ("1.5, 2.5, 3.5", [1.5, 2.5, 3.5]),
        ("", [1.0, 2.0, 3.0]),
        ("invalid", [1.0, 2.0, 3.0]),
        ("1.0,,3.0", [1.0, 3.0]),  # Should skip empty values
    ]

    for input_str, expected in test_cases:
        result = _parse_rr_levels(input_str)
        if result == expected:
            print(f"✅ Test passed: '{input_str}' -> {result}")
        else:
            print(f"❌ Test failed: '{input_str}' -> {result}, expected {expected}")

if __name__ == "__main__":
    print("Testing _parse_rr_levels method...")
    test_parse_rr_levels()
    print("Test completed!")
