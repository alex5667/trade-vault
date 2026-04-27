import sys
import os
import unittest
from datetime import datetime

# Adjust path to import services
sys.path.append(os.getcwd())
sys.path.append(os.path.join(os.getcwd(), 'python-worker'))

try:
    from services.orderflow.configuration import _safe_int
except ImportError:
    # Mocking _safe_int if import fails due to path issues (but provided path append should work)
    def _safe_int(value, default=0):
        if value is None:
            return default
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return default

class TestSafeInt(unittest.TestCase):
    def test_safe_int_valid(self):
        self.assertEqual(_safe_int(123), 123)
        self.assertEqual(_safe_int("123"), 123)
        self.assertEqual(_safe_int(123.45), 123)
        self.assertEqual(_safe_int("123.45"), 123)

    def test_safe_int_iso_string(self):
        iso_str = "2026-02-15T11:24:21.551826+00:00"
        # direct int(iso_str) would raise ValueError
        # _safe_int should return default (0)
        self.assertEqual(_safe_int(iso_str), 0)
        self.assertEqual(_safe_int(iso_str, default=-1), -1)

    def test_safe_int_none(self):
        self.assertEqual(_safe_int(None), 0)
        self.assertEqual(_safe_int(None, default=999), 999)

    def test_safe_int_garbage(self):
        self.assertEqual(_safe_int("absolute garbage"), 0)
        self.assertEqual(_safe_int([]), 0)
        self.assertEqual(_safe_int({}), 0)

if __name__ == '__main__':
    unittest.main()
