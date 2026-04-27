"""
Unit tests for context_utils.

Tests context manipulation and type conversion utilities.
"""

import unittest
from types import SimpleNamespace

from handlers.context_helpers.context_utils import (
    get_attr,
    set_attr,
    safe_float_pos,
    first_item,
    normalize_side_int,
    side_int_to_payload,
    to_float_or_nan,
    to_opt_float,
)


class TestContextUtils(unittest.TestCase):
    """Test suite for context utilities."""
    
    def test_get_attr_from_object(self):
        """Test getting attribute from object."""
        obj = SimpleNamespace(foo="bar", num=42)
        self.assertEqual(get_attr(obj, "foo"), "bar")
        self.assertEqual(get_attr(obj, "num"), 42)
        self.assertIsNone(get_attr(obj, "missing"))
        self.assertEqual(get_attr(obj, "missing", "default"), "default")
    
    def test_get_attr_from_dict(self):
        """Test getting attribute from dictionary."""
        d = {"foo": "bar", "num": 42}
        self.assertEqual(get_attr(d, "foo"), "bar")
        self.assertEqual(get_attr(d, "num"), 42)
        self.assertIsNone(get_attr(d, "missing"))
        self.assertEqual(get_attr(d, "missing", "default"), "default")
    
    def test_set_attr_on_object(self):
        """Test setting attribute on object."""
        obj = SimpleNamespace()
        result = set_attr(obj, "foo", "bar")
        self.assertTrue(result)
        self.assertEqual(obj.foo, "bar")
    
    def test_set_attr_on_dict(self):
        """Test setting attribute on dictionary."""
        d = {}
        result = set_attr(d, "foo", "bar")
        self.assertTrue(result)
        self.assertEqual(d["foo"], "bar")
    
    def test_safe_float_pos_valid(self):
        """Test safe_float_pos with valid positive numbers."""
        self.assertEqual(safe_float_pos(10.5), 10.5)
        self.assertEqual(safe_float_pos("42.3"), 42.3)
        self.assertEqual(safe_float_pos(1), 1.0)
    
    def test_safe_float_pos_invalid(self):
        """Test safe_float_pos with invalid inputs."""
        self.assertIsNone(safe_float_pos(0))
        self.assertIsNone(safe_float_pos(-5.0))
        self.assertIsNone(safe_float_pos(float("inf")))
        self.assertIsNone(safe_float_pos(float("nan")))
        self.assertIsNone(safe_float_pos("invalid"))
        self.assertIsNone(safe_float_pos(None))
    
    def test_first_item_list(self):
        """Test first_item with list."""
        self.assertEqual(first_item([1, 2, 3]), 1)
        self.assertEqual(first_item(["a", "b"]), "a")
    
    def test_first_item_tuple(self):
        """Test first_item with tuple."""
        self.assertEqual(first_item((10, 20)), 10)
    
    def test_first_item_non_sequence(self):
        """Test first_item with non-sequence."""
        self.assertEqual(first_item(42), 42)
        self.assertEqual(first_item("hello"), "hello")
    
    def test_first_item_empty(self):
        """Test first_item with empty sequence."""
        self.assertEqual(first_item([]), [])
        self.assertEqual(first_item(()), ())
    
    def test_normalize_side_int_from_int(self):
        """Test normalize_side_int with integers."""
        self.assertEqual(normalize_side_int(1), 1)
        self.assertEqual(normalize_side_int(-1), -1)
        self.assertEqual(normalize_side_int(100), 1)
        self.assertEqual(normalize_side_int(-50), -1)
        self.assertIsNone(normalize_side_int(0))
    
    def test_normalize_side_int_from_string(self):
        """Test normalize_side_int with strings."""
        # Long variants
        self.assertEqual(normalize_side_int("LONG"), 1)
        self.assertEqual(normalize_side_int("long"), 1)
        self.assertEqual(normalize_side_int("BUY"), 1)
        self.assertEqual(normalize_side_int("buy"), 1)
        self.assertEqual(normalize_side_int("BID"), 1)
        self.assertEqual(normalize_side_int("1"), 1)
        self.assertEqual(normalize_side_int("+1"), 1)
        
        # Short variants
        self.assertEqual(normalize_side_int("SHORT"), -1)
        self.assertEqual(normalize_side_int("short"), -1)
        self.assertEqual(normalize_side_int("SELL"), -1)
        self.assertEqual(normalize_side_int("sell"), -1)
        self.assertEqual(normalize_side_int("ASK"), -1)
        self.assertEqual(normalize_side_int("-1"), -1)
    
    def test_normalize_side_int_invalid(self):
        """Test normalize_side_int with invalid inputs."""
        self.assertIsNone(normalize_side_int(None))
        self.assertIsNone(normalize_side_int(""))
        self.assertIsNone(normalize_side_int("invalid"))
        self.assertIsNone(normalize_side_int("0"))
    
    def test_side_int_to_payload(self):
        """Test side_int_to_payload conversion."""
        self.assertEqual(side_int_to_payload(1), "LONG")
        self.assertEqual(side_int_to_payload(-1), "SHORT")
        self.assertIsNone(side_int_to_payload(0))
        self.assertIsNone(side_int_to_payload(None))
        self.assertIsNone(side_int_to_payload(2))
    
    def test_to_float_or_nan_valid(self):
        """Test to_float_or_nan with valid inputs."""
        self.assertEqual(to_float_or_nan(42), 42.0)
        self.assertEqual(to_float_or_nan("3.14"), 3.14)
        self.assertEqual(to_float_or_nan(-10.5), -10.5)
    
    def test_to_float_or_nan_invalid(self):
        """Test to_float_or_nan with invalid inputs."""
        import math
        self.assertTrue(math.isnan(to_float_or_nan("invalid")))
        self.assertTrue(math.isnan(to_float_or_nan(None)))
        self.assertTrue(math.isnan(to_float_or_nan(float("inf"))))
        self.assertTrue(math.isnan(to_float_or_nan(float("nan"))))
    
    def test_to_opt_float_valid(self):
        """Test to_opt_float with valid inputs."""
        self.assertEqual(to_opt_float(42), 42.0)
        self.assertEqual(to_opt_float("3.14"), 3.14)
        self.assertEqual(to_opt_float(-10.5), -10.5)
    
    def test_to_opt_float_invalid(self):
        """Test to_opt_float with invalid inputs."""
        self.assertIsNone(to_opt_float(None))
        self.assertIsNone(to_opt_float("invalid"))
        self.assertIsNone(to_opt_float(float("inf")))
        self.assertIsNone(to_opt_float(float("nan")))


if __name__ == "__main__":
    unittest.main()
