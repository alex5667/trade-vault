"""
Tests for ChannelStatusChecker — no real Redis needed (uses unittest.mock).
"""
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import unittest
from unittest.mock import MagicMock, patch
import logging


class TestChannelStatusChecker(unittest.TestCase):

    def _make_checker(self, redis_mock=None):
        from app.channel_status import ChannelStatusChecker
        r = redis_mock or MagicMock()
        logger = logging.getLogger("test")
        return ChannelStatusChecker(r, logger), r

    # ------------------------------------------------------------------ #
    # get_channel_status
    # ------------------------------------------------------------------ #

    def test_active_status_returned(self):
        checker, r = self._make_checker()
        r.hget.return_value = "ACTIVE"
        status = checker.get_channel_status("testchan")
        self.assertEqual(status, "ACTIVE")

    def test_fallback_to_plain_get(self):
        checker, r = self._make_checker()
        r.hget.return_value = None
        r.get.return_value = "INACTIVE"
        status = checker.get_channel_status("testchan")
        self.assertEqual(status, "INACTIVE")

    def test_missing_status_returns_none(self):
        checker, r = self._make_checker()
        r.hget.return_value = None
        r.get.return_value = None
        status = checker.get_channel_status("testchan")
        self.assertIsNone(status)

    # ------------------------------------------------------------------ #
    # is_channel_active
    # ------------------------------------------------------------------ #

    def test_active_channel_is_active(self):
        checker, r = self._make_checker()
        r.hget.return_value = "ACTIVE"
        self.assertTrue(checker.is_channel_active("chan"))

    def test_inactive_channel_not_active(self):
        checker, r = self._make_checker()
        r.hget.return_value = "INACTIVE"
        self.assertFalse(checker.is_channel_active("chan"))

    def test_archived_channel_not_active(self):
        checker, r = self._make_checker()
        r.hget.return_value = "ARCHIVED"
        self.assertFalse(checker.is_channel_active("chan"))

    def test_missing_status_considered_active(self):
        # backwards compat: no status key → treat as ACTIVE
        checker, r = self._make_checker()
        r.hget.return_value = None
        r.get.return_value = None
        self.assertTrue(checker.is_channel_active("chan"))

    # ------------------------------------------------------------------ #
    # filter_active_channels
    # ------------------------------------------------------------------ #

    def test_filter_removes_inactive(self):
        checker, r = self._make_checker()

        def hget_side(key, field):
            if "inactive" in key:
                return "INACTIVE"
            return "ACTIVE"

        r.hget.side_effect = hget_side
        channels = ["active_chan", "inactive_one", "active_two"]
        result = checker.filter_active_channels(channels)
        # Only the two active ones should be returned
        self.assertIn("active_chan", result)
        self.assertIn("active_two", result)
        self.assertNotIn("inactive_one", result)

    def test_integer_ids_always_pass(self):
        checker, r = self._make_checker()
        channels = [12345, 67890]
        result = checker.filter_active_channels(channels)
        self.assertEqual(result, channels)

    # ------------------------------------------------------------------ #
    # set_channel_status
    # ------------------------------------------------------------------ #

    def test_set_channel_status_calls_redis_set(self):
        checker, r = self._make_checker()
        r.set.return_value = True
        success = checker.set_channel_status("mychan", "ACTIVE")
        self.assertTrue(success)
        r.set.assert_called_once()

    def test_set_channel_status_exception_returns_false(self):
        checker, r = self._make_checker()
        r.set.side_effect = Exception("redis down")
        success = checker.set_channel_status("mychan", "ACTIVE")
        self.assertFalse(success)


if __name__ == "__main__":
    unittest.main()
