import os
import sys
import unittest
from unittest.mock import patch

# Allow importing the script
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'scripts')))
import ci_alerts_links_exist_check


class TestCiAlertsLinksExistCheck(unittest.TestCase):
    def test_extract_uid(self):
        # Valid cases
        self.assertEqual(ci_alerts_links_exist_check._extract_uid("/d/uid123/my-dashboard"), "uid123")
        self.assertEqual(ci_alerts_links_exist_check._extract_uid("/d-solo/uid456/panel"), "uid456")
        self.assertEqual(ci_alerts_links_exist_check._extract_uid("/d/abc-def/test?orgId=1"), "abc-def")
        
        # Invalid cases
        self.assertEqual(ci_alerts_links_exist_check._extract_uid(""), "")
        self.assertEqual(ci_alerts_links_exist_check._extract_uid("invalid_path"), "")
        self.assertEqual(ci_alerts_links_exist_check._extract_uid("/other/uid123/dash"), "")
        self.assertEqual(ci_alerts_links_exist_check._extract_uid("/d/"), "")

    @patch("ci_alerts_links_exist_check.os.path.isfile")
    def test_runbook_file_exists(self, mock_isfile):
        # Valid
        mock_isfile.return_value = True
        self.assertTrue(ci_alerts_links_exist_check._runbook_file_exists("/my_runbook.md"))
        mock_isfile.assert_called_with("monitoring/runbooks/my_runbook.md")
        
        # Explicitly checking stripping
        self.assertTrue(ci_alerts_links_exist_check._runbook_file_exists(" /my_runbook.md "))
        
        # Empty/invalid
        self.assertFalse(ci_alerts_links_exist_check._runbook_file_exists(""))
        self.assertFalse(ci_alerts_links_exist_check._runbook_file_exists("/"))
        
        # Does not exist
        mock_isfile.return_value = False
        self.assertFalse(ci_alerts_links_exist_check._runbook_file_exists("/missing.md"))


if __name__ == "__main__":
    unittest.main()
