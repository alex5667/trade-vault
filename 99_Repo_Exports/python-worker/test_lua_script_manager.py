"""
Unit tests for LuaScriptManager.

Tests Lua script management and execution.
"""

import unittest
from unittest.mock import Mock, MagicMock, patch

from services.dispatcher.lua_scripts import LuaScriptManager


class TestLuaScriptManager(unittest.TestCase):
    """Test suite for LuaScriptManager."""
    
    def setUp(self):
        """Create fresh manager for each test."""
        self.redis = Mock()
        self.logger = Mock()
        self.manager = LuaScriptManager(self.redis, logger=self.logger)
    
    def test_initialization(self):
        """Test manager initializes correctly."""
        self.assertEqual(self.manager.redis, self.redis)
        self.assertEqual(len(self.manager._scripts), 7)
        self.assertEqual(len(self.manager._sha_cache), 0)
    
    def test_get_sha_loads_script(self):
        """Test getting SHA loads script."""
        self.redis.script_load.return_value = "abc123"
        
        sha = self.manager.get_sha("release_lease")
        
        self.assertEqual(sha, "abc123")
        self.redis.script_load.assert_called_once()
        self.assertEqual(self.manager._sha_cache["release_lease"], "abc123")
    
    def test_get_sha_caches_result(self):
        """Test SHA is cached after first load."""
        self.redis.script_load.return_value = "abc123"
        
        sha1 = self.manager.get_sha("release_lease")
        sha2 = self.manager.get_sha("release_lease")
        
        self.assertEqual(sha1, sha2)
        self.redis.script_load.assert_called_once()  # Only called once
    
    def test_get_sha_unknown_script(self):
        """Test getting SHA for unknown script raises error."""
        with self.assertRaises(KeyError):
            self.manager.get_sha("unknown_script")
    
    def test_execute_with_evalsha(self):
        """Test execute uses evalsha."""
        self.redis.script_load.return_value = "abc123"
        self.redis.evalsha.return_value = [1, "ok"]
        
        result = self.manager.execute(
            "release_lease",
            keys=["lease:123"],
            args=["token456"]
        )
        
        self.assertEqual(result, [1, "ok"])
        self.redis.evalsha.assert_called_once_with(
            "abc123", 1, "lease:123", "token456"
        )
    
    def test_execute_fallback_to_eval(self):
        """Test execute falls back to eval on NOSCRIPT."""
        self.redis.script_load.return_value = "abc123"
        self.redis.evalsha.side_effect = Exception("NOSCRIPT")
        self.redis.eval.return_value = [1, "ok"]
        
        result = self.manager.execute(
            "release_lease",
            keys=["lease:123"],
            args=["token456"]
        )
        
        self.assertEqual(result, [1, "ok"])
        self.redis.eval.assert_called_once()
    
    def test_preload_all(self):
        """Test preloading all scripts."""
        self.redis.script_load.return_value = "sha"
        
        self.manager.preload_all()
        
        self.assertEqual(self.redis.script_load.call_count, 7)
        self.assertEqual(len(self.manager._sha_cache), 7)
    
    def test_all_scripts_defined(self):
        """Test all expected scripts are defined."""
        expected_scripts = [
            "xadd_or_setex_then_mark",
            "notify_gate_xadd_then_mark",
            "marker_after_delivery",
            "release_lease",
            "extend_lease",
            "reenqueue_and_ack",
            "dlq_and_ack",
        ]
        
        for script_name in expected_scripts:
            self.assertIn(script_name, self.manager._scripts)
            self.assertIsInstance(self.manager._scripts[script_name], str)
            self.assertGreater(len(self.manager._scripts[script_name]), 0)


if __name__ == "__main__":
    unittest.main()
