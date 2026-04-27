"""
Integration test for SignalDispatcher with LuaScriptManager.

Tests that SignalDispatcher correctly initializes and can use LuaScriptManager.
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
import os

# Mock environment before importing
os.environ.setdefault("SIGNAL_OUTBOX_STREAM", "test:outbox")
os.environ.setdefault("SIGNAL_DLQ_STREAM", "test:dlq")


class TestSignalDispatcherLuaIntegration(unittest.TestCase):
    """Test SignalDispatcher integration with LuaScriptManager."""
    
    @patch('services.signal_dispatcher.get_redis')
    @patch('services.signal_dispatcher.logger')
    def test_dispatcher_initializes_lua_manager(self, mock_logger, mock_get_redis):
        """Test that SignalDispatcher initializes LuaScriptManager."""
        # Setup
        mock_redis = Mock()
        mock_redis.script_load = Mock(return_value="sha123")
        mock_get_redis.return_value = mock_redis
        
        # Import after mocking
        from services.signal_dispatcher import SignalDispatcher
        
        # Create dispatcher
        dispatcher = SignalDispatcher()
        
        # Verify LuaScriptManager was created
        self.assertIsNotNone(dispatcher.lua_scripts)
        self.assertEqual(dispatcher.lua_scripts.redis, mock_redis)
        
        # Verify scripts were preloaded
        self.assertGreater(mock_redis.script_load.call_count, 0)
    
    @patch('services.signal_dispatcher.get_redis')
    @patch('services.signal_dispatcher.logger')
    def test_dispatcher_handles_lua_init_failure(self, mock_logger, mock_get_redis):
        """Test that SignalDispatcher handles LuaScriptManager init failure gracefully."""
        # Setup - make script_load fail
        mock_redis = Mock()
        mock_redis.script_load = Mock(side_effect=Exception("Redis error"))
        mock_get_redis.return_value = mock_redis
        
        # Import after mocking
        from services.signal_dispatcher import SignalDispatcher
        
        # Create dispatcher - should not crash
        dispatcher = SignalDispatcher()
        
        # Verify lua_scripts is None (failed to initialize)
        self.assertIsNone(dispatcher.lua_scripts)
    
    @patch('services.signal_dispatcher.get_redis')
    def test_dispatcher_lua_scripts_available(self, mock_get_redis):
        """Test that all expected Lua scripts are available."""
        # Setup
        mock_redis = Mock()
        mock_redis.script_load = Mock(return_value="sha123")
        mock_get_redis.return_value = mock_redis
        
        # Import after mocking
        from services.signal_dispatcher import SignalDispatcher
        
        # Create dispatcher
        dispatcher = SignalDispatcher()
        
        # Verify key scripts are available
        if dispatcher.lua_scripts:
            expected_scripts = [
                "release_lease",
                "extend_lease",
                "reenqueue_and_ack",
                "dlq_and_ack",
            ]
            
            for script_name in expected_scripts:
                self.assertIn(script_name, dispatcher.lua_scripts._scripts)
    
    @patch('services.signal_dispatcher.get_redis')
    def test_dispatcher_can_execute_lua_script(self, mock_get_redis):
        """Test that dispatcher can execute Lua scripts through manager."""
        # Setup
        mock_redis = Mock()
        mock_redis.script_load = Mock(return_value="sha123")
        mock_redis.evalsha = Mock(return_value=1)
        mock_get_redis.return_value = mock_redis
        
        # Import after mocking
        from services.signal_dispatcher import SignalDispatcher
        
        # Create dispatcher
        dispatcher = SignalDispatcher()
        
        # Execute a script
        if dispatcher.lua_scripts:
            result = dispatcher.lua_scripts.execute(
                "release_lease",
                keys=["test:lease"],
                args=["token123"]
            )
            
            # Verify execution
            self.assertEqual(result, 1)
            mock_redis.evalsha.assert_called_once()


if __name__ == "__main__":
    unittest.main()
