"""
Unit tests for HandlerStateManager.

Tests lifecycle state management in isolation.
"""

import time
import threading
import unittest

from handlers.lifecycle.state_manager import HandlerStateManager


class TestHandlerStateManager(unittest.TestCase):
    """Test suite for HandlerStateManager."""
    
    def setUp(self):
        """Create fresh state manager for each test."""
        self.state_manager = HandlerStateManager()
    
    def test_initial_state(self):
        """Test that manager starts in stopped state."""
        self.assertFalse(self.state_manager.is_running)
        self.assertFalse(self.state_manager.is_stopped())
        self.assertIsNone(self.state_manager.get_thread())
    
    def test_start(self):
        """Test starting the handler."""
        self.state_manager.start()
        self.assertTrue(self.state_manager.is_running)
        self.assertFalse(self.state_manager.is_stopped())
    
    def test_stop(self):
        """Test stopping the handler."""
        self.state_manager.start()
        self.state_manager.stop()
        
        self.assertFalse(self.state_manager.is_running)
        self.assertTrue(self.state_manager.is_stopped())
    
    def test_idempotent_start(self):
        """Test that multiple starts are safe."""
        self.state_manager.start()
        self.state_manager.start()
        self.assertTrue(self.state_manager.is_running)
    
    def test_idempotent_stop(self):
        """Test that multiple stops are safe."""
        self.state_manager.start()
        self.state_manager.stop()
        self.state_manager.stop()
        self.assertTrue(self.state_manager.is_stopped())
    
    def test_thread_management(self):
        """Test thread reference management."""
        thread = threading.Thread(target=lambda: None)
        
        self.state_manager.set_thread(thread)
        self.assertEqual(self.state_manager.get_thread(), thread)
        
        self.state_manager.set_thread(None)
        self.assertIsNone(self.state_manager.get_thread())
    
    def test_uptime_tracking(self):
        """Test uptime calculation."""
        uptime1 = self.state_manager.get_uptime()
        time.sleep(0.1)
        uptime2 = self.state_manager.get_uptime()
        
        self.assertGreater(uptime2, uptime1)
        self.assertGreaterEqual(uptime2 - uptime1, 0.1)
    
    def test_reset_start_time(self):
        """Test resetting start time."""
        time.sleep(0.1)
        uptime_before = self.state_manager.get_uptime()
        
        self.state_manager.reset_start_time()
        uptime_after = self.state_manager.get_uptime()
        
        self.assertLess(uptime_after, uptime_before)
        self.assertLess(uptime_after, 0.01)  # Should be near zero
    
    def test_wait_for_stop_timeout(self):
        """Test wait_for_stop with timeout."""
        start = time.time()
        result = self.state_manager.wait_for_stop(timeout=0.1)
        elapsed = time.time() - start
        
        self.assertFalse(result)  # Should timeout
        self.assertGreaterEqual(elapsed, 0.1)
    
    def test_wait_for_stop_signaled(self):
        """Test wait_for_stop when stop is signaled."""
        def stop_after_delay():
            time.sleep(0.1)
            self.state_manager.stop()
        
        thread = threading.Thread(target=stop_after_delay)
        thread.start()
        
        result = self.state_manager.wait_for_stop(timeout=1.0)
        thread.join()
        
        self.assertTrue(result)  # Should be signaled
        self.assertTrue(self.state_manager.is_stopped())
    
    def test_thread_safety(self):
        """Test concurrent access to state manager."""
        def toggle_state():
            for _ in range(100):
                self.state_manager.start()
                self.state_manager.stop()
        
        threads = [threading.Thread(target=toggle_state) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        # Should not crash and should be in a valid state
        self.assertIsInstance(self.state_manager.is_running, bool)
        self.assertIsInstance(self.state_manager.is_stopped(), bool)


if __name__ == "__main__":
    unittest.main()
