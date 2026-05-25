"""
Unit tests for NewsAnalyzerWorker lease+done-after-success idempotency
"""
from unittest.mock import Mock, patch

from news_pipeline.analyzer_worker import NewsAnalyzerWorker


class TestAnalyzerWorkerLease:
    """Test lease mechanism prevents stuck done keys"""

    def setup_method(self):
        self.redis = Mock()
        self.worker = NewsAnalyzerWorker(redis=self.redis)
        self.worker.consumer = "test-consumer"

    def test_handle_message_already_done_skips_processing(self):
        """Test that already done messages are skipped"""
        msg_id = "msg123"
        fields = {"uid": "test-uid"}

        self.redis.get.return_value = "1"  # already done

        self.worker.handle_message(msg_id, fields)

        # Should not attempt lease or processing
        assert not self.redis.set.called

    def test_handle_message_llm_failure_is_nonfatal(self):
        """LLM failure is non-fatal: rule classifier still runs, done_key is set.

        Design: deterministic rules run first; LLM is shadow enrichment only.
        A message is marked done if rule classification + Redis writes succeed,
        regardless of LLM outcome.
        """
        msg_id = "msg123"
        fields = {"uid": "test-uid", "title": "Test", "url": "http://test.com"}

        self.redis.get.return_value = None
        self.redis.set.return_value = True
        self.redis.setex.return_value = None
        self.redis.xadd.return_value = "123-0"

        self.worker.llm = Mock()
        self.worker.llm.analyze.side_effect = Exception("LLM failed")

        with patch('news_pipeline.analyzer_worker._parse_symbols_json', return_value=["GLOBAL"]):
            self.worker.handle_message(msg_id, fields)

        # Lease must be acquired and released
        lease_calls = [call for call in self.redis.set.call_args_list if "lease" in str(call)]
        assert len(lease_calls) >= 1
        self.redis.delete.assert_called_with("news:analysis:lease:test-uid")

        # Done key IS set because rule classifier succeeded (LLM failure is non-fatal)
        done_calls = [call for call in self.redis.set.call_args_list if "done:" in str(call)]
        assert len(done_calls) == 1

    def test_handle_message_success_sets_done_after_writes(self):
        """Test done key is set only after successful writes"""
        msg_id = "msg123"
        fields = {
            "uid": "test-uid",
            "title": "Test",
            "url": "http://test.com",
            "symbols": '["GLOBAL"]'
        }

        self.redis.get.return_value = None  # not done
        self.redis.set.return_value = True  # lease acquired
        self.redis.setex.return_value = None  # mock successful setex
        self.redis.xadd.return_value = "123-0"  # mock successful xadd

        # Mock successful LLM and Redis operations
        self.worker.llm = Mock()
        self.worker.llm.analyze.return_value = {
            "risk": 0.5, "surprise": 0.3, "confidence": 0.8,
            "tags_mask": 1, "primary_tag_id": 1
        }

        with patch('time.time', return_value=1000):
            self.worker.handle_message(msg_id, fields)

        # Verify done key is set
        done_calls = [call for call in self.redis.set.call_args_list if "done:" in str(call)]
        assert len(done_calls) == 1

        # Verify lease was acquired and released
        lease_calls = [call for call in self.redis.set.call_args_list if "lease" in str(call)]
        assert len(lease_calls) >= 1
        self.redis.delete.assert_called_with("news:analysis:lease:test-uid")

        # Lease should be deleted at the end
        self.redis.delete.assert_called_with("news:analysis:lease:test-uid")

    def test_handle_message_lease_timeout_allows_retry(self):
        """Test that lease timeout allows another consumer to retry"""
        msg_id = "msg123"
        fields = {"uid": "test-uid", "title": "Test", "url": "http://test.com"}

        # First consumer gets lease
        self.redis.get.return_value = None
        self.redis.set.return_value = True

        # Mock LLM failure for first worker
        self.worker.llm = Mock()
        self.worker.llm.analyze.side_effect = Exception("LLM timeout")

        with patch('news_pipeline.analyzer_worker._parse_symbols_json', return_value=["GLOBAL"]), \
             patch('time.time', return_value=1000):

            # First attempt - should fail but release lease
            worker1 = NewsAnalyzerWorker(redis=self.redis)
            worker1.consumer = "consumer-1"
            worker1.llm = self.worker.llm
            worker1.handle_message(msg_id, fields)

        # Verify lease was released
        self.redis.delete.assert_called_with("news:analysis:lease:test-uid")

        # Second attempt with successful LLM
        self.redis.setex.return_value = None  # mock successful setex
        self.redis.xadd.return_value = "123-0"  # mock successful xadd
        worker2 = NewsAnalyzerWorker(redis=self.redis)
        worker2.consumer = "consumer-2"
        worker2.llm = Mock()
        worker2.llm.analyze.return_value = {"risk": 0.1, "surprise": 0.0, "confidence": 0.8, "tags_mask": 0, "primary_tag_id": 0}

        with patch('time.time', return_value=1000):
            worker2.handle_message(msg_id, fields)

        # Should have set done key on success
        done_calls = [call for call in self.redis.set.call_args_list if "done:" in str(call)]
        assert len(done_calls) >= 1
