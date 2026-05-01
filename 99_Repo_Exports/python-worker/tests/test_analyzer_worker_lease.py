from __future__ import annotations
"""
Tests for analyzer_worker lease-based idempotency.
"""

import json
import os
from unittest.mock import Mock, patch

import pytest

from news_pipeline.analyzer_worker import NewsAnalyzerWorker


class MockRedis:
    def __init__(self):
        self.data = {}
        self.streams = {}

    def get(self, key):
        return self.data.get(key)

    def set(self, key, value, **kwargs):
        self.data[key] = value
        if "ex" in kwargs:
            # In real Redis this would expire, but for testing we'll just set
            pass
        return True

    def setex(self, key, ttl, value):
        self.data[key] = value
        return True

    def delete(self, key):
        self.data.pop(key, None)
        return 1

    def xadd(self, stream, fields, **kwargs):
        if stream not in self.streams:
            self.streams[stream] = []
        self.streams[stream].append(fields)
        return "123-0"


def test_analyzer_lease_prevents_duplicate_processing():
    """Test that lease prevents duplicate processing of same UID"""
    r = MockRedis()
    worker = NewsAnalyzerWorker(redis=r)

    # Mock LLM to avoid actual API calls
    worker.llm = Mock()
    worker.llm.analyze.return_value = {
        "risk": 0.8,
        "surprise": 0.6,
        "confidence": 0.9,
        "tags_mask": 1,
        "primary_tag_id": 1
    }

    fields1 = {
        "uid": "test-uid-1",
        "title": "Test News",
        "url": "https://example.com",
        "source": "test",
        "summary": "Test summary",
        "published_ts_ms": "1000",
        "symbols": '["BTCUSDT"]'
    }

    fields2 = {
        "uid": "test-uid-1",  # Same UID
        "title": "Test News Duplicate",
        "url": "https://example.com/dup",
        "source": "test",
        "summary": "Test summary duplicate",
        "published_ts_ms": "1000",
        "symbols": '["ETHUSDT"]'
    }

    # First call should process
    worker.handle_message("msg-1", fields1)

    # Check that processing happened
    done_key = "news:analysis:done:test-uid-1"
    lease_key = "news:analysis:lease:test-uid-1"
    heavy_key = "news:analysis:test-uid-1"

    assert done_key in r.data
    assert heavy_key in r.data
    assert lease_key not in r.data  # Should be cleaned up

    # Verify stream emission
    assert "news:analysis" in r.streams
    assert len(r.streams["news:analysis"]) == 1

    # Second call with same UID should be skipped
    worker.handle_message("msg-2", fields2)

    # Should still have only one stream entry
    assert len(r.streams["news:analysis"]) == 1


def test_analyzer_lease_allows_different_uids():
    """Test that different UIDs are processed independently"""
    r = MockRedis()
    worker = NewsAnalyzerWorker(redis=r)

    # Mock LLM
    worker.llm = Mock()
    worker.llm.analyze.return_value = {
        "risk": 0.5,
        "surprise": 0.3,
        "confidence": 0.8,
        "tags_mask": 2,
        "primary_tag_id": 2
    }

    fields1 = {
        "uid": "test-uid-1",
        "title": "Test News 1",
        "url": "https://example.com/1",
        "source": "test",
        "summary": "Test summary 1",
        "published_ts_ms": "1000",
        "symbols": '["BTCUSDT"]'
    }

    fields2 = {
        "uid": "test-uid-2",  # Different UID
        "title": "Test News 2",
        "url": "https://example.com/2",
        "source": "test",
        "summary": "Test summary 2",
        "published_ts_ms": "2000",
        "symbols": '["ETHUSDT"]'
    }

    # Both should be processed
    worker.handle_message("msg-1", fields1)
    worker.handle_message("msg-2", fields2)

    # Check both were processed
    assert "news:analysis:done:test-uid-1" in r.data
    assert "news:analysis:done:test-uid-2" in r.data
    assert "news:analysis:test-uid-1" in r.data
    assert "news:analysis:test-uid-2" in r.data

    # Verify stream emissions
    assert "news:analysis" in r.streams
    assert len(r.streams["news:analysis"]) == 2


def test_analyzer_lease_cleanup_on_error():
    """Test that lease is cleaned up even when processing fails"""
    r = MockRedis()
    worker = NewsAnalyzerWorker(redis=r)

    # Mock LLM to raise exception
    worker.llm = Mock()
    worker.llm.analyze.side_effect = Exception("LLM failed")

    fields = {
        "uid": "test-uid-fail",
        "title": "Test News Fail",
        "url": "https://example.com/fail",
        "source": "test",
        "summary": "Test summary fail",
        "published_ts_ms": "1000",
        "symbols": '["BTCUSDT"]'
    }

    # Processing should fail but lease should be cleaned up
    worker.handle_message("msg-fail", fields)

    # Done key should NOT be set (processing failed)
    done_key = "news:analysis:done:test-uid-fail"
    lease_key = "news:analysis:lease:test-uid-fail"
    heavy_key = "news:analysis:test-uid-fail"

    assert done_key not in r.data  # Not marked as done
    assert heavy_key not in r.data  # Heavy store failed
    assert lease_key not in r.data  # Lease cleaned up in finally

    # No stream emissions
    assert "news:analysis" not in r.streams or len(r.streams["news:analysis"]) == 0


def test_analyzer_lease_blocks_concurrent_processing():
    """Test that lease prevents concurrent processing of same UID"""
    r = MockRedis()
    worker1 = NewsAnalyzerWorker(redis=r)
    worker2 = NewsAnalyzerWorker(redis=r)

    # Mock LLM for both workers
    worker1.llm = Mock()
    worker1.llm.analyze.return_value = {
        "risk": 0.7,
        "surprise": 0.5,
        "confidence": 0.85,
        "tags_mask": 4,
        "primary_tag_id": 3
    }

    worker2.llm = Mock()
    worker2.llm.analyze.return_value = {
        "risk": 0.6,
        "surprise": 0.4,
        "confidence": 0.75,
        "tags_mask": 8,
        "primary_tag_id": 4
    }

    fields = {
        "uid": "test-uid-concurrent",
        "title": "Test News Concurrent",
        "url": "https://example.com/concurrent",
        "source": "test",
        "summary": "Test summary concurrent",
        "published_ts_ms": "1000",
        "symbols": '["BTCUSDT"]'
    }

    # First worker gets the lease
    worker1.handle_message("msg-concurrent", fields)

    # Check first worker processed
    done_key = "news:analysis:done:test-uid-concurrent"
    assert done_key in r.data

    # Second worker should be blocked by lease check
    # (In real Redis the lease would still exist, but in our mock it's cleaned up)
    # So we'll manually set a lease to simulate concurrent scenario
    lease_key = "news:analysis:lease:test-uid-concurrent"
    r.data[lease_key] = "news-analyzer-1"  # Simulate existing lease with CONSUMER value

    # Second worker should skip processing
    worker2.handle_message("msg-concurrent-2", fields)

    # Should still have only one stream entry
    stream_entries = r.streams.get("news:analysis", [])
    assert len(stream_entries) == 1
