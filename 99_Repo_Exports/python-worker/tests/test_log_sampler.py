"""
Tests for LogSampler utility.
"""

import pytest
from handlers.crypto_orderflow.utils.log_sampler import LogSampler, LogSamplerFactory, sampled_info


class TestLogSampler:
    """Test LogSampler functionality."""

    def test_basic_sampling(self):
        """Test basic sampling behavior."""
        sampler = LogSampler(sample_rate=3)

        # First message should be logged
        assert sampler.should_log("test") is True
        assert sampler.should_log("test") is False
        assert sampler.should_log("test") is False

        # Fourth message should be logged
        assert sampler.should_log("test") is True

    def test_different_keys(self):
        """Test different keys are sampled independently."""
        sampler = LogSampler(sample_rate=2)

        # Key "a" - should log 1st and 3rd messages
        assert sampler.should_log("a") is True
        assert sampler.should_log("a") is False
        assert sampler.should_log("a") is True

        # Key "b" - should log 1st message
        assert sampler.should_log("b") is True
        assert sampler.should_log("b") is False

    def test_stats(self):
        """Test stats collection."""
        sampler = LogSampler(sample_rate=2)

        sampler.should_log("test1")
        sampler.should_log("test1")
        sampler.should_log("test2")

        stats = sampler.get_stats()
        assert stats["test1"] == 2
        assert stats["test2"] == 1

    def test_threading_disabled(self):
        """Test non-threaded version."""
        sampler = LogSampler(sample_rate=2, use_threading=False)

        assert sampler.should_log("test") is True
        assert sampler.should_log("test") is False
        assert sampler.should_log("test") is True


class TestLogSamplerFactory:
    """Test LogSamplerFactory functionality."""

    def test_factory_creates_samplers(self):
        """Test factory creates and reuses samplers."""
        sampler1 = LogSamplerFactory.get_sampler("test_factory", 5)
        sampler2 = LogSamplerFactory.get_sampler("test_factory", 10)  # Different rate should be ignored

        assert sampler1 is sampler2
        assert sampler1.sample_rate == 5

    def test_factory_stats(self):
        """Test factory stats collection."""
        LogSamplerFactory._instances.clear()  # Reset for test

        sampler = LogSamplerFactory.get_sampler("test_stats", 2)
        sampler.should_log("test_stats")
        sampler.should_log("test_stats")

        stats = LogSamplerFactory.get_stats()
        assert "test_stats" in stats
        assert stats["test_stats"]["test_stats"] == 2


def test_sampled_info_no_logger_error():
    """Test sampled_info doesn't crash without logger."""
    # This should not raise an exception
    try:
        sampled_info(None, "test", "Test message")
    except Exception:
        pytest.fail("sampled_info should handle None logger gracefully")
