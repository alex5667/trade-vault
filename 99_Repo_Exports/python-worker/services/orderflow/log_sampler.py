"""
Log Sampling Utility

Provides configurable log sampling to reduce log noise by outputting only every N-th
similar message. Useful for high-frequency logging scenarios like Grafana update checks
metrics reporting, or repetitive status messages.

Environment Variables:
    LOG_SAMPLE_{NAME}_RATE - Sample rate for specific sampler (default: 1000)
    LOG_SAMPLE_{NAME}_THREADING - Use threading (default: true)

Examples:
    LOG_SAMPLE_UPDATE_CHECK_RATE=5000           # Every 5000th update check message
    LOG_SAMPLE_METRICS_RATE=100                 # Every 100th metrics message
    LOG_SAMPLE_PERIODIC_REPORTER_SUMMARY_RATE=10000  # Every 10000th summary message (default)
    LOG_SAMPLE_PERIODIC_REPORTER_TRIGGER_RATE=5000    # Every 5000th trigger message
    LOG_SAMPLE_DQ_VETO_RATE=500        # Every 500th data quality veto

Usage:
    from handlers.crypto_orderflow.utils.log_sampler import sampled_info, LogSamplerFactory

    # Simple usage - will sample every 1000th message by default
    sampled_info(logger, "my_log_type", "Message: %s", value)

    # Custom rate
    sampler = LogSamplerFactory.get_sampler("custom", 500)
    if sampler.should_log("custom"):
        logger.info("Custom message")

    # Check stats
    stats = LogSamplerFactory.get_stats()
    print(f"Log sampling stats: {stats}")
"""

from __future__ import annotations

import os
import threading
from typing import Optional, Dict, Any
from collections import defaultdict
import logging


def _env_int(name: str, default: int) -> int:
    """Безопасное извлечение int из ENV."""
    try:
        return int(os.getenv(name, str(default)) or default)
    except Exception:
        return int(default)


class LogSampler:
    """
    Configurable log sampler that outputs only every N-th message of the same type.

    Usage:
        sampler = LogSampler(sample_rate=1000)  # every 1000th message

        # Instead of:
        # logger.info("Update check succeeded")

        # Use:
        if sampler.should_log("update_check"):
            logger.info("Update check succeeded")
    """

    def __init__(self, sample_rate: int = 1000, *, use_threading: bool = True):
        """
        Args:
            sample_rate: Output every N-th message (default: 1000)
            use_threading: Use thread-safe counters (default: True)
        """
        self.sample_rate = max(1, sample_rate)
        self.use_threading = use_threading

        if use_threading:
            self._lock = threading.Lock()
            self._counters: Dict[str, int] = {}
        else:
            self._counters = defaultdict(int)

    def should_log(self, key: str) -> bool:
        """
        Check if message with given key should be logged.

        Args:
            key: Unique identifier for message type (e.g., "update_check", "metrics_report")

        Returns:
            True if message should be logged, False otherwise
        """
        if self.use_threading:
            with self._lock:
                return self._should_log_thread_unsafe(key)
        else:
            return self._should_log_thread_unsafe(key)

    def _should_log_thread_unsafe(self, key: str) -> bool:
        """Thread-unsafe implementation."""
        counter = self._counters.get(key, 0) + 1
        self._counters[key] = counter
        return (counter - 1) % self.sample_rate == 0

    def get_stats(self) -> Dict[str, Any]:
        """
        Get current sampling statistics.

        Returns:
            Dict with counters for each key
        """
        if self.use_threading:
            with self._lock:
                return dict(self._counters)
        else:
            return dict(self._counters)

    def reset(self, key: Optional[str] = None) -> None:
        """
        Reset counters.

        Args:
            key: Specific key to reset, or None to reset all
        """
        if self.use_threading:
            with self._lock:
                if key is None:
                    self._counters.clear()
                else:
                    self._counters.pop(key, None)
        else:
            if key is None:
                self._counters.clear()
            else:
                self._counters.pop(key, None)


class LogSamplerFactory:
    """
    Factory for creating configured LogSampler instances.

    Supports environment-based configuration for different log types.
    """

    _instances: Dict[str, LogSampler] = {}
    _lock = threading.Lock()

    @classmethod
    def get_sampler(cls, name: str, default_rate: int = 1000) -> LogSampler:
        """
        Get or create a LogSampler instance with environment-configured rate.

        Environment variables:
            LOG_SAMPLE_{NAME}_RATE - sample rate for specific sampler (default: 1000)
            LOG_SAMPLE_{NAME}_THREADING - use threading (default: true)

        Examples:
            LOG_SAMPLE_UPDATE_CHECK_RATE=5000  # every 5000th update check message
            LOG_SAMPLE_METRICS_RATE=100        # every 100th metrics message
        """
        with cls._lock:
            name_str = str(name)
            if name_str in cls._instances:
                return cls._instances[name_str]

            # Environment-based configuration
            name_str = str(name)
            env_rate = _env_int(f"LOG_SAMPLE_{name_str.upper()}_RATE", default_rate)
            env_threading = os.getenv(f"LOG_SAMPLE_{name_str.upper()}_THREADING", "1").lower() in ("1", "true", "yes", "on")

            sampler = LogSampler(sample_rate=env_rate, use_threading=env_threading)
            cls._instances[name_str] = sampler
            return sampler

    @classmethod
    def get_stats(cls) -> Dict[str, Dict[str, Any]]:
        """Get stats for all samplers."""
        with cls._lock:
            return {name: sampler.get_stats() for name, sampler in cls._instances.items()}


# Pre-configured samplers for common use cases
update_check_sampler = LogSamplerFactory.get_sampler("UPDATE_CHECK", 1000)
metrics_sampler = LogSamplerFactory.get_sampler("METRICS", 100)
health_sampler = LogSamplerFactory.get_sampler("HEALTH", 1000)
diagnostic_sampler = LogSamplerFactory.get_sampler("DIAGNOSTIC", 100)

# Periodic reporter samplers (higher rates for less frequent logging)
periodic_reporter_summary = LogSamplerFactory.get_sampler("PERIODIC_REPORTER_SUMMARY", 10000)
periodic_reporter_trigger = LogSamplerFactory.get_sampler("PERIODIC_REPORTER_TRIGGER", 5000)
periodic_reporter_formation = LogSamplerFactory.get_sampler("PERIODIC_REPORTER_FORMATION", 1000)
periodic_reporter_metrics_zset = LogSamplerFactory.get_sampler("PERIODIC_REPORTER_METRICS_ZSET", 1000)
periodic_reporter_metrics_stream = LogSamplerFactory.get_sampler("PERIODIC_REPORTER_METRICS_STREAM", 1000)
periodic_reporter_send_report = LogSamplerFactory.get_sampler("PERIODIC_REPORTER_SEND_REPORT", 500)
periodic_reporter_skip_insufficient = LogSamplerFactory.get_sampler("PERIODIC_REPORTER_SKIP_INSUFFICIENT", 1000)
periodic_reporter_no_trades = LogSamplerFactory.get_sampler("PERIODIC_REPORTER_NO_TRADES", 10000)


def sampled_log(logger: logging.Logger, level: int, key: str, message: str, *args, **kwargs) -> None:
    """
    Log message only if sampler allows it.

    Args:
        logger: Logger instance
        level: Logging level (logging.INFO, logging.DEBUG, etc.)
        key: Sampler key
        message: Log message
        *args, **kwargs: Additional arguments for logger.log()
    """
    key_str = str(key)
    sampler = LogSamplerFactory.get_sampler(key_str)
    if sampler.should_log(key_str):
        if logger:
            logger.log(level, message, *args, **kwargs)


def sampled_info(logger: logging.Logger, key: str, message: str, *args, **kwargs) -> None:
    """Sampled info logging."""
    sampled_log(logger, logging.INFO, key, message, *args, **kwargs)


def sampled_warning(logger: logging.Logger, key: str, message: str, *args, **kwargs) -> None:
    """Sampled warning logging."""
    sampled_log(logger, logging.WARNING, key, message, *args, **kwargs)


def sampled_error(logger: logging.Logger, key: str, message: str, *args, **kwargs) -> None:
    """Sampled error logging."""
    sampled_log(logger, logging.ERROR, key, message, *args, **kwargs)


def sampled_debug(logger: logging.Logger, key: str, message: str, *args, **kwargs) -> None:
    """Sampled debug logging."""
    sampled_log(logger, logging.DEBUG, key, message, *args, **kwargs)


# Backwards compatibility aliases
LogSamplerUtil = LogSampler  # for migration from other implementations
