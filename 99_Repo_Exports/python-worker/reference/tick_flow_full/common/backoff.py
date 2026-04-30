"""
Backoff utilities for retry logic in the scanner infrastructure.

Provides exponential backoff and retry mechanisms.
"""

import time
import random
from typing import Callable, Any, Optional, Union
from functools import wraps


class Backoff:
    """
    Exponential backoff with jitter for retry logic.
    """

    def __init__(
        self
        base_delay: float = 1.0
        max_delay: float = 60.0
        multiplier: float = 2.0
        jitter: bool = True
        max_attempts: Optional[int] = None
    ):
        """
        Initialize backoff strategy.

        Args:
            base_delay: Base delay in seconds
            max_delay: Maximum delay in seconds
            multiplier: Delay multiplier for each attempt
            jitter: Whether to add random jitter
            max_attempts: Maximum number of attempts (None for unlimited)
        """
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.multiplier = multiplier
        self.jitter = jitter
        self.max_attempts = max_attempts
        self.attempt = 0

    def reset(self):
        """Reset the backoff counter."""
        self.attempt = 0

    def get_delay(self) -> float:
        """
        Get the delay for the current attempt.

        Returns:
            Delay in seconds
        """
        self.attempt += 1

        # Exponential backoff
        delay = self.base_delay * (self.multiplier ** (self.attempt - 1))

        # Cap at max_delay
        delay = min(delay, self.max_delay)

        # Add jitter
        if self.jitter:
            delay = delay * (0.5 + random.random() * 0.5)  # 50-100% of delay

        return delay

    def should_retry(self) -> bool:
        """
        Check if should retry based on max_attempts.

        Returns:
            True if should retry, False otherwise
        """
        if self.max_attempts is None:
            return True
        return self.attempt < self.max_attempts


def retry_with_backoff(
    base_delay: float = 1.0
    max_delay: float = 60.0
    multiplier: float = 2.0
    jitter: bool = True
    max_attempts: int = 5
    exceptions: tuple = (Exception,)
):
    """
    Decorator for retrying functions with exponential backoff.

    Args:
        base_delay: Base delay in seconds
        max_delay: Maximum delay in seconds
        multiplier: Delay multiplier
        jitter: Whether to add jitter
        max_attempts: Maximum number of attempts
        exceptions: Tuple of exceptions to catch

    Returns:
        Decorated function
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            backoff = Backoff(base_delay, max_delay, multiplier, jitter, max_attempts)

            while backoff.should_retry():
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    delay = backoff.get_delay()
                    if backoff.should_retry():
                        time.sleep(delay)
                    else:
                        raise e

            # Should not reach here, but just in case
            raise RuntimeError("Retry logic failed")

        return wrapper
    return decorator


def sleep_s(seconds: float):
    """
    Sleep for the specified number of seconds.

    Args:
        seconds: Number of seconds to sleep
    """
    time.sleep(seconds)


def exponential_backoff_delay(attempt: int, base_delay: float = 1.0, max_delay: float = 60.0) -> float:
    """
    Calculate exponential backoff delay.

    Args:
        attempt: Current attempt number (starting from 1)
        base_delay: Base delay in seconds
        max_delay: Maximum delay in seconds

    Returns:
        Delay in seconds
    """
    delay = base_delay * (2 ** (attempt - 1))
    return min(delay, max_delay)
