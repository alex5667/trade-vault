"""
Backoff utilities for retry logic in the scanner infrastructure.

Provides exponential backoff and retry mechanisms.
"""

import time
import random
from typing import Callable, Any, Optional
from functools import wraps


class Backoff:
    """
    Exponential backoff with jitter for retry logic.
    """

    def __init__(
        self,
        base_delay: float = 1.0,
        max_delay: float = 60.0,
        multiplier: float = 2.0,
        jitter: bool = True,
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
        Get the delay for the current attempt (increments internal counter).

        Returns:
            Delay in seconds
        """
        self.attempt += 1

        # Clamp exponent to prevent OverflowError when attempt grows
        # unboundedly during prolonged outages.  Once multiplier^exp
        # exceeds max_delay/base_delay the result is capped anyway, so
        # there is no need to compute larger powers.
        try:
            import math
            if self.multiplier > 1 and self.base_delay > 0:
                max_useful_exp = math.ceil(
                    math.log(self.max_delay / self.base_delay) / math.log(self.multiplier)
                ) + 1
            else:
                max_useful_exp = 0
            exp = min(self.attempt - 1, max(max_useful_exp, 0))
        except (ValueError, ZeroDivisionError):
            exp = min(self.attempt - 1, 30)  # safe fallback

        # Exponential backoff (overflow-safe)
        try:
            delay = self.base_delay * (self.multiplier ** exp)
        except OverflowError:
            delay = self.max_delay

        # Cap at max_delay
        delay = min(delay, self.max_delay)

        # Add jitter
        if self.jitter:
            delay = delay * (0.5 + random.random() * 0.5)  # 50-100% of delay

        return delay

    def next_sleep(self) -> float:
        """Alias for get_delay() — matches call sites in consume_ticks/consume_books."""
        return self.get_delay()

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
    base_delay: float = 1.0,
    max_delay: float = 60.0,
    multiplier: float = 2.0,
    jitter: bool = True,
    max_attempts: int = 5,
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
    try:
        delay = base_delay * (2 ** min(attempt - 1, 30))
    except OverflowError:
        delay = max_delay
    return min(delay, max_delay)
