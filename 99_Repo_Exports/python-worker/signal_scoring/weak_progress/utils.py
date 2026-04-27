"""
Utility functions for weak progress calculations.
"""


def compute_weak_progress(high: float, low: float, atr: float, eps: float = 1e-6) -> float:
    """
    Compute weak progress metric.

    weak_progress = |range| / ATR

    Where range is the impulse bar/cluster range, ATR is current ATR.

    Args:
        high: High price
        low: Low price
        atr: Current ATR value
        eps: Epsilon to avoid division by zero

    Returns:
        Weak progress value (0..inf), where:
        < 0.3: weak progress (good for fade)
        > 0.7: strong progress (good for continuation)
    """
    price_range = abs(high - low)
    return price_range / max(atr, eps)


def classify_progress_strength(weak_progress: float) -> str:
    """
    Classify progress strength based on weak_progress value.

    Args:
        weak_progress: Weak progress metric

    Returns:
        Classification string: "weak", "moderate", "strong"
    """
    if weak_progress <= 0.3:
        return "weak"
    elif weak_progress <= 0.7:
        return "moderate"
    else:
        return "strong"


def is_progress_strong_for_continuation(weak_progress: float, threshold: float = 0.7) -> bool:
    """Check if progress is strong enough for continuation patterns."""
    return weak_progress >= threshold


def is_progress_weak_for_fade(weak_progress: float, threshold: float = 0.35) -> bool:
    """Check if progress is weak enough for fade patterns."""
    return weak_progress <= threshold
