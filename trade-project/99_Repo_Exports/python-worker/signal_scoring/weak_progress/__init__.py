from .config import PATTERN_WP_CONFIG, PatternFamily, WeakProgressConfig, get_weak_progress_config
from .filters import continuation_preconditions_passed, fade_confirmation_passed, fade_preconditions_passed
from .scorer import apply_weak_progress_and_fade_filters, compute_progress_score, validate_signal_for_weak_progress
from .utils import (
    classify_progress_strength,
    compute_weak_progress,
    is_progress_strong_for_continuation,
    is_progress_weak_for_fade,
)

__all__ = [
    "WeakProgressConfig",
    "PatternFamily",
    "PATTERN_WP_CONFIG",
    "get_weak_progress_config",
    "compute_progress_score",
    "apply_weak_progress_and_fade_filters",
    "validate_signal_for_weak_progress",
    "fade_preconditions_passed",
    "fade_confirmation_passed",
    "continuation_preconditions_passed",
    "compute_weak_progress",
    "classify_progress_strength",
    "is_progress_strong_for_continuation",
    "is_progress_weak_for_fade",
]
