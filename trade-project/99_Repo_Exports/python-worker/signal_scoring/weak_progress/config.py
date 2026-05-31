from __future__ import annotations

"""
Weak Progress configuration for signal scoring.
"""

from dataclasses import dataclass
from typing import Literal

PatternFamily = Literal["continuation", "fade", "other"]


@dataclass
class WeakProgressConfig:
    """Configuration for weak progress scoring and filtering."""
    family: PatternFamily

    # Continuation: strong progress = bonus
    cont_strong_min: float = 0.7    # >= this -> strong progress (in ATR)
    cont_weak_max: float = 0.3      # <= this -> weak progress

    # Fade: weak progress is mandatory
    fade_weak_max: float = 0.35     # mandatory condition for weakProgress in fade
    fade_min_delta_z: float = 1.8   # high volume/Delta threshold for current impulse
    fade_min_volume_z: float = 1.5  # alternative volume-based threshold
    fade_confirm_delta_z: float = 1.5  # confirming reverse deltaSpikeZ

    # Contribution to confidence score
    bonus_cont_strong: int = 12     # +X for continuation with strong progress
    penalty_cont_weak: int = 15     # -Z for continuation with weak progress

    bonus_fade_weak: int = 10       # +Y for fade with weak progress
    penalty_fade_strong: int = 10   # penalty if progress too strong for fade

    # What to do if weakProgress not calculated
    missing_wp_penalty: int = 10    # penalty to score if no weakProgress


# Default configurations for different patterns
PATTERN_WP_CONFIG: dict[str, WeakProgressConfig] = {
    # Continuation patterns - need strong progress
    "breakout_R1": WeakProgressConfig(
        family="continuation",
        cont_strong_min=0.8,  # Higher threshold for strong breakouts
        bonus_cont_strong=15,  # Higher bonus
    ),
    "breakout_R2": WeakProgressConfig(
        family="continuation",
        cont_strong_min=0.75,
        bonus_cont_strong=12,
    ),
    "trend_continuation": WeakProgressConfig(
        family="continuation",
        cont_strong_min=0.7,
        bonus_cont_strong=10,
    ),
    "momentum_continuation": WeakProgressConfig(
        family="continuation",
        cont_strong_min=0.8,
        bonus_cont_strong=14,
    ),

    # Fade patterns - need weak progress
    "fade_PDH": WeakProgressConfig(
        family="fade",
        fade_weak_max=0.3,  # Stricter weak progress requirement
        fade_min_delta_z=2.0,  # Need stronger impulse to fade
        bonus_fade_weak=12,
        penalty_fade_strong=15,  # Higher penalty for strong progress in fade
    ),
    "fade_PDL": WeakProgressConfig(
        family="fade",
        fade_weak_max=0.3,
        fade_min_delta_z=2.0,
        bonus_fade_weak=12,
    ),
    "fade_HTF_OB": WeakProgressConfig(
        family="fade",
        fade_weak_max=0.35,  # Slightly more lenient
        fade_min_delta_z=1.8,
        bonus_fade_weak=10,
    ),
    "fade_liquidity": WeakProgressConfig(
        family="fade",
        fade_weak_max=0.4,
        fade_min_delta_z=1.5,
        bonus_fade_weak=8,
    ),

    # Absorption patterns - mixed requirements
    "absorption": WeakProgressConfig(
        family="fade",  # Treat as fade-like
        fade_weak_max=0.4,
        fade_min_delta_z=1.5,
        bonus_fade_weak=8,
    ),

    # Extreme patterns - special handling
    "extreme_high": WeakProgressConfig(
        family="continuation",
        cont_strong_min=0.9,  # Need very strong progress
        bonus_cont_strong=20,
        penalty_cont_weak=20,  # High penalty for weak
    ),

    # Default fallback
    "default_continuation": WeakProgressConfig(family="continuation"),
    "default_fade": WeakProgressConfig(family="fade"),
    "default_other": WeakProgressConfig(family="other"),
}


def get_weak_progress_config(pattern_name: str | None) -> WeakProgressConfig:
    """
    Get weak progress configuration for a pattern.

    Args:
        pattern_name: Name of the signal pattern

    Returns:
        WeakProgressConfig for the pattern
    """
    if not pattern_name:
        return PATTERN_WP_CONFIG["default_other"]

    # Direct match
    if pattern_name in PATTERN_WP_CONFIG:
        return PATTERN_WP_CONFIG[pattern_name]

    # Pattern family inference
    pattern_lower = pattern_name.lower()
    if any(word in pattern_lower for word in ["breakout", "continuation", "momentum"]):
        return PATTERN_WP_CONFIG["default_continuation"]
    elif any(word in pattern_lower for word in ["fade", "absorption", "reversal"]):
        return PATTERN_WP_CONFIG["default_fade"]

    return PATTERN_WP_CONFIG["default_other"]
