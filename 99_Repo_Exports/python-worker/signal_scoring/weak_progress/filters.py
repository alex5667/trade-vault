"""
Fade pattern filters and preconditions.
"""

from signal_scoring.ctx import SignalContext

from .config import WeakProgressConfig


def fade_preconditions_passed(ctx: SignalContext, cfg: WeakProgressConfig) -> bool:
    """
    Mandatory conditions for fade patterns:
    - weakProgress is sufficiently low
    - strong volume/delta in current impulse

    Args:
        ctx: Signal context
        cfg: Weak progress configuration

    Returns:
        True if fade preconditions are met
    """
    # Check weak progress
    if ctx.weak_progress is None:
        return False

    if ctx.weak_progress > cfg.fade_weak_max:
        # Progress too strong -> not good for fade
        return False

    # Check high volume / Delta in current impulse
    delta_z = ctx.delta_spike_z or 0.0
    volume_z = ctx.volume_z or 0.0

    has_strong_impulse = (
        abs(delta_z) >= cfg.fade_min_delta_z or
        volume_z >= cfg.fade_min_volume_z
    )

    if not has_strong_impulse:
        # No strong impulse to fade against
        return False

    return True


def fade_confirmation_passed(ctx: SignalContext, cfg: WeakProgressConfig) -> bool:
    """
    Confirmation for fade patterns - reverse deltaSpikeZ at required level.

    Args:
        ctx: Signal context
        cfg: Weak progress configuration

    Returns:
        True if fade confirmation is present
    """
    if ctx.reverse_delta_spike_z is None:
        return False

    return abs(ctx.reverse_delta_spike_z) >= cfg.fade_confirm_delta_z


def continuation_preconditions_passed(ctx: SignalContext, cfg: WeakProgressConfig) -> bool:
    """
    Check if continuation pattern meets basic preconditions.

    For continuation, we want some minimum progress strength.
    Not as strict as fade preconditions.

    Args:
        ctx: Signal context
        cfg: Weak progress configuration

    Returns:
        True if continuation can proceed
    """
    if ctx.weak_progress is None:
        # Allow continuation even without weak_progress (less strict than fade)
        return True

    # For continuation, we prefer but don't require strong progress
    # Just check that it's not extremely weak (which would be fade territory)
    return ctx.weak_progress > 0.1  # Minimum threshold to avoid noise
