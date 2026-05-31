"""
Weak progress scoring logic for signal confidence.
"""

from signal_scoring.ctx import SignalContext

from .config import WeakProgressConfig, get_weak_progress_config
from .filters import continuation_preconditions_passed, fade_confirmation_passed, fade_preconditions_passed
from .utils import classify_progress_strength


def compute_progress_score(ctx: SignalContext, cfg: WeakProgressConfig) -> int:
    """
    Return integer correction to confidence based on weakProgress.

    Args:
        ctx: Signal context
        cfg: Weak progress configuration

    Returns:
        Score delta (-100..+100) to add to base confidence
    """
    # If no weakProgress calculated - optional penalty
    if ctx.weak_progress is None:
        return -cfg.missing_wp_penalty

    wp = ctx.weak_progress
    score_delta = 0

    if cfg.family == "continuation":
        # Continuation signal should "catch up" to ATR
        if wp >= cfg.cont_strong_min:
            # Strong progress -> strengthen continuation
            score_delta += cfg.bonus_cont_strong
        elif wp <= cfg.cont_weak_max:
            # Weak progress -> candidate for fade, weaken continuation
            score_delta -= cfg.penalty_cont_weak

    elif cfg.family == "fade":
        # Fade signal: weak progress is required
        if wp <= cfg.fade_weak_max:
            score_delta += cfg.bonus_fade_weak
        else:
            # Too strong progress -> bad idea to fade, penalize
            score_delta -= cfg.penalty_fade_strong

    # 'other' - can leave as 0 or add custom logic
    return score_delta


def apply_weak_progress_and_fade_filters(
    ctx: SignalContext,
    pattern_cfg: WeakProgressConfig,
    base_conf: int,
) -> int:
    """
    1) For fade patterns, check mandatory conditions (weakProgress + volume/Delta)
    2) Calculate progress_score and adjust base_conf

    Args:
        ctx: Signal context
        pattern_cfg: Weak progress configuration for this pattern
        base_conf: Base confidence from other factors (0-100)

    Returns:
        Final confidence (0-100)
    """
    # 1. Hard filters based on pattern family
    if pattern_cfg.family == "fade":
        # Strict fade preconditions
        if not fade_preconditions_passed(ctx, pattern_cfg):
            # Fade preconditions not met -> signal doesn't qualify
            ctx.confidence = 0
            ctx.progress_score_component = -100  # Hard rejection
            return 0

        if not fade_confirmation_passed(ctx, pattern_cfg):
            # No confirming reverse deltaSpikeZ -> heavily penalize
            ctx.confidence = 0
            ctx.progress_score_component = -100  # Hard rejection
            return 0

    elif pattern_cfg.family == "continuation":
        # Continuation preconditions (less strict)
        if not continuation_preconditions_passed(ctx, pattern_cfg):
            # Continuation doesn't meet basic criteria -> penalize but don't kill
            ctx.confidence = max(0, base_conf - 20)
            ctx.progress_score_component = -20
            return ctx.confidence

    # 2. Progress score component
    progress_delta = compute_progress_score(ctx, pattern_cfg)
    ctx.progress_score_component = progress_delta

    # 3. Apply to base confidence
    conf = base_conf + progress_delta
    conf = max(0, min(100, conf))

    ctx.confidence = conf
    return conf


def validate_signal_for_weak_progress(ctx: SignalContext) -> dict:
    """
    Validate signal against weak progress requirements.
    Returns diagnostic information.

    Args:
        ctx: Signal context

    Returns:
        Dict with validation results and diagnostics
    """
    cfg = get_weak_progress_config(ctx.pattern_name)

    result = {
        "pattern_family": cfg.family,
        "weak_progress": ctx.weak_progress,
        "progress_strength": None,
        "fade_preconditions": None,
        "fade_confirmation": None,
        "continuation_preconditions": None,
        "progress_score": 0,
        "is_valid": True,
        "reasons": [],
    }

    # Classify progress strength
    if ctx.weak_progress is not None:
        result["progress_strength"] = classify_progress_strength(ctx.weak_progress)

    # Check preconditions based on family
    if cfg.family == "fade":
        result["fade_preconditions"] = fade_preconditions_passed(ctx, cfg)
        result["fade_confirmation"] = fade_confirmation_passed(ctx, cfg)

        if not result["fade_preconditions"]:
            result["is_valid"] = False
            result["reasons"].append("fade_preconditions_failed")

        if not result["fade_confirmation"]:
            result["is_valid"] = False
            result["reasons"].append("fade_confirmation_failed")

    elif cfg.family == "continuation":
        result["continuation_preconditions"] = continuation_preconditions_passed(ctx, cfg)
        if not result["continuation_preconditions"]:
            result["is_valid"] = False
            result["reasons"].append("continuation_preconditions_failed")

    # Calculate progress score
    result["progress_score"] = compute_progress_score(ctx, cfg)

    return result
