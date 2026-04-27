"""Meta-feature coverage guard (P29).

Pure functions to compute feature coverage and apply safety guards
when model feature missingness is too high.
"""

from types import SimpleNamespace
from typing import List, Tuple, Set


def compute_meta_feature_coverage(
    model_features: List[str],
    missing_features: List[str],
    max_list: int = 32
) -> SimpleNamespace:
    """Calculates coverage stats for meta-model features.
    
    Args:
        model_features: List of features expected by the model.
        missing_features: List of features found missing during build.
        max_list: Max missing features to include in the output list.
        
    Returns:
        SimpleNamespace with coverage, missing_rate, and feature lists.
    """
    if not model_features:
        return SimpleNamespace(
            model_total=0,
            model_missing=0,
            coverage=1.0,
            missing_rate=0.0,
            missing_model_features=[]
        )

    model_set = set(model_features)
    missing_set = set(missing_features)
    
    # Only care about features that are actually in the model set
    missing_model = [f for f in missing_features if f in model_set]
    
    total = len(model_features)
    missing_cnt = len(missing_model)
    coverage = (total - missing_cnt) / total if total > 0 else 1.0
    
    return SimpleNamespace(
        model_total=total,
        model_missing=missing_cnt,
        coverage=round(coverage, 4),
        missing_rate=round(1.0 - coverage, 4),
        missing_model_features=missing_model[:max_list]
    )


def apply_meta_coverage_guard(
    meta_mode: str,
    cov: SimpleNamespace,
    min_coverage: float = 0.85,
    max_missing: int = 999
) -> Tuple[str, str]:
    """Downgrades meta_mode from ENFORCE to SHADOW if coverage is low.
    
    Args:
        meta_mode: Current meta mode (SHADOW/ENFORCE).
        cov: Coverage object from compute_meta_feature_coverage.
        min_coverage: Minimum coverage threshold (0.0 to 1.0).
        max_missing: Maximum absolute missing model features allowed.
        
    Returns:
        tuple (new_mode, reason)
    """
    mode = str(meta_mode).upper()
    if mode != "ENFORCE":
        return mode, ""

    reason = ""
    if cov.coverage < min_coverage:
        reason = f"LOW_COVERAGE({cov.coverage:.3f}<{min_coverage:.3f})"
    elif cov.model_missing > max_missing:
        reason = f"TOO_MANY_MISSING({cov.model_missing}>{max_missing})"

    if reason:
        return "SHADOW", reason
    
    return "ENFORCE", ""
