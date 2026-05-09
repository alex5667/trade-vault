from __future__ import annotations


def bucket_from_scenario(s: str) -> str:
    """Determine bucket (trend/range/other) from scenario string.
    
    Bucket classification:
    - range: range, chop, meanrev scenarios
    - trend: trend, bull, bear, continuation scenarios
    - trend (default for reversal): reversal scenarios default to trend (safer for enforcement share routing)
    - other: unknown scenarios
    
    Args:
        s: Scenario string (e.g., "range_meanrev", "continuation", "reversal")
        
    Returns:
        Bucket name: "trend", "range", or "other"
    """
    ss = (s or "").lower()
    if "range" in ss or "chop" in ss or "meanrev" in ss:
        return "range"
    if "trend" in ss or "bull" in ss or "bear" in ss or "cont" in ss:
        return "trend"
    # reversal can be either; default to trend (safer for enforcement share routing)
    if "reversal" in ss or "rev" in ss:
        return "trend"
    return "other"

