from __future__ import annotations

def bucket_from_scenario(scenario_v4: str) -> str:
    s = (scenario_v4 or "").lower()
    if "range" in s or "meanrev" in s or "chop" in s:
        return "range"
    if "trend" in s or "continuation" in s or "reversal" in s:
        return "trend"
    return "other"

