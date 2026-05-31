# contexts.py — add NewsFeatures + alias (insert near the top, after imports)

from dataclasses import dataclass

@dataclass(slots=True, frozen=True)
class NewsFeatures:
    ref: str = ""
    news_risk: float = 0.0
    surprise_score: float = 0.0
    news_grade_id: int = 0
    event_tminus_sec: int = -1
    event_grade_id: int = 0
    tags_mask: int = 0
    primary_tag_id: int = 0
    confidence: float = 0.0
    horizon_sec: int = 0
    asof_ts_ms: int = 0

# Backward compat: if older code uses NewsCtx
NewsCtx = NewsFeatures

# In OrderflowSignalContext:
#   news: Optional[NewsCtx] = None
