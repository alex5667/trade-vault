from __future__ import annotations

import json
import math
from dataclasses import dataclass
from typing import Any, Optional

@dataclass(frozen=True)
class SentimentContextSnapshot:
    schema_version: int
    provider: str
    ts_ms: int
    ingest_ts_ms: int

    fear_greed_value: int
    fear_greed_delta_1d: int
    fear_greed_delta_7d: int
    sentiment_regime: str
    sentiment_risk_multiplier: float
    value_classification: str
    time_until_update_sec: int
    quality_status: str

def _f(v: Any, d: float = 0.0) -> float:
    try:
        x = float(v)
        return x if math.isfinite(x) else d
    except Exception:
        return d

def _i(v: Any, d: int = 0) -> int:
    try:
        return int(float(v))
    except Exception:
        return d

async def aread_sentiment_context(redis) -> Optional[SentimentContextSnapshot]:
    if redis is None:
        return None

    try:
        raw = await redis.get("ctx:sentiment:global")
        if raw is None:
            return None
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", errors="replace")
        obj = json.loads(raw)
    except Exception:
        return None

    if not isinstance(obj, dict):
        return None

    return SentimentContextSnapshot(
        schema_version=_i(obj.get("schema_version"), 1)
        provider=str(obj.get("provider") or "alternative_me")
        ts_ms=_i(obj.get("ts_ms"))
        ingest_ts_ms=_i(obj.get("ingest_ts_ms"))

        fear_greed_value=_i(obj.get("fear_greed_value"))
        fear_greed_delta_1d=_i(obj.get("fear_greed_delta_1d"))
        fear_greed_delta_7d=_i(obj.get("fear_greed_delta_7d"))
        sentiment_regime=str(obj.get("sentiment_regime") or "unknown")
        sentiment_risk_multiplier=_f(obj.get("sentiment_risk_multiplier"), 1.0)
        value_classification=str(obj.get("value_classification") or "")
        time_until_update_sec=_i(obj.get("time_until_update_sec"))
        quality_status=str(obj.get("quality_status") or "UNKNOWN")
    )
