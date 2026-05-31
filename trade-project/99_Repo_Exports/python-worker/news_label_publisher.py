"""Trade-side reference implementation: publish labels for news events.

This file is NOT imported by news_agent.
It is a reference you can copy into scanner_infra / trade core.

Context
-------
Trade has event_id + symbol from the news prior / reco cache (read at entry-time).
After the event horizon passes (1m/5m/15m), trade computes market metrics and calls
this function to publish labels that news_agent uses for offline walk-forward training.

Input required from trade
--------------------------
- event_id + symbol: from news prior / trade reco cache (see news:prior:<sym>, news:reco:<sym>)
- market metrics at 1m/5m/15m: impulse_bps = 10000*(price_after-price_before)/price_before
  range_expansion_ratio = TR / baseline_TR
  volatility_spike_ratio = RV_after / RV_before
- execution-quality context at prior publication time (spread_bps, lag_ms, dq_flags)

Output
------
XADD stream:news_labels  payload=<NewsFeedbackLabelDTO JSON>
"""

from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import json
import time
from typing import Any, Dict, Optional


def publish_news_labels(
    *,
    redis_sync,  # sync Redis client (e.g. redis.Redis)
    stream: str = "stream:news_labels",
    event_id: str,
    symbol: str,
    # Price impulse (signed bps: positive = up, negative = down)
    impulse_1m_bps: Optional[float] = None,
    impulse_5m_bps: Optional[float] = None,
    impulse_15m_bps: Optional[float] = None,
    # Range expansion (TR / baseline_TR; >= 1 means expansion)
    range_expansion_1m_ratio: Optional[float] = None,
    range_expansion_5m_ratio: Optional[float] = None,
    range_expansion_15m_ratio: Optional[float] = None,
    # Volatility spike (RV / baseline_RV; >= 1 means spike)
    volatility_spike_1m_ratio: Optional[float] = None,
    volatility_spike_5m_ratio: Optional[float] = None,
    volatility_spike_15m_ratio: Optional[float] = None,
    # Execution-quality context at prior time
    spread_bps: Optional[float] = None,
    lag_ms: Optional[int] = None,
    dq_level: Optional[int] = None,
    dq_flags: Optional[Dict[str, Any]] = None,
    raw: Optional[Dict[str, Any]] = None,
) -> None:
    """Publish a label record from trade to news_agent via Redis stream."""
    payload = {
        "schema_ver": "v1",
        "event_id": event_id,
        "symbol": symbol,
        "label_ts_ms": get_ny_time_millis(),
        "impulse_1m_bps": impulse_1m_bps,
        "impulse_5m_bps": impulse_5m_bps,
        "impulse_15m_bps": impulse_15m_bps,
        "range_expansion_1m_ratio": range_expansion_1m_ratio,
        "range_expansion_5m_ratio": range_expansion_5m_ratio,
        "range_expansion_15m_ratio": range_expansion_15m_ratio,
        "volatility_spike_1m_ratio": volatility_spike_1m_ratio,
        "volatility_spike_5m_ratio": volatility_spike_5m_ratio,
        "volatility_spike_15m_ratio": volatility_spike_15m_ratio,
        "spread_bps": spread_bps,
        "lag_ms": lag_ms,
        "dq_level": dq_level,
        "dq_flags": dq_flags or {},
        "raw": raw or {},
    }
    redis_sync.xadd(
        stream,
        {"payload": json.dumps(payload, ensure_ascii=False)},
        maxlen=200_000,
        approximate=True,
    )
