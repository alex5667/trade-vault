from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass, asdict
from typing import Any, Dict, Tuple

try:
    import redis
except Exception:  # pragma: no cover
    redis = None  # type: ignore


@dataclass
class BudgetDecision:
    allowed: bool
    reason: str
    estimated_cost_usd: float
    daily_spend_usd: float
    daily_limit_usd: float
    hourly_calls: int
    hourly_limit: int

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class VertexBudgetGuardV1:
    """Simple Redis-backed budget/call-rate guard for Vertex triage.

    Properties:
      - deterministic UTC buckets (day/hour)
      - fail-open when Redis is unavailable if VERTEX_BUDGET_FAIL_POLICY=OPEN
      - tracks both estimated USD and request counts
    """

    def __init__(self, redis_url: str) -> None:
        self._redis_url = redis_url
        self._daily_limit_usd = float(os.getenv("VERTEX_MAX_DAILY_USD", "25") or 25.0)
        self._hourly_limit = int(os.getenv("VERTEX_MAX_CALLS_PER_HOUR", "300") or 300)
        self._fail_policy = str(os.getenv("VERTEX_BUDGET_FAIL_POLICY", "OPEN") or "OPEN").upper()
        self._r = None
        if redis is not None:
            self._r = redis.Redis.from_url(redis_url, decode_responses=True)

    @staticmethod
    def _utc_day(ts: int) -> str:
        return time.strftime("%Y%m%d", time.gmtime(ts))

    @staticmethod
    def _utc_hour(ts: int) -> str:
        return time.strftime("%Y%m%d%H", time.gmtime(ts))

    def _keys(self, provider: str, model: str, ts: int) -> Tuple[str, str]:
        d = self._utc_day(ts)
        h = self._utc_hour(ts)
        return (
            f"metrics:vertex:budget:usd:{provider}:{model}:{d}",
            f"metrics:vertex:budget:calls:{provider}:{model}:{h}",
        )

    def check_and_reserve(self, *, provider: str, model: str, estimated_cost_usd: float, ts: int) -> BudgetDecision:
        if self._r is None:
            allowed = self._fail_policy == "OPEN"
            return BudgetDecision(allowed=allowed, reason="redis_unavailable", estimated_cost_usd=estimated_cost_usd,
                                  daily_spend_usd=0.0, daily_limit_usd=self._daily_limit_usd,
                                  hourly_calls=0, hourly_limit=self._hourly_limit)
        day_key, hour_key = self._keys(provider, model, ts)
        try:
            p = self._r.pipeline()
            p.get(day_key)
            p.get(hour_key)
            day_raw, hour_raw = p.execute()
            current_usd = float(day_raw or 0.0)
            current_calls = int(hour_raw or 0)
            if (current_usd + estimated_cost_usd) > self._daily_limit_usd:
                return BudgetDecision(False, "daily_budget_exceeded", estimated_cost_usd, current_usd,
                                      self._daily_limit_usd, current_calls, self._hourly_limit)
            if (current_calls + 1) > self._hourly_limit:
                return BudgetDecision(False, "hourly_call_limit_exceeded", estimated_cost_usd, current_usd,
                                      self._daily_limit_usd, current_calls, self._hourly_limit)
            p = self._r.pipeline()
            p.incrbyfloat(day_key, float(estimated_cost_usd))
            p.expire(day_key, 3 * 86400)
            p.incr(hour_key, 1)
            p.expire(hour_key, 2 * 86400)
            p.execute()
            return BudgetDecision(True, "ok", estimated_cost_usd, current_usd + estimated_cost_usd,
                                  self._daily_limit_usd, current_calls + 1, self._hourly_limit)
        except Exception:
            allowed = self._fail_policy == "OPEN"
            return BudgetDecision(allowed=allowed, reason="guard_error", estimated_cost_usd=estimated_cost_usd,
                                  daily_spend_usd=0.0, daily_limit_usd=self._daily_limit_usd,
                                  hourly_calls=0, hourly_limit=self._hourly_limit)


def estimate_vertex_triage_cost_usd(input_chars: int, output_chars: int) -> float:
    """Conservative local estimate for budget-guarding only.

    Intentionally rough. Real billing should come from provider metadata when/if available.
    """
    per_million_chars = float(os.getenv("VERTEX_TRIAGE_EST_USD_PER_MCHARS", "0.35") or 0.35)
    total = max(0, int(input_chars)) + max(0, int(output_chars))
    return (float(total) / 1_000_000.0) * per_million_chars

