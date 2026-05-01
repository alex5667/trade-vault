from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Literal

import redis

Status = Literal["active", "degraded", "disabled"]


class RegimeRuntimeState:
    def __init__(self, redis_dsn: str):
        self.redis = redis.from_url(redis_dsn, decode_responses=True)

    def get_state(
        self,
        venue: str,
        symbol: str,
        timeframe: str,
        family: str,
    ) -> tuple[Status, float]:
        """
        Возвращает (status, threshold_mult).
        Если ничего не найдено — считаем active/1.0.
        """
        key = f"signals:regime_state:{venue}:{symbol}:{timeframe}:{family}"
        raw = self.redis.get(key)
        if not raw:
            return "active", 1.0

        data = json.loads(raw)
        status: Status = data.get("status", "active")
        threshold_mult: float = float(data.get("threshold_mult", 1.0))

        disable_until_str = data.get("disable_until")
        if status == "disabled" and disable_until_str:
            disable_until = datetime.fromisoformat(disable_until_str)
            now = datetime.now(timezone.utc)
            if now > disable_until:
                # просрочен disable → можно трактовать как active (или дать RegimeGuard поменять)
                status = "active"
                threshold_mult = 1.0

        return status, threshold_mult
