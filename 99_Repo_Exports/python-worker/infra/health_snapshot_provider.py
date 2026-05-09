from __future__ import annotations

import os
from typing import Any

"""
Опциональный провайдер health-снапшота.
Использование:
  repo = RedisTradeRepository(redis_client, health_provider=RedisHealthSnapshotProvider.from_env())

ВАЖНО:
  - Этот объект создаётся ОДИН раз (singleton), а не на каждое закрытие сделки.
  - Репозиторий остаётся чистым (только пишет), а сбор метрик делается отдельным компонентом.
"""


class RedisHealthSnapshotProvider:
    def __init__(self, health_metrics, *, snapshot_key_tpl: str = "orderflow:{symbol}:health_snapshot"):
        self._hm = health_metrics
        self._snapshot_key_tpl = snapshot_key_tpl

    @classmethod
    def from_env(cls) -> RedisHealthSnapshotProvider | None:
        """
        Включается только если HEALTH_SNAPSHOT_ENABLE=1.
        Это позволяет безопасно выключить сбор метрик без изменения кода.
        """
        if os.getenv("HEALTH_SNAPSHOT_ENABLE", "0") != "1":
            return None
        try:
            from health_metrics import HealthMetrics
            redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
            window_sec = int(os.getenv("HEALTH_WINDOW_SEC", "5"))
            hm = HealthMetrics(redis_url=redis_url, window_sec=window_sec)
            return cls(hm)
        except Exception:
            return None

    def get_snapshot(self, symbol: str) -> dict[str, Any]:
        """
        Возвращает dict полей для добавления в stream.
        Ключи оставляем совместимыми (health_*).
        """
        out: dict[str, Any] = {}
        key = self._snapshot_key_tpl.format(symbol=symbol)
        snap = self._hm._redis.hgetall(key) or {}

        # snap может быть bytes->bytes; приводим к str внутри hm-redis обычно,
        # но на всякий случай приводим через str().
        def _get(m, k, default="0.0"):
            v = m.get(k, default)
            return v.decode("utf-8", "replace") if isinstance(v, (bytes, bytearray)) else str(v)

        if snap:
            out["health_l2_stale_ratio_tick"] = _get(snap, "l2_stale_ratio_tick", "0.0")
            out["health_l2_stale_ratio_now"] = _get(snap, "l2_stale_ratio_now", "0.0")
            out["health_avg_l2_age_ms"] = _get(snap, "avg_l2_age_ms", "0.0")
            out["health_avg_l2_age_tick_ms"] = _get(snap, "avg_l2_age_tick_ms", "0.0")

        # Эти метрики лежат отдельными ключами.
        try:
            out["health_signal_emit_rate"] = self._hm._redis.get(f"orderflow:{symbol}:signal_emit_rate") or "0.0"
            out["health_dlq_rate"] = self._hm._redis.get(f"orderflow:{symbol}:dlq_rate") or "0.0"
        except Exception:
            pass

        return out

