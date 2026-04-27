from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass(frozen=True)
class EnvelopeStoreSettings:
    prefix: str = os.getenv("SIGNAL_ENV_PREFIX", "sig:env")
    ttl_sec: int = int(os.getenv("SIGNAL_ENV_STORE_TTL_SEC", "172800"))  # 48h


class EnvelopeStore:
    """
    "Сверхидеал++":
    - хранит исходный envelope по sid, чтобы можно было сделать selective replay/repair
    - хранение идемпотентное: SETNX (первый wins), затем TTL refresh best-effort
    """

    def __init__(self, redis_client: Any, *, settings: Optional[EnvelopeStoreSettings] = None) -> None:
        self.redis = redis_client
        self.settings = settings or EnvelopeStoreSettings()

    def key(self, sid: str) -> str:
        return f"{self.settings.prefix}:{sid}"

    def save_once(self, sid: str, env: Dict[str, Any]) -> None:
        k = self.key(sid)
        payload = json.dumps(env, ensure_ascii=False, separators=(",", ":"))
        ttl = int(self.settings.ttl_sec)
        try:
            ok = self.redis.set(k, payload, nx=True, ex=ttl)
            if ok:
                return
        except Exception:
            # fail-open
            return
        # если уже было — попробуем освежить TTL (best-effort)
        try:
            self.redis.expire(k, ttl)
        except Exception:
            pass

    def load(self, sid: str) -> Optional[Dict[str, Any]]:
        k = self.key(sid)
        raw = None
        try:
            raw = self.redis.get(k)
        except Exception:
            return None
        if not raw:
            return None
        try:
            if isinstance(raw, (bytes, bytearray)):
                raw = raw.decode("utf-8", errors="ignore")
            if isinstance(raw, str):
                return json.loads(raw)
        except Exception:
            return None
        return None
