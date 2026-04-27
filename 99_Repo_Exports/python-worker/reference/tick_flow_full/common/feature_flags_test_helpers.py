from __future__ import annotations

from typing import Any, Optional


class FakeRedis:
    """
    Minimal redis-like stub for tests:
      - get(key) -> value
      - set(key, value) helper
    """
    def __init__(self) -> None:
        self._kv: dict[str, Any] = {}

    def get(self, key: str) -> Optional[Any]:
        return self._kv.get(key)

    def set(self, key: str, value: Any) -> None:
        self._kv[key] = value
