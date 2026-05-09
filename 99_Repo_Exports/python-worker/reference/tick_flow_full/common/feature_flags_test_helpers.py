from __future__ import annotations

from typing import Any


class FakeRedis:
    """
    Minimal redis-like stub for tests:
      - get(key) -> value
      - set(key, value) helper
    """
    def __init__(self) -> None:
        self._kv: dict[str, Any] = {}

    def get(self, key: str) -> Any | None:
        return self._kv.get(key)

    def set(self, key: str, value: Any) -> None:
        self._kv[key] = value
