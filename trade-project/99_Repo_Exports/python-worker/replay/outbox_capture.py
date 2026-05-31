from __future__ import annotations

"""
OutboxCapture: in-memory sink compatible with outbox.publish(payload).

Replay runner uses it to collect emitted signals deterministically without Redis.
"""

from dataclasses import dataclass, field
from typing import Any


@dataclass
class OutboxCapture:
    items: list[dict[str, Any]] = field(default_factory=list)

    def publish(self, payload: dict[str, Any]) -> None:
        # store a shallow copy to avoid later mutations affecting history
        self.items.append(dict(payload))

    def __len__(self) -> int:
        return len(self.items)
