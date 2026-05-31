from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class ConfirmResult:
    passed: bool
    veto: bool = False
    flags: dict[str, Any] = field(default_factory=dict)
    reasons: list[str] = field(default_factory=list)
