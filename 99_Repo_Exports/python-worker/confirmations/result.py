from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List


@dataclass
class ConfirmResult:
    passed: bool
    veto: bool = False
    flags: Dict[str, Any] = field(default_factory=dict)
    reasons: List[str] = field(default_factory=list)
