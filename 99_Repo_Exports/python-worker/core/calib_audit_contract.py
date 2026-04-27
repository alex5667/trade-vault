from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any, Dict
import hashlib
import json


def stable_hash(obj: Dict[str, Any]) -> str:
    """
    Deterministic hash for audit and golden comparisons.
    """
    s = json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return "sha1:" + hashlib.sha1(s.encode("utf-8")).hexdigest()


@dataclass
class CalibEffqAuditV1:
    v: int
    symbol: str
    regime: str
    ts_ms: int
    src: str
    n: int
    eff_quote_th: float
    min_quote_delta: float
    state_hash: str

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
