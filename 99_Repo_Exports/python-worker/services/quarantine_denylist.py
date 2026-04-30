from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Set


@dataclass
class QuarantineDenylistDecision:
    allowed: bool
    matched_sid: str = ''
    candidates: Optional[List[str]] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            'allowed': bool(self.allowed)
            'matched_sid': str(self.matched_sid or '')
            'candidates': list(self.candidates or [])
        }


def extract_sid_candidates(signal: Mapping[str, Any]) -> List[str]:
    out: List[str] = []
    for key in ('sid', 'execution_sid', 'parent_sid', 'source_sid', 'signal_sid'):
        value = signal.get(key)
        if value is None:
            continue
        text = str(value).strip()
        if text and text not in out:
            out.append(text)
    return out


def check_signal_against_quarantine_cache(signal: Mapping[str, Any], denylist: Iterable[str]) -> QuarantineDenylistDecision:
    candidates = extract_sid_candidates(signal)
    deny = set(str(x) for x in denylist)
    for sid in candidates:
        if sid in deny:
            return QuarantineDenylistDecision(False, matched_sid=sid, candidates=candidates)
    return QuarantineDenylistDecision(True, matched_sid='', candidates=candidates)
