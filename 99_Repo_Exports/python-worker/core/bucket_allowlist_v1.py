from __future__ import annotations

"""Bucket allowlist helper.

Purpose:
- Deterministic, low-overhead parsing of allowlists like:
    "HIGH_VOL_LOW_LIQ, HIGH_VOL" or "all".
- Used to make "bucket-aware enforcement" consistent across gates.

Rules:
- allow == "all" / "*" / "any" -> True
- allow == "none" / "0" / "off" -> False
- allow empty -> True only for default_bucket
- list is comma/semicolon separated
"""

from typing import Set


def _norm_bucket(b: str) -> str:
    return str(b or "").strip().upper() or "NORMAL"


def _parse_allowlist(raw: str) -> Set[str]:
    out: Set[str] = set()
    for part in str(raw or "").replace(";", ",").split(","):
        x = part.strip().upper()
        if x:
            out.add(x)
    return out


def bucket_allowed(bucket: str, allow: str, *, default_bucket: str = "HIGH_VOL_LOW_LIQ") -> bool:
    b = _norm_bucket(bucket)
    s = str(allow or "").strip().lower()
    if s in ("all", "*", "any"):
        return True
    if s in ("none", "0", "off", "false"):
        return False

    allow_set = _parse_allowlist(allow)
    if not allow_set:
        return b == _norm_bucket(default_bucket)
    return b in allow_set
