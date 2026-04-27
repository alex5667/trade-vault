from __future__ import annotations

import hashlib
import json
from typing import Any, Dict, List, Tuple


def _to_str(x: Any) -> str:
    if x is None:
        return ""
    if isinstance(x, bytes):
        return x.decode("utf-8", errors="replace")
    return str(x)


def _truncate_utf8(s: str, max_bytes: int) -> Tuple[str, bool]:
    if max_bytes <= 0:
        return s, False
    b = s.encode("utf-8", errors="replace")
    if len(b) <= max_bytes:
        return s, False
    cut = b[:max_bytes]
    return cut.decode("utf-8", errors="ignore") + "…(truncated)", True


def sanitize_for_dlq(
    payload: Dict[str, Any],
    *,
    max_field_bytes: int = 16_384,
    max_total_bytes: int = 65_536,
    hash_truncated: bool = True,
) -> Dict[str, str]:
    """
    Limits payload size before DLQ to avoid unbounded growth.
    Returns flat dict[str, str] with truncation markers.
    """
    trunc_fields: List[str] = []

    def flatten(d: Dict[str, Any]) -> Dict[str, str]:
        out: Dict[str, str] = {}
        for k, v in d.items():
            if k == "fields" and isinstance(v, dict):
                out["fields_json"] = json.dumps(v, ensure_ascii=False, separators=(",", ":"))
            elif isinstance(v, (dict, list)):
                out[k] = json.dumps(v, ensure_ascii=False, separators=(",", ":"))
            else:
                out[k] = _to_str(v)
        return out

    flat = flatten(payload)
    out: Dict[str, str] = {}

    # per-field truncate
    for k, s in flat.items():
        s2, trunc = _truncate_utf8(s, max_field_bytes)
        out[k] = s2
        if trunc:
            trunc_fields.append(k)
            if hash_truncated:
                out[f"{k}__sha256"] = hashlib.sha256(s.encode("utf-8", errors="replace")).hexdigest()

    def total_bytes(d: Dict[str, str]) -> int:
        return sum(len(v.encode("utf-8", errors="replace")) for v in d.values())

    if total_bytes(out) > max_total_bytes:
        for key in ("fields_json", "error"):
            if key in out:
                cap = max(256, max_total_bytes // 2)
                s2, trunc = _truncate_utf8(out[key], cap)
                out[key] = s2
                if trunc and key not in trunc_fields:
                    trunc_fields.append(key)

    if total_bytes(out) > max_total_bytes:
        smaller = max(256, max_total_bytes // 8)
        for k in list(out.keys()):
            if k.endswith("__sha256"):
                continue
            out[k], _ = _truncate_utf8(out[k], smaller)

    if trunc_fields:
        out["dlq_truncated"] = "1"
        out["dlq_trunc_fields"] = ",".join(sorted(set(trunc_fields)))

    return out

