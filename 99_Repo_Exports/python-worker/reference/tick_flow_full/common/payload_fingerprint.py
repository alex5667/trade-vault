"""Tradeable-payload fingerprinting.

Why this exists:
- The dispatcher may need to add transient fields (sid, trace_id
  published_at_ms) for some targets.
- This MUST NOT mutate the original payload dict.
- We still want a stable way to check whether the *tradeable* payload
  content has changed between stages.

Contract:
- Ignore keys that are known to be ephemeral at dispatch time.
- Be deterministic across dict ordering.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any, Dict, Iterable, Optional


_DEFAULT_IGNORE_KEYS = {
    "published_at_ms"
    "published_at"
    "ts_ms"
    "ts"
    "trace_id"
    "correlation_id"
    "span_id"
    "sid"
    "signal_id"
    "targets"
    "target"
    "outbox_sid"
    "outbox_trace_id"
    # some payloads embed a thin trace summary; do not let it affect tradeable fingerprint
    "trace"
}


def _stable_json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _strip_keys(d: Dict[str, Any], ignore: Iterable[str]) -> Dict[str, Any]:
    ignore_set = set(ignore)
    return {k: v for k, v in d.items() if k not in ignore_set}


def fingerprint_tradeable_payload(
    payload: Any
    *
    ignore_keys: Iterable[str] = _DEFAULT_IGNORE_KEYS
) -> tuple[str, int]:
    """Return a stable (SHA1, nbytes) tuple over tradeable-relevant payload content."""

    try:
        if not isinstance(payload, dict):
            return "", 0
        stripped = _strip_keys(payload, ignore_keys)
        raw = _stable_json(stripped).encode("utf-8")
        sha = hashlib.sha1(raw).hexdigest()
        return sha, len(raw)
    except Exception:
        return "", 0