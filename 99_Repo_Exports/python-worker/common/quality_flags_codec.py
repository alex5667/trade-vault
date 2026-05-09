from __future__ import annotations

"""
Unified quality_flags codec (Phase 3 contract unification).

Problem
-------
Two producer styles exist on the wire for the `quality_flags` field:

  1. Go workers (go-worker/internal/models/market_data.go, liquidation/controller.go)
     emit a comma-separated string, defaulting to "ok":
         "ok"  |  "ts_fallback"  |  "ok,tick_gap"

  2. Python workers (core/outbox_envelope.py) emit a JSON-encoded list[str]:
         "[]"  |  '["hlc_fallback"]'  |  '["atr_fallback","hlc_fallback"]'

Both styles mean the same thing, but consumers have to know which producer
they're parsing. This module exposes a single normalization step that
accepts either form and returns list[str].

Forward-compat (future schema_version=2)
----------------------------------------
When the contract is bumped, producers SHOULD only emit the JSON-list form.
The codec accepts the legacy string form indefinitely so rollouts can be
staged — remove the legacy branch only after all producers are upgraded.
"""


import json
from collections.abc import Sequence
from typing import Any

# Comma-separated tokens that mean "no DQ issue" on the Go side.
# "ok" is the canonical default emitted when everything is clean.
_OK_SENTINELS = frozenset({"ok", "clean", ""})


def decode_quality_flags(raw: Any) -> list[str]:
    """Accept any of (None, list, tuple, set, JSON string, comma-separated string)
    and return a normalised, de-duplicated, sorted list of flags.

    Empty / "ok" / malformed → [].

    This function NEVER raises. Parse failures return [].
    """
    if raw is None:
        return []

    # Already a collection — filter and normalise tokens.
    if isinstance(raw, (list, tuple, set, frozenset)):
        return _canonicalize(raw)

    if isinstance(raw, (bytes, bytearray)):
        try:
            raw = raw.decode("utf-8", "replace")
        except Exception:
            return []

    if not isinstance(raw, str):
        return []

    s = raw.strip()
    if not s:
        return []

    # Try JSON first — this is the canonical format.
    if s[0] in "[{":
        try:
            parsed = json.loads(s)
        except Exception:
            parsed = None
        if isinstance(parsed, list):
            return _canonicalize(parsed)
        if isinstance(parsed, dict):
            # Legacy dict form {"flag": true, ...} — accept truthy keys.
            return _canonicalize(k for k, v in parsed.items() if v)
        # fall through to comma-split

    # Legacy Go form: comma-separated. Split conservatively.
    tokens = [t.strip() for t in s.split(",")]
    return _canonicalize(tokens)


def encode_quality_flags(flags: Sequence[str]) -> str:
    """Produce the canonical JSON-list form. Always valid JSON, always sorted."""
    normalised = _canonicalize(flags)
    return json.dumps(normalised, ensure_ascii=False, separators=(",", ":"))


def is_clean(raw: Any) -> bool:
    """True when the normalised flag list is empty (no DQ issue)."""
    return len(decode_quality_flags(raw)) == 0


# ── internals ────────────────────────────────────────────────────────────

def _canonicalize(items: Any) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for x in items:
        if x is None:
            continue
        token = str(x).strip().lower()
        if not token or token in _OK_SENTINELS:
            continue
        if token in seen:
            continue
        seen.add(token)
        out.append(token)
    out.sort()
    return out
