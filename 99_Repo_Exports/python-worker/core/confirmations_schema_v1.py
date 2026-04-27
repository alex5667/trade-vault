from __future__ import annotations

"""Confirmation flags as first-class ML features.

Why:
  - Telegram/UI confirmations are currently free-form strings ("rsi_agree=1", "div_match=1", ...).
  - For Stage 4 (ML), these must become stable, low-cardinality, schema-versioned features
    so Train==Serve parity is guaranteed.

Design:
  - Keep a small allow-list of confirmations we treat as first-class (binary) features.
  - Parse from:
      * indicators (preferred for online serving) — keys: conf_<key> or <key>
      * confirmations[] list (offline datasets / UI payloads)
  - Never raise; always return deterministic 0/1.

Schema version: v1
Keys (legacy/4-key): rsi_agree, div_match, sweep_eqh, sweep_eql
Keys (new/11-key):   + weak_progress, absorption, reclaim, sweep,
                       iceberg_strict, fp_edge_absorb, abs_lvl_ok
"""

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple


# ---------------------------------------------------------------------------
# LEGACY 4-KEY API  (kept for backward compatibility)
# ---------------------------------------------------------------------------

# Low-cardinality allow-list. Extend only intentionally (schema bump required).
CONF_KEYS_V1 = (
    "rsi_agree",
    "div_match",
    "sweep_eqh",
    "sweep_eql",
)


def _as_int01(v: Any) -> int:
    """Convert any value to 0 or 1 (truthy vs falsy). Never raises."""
    try:
        if v is None:
            return 0
        if isinstance(v, bool):
            return 1 if v else 0
        if isinstance(v, (int, float)):
            return 1 if float(v) > 0.0 else 0
        s = str(v).strip().lower()
        if s in ("1", "true", "t", "yes", "y", "on"):
            return 1
        if s in ("0", "false", "f", "no", "n", "off", ""):
            return 0
        # best-effort numeric
        return 1 if float(s) > 0.0 else 0
    except Exception:
        return 0


def parse_confirmations_list(confirmations: Sequence[str] | None) -> Dict[str, int]:
    """Parse UI/Telegram-style confirmations like ["rsi_agree=1", "absorption=123.4", ...].

    Only allow-listed keys are returned; all others are silently ignored.
    If a key appears multiple times, the maximum value wins (OR semantics).
    """
    out = {k: 0 for k in CONF_KEYS_V1}
    if not confirmations:
        return out

    for raw in confirmations:
        if not raw:
            continue
        try:
            s = str(raw)
            # Support both "key=value" and bare "key" (treated as key=1)
            k, v = s.split("=", 1) if "=" in s else (s, "1")
            k = k.strip()
            if k in out:
                out[k] = max(out[k], _as_int01(v))
        except Exception:
            continue
    return out


def extract_confirmation_flags(
    confirmations: Sequence[str] | None = None,
    *,
    indicators: Mapping[str, Any] | None = None,
) -> Dict[str, int]:
    """Return deterministic binary flags for known confirmations.

    Priority:
      1) indicators["conf_<key>"]  (preferred: structured, lowest latency)
      2) indicators["<key>"]       (backward compat)
      3) parse_confirmations_list(confirmations)  (offline / UI fallback)

    This supports both online serving (where strategy.py sets indicators["conf_*"])
    and offline dataset building (where only the confirmations[] list is available).
    """
    base = parse_confirmations_list(confirmations)
    if not indicators:
        return base

    ind = indicators
    for k in CONF_KEYS_V1:
        v = None
        if f"conf_{k}" in ind:
            v = ind[f"conf_{k}"]
        elif k in ind:
            v = ind[k]
        if v is not None:
            base[k] = max(base[k], _as_int01(v))
    return base


# ---------------------------------------------------------------------------
# NEW V1 API — expanded 11-key schema with dataclass (Commit 1)
# Parallel to the old API; call sites can adopt incrementally.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ConfirmationSignalV1:
    """
    Normalized confirmation representation for ML feature parity (train==serve).
    Incoming confirmations are strings like:
      - "rsi_agree=1"
      - "div_match=1"
      - "sweep_eqh=1"
      - "sweep_eql=1"
      - "weak_progress=1"
      - "absorption=1"
      - "reclaim=1"
      - "sweep=1"
      - "iceberg_strict=1"
      - "fp_edge_absorb=1"
      - "abs_lvl_ok=1"
    """

    key: str
    value: float


# Canonicalize common spelling variants → canonical key name
_ALIASES: Dict[str, str] = {
    "fp_edge": "fp_edge_absorb",
    "fp_edge_absorption": "fp_edge_absorb",
    "absorb": "absorption",
    "absorb_lvl_ok": "abs_lvl_ok",
    "abs_level_ok": "abs_lvl_ok",
    "iceberg": "iceberg_strict",
}

# Full 11-key canonical set for the new v1 parser
_CANONICAL_KEYS_11 = frozenset({
    "rsi_agree",
    "div_match",
    "sweep_eqh",
    "sweep_eql",
    "weak_progress",
    "absorption",
    "reclaim",
    "sweep",
    "iceberg_strict",
    "fp_edge_absorb",
    "abs_lvl_ok",
})


def _parse_one_v1(item: str) -> Optional[ConfirmationSignalV1]:
    """Parse a single confirmation string into a ConfirmationSignalV1. Never raises."""
    if not item:
        return None
    s = str(item).strip()
    if not s:
        return None
    if "=" in s:
        k, v = s.split("=", 1)
        k = k.strip()
        v = v.strip()
    else:
        k, v = s.strip(), "1"

    if not k:
        return None

    # Resolve aliases → canonical name
    k_norm = _ALIASES.get(k, k)
    try:
        val = float(v)
    except Exception:
        # Tolerate booleans / tokens
        vv = str(v).strip().lower()
        if vv in ("true", "yes", "y", "ok"):
            val = 1.0
        else:
            val = 0.0
    return ConfirmationSignalV1(key=k_norm, value=val)


def parse_confirmations_v1(confirmations: Iterable[str]) -> List[ConfirmationSignalV1]:
    """Parse an iterable of confirmation strings into ConfirmationSignalV1 objects.

    Accepts None gracefully. Unknown/alias keys are resolved via _ALIASES.
    No allow-list filtering here — use confirmations_to_indicator_keys_v1 for
    canonical subset selection.
    """
    out: List[ConfirmationSignalV1] = []
    if confirmations is None:
        return out
    for it in confirmations:
        c = _parse_one_v1(str(it))
        if c is None:
            continue
        out.append(c)
    return out


def confirmations_to_indicator_keys_v1(parsed: List[ConfirmationSignalV1]) -> Dict[str, float]:
    """Map parsed confirmations to indicators keys.

    Convention:
      - conf:<key> -> numeric float (all keys, not filtered by canonical set)
      - b:<key>    -> numeric 0/1 for canonical 11-key subset (ML schema compat)
      - <key>      -> raw 0/1 for canonical keys (ML schema compat)
    """
    ind: Dict[str, float] = {}
    for c in parsed:
        # Always write conf:<key> = numeric value
        ind[f"conf:{c.key}"] = float(c.value)

        # For canonical keys: also write bool-ish forms used by ML schemas
        if c.key in _CANONICAL_KEYS_11:
            binary = 1.0 if float(c.value) > 0 else 0.0
            ind[c.key] = binary
            ind[f"b:{c.key}"] = binary
    return ind


def apply_confirmations_to_indicators(
    *,
    confirmations: Iterable[str],
    indicators: Dict[str, object],
    also_write_raw_keys: bool = True,
) -> Dict[str, object]:
    """Materialize confirmation-derived feature keys into indicators dict (in-place).

    Writes:
      - conf:<key>  — always (numeric float)
      - b:<key>     — always for canonical 11-key subset (binary 0/1)
      - <key>       — only if also_write_raw_keys=True and key not already set

    Returns indicators for convenience (same object, modified in place).
    Never raises — fail-safe for hot-path usage.
    """
    try:
        parsed = parse_confirmations_v1(confirmations)
        mapped = confirmations_to_indicator_keys_v1(parsed)
        for k, v in mapped.items():
            # Skip raw canonical keys if disabled (prevents overwriting existing indicators)
            if not also_write_raw_keys and k in _CANONICAL_KEYS_11:
                continue
            # Never overwrite already-computed indicator values
            if k not in indicators:
                indicators[k] = v
    except Exception:
        pass
    return indicators


def summarize_confirmations_v1(confirmations: Iterable[str]) -> Tuple[int, List[str]]:
    """Utility: (count, sorted_unique_keys) for logging/metrics telemetry."""
    parsed = parse_confirmations_v1(confirmations)
    keys = sorted({c.key for c in parsed})
    return len(parsed), keys
