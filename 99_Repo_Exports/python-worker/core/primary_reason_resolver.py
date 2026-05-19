"""Prioritized primary_reason / entry_tag selection.

Background (2026-05-18 audit): the orderflow strategy used
``primary_reason = confirmations[0].split("=", 1)[0]`` which depends on the
order in which ``confirmations.append(...)`` is called inside
``services/orderflow/strategy.py``. ``weak_progress`` is appended early
(line ~2685), before strong evidence such as ``obi_stable``, ``fp_edge_absorb``,
``ofi_stable``, ``cvdR``, ``iceberg`` (lines ~2707…3340). As a result
``weak_progress`` won the tie and became the ``entry_tag`` for ~74% of
trades, dragging cumulative PnL by -59.7% over 24h.

This module fixes the ordering problem deterministically:

* A configurable **priority list** picks the strongest reason that actually
  appears in ``confirmations`` — order of ``append`` no longer matters.
* A configurable **excluded set** prevents low-quality flags (``weak_progress``,
  ``weak_recent``) from ever becoming the primary reason, even if no stronger
  evidence is present. They remain available as features for scoring.

Both are ENV-overridable so the change is reversible.
"""
from __future__ import annotations

import os
from typing import Iterable, Sequence

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------

# Highest → lowest. Whoever matches first wins.
# Tokens are matched against ``key.split('=', 1)[0]`` of each confirmation
# string in case-insensitive way.
DEFAULT_PRIORITY: tuple[str, ...] = (
    "sweep_eqh",
    "sweep_eql",
    "iceberg",
    "ice_strict",
    "fp_edge_absorb",
    "absorption",
    "obi_stable",
    "ofi_stable",
    "cvdR",
    "delta_spike",
    "abs_lvl",
    "div_match",
    "rsi_agree",
)

# Tags that must NEVER be the primary reason — they only feed the scorer.
DEFAULT_EXCLUDED: frozenset[str] = frozenset({"weak_progress", "weak_recent"})

# Fallback used when nothing in ``confirmations`` survives the rules.
DEFAULT_FALLBACK: str = "delta_spike"


def _csv_lower(value: str | None) -> list[str]:
    if not value:
        return []
    return [x.strip().lower() for x in value.split(",") if x.strip()]


def load_priority(env_var: str = "PRIMARY_REASON_PRIORITY") -> list[str]:
    """Return the priority list, optionally overridden by env CSV."""
    override = _csv_lower(os.getenv(env_var))
    if override:
        return override
    return [r.lower() for r in DEFAULT_PRIORITY]


def load_excluded(env_var: str = "EXCLUDE_AS_PRIMARY_REASONS") -> set[str]:
    """Return the excluded set, optionally overridden by env CSV.

    Pass an empty CSV (``""``) to disable exclusion entirely.
    """
    raw = os.getenv(env_var)
    if raw is None:
        return set(DEFAULT_EXCLUDED)
    return set(_csv_lower(raw))


def reason_key(confirmation: str) -> str:
    """Strip ``=value`` suffix and lowercase: ``"obi_stable=2.10"`` → ``"obi_stable"``."""
    if not confirmation:
        return ""
    return confirmation.split("=", 1)[0].strip().lower()


def resolve_primary_reason(
    confirmations: Sequence[str] | Iterable[str],
    *,
    priority: Sequence[str] | None = None,
    excluded: Iterable[str] | None = None,
    fallback: str = DEFAULT_FALLBACK,
) -> str:
    """Pick the best primary reason from ``confirmations``.

    Selection rules (in order):
      1. Drop entries whose key is in ``excluded``.
      2. Walk ``priority`` top→down; return the first key that appears in
         the surviving confirmations.
      3. If no priority entry matches but other (non-excluded) keys exist,
         return the first surviving key (preserves legacy append-order
         behaviour for unknown evidence).
      4. Otherwise return ``fallback``.

    Args:
        confirmations: iterable of confirmation strings such as
            ``["weak_progress=1", "obi_stable=2.10"]``.
        priority: ranked list of canonical keys; defaults to
            :func:`load_priority`.
        excluded: keys that must never become the primary reason; defaults
            to :func:`load_excluded`.
        fallback: returned when nothing survives.

    Returns:
        Lowercase reason string.
    """
    if priority is None:
        priority = load_priority()
    if excluded is None:
        excluded = load_excluded()
    excluded_set = {e.lower() for e in excluded}

    keys: list[str] = []
    seen: set[str] = set()
    for c in confirmations or ():
        k = reason_key(str(c))
        if not k:
            continue
        if k in excluded_set:
            continue
        if k in seen:
            continue
        seen.add(k)
        keys.append(k)

    if not keys:
        return fallback.lower()

    available = set(keys)
    for p in priority:
        pl = p.lower()
        if pl in available:
            return pl

    # Unknown evidence — fall back to preserving append-order semantics.
    return keys[0]
