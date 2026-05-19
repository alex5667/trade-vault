"""Per-tag confidence floor.

Background: ``CRYPTO_SIGNAL_MIN_CONF=75`` was applied uniformly to all
``primary_reason`` tags. After the 2026-05-18 audit it became clear that
different tags have very different reliability and need different floors.

Configuration via env ``MIN_CONF_BY_TAG`` (CSV ``tag:percent``):

    MIN_CONF_BY_TAG="weak_progress:95,absorption:85,ok:75,iceberg:80"

Lookup is case-insensitive. Tags absent from the map keep the base floor
(``CRYPTO_SIGNAL_MIN_CONF``). Values are clamped to ``[0, 100]``.
"""
from __future__ import annotations

import logging
import os
from typing import Mapping

log = logging.getLogger(__name__)


def parse_min_conf_by_tag(spec: str | None) -> dict[str, float]:
    """Parse ``"tag:pct,tag:pct"`` into a normalised dict."""
    out: dict[str, float] = {}
    if not spec:
        return out
    for item in spec.split(","):
        item = item.strip()
        if not item or ":" not in item:
            continue
        tag, pct = item.split(":", 1)
        tag = tag.strip().lower()
        try:
            v = float(pct.strip())
        except (TypeError, ValueError):
            log.warning("per_tag_conf_floor: invalid value %r for tag %r", pct, tag)
            continue
        if v != v:  # NaN guard
            continue
        out[tag] = max(0.0, min(100.0, v))
    return out


def load_min_conf_by_tag(env_var: str = "MIN_CONF_BY_TAG") -> dict[str, float]:
    """Load the per-tag floor map from environment."""
    return parse_min_conf_by_tag(os.getenv(env_var))


def get_min_conf_for_tag(
    tag: str | None,
    base_min_pct: float,
    *,
    floors: Mapping[str, float] | None = None,
) -> float:
    """Return the effective min-confidence percent for a tag.

    Selection: ``max(base_min_pct, floors[tag])`` — per-tag floor can only
    **raise** the bar, never lower it. This keeps existing safety nets
    intact (meme CONF_RELAX, calibrated G9, etc.).

    Args:
        tag: entry_tag / primary_reason string. Case-insensitive. Empty/None
            returns the base.
        base_min_pct: baseline floor (e.g. from ``CRYPTO_SIGNAL_MIN_CONF``).
        floors: optional override map. Falls back to env if omitted.
    """
    base = float(base_min_pct) if base_min_pct is not None else 0.0
    if not tag:
        return base
    if floors is None:
        floors = load_min_conf_by_tag()
    key = str(tag).strip().lower()
    extra = floors.get(key)
    if extra is None:
        return base
    return max(base, float(extra))
