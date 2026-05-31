"""Outcome-reliability weights per gate reject_reason — IPS-style correction
for online calibrators (p_edge, reliability, adverse, …).

Background
----------
The trade pipeline records EVERY signal (real + virtual) into ``trades:closed``,
tagged with ``v_gate_reason`` — the reason code emitted by the gate that
blocked/shadowed the signal (empty / "OK" / "" for passed real trades).

Feeding ALL outcomes into a calibrator unweighted distorts thresholds: a trade
that closed during ``VETO_FREEZE_ACTIVE`` reflects degraded-environment
execution, not signal quality. A trade blocked by ``VETO_SPREAD_SHOCK`` had
outcome dominated by execution cost the gate already vetoed for. A
``SHADOW_VETO_*`` sample is more reliable (would-have-been-blocked, but soft
gate) — it still informs signal quality.

So instead of FILTERING those samples out (which causes selection bias and
makes recalibration of the rejection boundary impossible — see Hand & Henley
1997 «Statistical Classification Methods in Consumer Credit Scoring»; Bottou et
al 2013 «Counterfactual Reasoning and Learning Systems»), we KEEP them and
DOWN-WEIGHT them. Weight reflects: «how reliably does this trade's outcome
measure signal quality?»

Weight semantics
----------------
- 1.0  — passed real trade, outcome directly measures signal quality
- 0.7  — shadow-veto (gate did not block; cheap soft rule)
- 0.5  — context veto (BTC drop, HTF bias, regime) — outcome partially
         reflects market context that gate accounted for
- 0.3  — execution-cost veto (spread shock, burst) — outcome reflects
         execution quality the gate already priced in
- 0.1  — environment veto (freeze, daily DD, DQ failure) — outcome dominated
         by system state, NOT signal quality
- 0.0  — never (zero-weighted samples should just be filtered upstream)

The weights are intentionally soft (geometric 0.7 / 0.5 / 0.3 / 0.1) — strong
enough to suppress noisy outcomes, weak enough to keep distribution coverage
non-zero in every region (so recalibration after gate relaxation is possible).

ENV overrides
-------------
The default table is conservative and matches the gate semantics documented in
CLAUDE.md. Override via JSON env when tuning::

    REJECT_REASON_WEIGHTS_JSON='{"VETO_SPREAD_SHOCK": 0.5, "OK": 1.0}'

Or disable entirely (everything weight=1.0, equivalent to old behaviour)::

    REJECT_REASON_WEIGHTS_ENABLED=0

Reading
-------
- Hand & Henley (1997) — *Statistical Classification Methods in Consumer
  Credit Scoring* (foundational on reject inference)
- Bottou et al. (2013) — *Counterfactual Reasoning and Learning Systems* (JMLR)
- Swaminathan & Joachims (2015) — *Counterfactual Risk Minimization*
- Lopez de Prado (2018) — *Advances in Financial ML* gl.3 (Meta-Labeling), gl.7
"""
from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from typing import Mapping

logger = logging.getLogger(__name__)

# Sentinel: a "passed" sample (real trade, no gate veto, no shadow tag).
# Empty reason / "OK" / "PASSED" / "ALLOW" all map to this.
_PASSED_TOKENS = frozenset({"", "OK", "ok", "PASSED", "passed", "ALLOW", "allow"})


# Default weight table — keys are exact reject_reason strings (case-insensitive
# match on prefix because gates emit things like "VETO_BREADTH_RET_HIGH" while
# we only want a per-family weight here).
#
# Order matters: more specific prefixes MUST appear before generic ones —
# matching is first-prefix-wins by length-descending sort.
DEFAULT_WEIGHTS: dict[str, float] = {
    # ── passed real trades ──────────────────────────────────────────────
    # (handled separately via _PASSED_TOKENS; included here for docs)
    # "OK": 1.0,

    # ── environment vetoes — outcome dominated by system state ──────────
    "VETO_FREEZE_ACTIVE":        0.10,
    "VETO_DAILY_DD_KILLSWITCH":  0.10,
    "VETO_DQ_":                  0.10,  # any DQ-family veto
    "VETO_FEATURE_DRIFT":        0.20,
    "VETO_KILLSWITCH":           0.10,

    # ── execution-cost vetoes — outcome reflects execution issue ────────
    "VETO_SPREAD_SHOCK":         0.30,
    "VETO_CANCEL_SPIKE":         0.30,
    "VETO_STREAM_INTEGRITY":     0.30,
    "VETO_BOOK_SANITY":          0.30,
    "VETO_BOOK_TRADE_CONSIST":   0.30,

    # ── microstructure / burst — execution+signal mixed ─────────────────
    "VETO_BURST":                0.50,
    "VETO_MANIP":                0.40,
    "VETO_TAKER_FLOW":           0.50,
    "VETO_ABSORPTION":           0.50,

    # ── market-context vetoes — gate priced market regime, not signal ───
    "VETO_BTC_DROP_BLOCK_LONG":  0.50,
    "VETO_HTF_LONG_BIAS_BEAR":   0.50,
    "VETO_BREADTH":              0.60,
    "VETO_REGIME":               0.50,
    "VETO_SMT":                  0.50,
    "VETO_FUNDING_BASIS":        0.50,
    "VETO_LIQ_WALL":             0.55,
    "VETO_LIQMAP":               0.55,
    "VETO_NEWS_GATE":            0.40,

    # ── edge-cost / portfolio — gate already priced in cost ─────────────
    "VETO_EDGE_COST":            0.40,
    "VETO_PORTFOLIO":            0.40,
    "VETO_MAX_POSITIONS":        0.40,
    "VETO_MAX_TOTAL_NOTIONAL":   0.40,
    "VETO_SYMBOL_NOTIONAL":      0.40,
    "VETO_DUPLICATE":            0.30,
    "VETO_COOLDOWN":             0.50,

    # ── entry-policy / adverse cross ────────────────────────────────────
    "VETO_ENTRY_POLICY":         0.40,
    "VETO_ADVERSE_CROSS":        0.40,

    # ── ATR / horizon / dq-soft ─────────────────────────────────────────
    "VETO_ATR_HORIZON":          0.50,
    "VETO_ATR_FLOOR":            0.50,

    # ── soft / engine signals — high quality, mostly informational ──────
    "engine_veto":               0.40,  # strong_gate enforce + no soft-pass
    "soft_fail":                 0.80,  # ok_soft=1 bypass — high quality
    "strong_gate_shadow_veto":   0.60,

    # ── SHADOW_VETO_* — soft rule, signal still informative ─────────────
    "SHADOW_VETO_":              0.70,

    # ── canary samples — explicit exploration, near-unbiased ────────────
    "canary":                    0.90,
    "canary_shadow":             0.85,
}


_WEIGHTS_CACHE: dict[str, float] | None = None
_SORTED_PREFIXES_CACHE: list[tuple[str, float]] | None = None
_ENABLED_CACHE: bool | None = None


def _load_overrides() -> dict[str, float]:
    """Parse REJECT_REASON_WEIGHTS_JSON env override (best-effort)."""
    raw = os.getenv("REJECT_REASON_WEIGHTS_JSON", "").strip()
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
        if not isinstance(parsed, dict):
            logger.warning("REJECT_REASON_WEIGHTS_JSON not a dict, ignoring")
            return {}
        out: dict[str, float] = {}
        for k, v in parsed.items():
            try:
                fv = float(v)
                if 0.0 <= fv <= 1.0:
                    out[str(k)] = fv
                else:
                    logger.warning(
                        "REJECT_REASON_WEIGHTS_JSON: %r out of [0,1], ignoring", k
                    )
            except (TypeError, ValueError):
                logger.warning(
                    "REJECT_REASON_WEIGHTS_JSON: bad value for %r, ignoring", k
                )
        return out
    except (json.JSONDecodeError, TypeError) as e:
        logger.warning("REJECT_REASON_WEIGHTS_JSON parse error: %s", e)
        return {}


def _resolve_weights() -> tuple[dict[str, float], list[tuple[str, float]]]:
    """Build the active weight table and prefix-match list (memoised).

    Sort prefixes by length descending so the more specific match wins
    (e.g. "VETO_BREADTH_VOL_LOW" prefers "VETO_BREADTH" over "VETO_").
    """
    global _WEIGHTS_CACHE, _SORTED_PREFIXES_CACHE
    if _WEIGHTS_CACHE is not None and _SORTED_PREFIXES_CACHE is not None:
        return _WEIGHTS_CACHE, _SORTED_PREFIXES_CACHE
    merged = dict(DEFAULT_WEIGHTS)
    merged.update(_load_overrides())
    _WEIGHTS_CACHE = merged
    # Build sorted prefix list (longer first → more specific match wins).
    _SORTED_PREFIXES_CACHE = sorted(
        merged.items(), key=lambda kv: (-len(kv[0]), kv[0])
    )
    return _WEIGHTS_CACHE, _SORTED_PREFIXES_CACHE


def is_enabled() -> bool:
    """Master switch — when False every reject_reason returns weight=1.0
    (back-compat / kill switch). Memoised; reset by `reset_cache()`."""
    global _ENABLED_CACHE
    if _ENABLED_CACHE is None:
        raw = (os.getenv("REJECT_REASON_WEIGHTS_ENABLED", "0") or "0").strip().lower()
        _ENABLED_CACHE = raw in {"1", "true", "yes", "on"}
    return _ENABLED_CACHE


def reset_cache() -> None:
    """Clear memoised env state — for tests / hot-reload."""
    global _WEIGHTS_CACHE, _SORTED_PREFIXES_CACHE, _ENABLED_CACHE
    _WEIGHTS_CACHE = None
    _SORTED_PREFIXES_CACHE = None
    _ENABLED_CACHE = None


def weight_for_reason(reason: str | None) -> float:
    """Return outcome-reliability weight ∈ [0, 1] for a gate reject_reason.

    - empty / OK / PASSED / ALLOW → 1.0 (real trade)
    - master switch off            → 1.0
    - unknown reason               → 1.0 (fail-open: never drop signal silently)
    - matched prefix               → table value
    """
    if not is_enabled():
        return 1.0
    if reason is None:
        return 1.0
    s = str(reason).strip()
    if s in _PASSED_TOKENS:
        return 1.0
    if not s:
        return 1.0
    _, prefixes = _resolve_weights()
    # Case-sensitive first (gate reasons are upper-case by convention) — try
    # lower-case match as backup.
    for prefix, w in prefixes:
        if s.startswith(prefix):
            return w
    s_lower = s.lower()
    for prefix, w in prefixes:
        if s_lower.startswith(prefix.lower()):
            return w
    return 1.0  # fail-open


def reason_family(reason: str | None) -> str:
    """Return the matched prefix family for telemetry labels.

    Useful for `calibrator_input_by_reason_family_total{family}` so the
    Prometheus cardinality stays bounded (~30 families) instead of unbounded
    reason strings.
    """
    if reason is None:
        return "na"
    s = str(reason).strip()
    if s in _PASSED_TOKENS or not s:
        return "passed"
    _, prefixes = _resolve_weights()
    for prefix, _w in prefixes:
        if s.startswith(prefix) or s.lower().startswith(prefix.lower()):
            # Normalize SHADOW_VETO_ → shadow_veto family
            return prefix.rstrip("_").lower() or prefix.lower()
    return "unknown"


@dataclass(frozen=True)
class WeightedSample:
    """Helper struct for callers that want both reason and weight together."""
    reason: str
    weight: float
    family: str

    @classmethod
    def from_reason(cls, reason: str | None) -> "WeightedSample":
        s = (reason or "").strip()
        return cls(reason=s, weight=weight_for_reason(s), family=reason_family(s))
