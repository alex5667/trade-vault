from __future__ import annotations

"""
v14_of feature population helper — build_og_payload.

Produces the 16 `og_*` (OrderFlow rule-Gate consensus) keys declared in
core/ml_feature_schema_v14_of.py from runtime artifacts of of_confirm_engine:

  - ofc  (OFConfirmV3)            — final confirmation object (have, need, score,
                                    contrib, gate_bits, reason, evidence, ok)
  - dec  (StrongGateDecision)     — pre-finalize gate decision (may be None)
  - indicators (dict)             — per-tick indicator snapshot
                                    (weak_progress, legacy_of_score_min, etc.)

Strong-need (need_rev/need_cont/reason) is read from ofc.evidence under the
keys `strong_need_reversal`, `strong_need_continuation`, `strong_need_reason`.
of_confirm_engine writes these to evidence at the same site where
compute_strong_need_same_tick() is called.

Design:
  - Fail-open: every key defaults to 0.0; missing artifacts never raise.
  - Pure function: no I/O, no globals; safe to call on hot path.
  - Idempotent: identical inputs → identical output dict.
"""

import hashlib
from typing import Any


_OG_KEYS: tuple[str, ...] = (
    "og_have",
    "og_need",
    "og_have_minus_need",
    "og_ok",
    "og_score_minus_threshold",
    "og_contrib_z",
    "og_contrib_wp",
    "og_contrib_reclaim",
    "og_contrib_obi",
    "og_contrib_iceberg",
    "og_contrib_absorption",
    "og_gate_bits_count",
    "og_strong_need_rev",
    "og_strong_need_cont",
    "og_weak_progress_any",
    "og_reason_code_id",
)


def _f(x: Any, d: float = 0.0) -> float:
    try:
        if x is None:
            return d
        return float(x)
    except Exception:
        return d


def _i(x: Any, d: int = 0) -> int:
    try:
        if x is None:
            return d
        if isinstance(x, bool):
            return 1 if x else 0
        return int(float(x))
    except Exception:
        return d


def _reason_code_id(reason: str) -> float:
    """Stable hash of reason string → small int in [0, 64).

    blake2b is used for cross-process determinism (vs Python's randomized hash()).
    Bucket size 64 keeps cardinality low for tree-based models / one-hot expansion.
    """
    if not reason:
        return 0.0
    h = hashlib.blake2b(reason.encode("utf-8"), digest_size=8).digest()
    return float(int.from_bytes(h, "big") % 64)


def build_og_payload(
    *,
    ofc: Any | None = None,
    dec: Any | None = None,
    indicators: dict[str, Any] | None = None,
) -> dict[str, float]:
    """Build the 16-key og_* dict for inclusion in signals:of:inputs payload.

    All keys are guaranteed present (fail-open to 0.0).
    """
    out: dict[str, float] = {k: 0.0 for k in _OG_KEYS}
    ind = indicators or {}

    if ofc is None and dec is None and not ind:
        return out

    # Prefer ofc (post-finalize), fall back to dec.
    have = _i(getattr(ofc, "have", None), _i(getattr(dec, "have", None), 0))
    need = _i(getattr(ofc, "need", None), _i(getattr(dec, "need", None), 0))
    out["og_have"] = float(have)
    out["og_need"] = float(need)
    out["og_have_minus_need"] = float(have - need)
    out["og_ok"] = float(_i(getattr(ofc, "ok", None), 0))

    score = _f(getattr(ofc, "score", None), 0.0)
    threshold = _f(ind.get("legacy_of_score_min"), 0.0)
    out["og_score_minus_threshold"] = score - threshold

    # contrib (dict from OFConfirmV3)
    contrib = getattr(ofc, "contrib", None)
    if isinstance(contrib, dict):
        out["og_contrib_z"] = _f(contrib.get("z"), 0.0)
        out["og_contrib_wp"] = _f(contrib.get("weak_progress"), 0.0)
        out["og_contrib_reclaim"] = _f(contrib.get("reclaim"), 0.0)
        out["og_contrib_obi"] = _f(contrib.get("obi_stable"), 0.0)
        out["og_contrib_iceberg"] = _f(contrib.get("iceberg_strict"), 0.0)
        out["og_contrib_absorption"] = _f(contrib.get("absorption"), 0.0)

    # gate_bits popcount (number of distinct legs that fired)
    gate_bits = _i(getattr(ofc, "gate_bits", None), 0)
    out["og_gate_bits_count"] = float(bin(gate_bits & 0xFFFFFFFF).count("1"))

    # Strong-need policy values are surfaced through ofc.evidence
    # (of_confirm_engine writes them at the site of compute_strong_need_same_tick).
    evidence = getattr(ofc, "evidence", None)
    if isinstance(evidence, dict):
        out["og_strong_need_rev"] = float(_i(evidence.get("strong_need_reversal"), 0))
        out["og_strong_need_cont"] = float(_i(evidence.get("strong_need_continuation"), 0))
    else:
        evidence = {}

    # weak_progress: mirror of `indicators["weak_progress"]` (already used elsewhere as boolean leg)
    out["og_weak_progress_any"] = float(_i(ind.get("weak_progress"), 0))

    # reason_code: prefer ofc.reason, fall back to dec.need_reason, then evidence.strong_need_reason
    reason = ""
    if ofc is not None:
        reason = str(getattr(ofc, "reason", "") or "")
    if not reason and dec is not None:
        reason = str(getattr(dec, "need_reason", "") or "")
    if not reason:
        reason = str(evidence.get("strong_need_reason", "") or "")
    out["og_reason_code_id"] = _reason_code_id(reason)

    return out


def og_keys() -> tuple[str, ...]:
    """Public read-only accessor for the canonical og_* key tuple."""
    return _OG_KEYS
