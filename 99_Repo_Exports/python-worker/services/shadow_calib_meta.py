"""
Shadow/Calibration metadata field definitions — single source of truth.

Every component that copies, persists, or enriches calibration-related fields
(signal_pipeline → execution_router → binance_executor → trade_monitor →
 trade_close_joiner → calibrator) MUST use ``CALIB_FIELDS`` from this module
to avoid drift.

Fail-open: extraction helpers never raise; missing fields are silently skipped.
"""

from __future__ import annotations

from typing import Any, Dict, Sequence, Tuple

# -----------------------------------------------------------------------
# Canonical field whitelist
# -----------------------------------------------------------------------
# Ordered tuple so iteration is deterministic.
CALIB_FIELDS: Tuple[str, ...] = (
    "paper_only",
    "shadow_only",
    "is_virtual",
    "calib",
    "calib_kind",
    "calib_run_id",
    "candidate_window_ms",
    "baseline_window_ms",
    "cont_ctx_age_ms",
    "entry_reason",
    "parent_signal_id",
)


def extract_calib_fields(source: Any) -> Dict[str, Any]:
    """Extract calibration fields from *source* dict (or dict-like).

    Returns a dict containing only the fields that are present and not-None.
    Safe to call on ``None``, empty dicts, non-dicts — returns ``{}``.
    """
    if not isinstance(source, dict):
        return {}
    out: Dict[str, Any] = {}
    for key in CALIB_FIELDS:
        val = source.get(key)
        if val is not None:
            out[key] = val
    return out


def merge_calib_fields(
    target: Dict[str, Any],
    *sources: Any,
    overwrite: bool = False,
) -> None:
    """Merge calibration fields from *sources* into *target* (in-place).

    Priority: first non-None value across sources wins UNLESS *overwrite=True*
    in which case later sources overwrite earlier ones.

    Typical usage in joiner::

        merge_calib_fields(out, close_payload, sp_from_close, sp_from_decision, decision)
    """
    for key in CALIB_FIELDS:
        if not overwrite and key in target and target[key] is not None:
            continue
        for src in sources:
            if not isinstance(src, dict):
                continue
            val = src.get(key)
            if val is not None:
                target[key] = val
                break


def stamp_virtual_if_calib(payload: Dict[str, Any]) -> None:
    """If payload carries calib=1, ensure is_virtual=1.

    Paper/shadow calibration signals MUST never reach the prod execution path.
    """
    if int(payload.get("calib") or 0) == 1:
        payload["is_virtual"] = 1
