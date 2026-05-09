from __future__ import annotations

import json
import re
from typing import Any

from domain.evidence_keys import MetaKeys

# Stream contract for SRE metrics: metrics:of_gate
OF_GATE_SCHEMA_NAME = "of_gate_metrics"
OF_GATE_SCHEMA_VERSION = "1"

REQUIRED_FIELDS_V1 = (
    "ts_ms",
    "symbol",
    "scenario_v4",
    "ok",
    "ok_soft",
    "missing_legs",
)


def _as_int01(v: Any) -> int | None:
    try:
        x = int(v)
    except Exception:
        return None
    return x if x in (0, 1) else None


def _safe_str(v: Any) -> str:
    try:
        s = str(v)
    except Exception:
        s = ""
    return s


def derive_reason_code(row: dict[str, Any]) -> str:
    """
    Low-cardinality reason code (enum-like) for aggregation.
    Never put long free-form text here.
    """
    scenario_v4 = _safe_str(row.get("scenario_v4", "") or "")
    ok = _as_int01(row.get("ok", 0)) or 0
    ok_soft = _as_int01(row.get("ok_soft", 0)) or 0

    try:
        meta_veto = int(row.get(MetaKeys.VETO, 0) or 0)
    except Exception:
        meta_veto = 0

    if meta_veto == 1:
        return "meta_veto"
    if scenario_v4 == "dn_veto":
        return "dn_veto"
    if ok == 1:
        return "ok"
    if ok_soft == 1:
        return "soft_ok"

    try:
        if int(row.get("source_consistency_ok", 1) or 1) == 0:
            return "src_inconsistent"
    except Exception:
        pass
    try:
        if int(row.get("book_health_ok", 1) or 1) == 0:
            return "book_bad"
    except Exception:
        pass
    try:
        dh = float(row.get("data_health", 1.0) or 1.0)
        if dh < 0.70:
            return "data_health_bad"
    except Exception:
        pass

    return "rule_veto"


def why_label(x: Any) -> str:
    """Prometheus label sanitizer: low cardinality, safe charset, max 64 chars.

    Matches tick_flow_full/common/of_gate_metrics_contract.py#why_label.
    """
    s = (x or "na").strip().lower()
    if not s:
        return "na"
    s = re.sub(r"[^a-z0-9_]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    if len(s) > 64:
        s = s[:64]
    return s or "na"


def enrich_schema_fields(
    payload: dict[str, Any],
    *,
    schema_name: str | None = None,
    schema_version: int | None = None,
    reason_code: str | None = None,
) -> dict[str, Any]:
    """
    Low-cardinality schema markers for emitted metrics rows.

    Stage1-P1: supports optional keyword args (schema_name, schema_version,
    reason_code) in addition to the original positional-only signature so both
    old and new callers work without modification.
    """
    payload.setdefault("schema_name", schema_name if schema_name is not None else OF_GATE_SCHEMA_NAME)
    payload.setdefault(
        "schema_version",
        int(schema_version) if schema_version is not None else OF_GATE_SCHEMA_VERSION,
    )
    if reason_code is not None:
        payload.setdefault("reason_code", str(reason_code))
    else:
        payload.setdefault("reason_code", derive_reason_code(payload))

    # Optional detailed veto reason (top1). Low-cardinality & sanitized.
    # Must pass through why_label() to prevent high-cardinality free-form text.
    rc1 = payload.get("reason_code_top1")
    if rc1 not in (None, ""):
        payload["reason_code_top1"] = why_label(str(rc1))

    return payload


def validate_of_gate_row(row: dict[str, Any]) -> tuple[bool, str]:
    """
    Validate minimal contract for metrics:of_gate.
    Designed for both producers (payload dict) and consumers (rows from Redis Stream).
    """
    for k in REQUIRED_FIELDS_V1:
        if k not in row:
            return False, f"missing_{k}"

    # ts_ms must be int-like
    try:
        int(row.get("ts_ms"))
    except Exception:
        return False, "bad_ts_ms"

    if not _safe_str(row.get("symbol", "") or ""):
        return False, "bad_symbol"
    if not _safe_str(row.get("scenario_v4", "") or ""):
        return False, "bad_scenario_v4"

    ok = _as_int01(row.get("ok"))
    if ok is None:
        return False, "bad_ok"
    ok_soft = _as_int01(row.get("ok_soft"))
    if ok_soft is None:
        return False, "bad_ok_soft"
    if ok_soft == 1 and ok == 1:
        return False, "bad_ok_soft_implies_not_ok"

    # missing_legs should be JSON list (string) or list
    try:
        v = row.get("missing_legs", "[]")
        arr = json.loads(v) if isinstance(v, str) else v
        if not isinstance(arr, list):
            return False, "bad_missing_legs_type"
    except Exception:
        return False, "bad_missing_legs_json"

    return True, "ok"
