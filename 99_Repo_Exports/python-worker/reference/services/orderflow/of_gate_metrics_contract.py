from __future__ import annotations

import json
import re
from typing import Any, Dict, Tuple

# Canonical schema identity for the of-gate decision metrics stream.
SCHEMA_NAME_V1 = "of_gate_metrics"
SCHEMA_VERSION_V1 = 1

# Backward-compat aliases (producers that still use the old names work unchanged)
OF_GATE_SCHEMA_NAME = SCHEMA_NAME_V1
OF_GATE_SCHEMA_VERSION = str(SCHEMA_VERSION_V1)

# Required fields for producers (consumer-side can be more permissive).
REQUIRED_FIELDS_V1 = [
    "schema_name"
    "schema_version"
    "ts_ms"
    "symbol"
    "ok"
    "ok_soft"
    "missing_legs"
    "scenario_v4"
    "reason_code"
]

# Conservative time range checks (epoch ms).
EPOCH_MS_MIN = 1_500_000_000_000  # ~2017-07-14
EPOCH_MS_MAX = 2_400_000_000_000  # ~2046-01-01

# Scenario normalization: low-cardinality, stable, ASCII-only.
_SCEN_RE = re.compile(r"[^a-z0-9_]+")


def normalize_scenario_v4(x: Any) -> str:
    """Normalize scenario to low-cardinality, stable, ASCII-only string (max 32 chars)."""
    s = str(x or "na").strip().lower()
    if not s:
        return "na"
    s = s.replace("-", "_").replace(" ", "_")
    s = _SCEN_RE.sub("_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    if not s:
        return "na"
    # Bound length to avoid accidental high-cardinality explosions.
    if len(s) > 32:
        s = s[:32]
    return s


def _as_int01(v: Any, default: int = 0) -> int:
    try:
        i = int(v)
    except Exception:
        return int(default)
    return 1 if i == 1 else 0


def _as_int(v: Any, default: int = 0) -> int:
    try:
        return int(v)
    except Exception:
        return int(default)


def _as_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return float(default)


def scenario_key(row: Dict[str, Any]) -> str:
    """Extract and normalize scenario from a row dict."""
    return normalize_scenario_v4(row.get("scenario_v4") or row.get("scenario") or "na")


def derive_reason_code(row: Dict[str, Any]) -> str:
    """Derive coarse, low-cardinality reason codes from row fields.

    Priority: dq_fail > drift_block > ok_hard > ok_soft > veto
    Never put long free-form text here.
    """
    ok = _as_int01(row.get("ok", 0))
    ok_soft = _as_int01(row.get("ok_soft", 0))

    dq = str(row.get("dq_state", "") or row.get("dq", "") or "").lower()
    drift = str(row.get("drift_state", "") or "").lower()
    if dq and dq not in {"ok", "na"}:
        return "dq_fail"
    if drift in {"block", "fail", "veto"}:
        return "drift_block"

    if ok == 1:
        return "ok_hard"
    if ok_soft == 1:
        return "ok_soft"
    return "veto"


def why_label(x: Any) -> str:
    """Prometheus label sanitizer: low cardinality, safe charset, max 64 chars."""
    s = str(x or "na").strip().lower()
    if not s:
        return "na"
    s = re.sub(r"[^a-z0-9_]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    if len(s) > 64:
        s = s[:64]
    return s or "na"


def enrich_schema_fields(row: Dict[str, Any], **kwargs: Any) -> Dict[str, Any]:
    """Enrich producer rows with schema fields + normalized scenario + reason_code.

    Safe to call multiple times; it only fills missing / normalizes.
    Accepts **kwargs to set row fields from call-site without extra boilerplate.
    """
    if not isinstance(row, dict):
        return row

    # Allow passing values as kwargs to reduce call-site boilerplate
    for k, v in kwargs.items():
        if k not in row or row.get(k) in (None, ""):
            row[k] = v

    row.setdefault("schema_name", SCHEMA_NAME_V1)
    row.setdefault("schema_version", SCHEMA_VERSION_V1)

    # Scenario normalization
    row["scenario_v4"] = normalize_scenario_v4(row.get("scenario_v4") or row.get("scenario") or "na")

    # reason_code: accept provided but sanitize, otherwise derive
    rc = row.get("reason_code")
    if rc in (None, ""):
        row["reason_code"] = derive_reason_code(row)
    else:
        row["reason_code"] = why_label(rc)

    # Optional detailed veto reason enum (top1). Keep low-cardinality, ASCII-only.
    rt = row.get("reason_code_top1")
    if rt not in (None, ""):
        s = why_label(rt)
        row["reason_code_top1"] = s[:32] if len(s) > 32 else s

    return row


def validate_of_gate_row(row: Dict[str, Any]) -> Tuple[bool, str]:
    """Lightweight producer/consumer validation.

    Returns (ok, code) where code is a low-cardinality reason.
    Designed for both producers (payload dict) and consumers (rows from Redis Stream).
    """
    if not isinstance(row, dict):
        return False, "row_not_dict"

    # Required fields present
    for k in REQUIRED_FIELDS_V1:
        if k not in row:
            return False, f"missing_{k}"

    # Schema identity
    if str(row.get("schema_name")) != SCHEMA_NAME_V1:
        return False, "schema_name"
    if _as_int(row.get("schema_version"), -1) != SCHEMA_VERSION_V1:
        return False, "schema_version"

    # Time sanity
    ts = _as_int(row.get("ts_ms"), 0)
    if ts < EPOCH_MS_MIN or ts > EPOCH_MS_MAX:
        return False, "bad_ts_ms"

    # ok / ok_soft coherence
    ok = _as_int01(row.get("ok", 0))
    ok_soft = _as_int01(row.get("ok_soft", 0))
    if ok_soft == 1 and ok == 1:
        return False, "bad_ok_soft_implies_not_ok"

    # Scenario normalization must not be empty
    scen = normalize_scenario_v4(row.get("scenario_v4"))
    if scen in ("", None):
        return False, "bad_scenario_v4"
    row["scenario_v4"] = scen

    # reason_code must be non-empty (and sanitized)
    rc = why_label(row.get("reason_code"))
    if rc == "na":
        # still allow it, but mark as warning-level badness for contract checks
        return False, "bad_reason_code"
    row["reason_code"] = rc

    # Optional reason_code_top1 sanitize (non-fatal)
    if "reason_code_top1" in row:
        s = why_label(row.get("reason_code_top1"))
        row["reason_code_top1"] = s[:32] if len(s) > 32 else s

    # missing_legs: must be JSON list OR python list
    ml = row.get("missing_legs")
    if isinstance(ml, str):
        try:
            parsed = json.loads(ml)
        except Exception:
            return False, "bad_missing_legs_json"
        if not isinstance(parsed, list):
            return False, "bad_missing_legs_type"
    elif isinstance(ml, list):
        pass
    else:
        return False, "bad_missing_legs_type"

    return True, "ok"
