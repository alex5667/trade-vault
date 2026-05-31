from __future__ import annotations

import os
import math
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from common.json_fast import dumps1
from common.json_safe import to_json_safe
from common.outbox_contract import ContractViolation, assert_json_safe


def _env_str(name: str, default: str) -> str:
    try:
        return str(os.getenv(name, default) or default)
    except Exception:
        return default


def _env_int(name: str, default: int) -> int:
    try:
        return int(float(os.getenv(name, str(default)) or default))
    except Exception:
        return int(default)


def _is_finite_float(x: float) -> bool:
    try:
        return (not math.isnan(x)) and (not math.isinf(x))
    except Exception:
        return False


def payload_policy_mode() -> str:
    # off | warn | raise
    return _env_str("PAYLOAD_POLICY_MODE", "warn").strip().lower()


def payload_max_bytes() -> int:
    v = _env_int("PAYLOAD_MAX_BYTES", 8192)
    return max(512, int(v))


def payload_max_strlen() -> int:
    v = _env_int("PAYLOAD_MAX_STRLEN", 512)
    return max(64, int(v))


def payload_max_reasons() -> int:
    v = _env_int("PAYLOAD_MAX_REASONS", 16)
    return max(0, int(v))


def payload_max_parts_keys() -> int:
    v = _env_int("PAYLOAD_MAX_PARTS_KEYS", 32)
    return max(0, int(v))


def _bytes(obj: Any) -> int:
    try:
        return len(dumps1(obj).encode("utf-8", "ignore"))
    except Exception:
        return 10**9


def _truncate_str(s: Any, n: int) -> str:
    try:
        ss = str(s)
    except Exception:
        ss = ""
    if n <= 0:
        return ""
    if len(ss) <= n:
        return ss
    return ss[: max(0, n - 3)] + "..."


def validate_tradeable_signal_payload(payload: Dict[str, Any]) -> None:
    """
    Minimal strict schema for tradeable signal payload.
    Intentionally small and stable to avoid churn.
    """
    if not isinstance(payload, dict):
        raise ContractViolation("payload_not_dict", "$payload")

    # must be JSON-safe
    assert_json_safe(payload, path="$payload")

    # required fields
    req = ["sid", "signal_id", "kind", "side", "symbol", "ts", "price", "confidence", "conf_factor"]
    for k in req:
        if k not in payload:
            raise ContractViolation(f"missing:{k}", f"$payload.{k}")

    sid = payload.get("sid")
    if not isinstance(sid, str) or not sid.strip():
        raise ContractViolation("sid_invalid", "$payload.sid")
    if len(sid) > 128:
        raise ContractViolation("sid_too_long", "$payload.sid")

    if payload.get("signal_id") != sid:
        # keep strict: sid and signal_id must match (your builder sets both)
        raise ContractViolation("sid_signal_id_mismatch", "$payload.signal_id")

    kind = payload.get("kind")
    if not isinstance(kind, str) or not kind.strip():
        raise ContractViolation("kind_invalid", "$payload.kind")

    side = payload.get("side")
    if side not in ("LONG", "SHORT"):
        raise ContractViolation("side_invalid", "$payload.side")

    sym = payload.get("symbol")
    if not isinstance(sym, str) or not sym.strip():
        raise ContractViolation("symbol_invalid", "$payload.symbol")

    ts = payload.get("ts")
    if not isinstance(ts, int) or ts < 0:
        raise ContractViolation("ts_invalid", "$payload.ts")

    price = payload.get("price")
    if not isinstance(price, (int, float)) or float(price) <= 0.0 or (isinstance(price, float) and not _is_finite_float(price)):
        raise ContractViolation("price_invalid", "$payload.price")

    conf = payload.get("confidence")
    if not isinstance(conf, (int, float)) or not _is_finite_float(float(conf)):
        raise ContractViolation("confidence_invalid", "$payload.confidence")

    cf = payload.get("conf_factor")
    if not isinstance(cf, (int, float)) or not _is_finite_float(float(cf)):
        raise ContractViolation("conf_factor_invalid", "$payload.conf_factor")

    reasons = payload.get("reasons", [])
    if reasons is not None:
        if not isinstance(reasons, list):
            raise ContractViolation("reasons_not_list", "$payload.reasons")
        if len(reasons) > payload_max_reasons():
            raise ContractViolation("reasons_too_many", "$payload.reasons")
        for i, r in enumerate(reasons):
            if not isinstance(r, str):
                raise ContractViolation("reason_not_str", f"$payload.reasons[{i}]")

    parts = payload.get("parts", {})
    if parts is not None:
        if not isinstance(parts, dict):
            raise ContractViolation("parts_not_dict", "$payload.parts")
        if len(parts) > payload_max_parts_keys():
            raise ContractViolation("parts_too_many_keys", "$payload.parts")


def enforce_payload_budget(
    *,
    payload: Dict[str, Any],
    payload_meta: Optional[Dict[str, Any]] = None,
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    Enforces:
      - string caps
      - key count caps
      - total JSON bytes cap
    If overflow: moves large blobs to payload_meta and degrades payload to a safe minimal core.
    """
    pm: Dict[str, Any] = dict(payload_meta or {})
    p: Dict[str, Any] = dict(payload or {})

    # 1) cap strings in the top-level known noisy fields
    maxs = payload_max_strlen()
    for k in ("note", "text", "trace_summary"):
        if k in p and isinstance(p.get(k), str):
            p[k] = _truncate_str(p[k], maxs)
    # reasons strings cap
    if isinstance(p.get("reasons"), list):
        p["reasons"] = [_truncate_str(x, maxs) for x in p["reasons"][: payload_max_reasons()]]

    # 2) cap parts keys count (drop extra deterministically by sorted key)
    parts = p.get("parts")
    if isinstance(parts, dict) and len(parts) > payload_max_parts_keys():
        keys = sorted([str(k) for k in parts.keys()])
        keep = set(keys[: payload_max_parts_keys()])
        dropped = {k: parts[k] for k in keys if k not in keep}
        p["parts"] = {k: parts[k] for k in keys if k in keep}
        if dropped:
            pm.setdefault("parts_dropped", {})  # diagnostics-only
            pm["parts_dropped"] = dropped

    # 3) sanitize json-safe
    p = to_json_safe(p)
    pm = to_json_safe(pm)

    # 4) enforce total budget by degrading in steps
    maxb = payload_max_bytes()
    if _bytes(p) <= maxb:
        return p, pm

    # Step A: move parts entirely to meta
    if isinstance(p.get("parts"), dict) and p["parts"]:
        pm.setdefault("parts_full", {})
        try:
            pm["parts_full"] = pm["parts_full"] or {}
        except Exception:
            pm["parts_full"] = {}
        # merge current parts into parts_full
        try:
            if isinstance(pm["parts_full"], dict):
                pm["parts_full"].update(p["parts"])
        except Exception:
            pass
        p["parts"] = {}
        p = to_json_safe(p)
        pm = to_json_safe(pm)
        if _bytes(p) <= maxb:
            return p, pm

    # Step B: drop reasons
    if isinstance(p.get("reasons"), list) and p["reasons"]:
        pm["reasons_full"] = p["reasons"]
        p["reasons"] = []
        p = to_json_safe(p)
        pm = to_json_safe(pm)
        if _bytes(p) <= maxb:
            return p, pm

    # Step C: keep only minimal core
    core_keys = ["sid", "signal_id", "kind", "side", "symbol", "ts", "price", "confidence", "conf_factor"]
    core = {k: p.get(k) for k in core_keys if k in p}
    # keep optional small numeric scores if present
    for k in ("raw_score", "final_score", "decision_u16", "rc", "tf"):
        if k in p:
            core[k] = p.get(k)
    pm.setdefault("payload_stripped", True)
    return to_json_safe(core), to_json_safe(pm)


def enforce_and_validate_payload(
    *,
    payload: Dict[str, Any],
    payload_meta: Optional[Dict[str, Any]] = None,
    logger: Optional[Any] = None,
    where: str = "",
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    Single entry point:
      - enforce budget
      - validate schema
      - mode: off|warn|raise
    """
    mode = payload_policy_mode()
    if mode not in ("warn", "raise"):
        # still budget-enforce to avoid pathological sizes
        return enforce_payload_budget(payload=payload, payload_meta=payload_meta)

    p2, m2 = enforce_payload_budget(payload=payload, payload_meta=payload_meta)
    try:
        validate_tradeable_signal_payload(p2)
        return p2, m2
    except ContractViolation as e:
        if mode == "raise":
            raise
        # warn
        try:
            if logger is not None:
                logger.error(dumps1({"event": "payload_policy_violation", "where": where, "reason": e.reason, "path": e.path, "sid": str(p2.get("sid") or "")}))
        except Exception:
            pass
        return p2, m2
