from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any


def _to_str(x: Any) -> str:
    if x is None:
        return ""
    if isinstance(x, bytes):
        try:
            return x.decode("utf-8", errors="ignore")
        except Exception:
            return ""
    return str(x)


def intish(x: Any, default: int = 0) -> int:
    """
    Parse int-like values coming from Redis Streams / JSON.
    Accepts:
      - int/float/bool
      - bytes/str: "1", "0", "1.0", "true", "false"
      - key=value tokens: "ok=1"
    """
    if x is None:
        return default
    if isinstance(x, bool):
        return 1 if x else 0
    if isinstance(x, int):
        return x
    if isinstance(x, float):
        try:
            return int(x)
        except Exception:
            return default

    s = _to_str(x).strip()
    if not s:
        return default

    if "=" in s and len(s) <= 32:
        parts = s.split("=", 1)
        if len(parts) == 2 and parts[1].strip():
            s = parts[1].strip()

    low = s.lower()
    if low in ("true", "t", "yes", "y", "on"):
        return 1
    if low in ("false", "f", "no", "n", "off"):
        return 0

    try:
        return int(float(s))
    except Exception:
        return default


def _get(m: Mapping[str, Any], key: str) -> Any:
    """Return value for key from mapping where keys may be str or bytes."""
    if key in m:
        return m[key]
    try:
        b = key.encode("utf-8", errors="ignore")
        if b in m:  # type: ignore[operator]
            return m[b]  # type: ignore[index]
    except Exception:
        pass
    return None


def _maybe_json_dict(x: Any) -> dict[str, Any] | None:
    if x is None:
        return None
    if isinstance(x, dict):
        return x  # type: ignore[return-value]
    s = _to_str(x).strip()
    if not s:
        return None
    if not (s.startswith("{") and s.endswith("}")):
        return None
    try:
        v = json.loads(s)
        return v if isinstance(v, dict) else None
    except Exception:
        return None


OK_KEYS = ("ok", "rule_ok", "ok_rule", "ok_strict")
OK_SOFT_KEYS = ("ok_soft", "rule_ok_soft", "ok_rule_soft", "soft_ok")


def parse_ok_fields(row: Mapping[str, Any]) -> tuple[int, int]:
    """
    Return (ok_strict, ok_soft) for a metrics row.
    Searches:
      - stream fields
      - JSON in 'payload'
      - nested dicts 'evidence' / 'decision' / 'rule'
    """
    scopes: list[Mapping[str, Any]] = [row]

    payload = _maybe_json_dict(_get(row, "payload"))
    if payload:
        scopes.append(payload)
        rule = _maybe_json_dict(_get(payload, "rule"))
        if rule:
            scopes.append(rule)

    for k in ("evidence", "decision", "rule"):
        d = _maybe_json_dict(_get(row, k))
        if d:
            scopes.append(d)

    ok_val = None
    soft_val = None
    for scope in scopes:
        for k in OK_KEYS:
            v = _get(scope, k)
            if v is not None:
                ok_val = v
                break
        if ok_val is not None:
            break

    for scope in scopes:
        for k in OK_SOFT_KEYS:
            v = _get(scope, k)
            if v is not None:
                soft_val = v
                break
        if soft_val is not None:
            break

    return intish(ok_val, 0), intish(soft_val, 0)


def get_scenario(row: Mapping[str, Any]) -> str:
    return _to_str(_get(row, "scenario_v4") or _get(row, "scenario") or "na")


def get_ts_ms(row: Mapping[str, Any]) -> int:
    return intish(_get(row, "ts_ms") or _get(row, "ts") or _get(row, "timestamp") or 0, 0)
def derive_ok_fields_from_ofc(ofc: Any, evidence: dict[str, Any] | None = None) -> tuple[int, int, int, str]:
    """Derive (ok, ok_soft, ok_rule, ok_src) from OFConfirm-like object + evidence dict.

    Motivation: in some branches OFConfirm exposes ok_rule/allow but not ok, which made ok_rate always 0.
    """
    e = evidence if isinstance(evidence, dict) else {}
    if not e:
        try:
            ev = getattr(ofc, "evidence", None)
            if isinstance(ev, dict):
                e = ev
        except Exception:
            e = {}

    # ok_soft
    ok_soft_v = e.get("ok_soft")
    if ok_soft_v is not None:
        ok_soft = intish(ok_soft_v, 0)
    else:
        v = getattr(ofc, "ok_soft", None)
        ok_soft = intish(v, 0)

    # ok_rule (heuristic)
    v = getattr(ofc, "ok_rule", None)
    if v is not None:
        ok_rule = intish(v, 0)
    elif "ok_rule" in e:
        ok_rule = intish(e.get("ok_rule"), 0)
    elif "rule_ok" in e:
        ok_rule = intish(e.get("rule_ok"), 0)
    else:
        ok_rule = -1

    # ok (final)
    ok_src = "missing"
    v = getattr(ofc, "ok", None)
    if v is not None:
        ok = intish(v, 0)
        ok_src = "ofc.ok"
    else:
        v = getattr(ofc, "allow", None)
        if v is not None:
            ok = intish(v, 0)
            ok_src = "ofc.allow"
        elif "ok" in e:
            ok = intish(e.get("ok"), 0)
            ok_src = "evidence.ok"
        else:
            ok = -1

    # fallback
    if ok not in (0, 1):
        if ok_rule in (0, 1):
            ok = int(ok_rule)
            ok_src = "fallback.ok_rule"
        else:
            ok = 0
            ok_src = "fallback.0"

    if ok_rule not in (0, 1):
        ok_rule = int(ok)

    return int(ok), int(ok_soft), int(ok_rule), str(ok_src)
