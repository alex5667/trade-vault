from __future__ import annotations

import math
import os
from dataclasses import dataclass
from typing import Any

from common.json_fast import dumps1

_JSON_SCALARS = (str, int, float, bool, type(None))


@dataclass(frozen=True)
class ContractViolation(Exception):
    """
    Raised when an outbox contract is violated.
    Keep it structured so tests and logs can assert exact failure reasons.
    """
    reason: str
    path: str = "$"

    def __str__(self) -> str:
        return f"ContractViolation(reason={self.reason}, path={self.path})"


# Tradeable message MUST NOT contain these anywhere.
FORBIDDEN_TRADEABLE_KEYS: set[str] = {
    "trace",
    "events",
    "payload_meta",
    "parts_full",
}

# Targets inside envelope are tradeable-ish: keep them clean too.
FORBIDDEN_TARGET_KEYS: set[str] = {
    "trace",
    "events",
    "payload_meta",
    "parts_full",
}


def _env_str(name: str, default: str) -> str:
    try:
        return str(os.getenv(name, default) or default)
    except Exception:
        return default


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)) or default)
    except Exception:
        return default


def outbox_contract_mode() -> str:
    # off | warn | raise
    return _env_str("OUTBOX_CONTRACT_MODE", "off").strip().lower()


def outbox_contract_sample_rate() -> float:
    # keep default small; "raise" mode is for CI anyway
    r = _env_float("OUTBOX_CONTRACT_SAMPLE_RATE", 0.05)
    if not math.isfinite(r) or r <= 0.0:
        return 0.0
    if r > 1.0:
        return 1.0
    return float(r)


def _is_finite_float(x: float) -> bool:
    try:
        return (not math.isnan(x)) and (not math.isinf(x))
    except Exception:
        return False


def assert_json_safe(obj: Any, *, path: str = "$", max_depth: int = 12) -> None:
    """
    Strict: object must contain only JSON-safe primitives:
      str/int/float/bool/None/list/dict, and floats must be finite.
    """
    if max_depth <= 0:
        # depth cap reached; still must be scalar
        if not isinstance(obj, _JSON_SCALARS):
            raise ContractViolation("not_json_scalar_at_depth_cap", path)
        if isinstance(obj, float) and not _is_finite_float(obj):
            raise ContractViolation("nan_or_inf", path)
        return

    if isinstance(obj, _JSON_SCALARS):
        if isinstance(obj, float) and not _is_finite_float(obj):
            raise ContractViolation("nan_or_inf", path)
        return

    if isinstance(obj, list):
        for i, v in enumerate(obj):
            assert_json_safe(v, path=f"{path}[{i}]", max_depth=max_depth - 1)
        return

    if isinstance(obj, dict):
        for k, v in obj.items():
            # keys must be strings (envelope/payload contract)
            if not isinstance(k, str):
                raise ContractViolation("dict_key_not_str", f"{path}.{str(k)}")
            assert_json_safe(v, path=f"{path}.{k}", max_depth=max_depth - 1)
        return

    raise ContractViolation("not_json_safe_type", path)


def find_forbidden_keys(obj: Any, forbidden: set[str], *, path: str = "$", max_depth: int = 12) -> list[tuple[str, str]]:
    """
    Returns list of (path, key) where forbidden keys were found.
    """
    hits: list[tuple[str, str]] = []
    if max_depth <= 0:
        return hits
    if isinstance(obj, dict):
        for k, v in obj.items():
            kk = str(k)
            if kk in forbidden:
                hits.append((path, kk))
            hits.extend(find_forbidden_keys(v, forbidden, path=f"{path}.{kk}", max_depth=max_depth - 1))
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            hits.extend(find_forbidden_keys(v, forbidden, path=f"{path}[{i}]", max_depth=max_depth - 1))
    return hits


def strip_forbidden_keys(obj: Any, forbidden: set[str], *, max_depth: int = 12, max_list: int = 512, max_dict: int = 512) -> Any:
    """
    Defensive stripping for targets/envelopes:
    - removes forbidden dict keys recursively
    - keeps JSON-safe structure shape
    """
    if max_depth <= 0:
        return obj
    if isinstance(obj, list):
        return [strip_forbidden_keys(x, forbidden, max_depth=max_depth - 1, max_list=max_list, max_dict=max_dict) for x in obj[:max_list]]
    if isinstance(obj, dict):
        out: dict[str, Any] = {}
        i = 0
        for k, v in obj.items():
            if i >= max_dict:
                break
            kk = str(k)
            if kk in forbidden:
                continue
            out[kk] = strip_forbidden_keys(v, forbidden, max_depth=max_depth - 1, max_list=max_list, max_dict=max_dict)
            i += 1
        return out
    return obj


def _approx_bytes(obj: Any) -> int:
    try:
        s = dumps1(obj)
        return len(s.encode("utf-8", "ignore"))
    except Exception:
        # if not serializable -> treat as huge
        return 10**9


def validate_tradeable_payload(payload: dict[str, Any]) -> None:
    if not isinstance(payload, dict):
        raise ContractViolation("payload_not_dict", "$")
    assert_json_safe(payload, path="$payload")
    hits = find_forbidden_keys(payload, FORBIDDEN_TRADEABLE_KEYS, path="$payload")
    if hits:
        p, k = hits[0]
        raise ContractViolation(f"forbidden_key:{k}", p)


def validate_sidecar_meta(meta: dict[str, Any]) -> None:
    # Sidecar is allowed to contain heavy dicts (trace/payload_meta), but must be JSON-safe.
    if not isinstance(meta, dict):
        raise ContractViolation("meta_not_dict", "$")
    assert_json_safe(meta, path="$meta")


def validate_outbox_envelope(env: dict[str, Any]) -> None:
    if not isinstance(env, dict):
        raise ContractViolation("envelope_not_dict", "$")
    assert_json_safe(env, path="$env")
    # Envelope must not contain trace/events anywhere.
    hits = find_forbidden_keys(env, {"trace", "events"}, path="$env")
    if hits:
        p, k = hits[0]
        raise ContractViolation(f"forbidden_key:{k}", p)
    # Targets must not contain forbidden target keys.
    targets = env.get("targets")
    if isinstance(targets, dict):
        th = find_forbidden_keys(targets, FORBIDDEN_TARGET_KEYS, path="$env.targets")
        if th:
            p, k = th[0]
            raise ContractViolation(f"forbidden_key:{k}", p)


def contract_check_best_effort(
    *,
    kind: str,
    obj: dict[str, Any],
    where: str,
    sid: str = "",
    logger: Any | None = None,
) -> bool:
    """
    Enforcement wrapper:
      - mode=off  -> no-op
      - mode=warn -> log structured violation, return False
      - mode=raise-> raise ContractViolation (CI)
    """
    mode = outbox_contract_mode()
    if mode not in ("warn", "raise"):
        return True
    try:
        if kind == "payload":
            validate_tradeable_payload(obj)
        elif kind == "meta":
            validate_sidecar_meta(obj)
        elif kind == "envelope":
            validate_outbox_envelope(obj)
        else:
            return True
        return True
    except ContractViolation as e:
        if mode == "raise":
            raise
        # warn
        if logger is not None:
            try:
                logger.error(
                    dumps1(
                        {
                            "event": "outbox_contract_violation",
                            "where": str(where),
                            "kind": str(kind),
                            "sid": (sid or ""),
                            "reason": str(e.reason),
                            "path": str(e.path),
                        }
                    )
                )
            except Exception:
                pass
        return False
