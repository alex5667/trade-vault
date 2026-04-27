"""
Contract for cfg:ml_confirm:champion JSON (Redis).
Keep labels low-cardinality: DO NOT put run_id/model_path into Prometheus labels.
"""

import json
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple


ALLOWED_MODES = {"SHADOW", "CANARY", "ENFORCE"}


class ChampionCfgError(ValueError):
    """Error validating champion config."""
    pass


@dataclass(frozen=True)
class ChampionCfg:
    """
    Contract for cfg:ml_confirm:champion JSON (Redis).
    Keep labels low-cardinality: DO NOT put run_id/model_path into Prometheus labels.
    """
    schema_version: int
    kind: str
    run_id: str
    created_ms: int
    model_path: str
    mode: str                 # SHADOW|CANARY|ENFORCE
    enforce_share: float      # [0..1]
    calibrator_path: Optional[str] = None
    feature_version: Optional[str] = None
    model_type: Optional[str] = None
    checksum: Optional[str] = None


def _as_int(v: Any, field: str) -> int:
    if isinstance(v, bool) or v is None:
        raise ChampionCfgError(f"{field}: expected int")
    try:
        return int(v)
    except Exception as e:
        raise ChampionCfgError(f"{field}: cannot parse int: {v!r}") from e


def _as_float(v: Any, field: str) -> float:
    if isinstance(v, bool) or v is None:
        raise ChampionCfgError(f"{field}: expected float")
    try:
        return float(v)
    except Exception as e:
        raise ChampionCfgError(f"{field}: cannot parse float: {v!r}") from e


def _as_str(v: Any, field: str) -> str:
    if not isinstance(v, str) or not v.strip():
        raise ChampionCfgError(f"{field}: expected non-empty string")
    return v.strip()


def validate_champion_cfg(
    raw_json: str,
    *,
    allow_default_enforce_share: bool = False,
    default_enforce_share: Optional[float] = None,
) -> Tuple[ChampionCfg, Dict[str, Any]]:
    """
    Strict validator.
    - If enforce_share missing:
        - by default => INVALID (recommended)
        - if allow_default_enforce_share=True => default + return info.defaulted_fields
    Returns (cfg, info).
    """
    try:
        obj = json.loads(raw_json)
    except Exception as e:
        raise ChampionCfgError(f"bad_json: {e}") from e
    if not isinstance(obj, dict):
        raise ChampionCfgError("bad_json: expected object")

    defaulted: Dict[str, Any] = {}

    schema_version = _as_int(obj.get("schema_version"), "schema_version")
    if schema_version != 1:
        raise ChampionCfgError(f"schema_version: unsupported: {schema_version}")

    kind = _as_str(obj.get("kind"), "kind")
    run_id = _as_str(obj.get("run_id"), "run_id")
    created_ms = _as_int(obj.get("created_ms"), "created_ms")
    model_path = _as_str(obj.get("model_path"), "model_path")

    mode = _as_str(obj.get("mode"), "mode").upper()
    if mode not in ALLOWED_MODES:
        raise ChampionCfgError(f"mode: expected one of {sorted(ALLOWED_MODES)}, got {mode!r}")

    if "enforce_share" in obj and obj.get("enforce_share") is not None:
        enforce_share = _as_float(obj.get("enforce_share"), "enforce_share")
    else:
        if not allow_default_enforce_share:
            raise ChampionCfgError("enforce_share: missing")
        if default_enforce_share is None:
            raise ChampionCfgError("enforce_share: missing (no default provided)")
        enforce_share = float(default_enforce_share)
        defaulted["enforce_share"] = enforce_share

    if not (0.0 <= enforce_share <= 1.0):
        raise ChampionCfgError(f"enforce_share: out of range [0..1]: {enforce_share}")

    # invariants
    if mode == "SHADOW" and enforce_share != 0.0:
        raise ChampionCfgError("mode=SHADOW requires enforce_share=0.0")
    if mode == "ENFORCE" and enforce_share != 1.0:
        raise ChampionCfgError("mode=ENFORCE requires enforce_share=1.0")
    if mode == "CANARY" and not (0.0 < enforce_share < 1.0):
        raise ChampionCfgError("mode=CANARY requires 0.0 < enforce_share < 1.0")

    cfg = ChampionCfg(
        schema_version=schema_version,
        kind=kind,
        run_id=run_id,
        created_ms=created_ms,
        model_path=model_path,
        mode=mode,
        enforce_share=enforce_share,
        calibrator_path=(obj.get("calibrator_path") or None),
        feature_version=(obj.get("feature_version") or None),
        model_type=(obj.get("model_type") or None),
        checksum=(obj.get("checksum") or None),
    )
    return cfg, {"defaulted_fields": defaulted}










