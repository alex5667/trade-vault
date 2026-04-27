"""
Валидатор для cfg:ml_confirm:champion (JSON контракт).

Контракт schema_version=1:
  - Обязательные поля: schema_version, kind, run_id, created_ms, model_path, mode, enforce_share
  - Инварианты: mode ↔ enforce_share (SHADOW=0.0, ENFORCE=1.0, CANARY: 0.0 < enforce_share < 1.0)
  - Рекомендуемые: calibrator_path, feature_version, model_type, checksum, min_data_ts_ms, max_data_ts_ms

Использование:
  - На чтение: validate_champion_cfg(raw_json, default_enforce_share=None) → если missing enforce_share, это ошибка
  - На запись (promo callbacks): валидировать строго перед записью в Redis и на диск
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple

ALLOWED_MODES = {"SHADOW", "CANARY", "ENFORCE"}


@dataclass(frozen=True)
class ChampionCfg:
    """Валидированный champion конфиг."""
    schema_version: int
    kind: str
    run_id: str
    created_ms: int
    model_path: str
    mode: str
    enforce_share: float
    calibrator_path: Optional[str] = None
    feature_version: Optional[str] = None
    model_type: Optional[str] = None
    checksum: Optional[str] = None
    min_data_ts_ms: Optional[int] = None
    max_data_ts_ms: Optional[int] = None


class CfgError(ValueError):
    """Ошибка валидации конфига."""
    pass


def _as_int(v: Any, field: str) -> int:
    """Преобразование в int с валидацией."""
    if isinstance(v, bool) or v is None:
        raise CfgError(f"{field}: expected int, got {type(v).__name__}")
    try:
        return int(v)
    except Exception as e:
        raise CfgError(f"{field}: cannot parse int: {v!r}") from e


def _as_float(v: Any, field: str) -> float:
    """Преобразование в float с валидацией."""
    if isinstance(v, bool) or v is None:
        raise CfgError(f"{field}: expected float, got {type(v).__name__}")
    try:
        return float(v)
    except Exception as e:
        raise CfgError(f"{field}: cannot parse float: {v!r}") from e


def _as_str(v: Any, field: str) -> str:
    """Преобразование в non-empty string с валидацией."""
    if not isinstance(v, str) or not v.strip():
        raise CfgError(f"{field}: expected non-empty string")
    return v.strip()


def validate_champion_cfg(
    raw_json: str,
    *,
    default_enforce_share: Optional[float] = None
) -> Tuple[ChampionCfg, Dict[str, Any]]:
    """
    Валидация champion JSON конфига.
    
    Args:
        raw_json: JSON string из Redis
        default_enforce_share: Если enforce_share отсутствует, использовать это значение (не рекомендуется для ENFORCE/CANARY)
    
    Returns:
        (cfg, info) где info содержит {"defaulted_fields": {...}} для observability
    
    Raises:
        CfgError: если конфиг невалиден
    
    Инварианты:
        - mode=SHADOW ⇒ enforce_share == 0.0
        - mode=ENFORCE ⇒ enforce_share == 1.0
        - mode=CANARY ⇒ 0.0 < enforce_share < 1.0
    """
    try:
        obj = json.loads(raw_json)
    except Exception as e:
        raise CfgError(f"json: invalid: {e}") from e

    if not isinstance(obj, dict):
        raise CfgError("json: expected object")

    defaulted = {}

    # schema_version (обязательный)
    schema_version = _as_int(obj.get("schema_version"), "schema_version")
    if schema_version != 1:
        raise CfgError(f"schema_version: unsupported: {schema_version}")

    # Обязательные поля
    kind = _as_str(obj.get("kind"), "kind")
    run_id = _as_str(obj.get("run_id"), "run_id")
    created_ms = _as_int(obj.get("created_ms"), "created_ms")
    model_path = _as_str(obj.get("model_path"), "model_path")

    # mode (обязательный, нормализуем в uppercase)
    mode = _as_str(obj.get("mode"), "mode").upper()
    if mode not in ALLOWED_MODES:
        raise CfgError(f"mode: expected one of {sorted(ALLOWED_MODES)}, got {mode!r}")

    # enforce_share (обязательный, но может быть defaulted если разрешено)
    if "enforce_share" in obj and obj["enforce_share"] is not None:
        enforce_share = _as_float(obj["enforce_share"], "enforce_share")
    else:
        if default_enforce_share is None:
            raise CfgError("enforce_share: missing")
        enforce_share = float(default_enforce_share)
        defaulted["enforce_share"] = enforce_share

    if not (0.0 <= enforce_share <= 1.0):
        raise CfgError(f"enforce_share: out of range [0..1]: {enforce_share}")

    # Инварианты mode ↔ enforce_share
    if mode == "SHADOW" and enforce_share != 0.0:
        raise CfgError("mode=SHADOW requires enforce_share=0.0")
    if mode == "ENFORCE" and enforce_share != 1.0:
        raise CfgError("mode=ENFORCE requires enforce_share=1.0")
    if mode == "CANARY" and not (0.0 < enforce_share < 1.0):
        raise CfgError("mode=CANARY requires 0.0 < enforce_share < 1.0")

    # Опциональные поля
    calibrator_path = obj.get("calibrator_path")
    if calibrator_path is not None:
        calibrator_path = _as_str(calibrator_path, "calibrator_path")
    
    feature_version = obj.get("feature_version")
    if feature_version is not None:
        feature_version = _as_str(feature_version, "feature_version")
    
    model_type = obj.get("model_type")
    if model_type is not None:
        model_type = _as_str(model_type, "model_type")
    
    checksum = obj.get("checksum")
    if checksum is not None:
        checksum = _as_str(checksum, "checksum")
    
    min_data_ts_ms = obj.get("min_data_ts_ms")
    if min_data_ts_ms is not None:
        min_data_ts_ms = _as_int(min_data_ts_ms, "min_data_ts_ms")
    
    max_data_ts_ms = obj.get("max_data_ts_ms")
    if max_data_ts_ms is not None:
        max_data_ts_ms = _as_int(max_data_ts_ms, "max_data_ts_ms")

    cfg = ChampionCfg(
        schema_version=schema_version,
        kind=kind,
        run_id=run_id,
        created_ms=created_ms,
        model_path=model_path,
        mode=mode,
        enforce_share=enforce_share,
        calibrator_path=calibrator_path,
        feature_version=feature_version,
        model_type=model_type,
        checksum=checksum,
        min_data_ts_ms=min_data_ts_ms,
        max_data_ts_ms=max_data_ts_ms,
    )
    return cfg, {"defaulted_fields": defaulted}

