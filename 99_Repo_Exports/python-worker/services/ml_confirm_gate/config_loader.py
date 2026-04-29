import os
import json
import logging
from typing import Any, Dict, Optional, Tuple, List
from pydantic import BaseModel, ConfigDict, Field, field_validator
import redis

logger = logging.getLogger("ml_confirm_gate.config")

def _safe_loads(s: Any) -> Any:
    """Best effort JSON parse from bytes/str."""
    if s is None:
        return {}
    if isinstance(s, bytes):
        s = s.decode("utf-8", "ignore")
    raw = str(s).strip()
    if not raw:
        return {}
    try:
        obj = json.loads(raw)
    except Exception:
        return {}
    if isinstance(obj, str):
        try:
            obj2 = json.loads(obj)
        except Exception:
            return {}
        return obj2 if isinstance(obj2, dict) else {}
    return obj if isinstance(obj, dict) else {}


class MLConfirmConfig(BaseModel):
    """
    Pydantic schema for ML gate configuration validation.
    Enforces P0 range constraints for probability thresholds (p_min).
    """
    model_config = ConfigDict(extra="allow")

    p_min: float = 0.52
    p_min_by_bucket: Dict[str, float] = Field(default_factory=dict)
    util_floors: Dict[str, Any] = Field(default_factory=dict)
    edge_floors: Dict[str, Any] = Field(default_factory=dict)

    @field_validator("p_min")
    @classmethod
    def validate_p_min(cls, v: float) -> float:
        if not (0.0 <= v <= 0.95):
            raise ValueError(f"p_min must be in range [0.0, 0.95], got {v}")
        return v

    @field_validator("p_min_by_bucket")
    @classmethod
    def validate_p_min_by_bucket(cls, v: Dict[str, float]) -> Dict[str, float]:
        for k, val in v.items():
            if not (0.5 <= val <= 0.95):
                raise ValueError(f"p_min_by_bucket[{k}] must be in range [0.5, 0.95], got {val}")
        return v

    @field_validator("edge_floors")
    @classmethod
    def validate_edge_floors_dict(cls, v: Dict[str, Any], info) -> Dict[str, Any]:
        if not v:
            return v
        field_name = info.field_name
        g = v.get("global") or {}
        if isinstance(g, dict) and "floor" in g:
            f = float(g["floor"])
            if not (0.5 <= f <= 0.95):
                raise ValueError(f"{field_name}.global.floor must be in range [0.5, 0.95], got {f}")
        
        bb = v.get("by_bucket") or {}
        if isinstance(bb, dict):
            for k, bucket_cfg in bb.items():
                if isinstance(bucket_cfg, dict) and "floor" in bucket_cfg:
                    f = float(bucket_cfg["floor"])
                    if not (0.5 <= f <= 0.95):
                        raise ValueError(f"{field_name}.by_bucket[{k}].floor must be in range [0.5, 0.95], got {f}")
        return v


def load_config_from_redis(
    r: redis.Redis,
    champion_key: str,
    challenger_key: str,
    ab_variant: str,
    hash_fallback_key: str = "cfg:ml_confirm"
) -> Tuple[Dict[str, Any], str, str, str]:
    """
    Load configuration from Redis.
    Returns: (config_dict, cfg_source, cfg_key_used, error_str)
    """
    raw_payload = None
    cfg_source = "none"
    cfg_key_used = champion_key
    error_str = ""

    # Try champion
    raw_p = r.get(champion_key)
    if raw_p:
        try:
            p = _safe_loads(raw_p)
            if isinstance(p, dict) and p:
                raw_payload = raw_p
                cfg_source = "champion"
                cfg_key_used = champion_key
        except Exception:
            pass

    # Try challenger if explicitly requested
    if not raw_payload and ab_variant == "challenger" and challenger_key != champion_key:
        raw_p = r.get(challenger_key)
        if raw_p:
            try:
                p = _safe_loads(raw_p)
                if isinstance(p, dict) and p:
                    raw_payload = raw_p
                    cfg_source = "challenger"
                    cfg_key_used = challenger_key
            except Exception:
                pass

    # Hash fallback
    if not raw_payload:
        try:
            h = r.hgetall(hash_fallback_key)
            if h and isinstance(h, dict) and len(h) > 0:
                cfg_dict = {}
                for k, v in h.items():
                    cfg_dict[str(k)] = v
                cfg_dict.setdefault("mode", "SHADOW")
                cfg_dict.setdefault("fail_policy", "OPEN")
                cfg_dict.setdefault("enforce_share", 0.05)
                
                cfg_source = "hash_fallback"
                cfg_key_used = hash_fallback_key
                raw_payload = json.dumps(cfg_dict).encode("utf-8")
        except Exception as e:
            logger.error(f"Redis error in load_config_from_redis hash fallback: {e}")

    if not raw_payload:
        return {}, "none", "", "no_cfg"

    try:
        cfg_dict = _safe_loads(raw_payload)
        # Validate through Pydantic
        MLConfirmConfig(**cfg_dict)
        return cfg_dict, cfg_source, cfg_key_used, ""
    except Exception as e:
        return {}, cfg_source, cfg_key_used, f"parse_error:{type(e).__name__}"

def _safe_loads_ex(s: Any) -> Tuple[Dict[str, Any], str, int]:
    if s is None:
        return {}, "missing", 0
    if isinstance(s, bytes):
        s = s.decode("utf-8", "ignore")
    raw = str(s).strip()
    raw_len = len(raw)
    if not raw:
        return {}, "empty_dict", 0
    try:
        obj = json.loads(raw)
    except Exception as e:
        return {}, f"json_error:{type(e).__name__}", raw_len
    if isinstance(obj, str):
        try:
            obj2 = json.loads(obj)
            if isinstance(obj2, dict):
                return obj2, "", raw_len
            else:
                return {}, f"double_json_not_dict:{type(obj2).__name__}", raw_len
        except Exception as e2:
            return {}, f"double_json_error:{type(e2).__name__}", raw_len
    if isinstance(obj, dict):
        if not obj:
            return {}, "empty_dict", raw_len
        return obj, "", raw_len
    return {}, f"not_dict:{type(obj).__name__}", raw_len
