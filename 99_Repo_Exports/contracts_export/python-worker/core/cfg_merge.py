from __future__ import annotations

from typing import Any, Dict, Mapping


def merged_cfg(base: Mapping[str, Any], override: Mapping[str, Any]) -> Dict[str, Any]:
    """
    Shallow merge of cfg dictionaries:
      - base contains static config
      - override contains dynamic values (runtime.dynamic_cfg)

    override wins for keys present.
    """
    out: Dict[str, Any] = {}
    try:
        out.update(dict(base))
    except Exception:
        out = {}
    try:
        for k, v in dict(override).items():
            out[k] = v
    except Exception:
        pass
    return out


def get_int(cfg: Mapping[str, Any], key: str, default: int) -> int:
    try:
        return int(cfg.get(key, default))
    except Exception:
        return int(default)
