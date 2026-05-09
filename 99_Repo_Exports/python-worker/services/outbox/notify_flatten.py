from __future__ import annotations

from typing import Any

from common.json_fast import dumps1
from common.json_safe import to_json_safe


def flatten_notify_fields(payload: dict[str, Any]) -> list[str]:
    """
    Контракт для notify Lua:
      - возвращает flat список ["k1","v1","k2","v2",...]
      - все значения СТРОКИ (Redis Stream хранит только строки)
      - dict/list -> JSON строка (dumps1(to_json_safe(...)))
      - bool -> "true"/"false", None -> "null"
    """
    if not isinstance(payload, dict):
        return []

    flat: list[str] = []
    for k in sorted(payload.keys(), key=lambda x: str(x)):
        kk = str(k)

        v = payload.get(k)
        if isinstance(v, (dict, list)):
            vv = dumps1(to_json_safe(v))
        elif isinstance(v, bool):
            vv = "true" if v else "false"
        elif v is None:
            vv = "null"
        else:
            vv = str(v)

        flat.append(kk)
        flat.append(vv)

    return flat
