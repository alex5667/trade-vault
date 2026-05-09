from __future__ import annotations

import json
import os
import re

_RE_VER_SUFFIX = re.compile(r"([_-])v(\d+)$", re.IGNORECASE)


def _load_map() -> dict[str, str]:
    """
    Позволяет явно склеить legacy kind'ы:
      CONF_CAL_KIND_MAP_JSON='{"absorption_v2":"absorption"}'
    """
    raw = os.getenv("CONF_CAL_KIND_MAP_JSON", "").strip()
    if not raw:
        return {}
    try:
        obj = json.loads(raw)
        if isinstance(obj, dict):
            out: dict[str, str] = {}
            for k, v in obj.items():
                ks = (k or "").strip()
                vs = (v or "").strip()
                if ks and vs:
                    out[ks] = vs
            return out
    except Exception:
        pass
    return {}


_KIND_MAP = _load_map()


def normalize_kind(kind: str) -> str:
    """
    Делает ключ kind устойчивым:
      - применяет явную карту (если задана)
      - (опционально) срезает суффикс _vN / -vN
    """
    k = (kind or "*").strip()
    if not k or k == "*":
        return "*"

    # 1) explicit map (самое безопасное)
    mapped = _KIND_MAP.get(k)
    if mapped:
        return str(mapped)

    # 2) generic strip version suffix (по умолчанию включено)
    if os.getenv("CONF_CAL_KIND_STRIP_VERSION", "1").strip() == "1":
        m = _RE_VER_SUFFIX.search(k)
        if m:
            k2 = k[: m.start()]
            if k2:
                return k2
    return k
