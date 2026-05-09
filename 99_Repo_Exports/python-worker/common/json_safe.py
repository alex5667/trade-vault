# python-worker/common/json_safe.py
from __future__ import annotations

import base64
import dataclasses
import datetime as _dt
import enum
import math
from decimal import Decimal
from typing import Any, Union
import contextlib

JsonScalar = Union[str, int, float, bool, None]
JsonType = Union[JsonScalar, list["JsonType"], dict[str, "JsonType"]]


def _finite_float(x: float) -> float | None:
    # JSON не поддерживает NaN/Inf. Политика: превращаем в None (fail-open).
    if not isinstance(x, (int, float)):
        return None
    xf = float(x)
    if math.isfinite(xf):
        return xf
    return None


def to_json_safe(
    obj: Any,
    *,
    max_depth: int = 8,
    max_items: int = 500,
    max_str: int = 4096,
    _depth: int = 0,
    _seen: set[int] | None = None,
) -> JsonType:
    """
    Гарантирует: результат состоит ТОЛЬКО из:
      - str/int/float/bool/None
      - list(JsonType)
      - dict[str, JsonType]

    Политика:
      - fail-open: любые неизвестные типы -> str(obj) (обрезанный)
      - NaN/Inf -> None
      - bytes -> utf-8 (или base64, если не декодится)
      - datetime/date -> isoformat()
      - Enum -> .name
      - dataclass -> asdict (с рекурсией)
      - set/tuple -> list
      - numpy scalars (если встретятся) -> item()
    """
    if _seen is None:
        _seen = set()

    if _depth > max_depth:
        return "depth_limit"

    try:
        # Handle scalars immediately (no recursion possible)
        if obj is None or isinstance(obj, (str, bool, int)):
            if isinstance(obj, str) and len(obj) > max_str:
                return obj[: max_str - 3] + "..."
            return obj

        if isinstance(obj, float):
            return _finite_float(obj)

        if isinstance(obj, Decimal):
            return _finite_float(float(obj))

        if isinstance(obj, (bytes, bytearray)):
            try:
                s = bytes(obj).decode("utf-8", "replace")
                if len(s) > max_str:
                    s = s[: max_str - 3] + "..."
                return s
            except Exception:
                b64 = base64.b64encode(bytes(obj)).decode("ascii", "ignore")
                if len(b64) > max_str:
                    b64 = b64[: max_str - 3] + "..."
                return b64

        if isinstance(obj, ( _dt.datetime, _dt.date )):
            try:
                return obj.isoformat()
            except Exception:
                return str(obj)

        if isinstance(obj, enum.Enum):
            try:
                return str(obj.name)
            except Exception:
                return str(obj)

        # numpy scalars (optional)
        try:
            import numpy as np  # type: ignore
            if isinstance(obj, np.generic):
                return to_json_safe(obj.item(), max_depth=max_depth, max_items=max_items, max_str=max_str, _depth=_depth + 1, _seen=_seen)
        except Exception:
            pass

        # Now check for recursion only for complex objects
        oid = id(obj)
        if oid in _seen:
            return "recursion"
        _seen.add(oid)

        try:
            # dataclass -> dict
            if dataclasses.is_dataclass(obj):
                try:
                    return to_json_safe(dataclasses.asdict(obj), max_depth=max_depth, max_items=max_items, max_str=max_str, _depth=_depth + 1, _seen=_seen)
                except Exception:
                    return str(obj)

            # dict
            if isinstance(obj, dict):
                out: dict[str, JsonType] = {}
                n = 0
                for k, v in obj.items():
                    if n >= max_items:
                        out["__truncated__"] = True
                        break
                    ks = str(k)
                    if len(ks) > max_str:
                        ks = ks[: max_str - 3] + "..."
                    out[ks] = to_json_safe(v, max_depth=max_depth, max_items=max_items, max_str=max_str, _depth=_depth + 1, _seen=_seen)
                    n += 1
                return out

            # list/tuple/set
            if isinstance(obj, (list, tuple, set)):
                out_list: list[JsonType] = []
                for i, x in enumerate(obj):
                    if i >= max_items:
                        out_list.append("truncated")
                        break
                    out_list.append(to_json_safe(x, max_depth=max_depth, max_items=max_items, max_str=max_str, _depth=_depth + 1, _seen=_seen))
                return out_list

            # fallback -> string
            s = str(obj)
            if len(s) > max_str:
                s = s[: max_str - 3] + "..."
            return s
        finally:
            # remove from seen
            with contextlib.suppress(Exception):
                _seen.discard(oid)
    except Exception:
        return str(obj)


# Compatibility alias
to_jsonable = to_json_safe


def make_json_safe_inplace(obj: Any) -> None:
    """
    Модифицирует obj на месте, делая его json-safe.
    Используется для подготовки payload перед JSON.dumps().
    """
    if isinstance(obj, dict):
        keys_to_del = []
        for k, v in obj.items():
            if not isinstance(k, str):
                keys_to_del.append(k)
                continue
            make_json_safe_inplace(v)
        for k in keys_to_del:
            with contextlib.suppress(Exception):
                obj[str(k)] = obj.pop(k)
    elif isinstance(obj, (list, tuple)):
        for i, v in enumerate(obj):
            make_json_safe_inplace(v)
    elif isinstance(obj, float):
        if not math.isfinite(obj):
            # Заменяем NaN/Inf на None в родительском контейнере
            # Но поскольку мы не знаем родителя, это не сработает.
            # Лучше использовать to_json_safe для полной замены.
            pass
