from __future__ import annotations

from typing import Any, Dict, Iterable, Tuple
import math

JSONScalar = (str, int, float, bool, type(None))

def _is_json_scalar(x: Any) -> bool:
    if isinstance(x, float):
        return not (math.isnan(x) or math.isinf(x))
    return isinstance(x, JSONScalar)

def assert_json_safe(obj: Any, *, path: str = "$", max_depth: int = 32) -> None:
    """
    HARD CONTRACT:
      - только: str/int/float/bool/None/list/dict
      - без NaN/Inf
      - рекурсивно проверяем все элементы
    """
    if max_depth <= 0:
        raise AssertionError(f"json_safe: max_depth exceeded at {path}")

    if _is_json_scalar(obj):
        return

    if isinstance(obj, list):
        for i, v in enumerate(obj):
            assert_json_safe(v, path=f"{path}[{i}]", max_depth=max_depth - 1)
        return

    if isinstance(obj, dict):
        for k, v in obj.items():
            if not isinstance(k, str):
                raise AssertionError(f"json_safe: non-str key at {path}: {type(k).__name__}")
            assert_json_safe(v, path=f"{path}.{k}", max_depth=max_depth - 1)
        return

    raise AssertionError(f"json_safe: forbidden type at {path}: {type(obj).__name__}")

def assert_no_trace_in_tradeable_envelope(env: Dict[str, Any]) -> None:
    """
    HARD CONTRACT:
      - в tradeable envelope не должно быть полного trace/events
      - допускаются только trace_id/trace_summary + meta.trace_meta_key
    """
    forbidden_keys = ("trace", "events")
    for k in forbidden_keys:
        if k in env:
            raise AssertionError(f"envelope contains forbidden top-level key: {k}")

    # также запрещаем вложенные meta.trace / targets.trace и т.п.
    def _walk(x: Any, p: str) -> None:
        if isinstance(x, dict):
            for kk, vv in x.items():
                kl = str(kk).lower()
                if kl in ("trace", "events"):
                    raise AssertionError(f"envelope contains forbidden key '{kk}' at {p}")
                _walk(vv, f"{p}.{kk}")
        elif isinstance(x, list):
            for i, vv in enumerate(x):
                _walk(vv, f"{p}[{i}]")

    _walk(env, "$")
