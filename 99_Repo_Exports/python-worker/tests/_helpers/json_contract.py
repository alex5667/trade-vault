from __future__ import annotations

import math
from collections.abc import Iterable
from typing import Any

_JSON_SCALARS = (str, int, float, bool, type(None))

def iter_dict_items(d: dict) -> Iterable[tuple[Any, Any]]:
    # fail-open: если dict сломан, пусть тест свалится на assert ниже
    return d.items()

def assert_json_safe(x: Any, *, path: str = "$") -> None:
    """
    Строгий контракт "json-safe by construction":
      - только: str/int/float/bool/None/list/dict
      - float: нельзя NaN/Inf
      - dict keys -> str (после to_json_safe у вас именно так)
    """
    if isinstance(x, float):
        assert not (math.isnan(x) or math.isinf(x)), f"{path}: float is NaN/Inf"

    if isinstance(x, _JSON_SCALARS):
        return

    if isinstance(x, list):
        for i, v in enumerate(x):
            assert_json_safe(v, path=f"{path}[{i}]")
        return

    if isinstance(x, dict):
        for k, v in iter_dict_items(x):
            assert isinstance(k, str), f"{path}: dict key is not str: {type(k).__name__}"
            assert_json_safe(v, path=f"{path}.{k}")
        return

    raise AssertionError(f"{path}: NOT json-safe type: {type(x).__name__}")
