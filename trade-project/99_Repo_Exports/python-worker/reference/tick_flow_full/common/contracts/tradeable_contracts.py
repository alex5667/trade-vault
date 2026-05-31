from __future__ import annotations

import json
import math
from collections.abc import Iterable
from typing import Any

JSON_SCALARS = (str, int, float, bool, type(None))

# Запрещённые ключи в tradeable payload/envelope.
# Полный trace/события/тяжёлые parts должны жить только в sidecar meta.
FORBIDDEN_TOPLEVEL_KEYS = {
    "trace",          # никогда в tradeable
    "events",         # никогда в tradeable
    "parts_full",     # никогда в tradeable (только в meta.payload_meta)
}

# Дополнительная страховка: запрет "подстрок" в ключах (регрессии именования)
FORBIDDEN_KEY_SUBSTRINGS = ("trace.", "trace_", "events", "parts_full")

DEFAULT_MAX_JSON_BYTES = 64_000          # safety budget для payload/env (можно вынести в ENV)
DEFAULT_MAX_DEPTH = 10                   # защита от случайной рекурсии/раздувания
DEFAULT_MAX_KEYS = 256
DEFAULT_MAX_LIST = 512


def _is_finite_number(x: Any) -> bool:
    return isinstance(x, (int, float)) and (not (isinstance(x, float) and (math.isnan(x) or math.isinf(x))))


def assert_json_safe(
    obj: Any,
    *,
    where: str,
    max_depth: int = DEFAULT_MAX_DEPTH,
    max_keys: int = DEFAULT_MAX_KEYS,
    max_list: int = DEFAULT_MAX_LIST,
) -> None:
    """
    Жёстко проверяет: obj состоит ТОЛЬКО из str/int/float/bool/None/list/dict
    и не содержит NaN/Inf. Также ограничивает глубину/размер.
    """

    def walk(x: Any, depth: int) -> None:
        if depth > max_depth:
            raise AssertionError(f"{where}: max_depth exceeded ({max_depth})")

        if isinstance(x, JSON_SCALARS):
            if isinstance(x, float) and (math.isnan(x) or math.isinf(x)):
                raise AssertionError(f"{where}: NaN/Inf forbidden in JSON")
            return

        if isinstance(x, list):
            if len(x) > max_list:
                raise AssertionError(f"{where}: list too long: {len(x)} > {max_list}")
            for it in x:
                walk(it, depth + 1)
            return

        if isinstance(x, dict):
            if len(x) > max_keys:
                raise AssertionError(f"{where}: dict too large: {len(x)} > {max_keys}")
            for k, v in x.items():
                if not isinstance(k, str):
                    raise AssertionError(f"{where}: dict key must be str, got {type(k).__name__}")
                walk(v, depth + 1)
            return

        raise AssertionError(f"{where}: non-JSON type: {type(x).__name__}")

    walk(obj, 0)


def assert_tradeable_dict(
    d: dict[str, Any],
    *,
    where: str,
    max_json_bytes: int = DEFAULT_MAX_JSON_BYTES,
    forbidden_keys: Iterable[str] = FORBIDDEN_TOPLEVEL_KEYS,
) -> None:
    if not isinstance(d, dict):
        raise AssertionError(f"{where}: expected dict, got {type(d).__name__}")

    # Запрет ключей
    for k in d:
        ks = str(k)
        if ks in forbidden_keys:
            raise AssertionError(f"{where}: forbidden key: {ks}")
        for sub in FORBIDDEN_KEY_SUBSTRINGS:
            if sub in ks:
                raise AssertionError(f"{where}: forbidden key substring '{sub}' in key '{ks}'")

    # JSON-safe структура
    assert_json_safe(d, where=where)

    # Сериализация обязана проходить
    raw = json.dumps(d, ensure_ascii=False, separators=(",", ":")).encode("utf-8", "ignore")
    if len(raw) > int(max_json_bytes):
        raise AssertionError(f"{where}: json bytes too large: {len(raw)} > {max_json_bytes}")


def assert_outbox_sidecar_meta(meta: dict[str, Any], *, where: str) -> None:
    """
    Sidecar meta тоже должно быть JSON-safe, но может быть больше.
    Главное: payload_meta должен быть namespace-ом и НЕ ломать schema/trace поля.
    """
    if not isinstance(meta, dict):
        raise AssertionError(f"{where}: meta must be dict")

    assert_json_safe(meta, where=where, max_depth=12, max_keys=1024, max_list=2048)

    pm = meta.get("payload_meta")
    if pm is not None and not isinstance(pm, dict):
        raise AssertionError(f"{where}: meta.payload_meta must be dict if present")
