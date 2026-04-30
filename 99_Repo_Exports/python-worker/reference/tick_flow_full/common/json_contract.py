from __future__ import annotations

from typing import Any, Dict, List, Tuple
import math
import os

from common.json_fast import dumps1
from common.json_safe import to_json_safe

JSON_SCALARS = (str, int, float, bool, type(None))


def _is_finite_float(x: float) -> bool:
    return not (math.isnan(x) or math.isinf(x))


def assert_json_safe(obj: Any, *, path: str = "$", max_depth: int = 12) -> None:
    """
    ЖЁСТКИЙ КОНТРАКТ:
      obj должен состоять ТОЛЬКО из:
        - str/int/float/bool/None
        - list
        - dict (string keys)
      и float должен быть finite (не NaN/Inf).

    Использование:
      - в проде: под ENV-флагом (ASSERT_JSON_SAFE=1)
      - в тестах: всегда
    """
    if max_depth <= 0:
        raise AssertionError(f"json_safe depth exceeded at {path}")

    if isinstance(obj, JSON_SCALARS):
        if isinstance(obj, float) and not _is_finite_float(obj):
            raise AssertionError(f"non-finite float at {path}: {obj}")
        return

    if isinstance(obj, list):
        for i, x in enumerate(obj):
            assert_json_safe(x, path=f"{path}[{i}]", max_depth=max_depth - 1)
        return

    if isinstance(obj, dict):
        for k, v in obj.items():
            if not isinstance(k, str):
                raise AssertionError(f"non-string key at {path}: {type(k).__name__}")
            assert_json_safe(v, path=f"{path}.{k}", max_depth=max_depth - 1)
        return

    raise AssertionError(f"non-json type at {path}: {type(obj).__name__}")


def json_bytes(obj: Any) -> int:
    """
    Размер в байтах в "боевом" компактном JSON (dumps1)
    чтобы одинаково считать бюджеты в коде и тестах.
    """
    s = dumps1(obj)
    return len(s.encode("utf-8", "ignore"))


def _truncate_parts_dict(parts: Dict[str, Any], *, max_keys: int) -> Tuple[Dict[str, Any], bool]:
    if len(parts) <= max_keys:
        return parts, False
    out: Dict[str, Any] = {}
    i = 0
    for k in sorted(parts.keys()):
        if i >= max_keys:
            break
        out[k] = parts[k]
        i += 1
    return out, True


def enforce_payload_budgets(
    payload: Dict[str, Any]
    payload_meta: Dict[str, Any]
    *
    payload_max_bytes: int
    meta_max_bytes: int
) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    "Железный" слой:
      - tradeable payload обязан быть малым (payload_max_bytes)
      - sidecar meta тоже лимитируем (meta_max_bytes)
    Стратегия:
      1) если payload слишком большой:
         - сжимаем payload["parts"] до ключей/малого поднабора
         - режем reasons
         - ставим флаг parts_truncated=True
      2) если meta слишком большой:
         - режем parts_full по числу ключей
         - ставим meta_truncated=True и сохраняем parts_full_keys
    """
    # defensive sanitize (не доверяем вызывающему коду)
    payload = to_json_safe(payload) if isinstance(payload, dict) else {}
    payload_meta = to_json_safe(payload_meta) if isinstance(payload_meta, dict) else {}

    # ---------------------------
    # 1) payload budget
    # ---------------------------
    b = json_bytes(payload)
    if payload_max_bytes > 0 and b > payload_max_bytes:
        # 1) parts -> максимально компактно
        parts = payload.get("parts")
        if isinstance(parts, dict) and parts:
            keys = sorted(list(parts.keys()))
            payload["parts"] = {"_keys": keys[:64]}
            payload["parts_truncated"] = True
        else:
            payload["parts"] = {}
            payload["parts_truncated"] = True

        # 2) reasons -> до 8
        rs = payload.get("reasons")
        if isinstance(rs, list) and len(rs) > 8:
            payload["reasons"] = rs[:8]
            payload["reasons_truncated"] = True

        # пересчёт после ужатия
        payload = to_json_safe(payload)

    # ---------------------------
    # 2) meta budget
    # ---------------------------
    mb = json_bytes(payload_meta)
    if meta_max_bytes > 0 and mb > meta_max_bytes:
        # чаще всего раздувает parts_full
        pf = payload_meta.get("parts_full")
        if isinstance(pf, dict) and pf:
            keys = sorted(list(pf.keys()))
            # режем до 128 ключей, потом до 64 если всё ещё велико
            pf2, cut = _truncate_parts_dict(pf, max_keys=128)
            payload_meta["parts_full"] = pf2
            payload_meta["meta_truncated"] = True
            payload_meta["parts_full_keys"] = keys[:256]
            payload_meta["parts_full_truncated"] = bool(cut)

            payload_meta = to_json_safe(payload_meta)

            # если всё ещё велико — fallback "только ключи"
            if json_bytes(payload_meta) > meta_max_bytes:
                payload_meta["parts_full"] = {"_keys": keys[:512]}
                payload_meta["meta_truncated_hard"] = True
                payload_meta = to_json_safe(payload_meta)
        else:
            # общий fallback
            payload_meta = {"meta_truncated": True}

    return payload, payload_meta


def maybe_assert_json_safe(payload: Any, payload_meta: Any) -> None:
    """
    Включается в рантайме под ENV, чтобы не ломать hot path.
    """
    if os.getenv("ASSERT_JSON_SAFE", "0").lower() in {"1", "true", "yes"}:
        assert_json_safe(payload, path="$payload")
        assert_json_safe(payload_meta, path="$payload_meta")
