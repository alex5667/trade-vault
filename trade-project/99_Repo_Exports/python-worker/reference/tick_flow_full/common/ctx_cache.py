from __future__ import annotations

from collections.abc import Callable
from typing import Any


def _getattr_safe(obj: Any, name: str, default: Any = None) -> Any:
    try:
        return getattr(obj, name, default)
    except Exception:
        return default


def _setattr_safe(obj: Any, name: str, value: Any) -> None:
    try:
        setattr(obj, name, value)
    except Exception:
        pass


def cached_on_ctx(
    ctx: Any,
    *,
    slot: str,
    key: tuple[Any, ...],
    compute: Callable[[], Any],
) -> Any:
    """
    Small, deterministic, fail-open cache that lives on ctx.

    Contract:
      - ctx may be any object (dynamic attrs)
      - we store:
          ctx.<slot> = {"key": <tuple>, "val": <any>}
      - if key matches -> return cached val
      - if anything fails -> just compute and return (fail-open)
    """
    if ctx is None:
        return compute()

    box = _getattr_safe(ctx, slot, None)
    try:
        if isinstance(box, dict) and box.get("key") == key:
            return box.get("val")
    except Exception:
        # corrupted cache -> ignore
        pass

    val = compute()
    _setattr_safe(ctx, slot, {"key": key, "val": val})
    return val
