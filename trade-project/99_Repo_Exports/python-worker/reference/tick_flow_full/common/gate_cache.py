from __future__ import annotations

from collections.abc import Callable, Hashable
from typing import Any, TypeVar

T = TypeVar("T")

def _get_ctx_cache(ctx: Any) -> dict[Hashable, Any]:
    """
    Stable, fail-open per-ctx cache.
    - Stores only small objects (decisions / booleans / tuples).
    - Never raises (best-effort).
    """
    try:
        c = getattr(ctx, "_gate_cache", None)
        if isinstance(c, dict):
            return c
        c = {}
        ctx._gate_cache = c
        return c
    except Exception:
        # If ctx is immutable / slots-only, return ephemeral cache (still safe).
        return {}

def cached_call(ctx: Any, key: Hashable, fn: Callable[[], T]) -> T:
    """
    Compute-once helper.
    If ctx cache is not writable -> behaves like no-cache, but still correct.
    """
    c = _get_ctx_cache(ctx)
    if key in c:
        return c[key]  # type: ignore[return-value]
    v = fn()
    try:
        c[key] = v
    except Exception:
        pass
    return v


def cached_call_exc(ctx: Any, key: Hashable, fn: Callable[[], T]) -> T:
    """
    Compute-once helper that also caches exceptions.

    Critical for compatibility:
      - existing code often wraps the call into try/except with different DQ tags
      - we want: heavy work executes once, but later call sites still "see" the same exception
        and can keep their own error accounting/flags.
    """
    c = _get_ctx_cache(ctx)
    if key in c:
        v = c[key]
        if isinstance(v, BaseException):
            raise v
        return v  # type: ignore[return-value]
    try:
        v = fn()
        try:
            c[key] = v
        except Exception:
            pass
        return v
    except BaseException as e:
        try:
            c[key] = e
        except Exception:
            pass
        raise
