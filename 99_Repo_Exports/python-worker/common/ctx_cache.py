from __future__ import annotations

from collections.abc import Callable
from typing import Any
import contextlib

# ---------------------------------------------------------------------------
# Optional Prometheus metrics (fail-open if prometheus_client not available)
# ---------------------------------------------------------------------------
try:
    from prometheus_client import Counter

    _CTX_COMPUTE = Counter(
        "ctx_cache_compute_total",
        "Number of times cached_on_ctx called compute() (cache miss)",
        ["slot"],
    )
    _CTX_HIT = Counter(
        "ctx_cache_hit_total",
        "Number of times cached_on_ctx returned a cached value (cache hit)",
        ["slot"],
    )
    _METRICS_AVAILABLE = True
except Exception:  # pragma: no cover
    _METRICS_AVAILABLE = False


def _getattr_safe(obj: Any, name: str, default: Any = None) -> Any:
    try:
        return getattr(obj, name, default)
    except Exception:
        return default


def _setattr_safe(obj: Any, name: str, value: Any) -> None:
    with contextlib.suppress(Exception):
        setattr(obj, name, value)


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
      - we store a dictionary per slot to allow multiple keys (e.g. LONG vs SHORT)
          ctx.<slot> = {<key_tuple>: <val>}
      - if key matches -> return cached val
      - if anything fails -> just compute and return (fail-open)
    """
    if ctx is None:
        if _METRICS_AVAILABLE:
            with contextlib.suppress(Exception):
                _CTX_COMPUTE.labels(slot=slot).inc()
        return compute()

    box = _getattr_safe(ctx, slot, None)

    # TODO(ctx-cache-migration): remove after v<NEXT_TAG> / post-2026-07-01
    # Legacy format migration: {"key": key, "val": val} -> {key: val}
    # This block runs on every call if the old format is present.
    # Once all instances have gone through one rollout cycle, remove this block.
    if isinstance(box, dict) and "key" in box and "val" in box:
        old_k = box.pop("key", None)
        old_v = box.pop("val", None)
        if old_k is not None:
            box[old_k] = old_v

    if not isinstance(box, dict):
        box = {}
        _setattr_safe(ctx, slot, box)

    try:
        if key in box:
            if _METRICS_AVAILABLE:
                with contextlib.suppress(Exception):
                    _CTX_HIT.labels(slot=slot).inc()
            return box[key]
    except Exception:
        pass

    if _METRICS_AVAILABLE:
        with contextlib.suppress(Exception):
            _CTX_COMPUTE.labels(slot=slot).inc()

    val = compute()
    with contextlib.suppress(Exception):
        box[key] = val

    return val
