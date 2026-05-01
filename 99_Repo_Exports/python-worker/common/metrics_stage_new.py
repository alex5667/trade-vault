from __future__ import annotations
"""Fail-open stage metrics.

Several hot-path services emit simple counters/histograms for monitoring.
Instrumentation MUST NEVER break trading/dispatch paths.

This module centralizes best-effort metric emission.
"""


from typing import Any, Optional


def _get_metrics(handler: Any) -> Optional[Any]:
    # Try a few common patterns without importing heavy dependencies.
    if handler is None:
        return None
    for attr in ("metrics", "_metrics", "obs", "_obs"):
        m = getattr(handler, attr, None)
        if m is not None:
            return m
    return None


def _counter(obj: Any, name: str) -> Optional[Any]:
    for attr in ("counter", "get_counter", "c"):
        fn = getattr(obj, attr, None)
        if fn is None:
            continue
        try:
            c = fn(name)
            if c is not None:
                return c
        except Exception:
            continue
    # Some metrics implementations expose counters as dict-like.
    try:
        return obj.counters.get(name)  # type: ignore[attr-defined]
    except Exception:
        return None


def _hist(obj: Any, name: str) -> Optional[Any]:
    for attr in ("hist", "histogram", "get_hist", "h"):
        fn = getattr(obj, attr, None)
        if fn is None:
            continue
        try:
            h = fn(name)
            if h is not None:
                return h
        except Exception:
            continue
    try:
        return obj.hists.get(name)  # type: ignore[attr-defined]
    except Exception:
        return None


def stage_counter(handler: Any, name: str, *, kind: str = "") -> None:
    try:
        m = _get_metrics(handler)
        if m is None:
            return
        c = _counter(m, name)
        if c is None:
            return
        # Accept a few popular counter APIs.
        for fn_name in ("inc", "add"):
            fn = getattr(c, fn_name, None)
            if fn is None:
                continue
            try:
                # Some accept labels, some don't.
                try:
                    fn(1, kind=kind)  # type: ignore[misc]
                except TypeError:
                    fn(1)
                return
            except Exception:
                continue
    except Exception:
        return


def stage_ms_hist(handler: Any, name: str, *, ms: float, kind: str = "") -> None:
    try:
        m = _get_metrics(handler)
        if m is None:
            return
        h = _hist(m, name)
        if h is None:
            return
        for fn_name in ("observe", "add", "record"):
            fn = getattr(h, fn_name, None)
            if fn is None:
                continue
            try:
                try:
                    fn(ms, kind=kind)  # type: ignore[misc]
                except TypeError:
                    fn(ms)
                return
            except Exception:
                continue
    except Exception:
        return
