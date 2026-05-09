from __future__ import annotations

"""Fail-open stage metrics.

Several hot-path services emit simple counters/histograms for monitoring.
Instrumentation MUST NEVER break trading/dispatch paths.

This module centralizes best-effort metric emission.
"""


from typing import Any


def _get_metrics(handler: Any) -> Any | None:
    # Try a few common patterns without importing heavy dependencies.
    if handler is None:
        return None
    for attr in ("metrics", "_metrics", "obs", "_obs"):
        m = getattr(handler, attr, None)
        if m is not None:
            return m
    return None


def _counter(obj: Any, name: str) -> Any | None:
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


def _hist(obj: Any, name: str) -> Any | None:
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


# -------------------- Public API (fail-open) --------------------

def meta_feature_seen_total(host: Any, *, schema: str, feature: str) -> None:
    _inc(_get_metrics(host), "meta_feature_seen_total", 1, _tags(schema=schema, feature=feature))

def meta_feature_missing_total(host: Any, *, schema: str, feature: str) -> None:
    _inc(_get_metrics(host), "meta_feature_missing_total", 1, _tags(schema=schema, feature=feature))

def feature_missing_total(host: Any, *, feature: str) -> None:
    _inc(_get_metrics(host), "feature_missing_total", 1, _tags(feature=feature))


def candidates_total(host: Any, *, kind: str = "", symbol="") -> None:
    _inc(_get_metrics(host), "pipeline_candidates_total", 1, _tags(kind, symbol))

def veto_total(host: Any, *, reason_code: str, kind: str = "", symbol="") -> None:
    _inc(_get_metrics(host), "pipeline_veto_total", 1, _tags(kind, symbol, reason_code=reason_code))

def emit_ok_total(host: Any, *, kind: str = "", symbol="") -> None:
    _inc(_get_metrics(host), "pipeline_emit_ok_total", 1, _tags(kind, symbol))

def stage_ms_hist(host: Any, *, stage: str, ms: float, kind: str = "", symbol="") -> None:
    _obs(_get_metrics(host), "pipeline_stage_ms", float(ms), _tags(kind, symbol, stage=stage))

def dist(host: Any, name: str, value: float, *, kind: str = "", symbol="", **extra_tags: str) -> None:
    """Generic distribution/histogram datapoint.

    Kept positional for backward-compat: dist(host, "metric_name", 1.23, ...)
    """
    _obs(_get_metrics(host), str(name), float(value), _tags(kind, symbol, **extra_tags))


def _inc(m: Any, name: str, value: int = 1, tags: Dict[str, str] | None = None) -> None:
    try:
        if m is None:
            return
        if hasattr(m, "inc") and callable(m.inc):
            m.inc(name, value=value, tags=tags or {})
            return
        if hasattr(m, "incr") and callable(m.incr):
            m.incr(name, value=value, tags=tags or {})
            return
        if hasattr(m, "counter") and callable(m.counter):
            m.counter(name, value=value, tags=tags or {})
            return
        if hasattr(m, "count") and callable(m.count):
            m.count(name, value=value, tags=tags or {})
            return
    except Exception:
        return


def _obs(m: Any, name: str, value: float, tags: Dict[str, str] | None = None) -> None:
    try:
        if m is None:
            return
        if hasattr(m, "observe") and callable(m.observe):
            m.observe(name, value=value, tags=tags or {})
            return
        if hasattr(m, "histogram") and callable(m.histogram):
            m.histogram(name, value=value, tags=tags or {})
            return
        if hasattr(m, "timing") and callable(m.timing):
            m.timing(name, value=value, tags=tags or {})
            return
        if hasattr(m, "gauge") and callable(m.gauge):
            m.gauge(name, value=value, tags=tags or {})
            return
    except Exception:
        return


def _tags(kind: str = "", symbol="", **extra: str) -> Dict[str, str]:
    t: Dict[str, str] = {}
    if kind:
        t["kind"] = str(kind)
    if symbol:
        t["symbol"] = symbol
    for k, v in extra.items():
        if v is None:
            continue
        sv = str(v)
        if sv:
            t[str(k)] = sv
    return t


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
