from __future__ import annotations

import uuid
from typing import Any, Dict, Optional


def new_trace_id() -> str:
    return uuid.uuid4().hex


def get_trace_id_from_ctx(ctx: Any) -> str:
    try:
        v = getattr(ctx, "trace_id", "") or ""
        return str(v)
    except Exception:
        return ""


def set_trace_id_on_ctx(ctx: Any, trace_id: str) -> None:
    try:
        if ctx is not None and trace_id:
            setattr(ctx, "trace_id", str(trace_id))
    except Exception:
        return


def get_trace_id_from_env(env: Dict[str, Any]) -> str:
    """
    Reads trace_id from:
      - env["trace_id"]
      - env["meta"]["trace_id"]
    """
    try:
        v = str(env.get("trace_id") or "")
        if v:
            return v
        meta = env.get("meta") or {}
        if isinstance(meta, dict):
            v = str(meta.get("trace_id") or "")
            if v:
                return v
    except Exception:
        pass
    return ""


def ensure_trace_id(*, ctx: Any = None, env: Optional[Dict[str, Any]] = None, meta: Optional[Dict[str, Any]] = None) -> str:
    """
    Single source-of-truth:
      1) ctx.trace_id
      2) env/meta trace_id
      3) new uuid
    Also writes back into ctx + meta (best-effort).
    """
    tid = get_trace_id_from_ctx(ctx) if ctx is not None else ""
    if not tid and env is not None:
        tid = get_trace_id_from_env(env)
    if not tid and meta is not None:
        try:
            tid = str(meta.get("trace_id") or "")
        except Exception:
            tid = ""
    if not tid:
        tid = new_trace_id()
    set_trace_id_on_ctx(ctx, tid)
    try:
        if meta is not None and isinstance(meta, dict):
            meta.setdefault("trace_id", tid)
    except Exception:
        pass
    return tid

