from __future__ import annotations

"""
Replay runner:
  - reads JSONL records (type=ctx|tick)
  - calls adapter.process_ctx(...) or adapter.process_tick(...)
  - collects emitted payloads via adapter.outbox (OutboxCapture-like)

Adapter contract (minimal):
  - adapter.process_ctx(ctx_payload: dict) -> None
  - adapter.process_tick(tick_payload: dict) -> None   (optional)
  - adapter.outbox: object with .items list[dict]
"""

from typing import Any, Dict, Optional
from replay.jsonl import iter_jsonl


def replay_jsonl(
    *,
    adapter: Any,
    path: str,
    type_filter: str = "ctx",
    max_events: Optional[int] = None,
) -> Any:
    tf = (type_filter or "ctx").strip().lower()
    n = 0
    for rec in iter_jsonl(path, max_lines=max_events):
        if str(rec.get("type", "")).lower() != tf:
            continue
        p = rec.get("payload", None)
        if not isinstance(p, dict):
            continue
        if tf == "ctx":
            adapter.process_ctx(p)
        elif tf == "tick":
            if hasattr(adapter, "process_tick"):
                adapter.process_tick(p)
        n += 1
        if max_events is not None and n >= max_events:
            break
    return getattr(adapter, "outbox", None)