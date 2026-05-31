"""execution_event_writer.py — Write canonical execution facts to orders:exec stream.

Extracted from binance_executor.py (god-class decomposition).

Responsibilities:
- Write events to orders:exec Redis stream (primary journal)
- Mirror events to SQL journal (ExecutionJournalSink) if enabled
- Optional inline state projection (disabled by default)
- Queue helpers: DLQ push, ack-from-processing, requeue-with-retry
"""
from __future__ import annotations

import contextlib
import json
import time
from typing import Any, Callable

try:
    from services.execution_contracts import ExecutionEvent
except Exception:
    from execution_contracts import ExecutionEvent  # type: ignore[no-redef]

try:
    from common.contracts.registry import ExecutionEventV1
except Exception:
    try:
        from contracts.registry import ExecutionEventV1  # type: ignore[no-redef]
    except Exception:
        ExecutionEventV1 = None  # type: ignore[assignment,misc]

try:
    from common.normalization import get_side_int, normalize_side
except Exception:
    try:
        from normalization import get_side_int, normalize_side  # type: ignore[no-redef]
    except Exception:
        get_side_int = normalize_side = None  # type: ignore[assignment]

from services.execution.binance_order_mapper import _i

# Fill event action names that use ExecutionEventV1 schema
_FILL_ACTIONS = frozenset({"fill", "entry_filled", "exit_filled", "tp_filled", "sl_filled"})


def _ms_now() -> int:
    try:
        from utils.time_utils import get_ny_time_millis
        return get_ny_time_millis()
    except Exception:
        return int(time.time() * 1000)


def _mono_ms() -> int:
    return int(time.monotonic() * 1000)


class ExecutionEventWriter:
    """Writes execution facts to orders:exec Redis stream and SQL journal.

    This is a stateful service object that holds references to Redis client,
    stream config, and journal sink. All write operations are synchronous.

    Attributes:
        r:                       Redis client (decode_responses=True)
        exec_stream:             Redis stream key for execution events
        exec_stream_maxlen:      XADD maxlen cap (None = unlimited)
        queue:                   Main orders queue key
        queue_processing:        BRPOPLPUSH processing list key
        queue_dlq:               DLQ list key
        exec_inline_state_projection:  If True, project state inline (default False)
        execution_journal:       ExecutionJournalSink (optional)
        _project_fn:             Callback for inline projection (injected by executor)
    """

    def __init__(
        self,
        *,
        r: Any,
        exec_stream: str,
        exec_stream_maxlen: int | None,
        queue: str,
        queue_processing: str,
        queue_dlq: str,
        exec_inline_state_projection: bool = False,
        execution_journal: Any = None,
        project_fn: Callable[[str, dict[str, Any], str], None] | None = None,
    ) -> None:
        self.r = r
        self.exec_stream = exec_stream
        self.exec_stream_maxlen = exec_stream_maxlen
        self.queue = queue
        self.queue_processing = queue_processing
        self.queue_dlq = queue_dlq
        self.exec_inline_state_projection = exec_inline_state_projection
        self.execution_journal = execution_journal
        self._project_fn = project_fn

    # ------------------------------------------------------------------
    # Primary journal write
    # ------------------------------------------------------------------

    def write(self, fields: dict[str, Any]) -> str:
        """Write one canonical fact to ``orders:exec``. Returns stream entry ID.

        The executor appends to the primary journal synchronously. Projection
        into ``orders:state:{sid}`` is optional (disabled by default) so
        derived-state materialisation can run in a separate deterministic worker.
        """
        raw = dict(fields or {})
        sid = (raw.get("sid") or "").strip()
        symbol = (raw.get("symbol") or "").strip().upper()
        action = str(raw.get("action") or raw.get("event_type") or "event").strip() or "event"
        event_type = str(raw.get("event_type") or action).strip() or "event"
        status = (raw.get("status") or "ok").strip() or "ok"
        ts_event_ms = raw.get("ts_event_ms") or raw.get("ts_ms") or _ms_now()

        # Derive side_int if absent
        side_int = raw.get("side_int")
        if side_int is None and get_side_int is not None:
            raw_side = raw.get("side") or raw.get("logical_side") or raw.get("direction")
            if raw_side:
                side_int = get_side_int(str(raw_side))

        stream_fields: dict[str, Any]
        try:
            if action in _FILL_ACTIONS and ExecutionEventV1 is not None and normalize_side is not None:
                from common.contracts.registry import Side  # type: ignore[import]
                ev_v1 = ExecutionEventV1(
                    exec_id=str(raw.get("exec_id") or f"exec:{sid}:{ts_event_ms}"),
                    order_id=str(raw.get("order_id") or raw.get("binance_order_id") or ""),
                    client_order_id=str(raw.get("client_order_id") or raw.get("entry_client_order_id") or ""),
                    symbol=symbol,
                    ts_ms=ts_event_ms,
                    side=Side(normalize_side(str(raw.get("side") or raw.get("logical_side") or "")).value),
                    price=float(raw.get("avg_price") or raw.get("price") or 0.0),
                    qty=float(raw.get("filled_qty") or raw.get("qty") or 0.0),
                    side_int=side_int or 0,
                    status=status.upper(),
                    meta={k: v for k, v in raw.items() if v is not None},
                )
                stream_fields = ev_v1.model_dump()
            else:
                core_keys = {
                    "sid", "symbol", "action", "event_type", "status", "severity",
                    "ts_event_ms", "ts_exec_start_ms", "ts_queue_ms", "ts_state_commit_ms",
                    "ts_ms", "mono_ms",
                }
                payload = {k: v for k, v in raw.items() if k not in core_keys and v is not None}
                if side_int is not None:
                    payload["side_int"] = side_int
                event = ExecutionEvent(
                    sid=sid,
                    symbol=symbol,
                    action=action,
                    event_type=event_type,
                    status=status,
                    ts_event_ms=ts_event_ms,
                    ts_exec_start_ms=_i(raw.get("ts_exec_start_ms")) or None,
                    ts_queue_ms=_i(raw.get("ts_queue_ms")) or None,
                    ts_state_commit_ms=_i(raw.get("ts_state_commit_ms")) or None,
                    severity=(raw.get("severity") or "").strip() or None,
                    payload={**payload, "mono_ms": str(_mono_ms()), "venue": "binance"},
                )
                stream_fields = event.to_stream_fields()
        except Exception as exc:
            stream_fields = dict(raw)
            stream_fields.update({"ts_ms": str(ts_event_ms), "error_mapping": str(exc)})

        stream_fields.setdefault("ts_ms", str(ts_event_ms))

        stream_id = ""
        try:
            stream_id = str(
                self.r.xadd(
                    self.exec_stream,
                    {k: str(v) for k, v in stream_fields.items() if v is not None},
                    maxlen=self.exec_stream_maxlen or 100_000,
                    approximate=True,
                )
                or ""
            )
        except Exception:
            stream_id = ""

        with contextlib.suppress(Exception):
            if self.execution_journal is not None:
                self.execution_journal.record_event(stream_fields)

        with contextlib.suppress(Exception):
            if sid and self.exec_inline_state_projection and self._project_fn is not None:
                self._project_fn(sid, stream_fields, stream_id)

        return stream_id

    def write_state_patch(self, sid: str, patch: dict[str, Any]) -> None:
        """Append a derived-state patch event instead of mutating Redis state inline."""
        doc = dict(patch or {})
        if not sid:
            return
        symbol = (doc.get("symbol") or "").strip().upper()
        action = (doc.get("action") or "state_patch").strip() or "state_patch"
        self.write({
            "sid": sid,
            "symbol": symbol,
            "action": action,
            "event_type": "state_patch",
            "status": (doc.get("status") or "ok").strip() or "ok",
            **doc,
        })

    # ------------------------------------------------------------------
    # Queue helpers (at-least-once delivery contract)
    # ------------------------------------------------------------------

    def dlq(self, raw: str, reason: str) -> None:
        """Push unprocessable message to DLQ list (fail-open)."""
        with contextlib.suppress(Exception):
            self.r.lpush(
                self.queue_dlq,
                json.dumps({"reason": reason, "raw": raw, "ts_ms": _ms_now()}),
            )

    def ack_processing(self, raw: str) -> None:
        """Remove message from processing list (BRPOPLPUSH safety net)."""
        with contextlib.suppress(Exception):
            self.r.lrem(self.queue_processing, 1, raw)

    def requeue(self, payload: dict[str, Any], raw: str, reason: str) -> None:
        """Push back to main queue with incremented retry counter.

        If the push fails, falls back to DLQ to avoid silent message loss.
        """
        retry_n = payload.get("retry_n") or 0
        payload["retry_n"] = retry_n + 1
        payload["retry_reason"] = reason
        new_raw = json.dumps(payload, ensure_ascii=False, default=str)
        try:
            self.r.rpush(self.queue, new_raw)
        except Exception:
            self.dlq(raw, f"requeue_failed:{reason}")
