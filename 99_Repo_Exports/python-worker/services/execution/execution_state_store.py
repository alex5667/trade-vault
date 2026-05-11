"""execution_state_store.py — Order execution state management (FSM + Redis cache).

Extracted from binance_executor.py (god-class decomposition).

Responsibilities:
- Load / save materialized orders:state:{sid} from Redis
- FSM transitions (idempotent, journal-first)
- Replay / rehydrate from orders:exec stream on state miss
- Quarantine SIDs with replay mismatch
- Mark PENDING_RECONCILE FSM state
"""
from __future__ import annotations

import contextlib
import json
import time
from typing import Any, Callable

try:
    from services.execution_contracts import build_materialized_state_view
except Exception:
    from execution_contracts import build_materialized_state_view  # type: ignore[no-redef]

try:
    from services.execution_state_replay import (
        persist_state_snapshot,
        project_event_into_state,
        rebuild_state_with_fallback,
    )
except Exception:
    from execution_state_replay import (  # type: ignore[no-redef]
        persist_state_snapshot,
        project_event_into_state,
        rebuild_state_with_fallback,
    )

from services.execution.binance_order_mapper import (
    FSM_EMERGENCY_FLATTENED,
    FSM_EXIT_FILLED,
    FSM_PENDING_RECONCILE,
    TERMINAL_FSM_STATES,
)

try:
    from services.execution_metrics import (
        EXECUTION_RECONCILE_PENDING_TOTAL,  # type: ignore
        EXECUTION_STATE_TRANSITION_TOTAL,  # type: ignore
    )  # type: ignore
except Exception:
    EXECUTION_RECONCILE_PENDING_TOTAL = EXECUTION_STATE_TRANSITION_TOTAL = None  # type: ignore[assignment]

try:
    # Local metrics defined in binance_executor (module-level)
    from services.execution.binance_executor_app import (
        EXECUTION_RECONCILE_PENDING_TOTAL as _RECON_TOTAL,  # noqa: F401  # type: ignore
        EXECUTION_STATE_TRANSITION_TOTAL as _TRANS_TOTAL,  # noqa: F401  # type: ignore
    )
except Exception:
    pass  # type: ignore


def _ms_now() -> int:
    try:
        from utils.time_utils import get_ny_time_millis
        return get_ny_time_millis()
    except Exception:
        return int(time.time() * 1000)


def _mono_ms() -> int:
    return int(time.monotonic() * 1000)


def _i(x: Any, default: int = 0) -> int:
    try:
        if x is None:
            return default
        return int(float(x))
    except Exception:
        return default


class ExecutionStateStore:
    """Manages execution FSM state stored in orders:state:{sid} Redis keys.

    This is a stateful service that wraps Redis and the execution journal.
    It implements the journal-first pattern: exec stream is authoritative,
    orders:state:{sid} is a derived/materialized view.

    Design contract:
    - When exec_journal_primary=True (default): stream is source-of-truth
    - When exec_inline_state_projection=False (default): projection runs in
      a separate worker (ExecutionProjectionWorker), not inline on hot path
    - All state mutations go through _transition_state → _exec_event → XADD
    """

    def __init__(
        self,
        *,
        r: Any,
        state_key_prefix: str = "orders:state:",
        state_ttl: int = 86400,
        exec_stream: str = "orders:exec",
        exec_replay_scan_count: int = 20_000,
        exec_rehydrate_on_state_miss: bool = True,
        exec_journal_primary: bool = True,
        exec_state_derived_view: bool = True,
        exec_inline_state_projection: bool = False,
        exec_replay_quarantine_on_mismatch: bool = True,
        exec_replay_quarantine_prefix: str = "orders:quarantine:state:",
        exec_replay_checkpoint_key_prefix: str = "orders:exec:replay:cursor:",
        # Guard integration (injected by executor app)
        guard_acquire_fn: Callable[[str, str, dict[str, Any]], dict[str, Any]] | None = None,
        guard_release_fn: Callable[[str, str], None] | None = None,
        guard_cas_metric_fn: Callable[[str, str, str], None] | None = None,
        state_is_terminalish_fn: Callable[[dict[str, Any]], bool] | None = None,
        # Event write (injected by executor app)
        write_event_fn: Callable[[dict[str, Any]], str] | None = None,
        # SQL journal (injected)
        execution_journal: Any = None,
        quarantine_ledger: Any = None,
        # Flags
        exec_single_active_position_per_symbol: bool = False,
        exec_single_active_position_exchange_truth_release: bool = True,
        exec_single_active_position_release_on_terminal: bool = True,
    ) -> None:
        self.r = r
        self.state_key_prefix = state_key_prefix.rstrip(":") + ":"
        self.state_ttl = state_ttl
        self.exec_stream = exec_stream
        self.exec_replay_scan_count = exec_replay_scan_count
        self.exec_rehydrate_on_state_miss = exec_rehydrate_on_state_miss
        self.exec_journal_primary = exec_journal_primary
        self.exec_state_derived_view = exec_state_derived_view
        self.exec_inline_state_projection = exec_inline_state_projection
        self.exec_replay_quarantine_on_mismatch = exec_replay_quarantine_on_mismatch
        self.exec_replay_quarantine_prefix = exec_replay_quarantine_prefix
        self.exec_replay_checkpoint_key_prefix = exec_replay_checkpoint_key_prefix

        # Injected callbacks (keeps this class free of circular imports)
        self._guard_acquire_fn = guard_acquire_fn
        self._guard_release_fn = guard_release_fn
        self._guard_cas_metric_fn = guard_cas_metric_fn
        self._state_is_terminalish_fn = state_is_terminalish_fn
        self._write_event_fn = write_event_fn

        self.execution_journal = execution_journal
        self.quarantine_ledger = quarantine_ledger

        self.exec_single_active_position_per_symbol = exec_single_active_position_per_symbol
        self.exec_single_active_position_exchange_truth_release = exec_single_active_position_exchange_truth_release
        self.exec_single_active_position_release_on_terminal = exec_single_active_position_release_on_terminal

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _write_event(self, fields: dict[str, Any]) -> str:
        if self._write_event_fn is not None:
            return self._write_event_fn(fields)
        return ""

    def _state_is_terminalish(self, state: dict[str, Any] | None) -> bool:
        if self._state_is_terminalish_fn is not None:
            return self._state_is_terminalish_fn(state)  # type: ignore
        doc = dict(state or {})
        fsm = (doc.get("fsm_state") or "").strip().upper()
        if fsm in TERMINAL_FSM_STATES:  # type: ignore
            return True
        status = (doc.get("status") or "").strip().lower()
        return status in {"closed", "cancelled", "canceled", "failed", "exited", "exit_filled", "emergency_flattened"}

    def _replay_checkpoint_key(self, sid: str) -> str:
        return f"{self.exec_replay_checkpoint_key_prefix}{sid}"

    def _state_redis_key(self, sid: str) -> str:
        return f"{self.state_key_prefix}{sid}"

    # ------------------------------------------------------------------
    # Load
    # ------------------------------------------------------------------

    def load_cache(self, sid: str) -> dict[str, Any]:
        """Return the raw orders:state cache document without replay side effects."""
        try:
            raw = self.r.get(self._state_redis_key(sid))
            if raw:
                doc = json.loads(raw)
                if isinstance(doc, dict):
                    return doc
        except Exception:
            pass
        return {}

    def load(self, sid: str) -> dict[str, Any]:
        """Load execution state, preferring the primary journal over the cache.

        When exec_journal_primary=True (default): replays orders:exec first.
        Falls back to Redis cache on miss.
        """
        if self.exec_journal_primary:
            state = self.recover_from_stream(sid)
            if state:
                return build_materialized_state_view(state)
            return self.load_cache(sid)
        try:
            raw = self.r.get(self._state_redis_key(sid))
            if raw:
                doc = json.loads(raw)
                if isinstance(doc, dict):
                    return build_materialized_state_view(doc)
            return self.recover_from_stream(sid)
        except Exception:
            return self.recover_from_stream(sid)

    # ------------------------------------------------------------------
    # Persist / project
    # ------------------------------------------------------------------

    def persist_cache(self, sid: str, state: dict[str, Any]) -> dict[str, Any]:
        """Persist one derived orders:state snapshot in canonical materialized form."""
        try:
            existing = self.load_cache(sid)
            merged = dict(existing)
            merged.update(state or {})
            if "created_at_ms" not in merged:
                merged["created_at_ms"] = int(existing.get("created_at_ms") or _ms_now())
            merged["updated_at_ms"] = _ms_now()
            doc = build_materialized_state_view({"ts_ms": _ms_now(), "venue": "binance", **merged})
            self.r.set(
                self._state_redis_key(sid),
                json.dumps(doc, ensure_ascii=False, default=str),
                ex=self.state_ttl if self.state_ttl > 0 else None,
            )
            # Guard integration
            if self.exec_single_active_position_per_symbol:
                symbol = (doc.get("symbol") or "").strip().upper()
                if symbol:
                    self._update_guard_on_state_persist(sid=sid, symbol=symbol, doc=doc)
            # SQL mirror
            with contextlib.suppress(Exception):
                if self.execution_journal is not None:
                    self.execution_journal.upsert_order_snapshot(doc)
                    self.execution_journal.upsert_protection_refs(doc)
            return doc
        except Exception:
            return dict(state or {})

    def _update_guard_on_state_persist(self, *, sid: str, symbol: str, doc: dict[str, Any]) -> None:
        """Update active-symbol guard after state persistence."""
        if not self._state_is_terminalish(doc):
            # Non-terminal: guard is active
            if self._guard_acquire_fn is not None:
                try:
                    guard_doc = dict(doc)
                    guard_doc.update({
                        "guard_release_policy": "exchange_truth" if self.exec_single_active_position_exchange_truth_release else "local_terminal",
                        "guard_release_pending": False,
                        "guard_release_reason": "",
                        "state_terminalish": False,
                    })
                    res = self._guard_acquire_fn(symbol, sid, guard_doc)
                    if self._guard_cas_metric_fn:
                        self._guard_cas_metric_fn(symbol, "success" if res.get("applied") else "rejected", res.get("reason") or "unknown")
                except Exception:
                    if self._guard_cas_metric_fn:
                        self._guard_cas_metric_fn(symbol, "error", "exception")
        elif self.exec_single_active_position_exchange_truth_release:
            # Terminal + exchange_truth: mark pending-release, do NOT delete guard
            if self._guard_acquire_fn is not None:
                try:
                    guard_doc = dict(doc)
                    guard_doc.update({
                        "guard_release_policy": "exchange_truth",
                        "guard_release_pending": True,
                        "guard_release_reason": "await_exchange_flat_no_orders",
                        "state_terminalish": True,
                    })
                    res = self._guard_acquire_fn(symbol, sid, guard_doc)
                    if self._guard_cas_metric_fn:
                        self._guard_cas_metric_fn(symbol, "success" if res.get("applied") else "rejected", res.get("reason") or "unknown")
                except Exception:
                    if self._guard_cas_metric_fn:
                        self._guard_cas_metric_fn(symbol, "error", "exception")
        else:
            # Legacy: release guard locally on terminal
            if self.exec_single_active_position_release_on_terminal and self._guard_release_fn is not None:
                self._guard_release_fn(symbol, sid)

    def save(self, sid: str, state: dict[str, Any]) -> None:
        """Update the derived orders:state cache from the primary execution journal."""
        try:
            if not self.exec_inline_state_projection:
                # Journal-first: emit a state_patch event; projection worker handles cache
                self._write_event({
                    "sid": sid,
                    "symbol": (state.get("symbol") or "").strip().upper(),
                    "action": (state.get("action") or "state_patch").strip() or "state_patch",
                    "event_type": "state_patch",
                    "status": (state.get("status") or "ok").strip() or "ok",
                    **state,
                })
                return
            # Inline projection (dev / non-prod mode)
            base: dict[str, Any] = {}
            if self.exec_journal_primary:
                base = self.recover_from_stream(sid) or {}
            if not base:
                base = self.load_cache(sid)
            merged = dict(base)
            merged.update(state or {})
            self.persist_cache(sid, merged)
        except Exception:
            pass  # fail-open: state is best-effort; exec stream is authoritative

    def project_from_event(self, sid: str, event_fields: dict[str, Any], *, stream_id: str = "") -> dict[str, Any]:
        """Project one newly appended exec event into the materialized state cache."""
        if not sid or not self.exec_state_derived_view:
            return {}
        try:
            base = self.load_cache(sid)
            ev = dict(event_fields or {})
            projected = project_event_into_state(ev, base_state=base, stream_id=stream_id)
            projected["ts_state_commit_ms"] = _ms_now()
            return self.persist_cache(sid, projected)
        except Exception:
            return {}

    # ------------------------------------------------------------------
    # Replay / rehydration
    # ------------------------------------------------------------------

    def recover_from_stream(self, sid: str) -> dict[str, Any]:
        """Best-effort rehydrate of orders:state:{sid} from orders:exec stream.

        Redis state keys are a materialized view; the stream is the authoritative
        fact log. On state-miss (worker restart), we replay the most recent
        execution facts for that sid and persist the rebuilt snapshot back to Redis.
        """
        if not self.exec_rehydrate_on_state_miss:
            return {}
        try:
            checkpoint_id = str(self.r.get(self._replay_checkpoint_key(sid)) or "")
            result = rebuild_state_with_fallback(
                self.r,
                exec_stream=self.exec_stream,
                sid=sid,
                scan_count=self.exec_replay_scan_count,
                checkpoint_id=checkpoint_id,
                sql_dsn=getattr(self, "execution_journal_dsn", ""),
            )
            state_doc = result.state_doc
            if not state_doc:
                return {}
            persist_state_snapshot(
                self.r,
                state_key=self._state_redis_key(sid),
                state_doc=state_doc,
                ttl_sec=self.state_ttl if self.state_ttl > 0 else 0,
                checkpoint_key=self._replay_checkpoint_key(sid),
            )
            self._write_event({
                "sid": sid,
                "action": "rehydrate",
                "event_type": "state_rehydrated_from_stream",
                "fsm_state": state_doc.get("fsm_state"),
                "stream_last_id": state_doc.get("stream_last_id"),
                "stream_replayed_events": state_doc.get("stream_replayed_events"),
                "rehydrate_source": result.source,
                "replay_truncated": int(bool(result.truncated)),
                "checkpoint_id": result.checkpoint_id,
                "retention_guard_triggered": int(bool(result.retention_guard_triggered)),
                "replay_latency_ms": int(result.latency_ms),
            })
            return state_doc
        except Exception:
            return {}

    def quarantine_for_replay_mismatch(
        self, sid: str, *, mismatch: dict[str, Any], state_doc: dict[str, Any]
    ) -> None:
        """Write quarantine event in Redis + QuarantineLedger for a replay mismatch."""
        if not self.exec_replay_quarantine_on_mismatch:
            return
        try:
            qkey = f"{self.exec_replay_quarantine_prefix}{sid}"
            now_ms = _ms_now()
            payload = dict(state_doc or {})
            payload.update({
                "sid": sid,
                "quarantined_at_ms": now_ms,
                "quarantine_reason": "replay_mismatch",
                "quarantine_source": "executor_rehydrate",
                "replay_mismatch": mismatch,
            })
            pipe = self.r.pipeline()
            pipe.set(qkey, json.dumps(payload, ensure_ascii=False, default=str))
            pipe.sadd(f"{self.exec_replay_quarantine_prefix}sids", sid)
            pipe.xadd(
                f"{self.exec_replay_quarantine_prefix}events",
                {"sid": sid, "event": "REPLAY_MISMATCH_QUARANTINED", "ts_ms": str(now_ms)},
                maxlen=10_000,
                approximate=True,
            )
            pipe.execute()
            with contextlib.suppress(Exception):
                if self.quarantine_ledger is not None:
                    self.quarantine_ledger.record_quarantine_event({
                        "sid": sid,
                        "symbol": str((state_doc or {}).get("symbol") or ""),
                        "action": "REPLAY_MISMATCH_QUARANTINED",
                        "severity": "critical" if any(k in {"status", "fsm_state"} for k in mismatch) else "warning",
                        "reason": "replay_mismatch",
                        "source": "executor_rehydrate",
                        "quarantine_key": qkey,
                        "state": payload,
                        "event_ts_ms": now_ms,
                        "created_at_ms": now_ms,
                    })
        except Exception:
            return

    # ------------------------------------------------------------------
    # FSM transitions
    # ------------------------------------------------------------------

    def transition(
        self,
        sid: str,
        *,
        symbol: str,
        action: str,
        next_state: str,
        details: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Append one idempotent FSM transition to the journal and project the cache.

        Journal-first: event is written to orders:exec first; cache is updated
        deterministically by the projection worker (or inline when
        EXEC_INLINE_STATE_PROJECTION=1).
        """
        prev = self.load(sid)
        prev_state = (prev.get("fsm_state") or "")
        if prev_state == next_state:
            return prev

        event_doc = dict(details or {})
        event_doc.update({
            "sid": sid,
            "symbol": symbol,
            "action": action,
            "event_type": "state_transition",
            "prev_state": prev_state,
            "fsm_prev_state": prev_state,
            "fsm_state": next_state,
            "fsm_ts_ms": _ms_now(),
            "fsm_mono_ms": _mono_ms(),
        })
        if next_state in {FSM_EXIT_FILLED, FSM_EMERGENCY_FLATTENED}:
            rc = (
                event_doc.get("close_reason_tag")
                or event_doc.get("reason_tag")
                or event_doc.get("resize_mode")
                or event_doc.get("cancel_mode")
                or "unknown_exit"
            )
            event_doc["reason_code"] = rc

        if EXECUTION_STATE_TRANSITION_TOTAL is not None:
            with contextlib.suppress(Exception):
                EXECUTION_STATE_TRANSITION_TOTAL.labels(
                    action=action, symbol=symbol, next_state=next_state
                ).inc()

        self._write_event(event_doc)

        merged = dict(prev)
        merged.update(details or {})
        if next_state in {FSM_EXIT_FILLED, FSM_EMERGENCY_FLATTENED} and "reason_code" not in merged:
            merged["reason_code"] = event_doc["reason_code"]
        merged["sid"] = sid
        merged["symbol"] = symbol
        merged["action"] = action
        merged["fsm_prev_state"] = prev_state
        merged["fsm_state"] = next_state
        return build_materialized_state_view(merged)

    def mark_pending_reconcile(
        self, sid: str, *, symbol: str, action: str, reason: str
    ) -> None:
        if EXECUTION_RECONCILE_PENDING_TOTAL is not None:
            with contextlib.suppress(Exception):
                EXECUTION_RECONCILE_PENDING_TOTAL.labels(action=action, symbol=symbol).inc()
        self.transition(
            sid,
            symbol=symbol,
            action=action,
            next_state=FSM_PENDING_RECONCILE,
            details={"reconcile_reason": reason},
        )
