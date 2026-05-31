"""maker_tp_watchdog.py — Maker TP limit tracking and lifecycle watchdog.

Extracted from binance_executor.py (god-class decomposition).

Responsibilities:
- Monitor maker TP ladder (TAKE_PROFIT_LIMIT) fills in background threads
- Observe mark-price / contract spread vs TP trigger
- Track pending/filled/missed TP state per SID level
- Lifecycle watchdog: detects unexpected exits and logs them
"""
from __future__ import annotations

import threading
import time
from typing import Any, TYPE_CHECKING

from services.execution.binance_order_mapper import _f, _tp_state_name

if TYPE_CHECKING:
    from services.binance_futures_client import BinanceFuturesClient


def _ms_now() -> int:
    try:
        from utils.time_utils import get_ny_time_millis
        return get_ny_time_millis()
    except Exception:
        return int(time.time() * 1000)


class MakerTpWatchdog:
    """Monitors maker TP (TAKE_PROFIT_LIMIT) fill progress in daemon threads.

    Each TP level gets its own tracking thread that polls for:
    - Mark price cross above/below trigger price (exchange fill proxy)
    - Spread between mark and contract price
    - Order status via user-stream cache

    The lifecycle watchdog monitors the trade overall and fires events on
    unexpected exits or long-running positions.
    """

    def __init__(
        self,
        *,
        tp_limit_poll_s: float = 2.0,
        tp_limit_timeout_s: float = 86400.0,
        tp_limit_spread_warn_bps: float = 20.0,
        tp_limit_trigger_working_type: str = "MARK_PRICE",
        lifecycle_poll_s: float = 30.0,
        lifecycle_max_duration_s: float = 86400.0,
        write_event_fn: Any = None,
        lookup_user_stream_fn: Any = None,  # reconcile service lookup
    ) -> None:
        self.tp_limit_poll_s = tp_limit_poll_s
        self.tp_limit_timeout_s = tp_limit_timeout_s
        self.tp_limit_spread_warn_bps = tp_limit_spread_warn_bps
        self.tp_limit_trigger_working_type = tp_limit_trigger_working_type
        self.lifecycle_poll_s = lifecycle_poll_s
        self.lifecycle_max_duration_s = lifecycle_max_duration_s
        self._write_event_fn = write_event_fn
        self._lookup_user_stream_fn = lookup_user_stream_fn
        self._active_threads: list[threading.Thread] = []
        self._tp_states: dict[str, dict[int, str]] = {}
        self._tp_lock = threading.Lock()

    def _write_event(self, fields: dict[str, Any]) -> None:
        if self._write_event_fn:
            self._write_event_fn(fields)

    # ------------------------------------------------------------------
    # TP state management
    # ------------------------------------------------------------------

    def note_tp_state(self, sid: str, level: int, state: str) -> None:
        """Record TP level state (PENDING/FILLED/MISSED/CANCELLED)."""
        with self._tp_lock:
            if sid not in self._tp_states:
                self._tp_states[sid] = {}
            self._tp_states[sid][level] = state

    def emit_tp_state(self, sid: str, symbol: str) -> None:
        """Emit current TP state dict as an execution event."""
        with self._tp_lock:
            states = dict(self._tp_states.get(sid) or {})
        if not states:
            return
        self._write_event({
            "sid": sid, "symbol": symbol,
            "action": "tp_state_snapshot",
            "event_type": "TP_STATE_SNAPSHOT",
            **{_tp_state_name(lvl, state): "1" for lvl, state in states.items()},
        })

    # ------------------------------------------------------------------
    # Spread observation
    # ------------------------------------------------------------------

    def observe_mark_contract_spread(
        self,
        *,
        sid: str,
        symbol: str,
        level: int,
        mark_price: float,
        tp_trigger_price: float,
        logical_side: str,
    ) -> None:
        """Emit warning if mark-contract spread warrants attention."""
        spread_bps = abs(mark_price - tp_trigger_price) / max(tp_trigger_price, 1e-10) * 10000.0
        if spread_bps > self.tp_limit_spread_warn_bps:
            self._write_event({
                "sid": sid, "symbol": symbol,
                "action": "tp_spread_warn",
                "event_type": "TP_SPREAD_WARN",
                "severity": "warning",
                "tp_level": level,
                "mark_price": mark_price,
                "tp_trigger_price": tp_trigger_price,
                "spread_bps": round(spread_bps, 2),
            })

    def observe_sl_tp_trigger_semantics(
        self,
        *,
        sid: str,
        symbol: str,
        sl_price: float,
        tp_levels: list[float],
        logical_side: str,
        working_type: str,
    ) -> None:
        """Emit event recording SL/TP trigger semantics for replay audit."""
        self._write_event({
            "sid": sid, "symbol": symbol,
            "action": "protection_semantics",
            "event_type": "PROTECTION_SEMANTICS",
            "sl_price": sl_price,
            "tp_count": len(tp_levels),
            "tp_prices": str(tp_levels),
            "working_type": working_type,
            "logical_side": logical_side,
        })

    # ------------------------------------------------------------------
    # Maker TP tracking thread
    # ------------------------------------------------------------------

    def _track_tp_level(
        self,
        *,
        sid: str,
        symbol: str,
        level: int,
        tp_trigger_price: float,
        tp_order_id: Any,
        tp_client_order_id: str | None,
        logical_side: str,
        client: "BinanceFuturesClient",
    ) -> None:
        """Background thread: poll until TP level confirmed filled or timeout."""
        deadline = time.monotonic() + self.tp_limit_timeout_s
        self.note_tp_state(sid, level, "PENDING")

        while time.monotonic() < deadline:
            try:
                # 1. Check user-stream cache first
                if tp_client_order_id and self._lookup_user_stream_fn:
                    ev = self._lookup_user_stream_fn(plain_client_id=tp_client_order_id)
                    if ev:
                        status = str((ev.get("order") or ev).get("X") or (ev.get("order") or ev).get("status") or "")
                        if status in {"FILLED", "PARTIALLY_FILLED"}:
                            self.note_tp_state(sid, level, "FILLED")
                            self._write_event({
                                "sid": sid, "symbol": symbol,
                                "action": f"tp{level}_filled",
                                "event_type": f"TP{level}_FILLED",
                                "tp_level": level,
                                "tp_trigger_price": tp_trigger_price,
                                "fill_status": status,
                            })
                            return

                # 2. Mark price proximity check
                try:
                    mark_resp = client.get_mark_price(symbol)
                    mark = _f(mark_resp.get("markPrice") or mark_resp.get("price"), 0.0)  # type: ignore
                    if mark > 0:
                        self.observe_mark_contract_spread(
                            sid=sid, symbol=symbol, level=level,
                            mark_price=mark, tp_trigger_price=tp_trigger_price,
                            logical_side=logical_side,
                        )
                except Exception:
                    pass

                time.sleep(self.tp_limit_poll_s)
            except Exception:
                time.sleep(self.tp_limit_poll_s)

        # Timeout
        self.note_tp_state(sid, level, "MISSED")
        self._write_event({
            "sid": sid, "symbol": symbol,
            "action": f"tp{level}_watchdog_timeout",
            "event_type": f"TP{level}_WATCHDOG_TIMEOUT",
            "severity": "warning",
            "tp_level": level,
        })

    def start_maker_tp_watchdogs(
        self,
        *,
        sid: str,
        symbol: str,
        logical_side: str,
        tp_levels: list[float],
        tp_order_ids: list[Any],
        tp_client_order_ids: list[str | None],
        client: "BinanceFuturesClient",
    ) -> None:
        """Start one daemon thread per TP level to track fill progress."""
        for i, (tp_price, tp_oid, tp_cid) in enumerate(
            zip(tp_levels, tp_order_ids, tp_client_order_ids)
        ):
            level = i + 1
            t = threading.Thread(
                target=self._track_tp_level,
                kwargs={
                    "sid": sid,
                    "symbol": symbol,
                    "level": level,
                    "tp_trigger_price": tp_price,
                    "tp_order_id": tp_oid,
                    "tp_client_order_id": tp_cid,
                    "logical_side": logical_side,
                    "client": client,
                },
                daemon=True,
            )
            t.name = f"tp-watchdog-{symbol[:6]}-{sid[:8]}-tp{level}"
            t.start()
            self._active_threads.append(t)

    # ------------------------------------------------------------------
    # Lifecycle watchdog
    # ------------------------------------------------------------------

    def _lifecycle_watch(
        self,
        *,
        sid: str,
        symbol: str,
        logical_side: str,
        opened_at_ms: int,
        client: "BinanceFuturesClient",
        state_load_fn: Any,
    ) -> None:
        """Background thread: monitor trade lifecycle for unexpected exits."""
        deadline = time.monotonic() + self.lifecycle_max_duration_s

        while time.monotonic() < deadline:
            try:
                state = state_load_fn(sid) if state_load_fn else {}
                fsm = (state.get("fsm_state") or "").strip().upper()
                if fsm in {"EXIT_FILLED", "EMERGENCY_FLATTENED", "FAILED"}:
                    age_ms = _ms_now() - opened_at_ms
                    self._write_event({
                        "sid": sid, "symbol": symbol,
                        "action": "lifecycle_terminal_detected",
                        "event_type": "LIFECYCLE_TERMINAL_DETECTED",
                        "terminal_state": fsm,
                        "trade_duration_ms": age_ms,
                    })
                    return

                age_ms = _ms_now() - opened_at_ms
                if age_ms > self.lifecycle_max_duration_s * 1000:
                    self._write_event({
                        "sid": sid, "symbol": symbol,
                        "action": "lifecycle_max_duration_exceeded",
                        "event_type": "LIFECYCLE_MAX_DURATION",
                        "severity": "warning",
                        "trade_duration_ms": age_ms,
                    })
                    return

                time.sleep(self.lifecycle_poll_s)
            except Exception:
                time.sleep(self.lifecycle_poll_s)

    def start_lifecycle_watchdog(
        self,
        *,
        sid: str,
        symbol: str,
        logical_side: str,
        opened_at_ms: int,
        client: "BinanceFuturesClient",
        state_load_fn: Any = None,
    ) -> None:
        """Start lifecycle watchdog daemon thread."""
        t = threading.Thread(
            target=self._lifecycle_watch,
            kwargs={
                "sid": sid,
                "symbol": symbol,
                "logical_side": logical_side,
                "opened_at_ms": opened_at_ms,
                "client": client,
                "state_load_fn": state_load_fn,
            },
            daemon=True,
        )
        t.name = f"lifecycle-{symbol[:6]}-{sid[:8]}"
        t.start()
        self._active_threads.append(t)
