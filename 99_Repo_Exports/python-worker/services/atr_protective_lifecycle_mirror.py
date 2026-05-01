from __future__ import annotations
"""
Phase 8.6 — Protective Lifecycle Mirror

Fail-open shadow mirror of post-trade protective lifecycle into the
ATR control-plane graph.  Every method is catch-all: errors are logged
but NEVER propagate to the legacy trading path.

Usage (from TradeMonitor):
    mirror = ProtectiveLifecycleMirror()
    mirror.on_position_opened(signal_id, symbol, side, entry, sl, tp1, ts)

ENV:
    ATR_GRAPH_PROTECTIVE_ENABLE   = 0|1  (default 0)
    ATR_GRAPH_PROTECTIVE_MODE     = shadow_compare | graph_read_primary
    ATR_GRAPH_PROTECTIVE_SYMBOLS  = BTCUSDT,ETHUSDT
"""

import json
import logging
import os
import time
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Set

from prometheus_client import Counter

logger = logging.getLogger("atr_protective_lifecycle_mirror")

# ─── ENV ──────────────────────────────────────────────────────────────────────

_ENABLE = os.getenv("ATR_GRAPH_PROTECTIVE_ENABLE", "0") == "1"
_MODE = os.getenv("ATR_GRAPH_PROTECTIVE_MODE", "shadow_compare")

_BOUNDED_SYMBOLS_RAW = os.getenv("ATR_GRAPH_PROTECTIVE_SYMBOLS", "BTCUSDT,ETHUSDT")
_BOUNDED_SYMBOLS: Set[str] = {
    s.strip().upper() for s in _BOUNDED_SYMBOLS_RAW.split(",") if s.strip()
}

# ─── Prometheus metrics ───────────────────────────────────────────────────────

MIRROR_EVENT_TOTAL = Counter(
    "atr_protective_mirror_event_total",
    "Protective lifecycle events mirrored into graph",
    ["event_type"],
)
MIRROR_ERROR_TOTAL = Counter(
    "atr_protective_mirror_error_total",
    "Errors during protective lifecycle mirroring",
)
MIRROR_INVARIANT_VIOLATION_TOTAL = Counter(
    "atr_protective_invariant_violation_total",
    "Protective invariant violations detected during mirroring",
    ["invariant"],
)

# ─── Helpers ──────────────────────────────────────────────────────────────────

def _gen_id(prefix: str) -> str:
    ts = datetime.now(tz=timezone.utc).strftime("%Y%m%d_%H%M%S")
    return f"{prefix}_{ts}_{uuid.uuid4().hex[:8]}"


def _now_ms() -> int:
    return int(time.time() * 1000)


# ─── Node type constants ─────────────────────────────────────────────────────

NODE_PROTECTIVE_POSITION = "ProtectivePositionState"
NODE_BREAK_EVEN = "BreakEvenState"
NODE_TRAILING = "TrailingState"
NODE_CLOSEOUT = "CloseoutState"
NODE_POST_TRADE_FEEDBACK = "PostTradeFeedbackState"


def _node_id(signal_id: str, node_type: str) -> str:
    """Deterministic node_id for a given signal + node type."""
    return f"prot:{signal_id}:{node_type}"


# ─── Core Mirror ──────────────────────────────────────────────────────────────

class ProtectiveLifecycleMirror:
    """
    Fail-open shadow mirror of post-trade protective lifecycle into graph.

    All public methods silently swallow exceptions so that the legacy
    trade monitor path is never affected.
    """

    def __init__(self) -> None:
        self.enabled = _ENABLE
        self.bounded_symbols = _BOUNDED_SYMBOLS
        if self.enabled:
            logger.info(
                "✅ ProtectiveLifecycleMirror enabled mode=%s symbols=%s",
                _MODE, ",".join(sorted(self.bounded_symbols)),
            )
        else:
            logger.info("⏸️ ProtectiveLifecycleMirror disabled (ATR_GRAPH_PROTECTIVE_ENABLE=0)")

    # ── Guards ────────────────────────────────────────────────────────────

    def _should_mirror(self, symbol: str) -> bool:
        if not self.enabled:
            return False
        return symbol.upper() in self.bounded_symbols

    # ── DB access (lazy import to avoid circular deps) ────────────────────

    @staticmethod
    def _get_conn():
        from services.analytics_db import get_conn
        return get_conn()

    @staticmethod
    def _emit_event(
        conn,
        event_type: str,
        signal_id: str,
        symbol: str,
        payload: Dict[str, Any],
    ) -> str:
        """Write a protective lifecycle event to the journal."""
        event_id = _gen_id("pev")
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO atr_control_plane_events (
                    event_id, event_type, aggregate_type, aggregate_id,
                    scope_kind, scope_value, actor, reason_code, event_json
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                event_id, event_type, "protective_lifecycle", signal_id,
                "symbol", symbol, "protective_mirror", event_type,
                json.dumps(payload),
            ))
        return event_id

    @staticmethod
    def _upsert_node(
        conn,
        node_id: str,
        node_type: str,
        symbol: str,
        state: Dict[str, Any],
        event_id: str,
    ) -> None:
        """Create or update a protective graph node."""
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO atr_control_plane_nodes (
                    node_id, node_type, scope_kind, scope_value,
                    node_state_json, version, last_event_id
                ) VALUES (%s, %s, 'symbol', %s, %s, 1, %s)
                ON CONFLICT (node_id) DO UPDATE SET
                    node_state_json = EXCLUDED.node_state_json,
                    version = atr_control_plane_nodes.version + 1,
                    last_event_id = EXCLUDED.last_event_id,
                    updated_at = now()
            """, (
                node_id, node_type, symbol,
                json.dumps(state), event_id,
            ))

    def _record_invariant_drift(
        self,
        conn,
        signal_id: str,
        drift_kind: str,
        severity: str,
        reason_code: str,
        drift_json: Dict[str, Any],
    ) -> None:
        """Record a protective invariant violation as a drift."""
        drift_id = _gen_id("pdrift")
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO atr_protective_drifts (
                    drift_id, signal_id, drift_kind, severity, status,
                    reason_code, drift_json
                ) VALUES (%s, %s, %s, %s, 'open', %s, %s)
            """, (
                drift_id, signal_id, drift_kind, severity,
                reason_code, json.dumps(drift_json),
            ))
        MIRROR_INVARIANT_VIOLATION_TOTAL.labels(invariant=drift_kind).inc()
        logger.warning(
            "🚨 Protective invariant violation: %s on %s (%s)",
            drift_kind, signal_id, severity,
        )

    # ── Lifecycle events ──────────────────────────────────────────────────

    def on_position_opened(
        self,
        signal_id: str,
        symbol: str,
        side: str,
        entry_price: float,
        sl: float,
        tp1: float,
        ts_ms: int,
    ) -> None:
        """Mirror POSITION_OPENED into graph."""
        if not self._should_mirror(symbol):
            return
        try:
            payload = {
                "symbol": symbol, "side": side,
                "entry_price": entry_price, "sl": sl, "tp1": tp1,
                "ts_ms": ts_ms,
            }
            with self._get_conn() as conn:
                event_id = self._emit_event(
                    conn, "position_opened", signal_id, symbol, payload,
                )
                # Position node
                self._upsert_node(conn, _node_id(signal_id, NODE_PROTECTIVE_POSITION),
                    NODE_PROTECTIVE_POSITION, symbol, {
                        "status": "open", "side": side,
                        "entry_price": entry_price, "current_sl": sl,
                        "tp1": tp1, "opened_at_ms": ts_ms,
                    }, event_id)
                # BreakEven node (inactive)
                self._upsert_node(conn, _node_id(signal_id, NODE_BREAK_EVEN),
                    NODE_BREAK_EVEN, symbol, {
                        "status": "inactive", "tp1_reached": False,
                    }, event_id)
                # Trailing node (inactive)
                self._upsert_node(conn, _node_id(signal_id, NODE_TRAILING),
                    NODE_TRAILING, symbol, {
                        "status": "inactive",
                        "last_trailing_sl": None, "max_favorable": entry_price,
                    }, event_id)
                conn.commit()
            MIRROR_EVENT_TOTAL.labels(event_type="position_opened").inc()
        except Exception as exc:
            MIRROR_ERROR_TOTAL.inc()
            logger.error("Mirror.on_position_opened failed for %s: %s", signal_id, exc, exc_info=True)

    def on_tp1_reached(
        self,
        signal_id: str,
        symbol: str,
        tp1_price: float,
        ts_ms: int,
    ) -> None:
        """Mirror TP1_REACHED into graph, update BreakEvenState to armed."""
        if not self._should_mirror(symbol):
            return
        try:
            payload = {"tp1_price": tp1_price, "ts_ms": ts_ms}
            with self._get_conn() as conn:
                event_id = self._emit_event(
                    conn, "tp1_reached", signal_id, symbol, payload,
                )
                # Update BE node → armed (TP1 reached, BE not yet activated)
                self._upsert_node(conn, _node_id(signal_id, NODE_BREAK_EVEN),
                    NODE_BREAK_EVEN, symbol, {
                        "status": "armed", "tp1_reached": True,
                        "tp1_price": tp1_price, "tp1_reached_at_ms": ts_ms,
                    }, event_id)
                conn.commit()
            MIRROR_EVENT_TOTAL.labels(event_type="tp1_reached").inc()
        except Exception as exc:
            MIRROR_ERROR_TOTAL.inc()
            logger.error("Mirror.on_tp1_reached failed for %s: %s", signal_id, exc, exc_info=True)

    def on_break_even_activated(
        self,
        signal_id: str,
        symbol: str,
        new_sl: float,
        ts_ms: int,
    ) -> None:
        """Mirror BREAK_EVEN_ACTIVATED. Validates P1: BE cannot activate before TP1."""
        if not self._should_mirror(symbol):
            return
        try:
            with self._get_conn() as conn:
                # P1 check: TP1 must be reached before BE
                be_node_id = _node_id(signal_id, NODE_BREAK_EVEN)
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT node_state_json FROM atr_control_plane_nodes WHERE node_id = %s",
                        (be_node_id,),
                    )
                    row = cur.fetchone()

                be_state = (row[0] if row else {}) if row else {}
                if isinstance(be_state, str):
                    be_state = json.loads(be_state)

                if not be_state.get("tp1_reached"):
                    self._record_invariant_drift(
                        conn, signal_id,
                        drift_kind="be_before_tp1",
                        severity="critical",
                        reason_code="BE_activated_without_tp1_reached_in_graph",
                        drift_json={"new_sl": new_sl, "be_state": be_state},
                    )

                payload = {"new_sl": new_sl, "ts_ms": ts_ms}
                event_id = self._emit_event(
                    conn, "break_even_activated", signal_id, symbol, payload,
                )
                self._upsert_node(conn, be_node_id,
                    NODE_BREAK_EVEN, symbol, {
                        "status": "activated",
                        "tp1_reached": True,
                        "be_sl": new_sl,
                        "activated_at_ms": ts_ms,
                    }, event_id)
                # Update position node SL
                self._upsert_node(conn, _node_id(signal_id, NODE_PROTECTIVE_POSITION),
                    NODE_PROTECTIVE_POSITION, symbol, {
                        "status": "open", "current_sl": new_sl,
                        "be_activated": True, "updated_at_ms": ts_ms,
                    }, event_id)
                conn.commit()
            MIRROR_EVENT_TOTAL.labels(event_type="break_even_activated").inc()
        except Exception as exc:
            MIRROR_ERROR_TOTAL.inc()
            logger.error("Mirror.on_break_even_activated failed for %s: %s", signal_id, exc, exc_info=True)

    def on_trailing_activated(
        self,
        signal_id: str,
        symbol: str,
        ts_ms: int,
    ) -> None:
        """Mirror TRAILING_ACTIVE. Validates P2: trailing cannot activate before BE."""
        if not self._should_mirror(symbol):
            return
        try:
            with self._get_conn() as conn:
                # P2 check: BE must be activated before trailing
                be_node_id = _node_id(signal_id, NODE_BREAK_EVEN)
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT node_state_json FROM atr_control_plane_nodes WHERE node_id = %s",
                        (be_node_id,),
                    )
                    row = cur.fetchone()

                be_state = (row[0] if row else {}) if row else {}
                if isinstance(be_state, str):
                    be_state = json.loads(be_state)

                if be_state.get("status") != "activated":
                    self._record_invariant_drift(
                        conn, signal_id,
                        drift_kind="trailing_before_be",
                        severity="critical",
                        reason_code="trailing_activated_before_be_activated",
                        drift_json={"be_state": be_state},
                    )

                payload = {"ts_ms": ts_ms}
                event_id = self._emit_event(
                    conn, "trailing_activated", signal_id, symbol, payload,
                )
                self._upsert_node(conn, _node_id(signal_id, NODE_TRAILING),
                    NODE_TRAILING, symbol, {
                        "status": "active", "activated_at_ms": ts_ms,
                        "last_trailing_sl": None, "moves_count": 0,
                    }, event_id)
                conn.commit()
            MIRROR_EVENT_TOTAL.labels(event_type="trailing_activated").inc()
        except Exception as exc:
            MIRROR_ERROR_TOTAL.inc()
            logger.error("Mirror.on_trailing_activated failed for %s: %s", signal_id, exc, exc_info=True)

    def on_sl_moved(
        self,
        signal_id: str,
        symbol: str,
        side: str,
        old_sl: float,
        new_sl: float,
        max_favorable: float,
        ts_ms: int,
    ) -> None:
        """
        Mirror SL_MOVED. Validates P3: SL may only move toward profit
        after protective activation.
        """
        if not self._should_mirror(symbol):
            return
        try:
            with self._get_conn() as conn:
                # P3 check: ratchet-only (SL must move toward profit)
                side_upper = side.upper()
                ratchet_ok = True
                if side_upper in ("LONG", "BUY"):
                    # For LONG: new_sl must be >= old_sl
                    if new_sl < old_sl - 1e-10:
                        ratchet_ok = False
                elif side_upper in ("SHORT", "SELL"):
                    # For SHORT: new_sl must be <= old_sl
                    if new_sl > old_sl + 1e-10:
                        ratchet_ok = False

                if not ratchet_ok:
                    self._record_invariant_drift(
                        conn, signal_id,
                        drift_kind="sl_ratchet_backwards",
                        severity="critical",
                        reason_code=f"sl_moved_against_profit_side={side_upper}",
                        drift_json={
                            "side": side_upper,
                            "old_sl": old_sl, "new_sl": new_sl,
                        }
                    )

                payload = {
                    "old_sl": old_sl, "new_sl": new_sl,
                    "max_favorable": max_favorable, "ts_ms": ts_ms,
                }
                event_id = self._emit_event(
                    conn, "sl_moved", signal_id, symbol, payload,
                )
                # Update trailing node
                trail_node_id = _node_id(signal_id, NODE_TRAILING)
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT node_state_json FROM atr_control_plane_nodes WHERE node_id = %s",
                        (trail_node_id,),
                    )
                    row = cur.fetchone()

                trail_state = (row[0] if row else {}) if row else {}
                if isinstance(trail_state, str):
                    trail_state = json.loads(trail_state)
                moves = int(trail_state.get("moves_count", 0)) + 1

                self._upsert_node(conn, trail_node_id,
                    NODE_TRAILING, symbol, {
                        "status": "active",
                        "last_trailing_sl": new_sl,
                        "max_favorable": max_favorable,
                        "moves_count": moves,
                        "last_move_at_ms": ts_ms,
                    }, event_id)
                # Update position current_sl
                self._upsert_node(conn, _node_id(signal_id, NODE_PROTECTIVE_POSITION),
                    NODE_PROTECTIVE_POSITION, symbol, {
                        "status": "open", "current_sl": new_sl,
                        "updated_at_ms": ts_ms,
                    }, event_id)
                conn.commit()
            MIRROR_EVENT_TOTAL.labels(event_type="sl_moved").inc()
        except Exception as exc:
            MIRROR_ERROR_TOTAL.inc()
            logger.error("Mirror.on_sl_moved failed for %s: %s", signal_id, exc, exc_info=True)

    def on_position_closed(
        self,
        signal_id: str,
        symbol: str,
        exit_price: float,
        pnl_bps: float,
        close_reason: str,
        max_mae_pct: float,
        ts_ms: int,
    ) -> None:
        """
        Mirror POSITION_CLOSED.
        P4: position_closed must end active trailing/break-even lifecycle.
        """
        if not self._should_mirror(symbol):
            return
        try:
            payload = {
                "exit_price": exit_price, "pnl_bps": pnl_bps,
                "close_reason": close_reason, "max_mae_pct": max_mae_pct,
                "ts_ms": ts_ms,
            }
            with self._get_conn() as conn:
                event_id = self._emit_event(
                    conn, "position_closed", signal_id, symbol, payload,
                )
                # Closeout node
                self._upsert_node(conn, _node_id(signal_id, NODE_CLOSEOUT),
                    NODE_CLOSEOUT, symbol, {
                        "close_reason": close_reason,
                        "exit_price": exit_price,
                        "pnl_bps": pnl_bps,
                        "max_mae_pct": max_mae_pct,
                        "closed_at_ms": ts_ms,
                    }, event_id)
                # Update position → closed
                self._upsert_node(conn, _node_id(signal_id, NODE_PROTECTIVE_POSITION),
                    NODE_PROTECTIVE_POSITION, symbol, {
                        "status": "closed", "exit_price": exit_price,
                        "close_reason": close_reason, "closed_at_ms": ts_ms,
                    }, event_id)
                # P4: Deactivate BE and trailing nodes
                self._upsert_node(conn, _node_id(signal_id, NODE_BREAK_EVEN),
                    NODE_BREAK_EVEN, symbol, {
                        "status": "closed", "closed_at_ms": ts_ms,
                    }, event_id)
                self._upsert_node(conn, _node_id(signal_id, NODE_TRAILING),
                    NODE_TRAILING, symbol, {
                        "status": "closed", "closed_at_ms": ts_ms,
                    }, event_id)
                conn.commit()
            MIRROR_EVENT_TOTAL.labels(event_type="position_closed").inc()
        except Exception as exc:
            MIRROR_ERROR_TOTAL.inc()
            logger.error("Mirror.on_position_closed failed for %s: %s", signal_id, exc, exc_info=True)

    def on_slippage_feedback(
        self,
        signal_id: str,
        symbol: str,
        slippage_bps: float,
        slippage_ema: float,
    ) -> None:
        """
        Mirror slippage feedback into graph.
        P5: slippage feedback must link to closed trade.
        """
        if not self._should_mirror(symbol):
            return
        try:
            with self._get_conn() as conn:
                # P5 check: closeout node must exist
                closeout_id = _node_id(signal_id, NODE_CLOSEOUT)
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT 1 FROM atr_control_plane_nodes WHERE node_id = %s",
                        (closeout_id,),
                    )
                    has_closeout = cur.fetchone() is not None

                if not has_closeout:
                    self._record_invariant_drift(
                        conn, signal_id,
                        drift_kind="slippage_feedback_mismatch",
                        severity="warn",
                        reason_code="slippage_feedback_without_closeout_node",
                        drift_json={
                            "slippage_bps": slippage_bps,
                            "slippage_ema": slippage_ema,
                        }
                    )

                payload = {
                    "slippage_bps": slippage_bps,
                    "slippage_ema": slippage_ema,
                }
                event_id = self._emit_event(
                    conn, "slippage_feedback_updated", signal_id, symbol, payload,
                )
                self._upsert_node(conn, _node_id(signal_id, NODE_POST_TRADE_FEEDBACK),
                    NODE_POST_TRADE_FEEDBACK, symbol, {
                        "slippage_bps": slippage_bps,
                        "slippage_ema": slippage_ema,
                        "updated_at_ms": _now_ms(),
                    }, event_id)
                conn.commit()
            MIRROR_EVENT_TOTAL.labels(event_type="slippage_feedback_updated").inc()
        except Exception as exc:
            MIRROR_ERROR_TOTAL.inc()
            logger.error("Mirror.on_slippage_feedback failed for %s: %s", signal_id, exc, exc_info=True)
