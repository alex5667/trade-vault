from __future__ import annotations

"""
Phase 8.6 — Graph-Backed Protective State Resolver

Read-only service that returns the canonical protective state for a
signal_id by reading graph nodes.  Also supports building the legacy
state from Redis open_positions for dual-read comparison.

Consumers (Phase 8.6): auditor, cert service, diagnostics, postmortem.
NOT for broker execution.
"""

import json
import logging
from typing import Any

logger = logging.getLogger("atr_graph_backed_protective_resolver")


class ATRGraphBackedProtectiveResolver:
    """
    Resolves the canonical protective lifecycle state for a signal_id
    from graph nodes created by ProtectiveLifecycleMirror.
    """

    @staticmethod
    def resolve_from_graph(signal_id: str) -> dict[str, Any] | None:
        """
        Build canonical protective state from graph nodes.

        Returns dict:
            signal_id, symbol, position_state, break_even_state,
            trailing_state, tp1_reached, current_sl, max_favorable,
            closeout_state, slippage_feedback, projection_ver
        """
        try:
            import psycopg2.extras

            from services.analytics_db import get_conn

            with get_conn() as conn, conn.cursor(
                cursor_factory=psycopg2.extras.RealDictCursor,
            ) as cur:
                # Fetch all protective nodes for this signal
                cur.execute("""
                    SELECT node_id, node_type, node_state_json, scope_value, version
                    FROM atr_control_plane_nodes
                    WHERE node_id LIKE %s
                    ORDER BY node_type
                """, (f"prot:{signal_id}:%",))
                rows = cur.fetchall()

                if not rows:
                    return None

                nodes: dict[str, dict[str, Any]] = {}
                symbol = "UNKNOWN"
                max_version = 0
                for r in rows:
                    state = r["node_state_json"]
                    if isinstance(state, str):
                        state = json.loads(state)
                    nodes[r["node_type"]] = state
                    symbol = r["scope_value"] or symbol
                    max_version = max(max_version, r.get("version", 0) or 0)

                pos = nodes.get("ProtectivePositionState", {})
                be = nodes.get("BreakEvenState", {})
                trail = nodes.get("TrailingState", {})
                closeout = nodes.get("CloseoutState")
                feedback = nodes.get("PostTradeFeedbackState")

                return {
                    "signal_id": signal_id,
                    "symbol": symbol,
                    "position_state": pos.get("status", "unknown"),
                    "break_even_state": be.get("status", "unknown"),
                    "trailing_state": trail.get("status", "unknown"),
                    "tp1_reached": bool(be.get("tp1_reached", False)),
                    "current_sl": pos.get("current_sl"),
                    "max_favorable": trail.get("max_favorable"),
                    "closeout_state": closeout,
                    "slippage_feedback": feedback,
                    "projection_ver": max_version,
                }
        except Exception as exc:
            logger.error("resolve_from_graph(%s) failed: %s", signal_id, exc, exc_info=True)
            return None

    @staticmethod
    def resolve_legacy_from_redis(
        signal_id: str,
        redis_client: Any = None,
    ) -> dict[str, Any] | None:
        """
        Build legacy protective state from Redis open_positions hash.

        Used for dual-read comparison by the equivalence cert service.
        Falls back to None if position not found or already closed.
        """
        try:
            if redis_client is None:
                from core.redis_client import get_redis
                redis_client = get_redis()

            # Look up position by SID
            pos_key = None
            # Try direct sid → pos_id mapping
            pos_id = redis_client.get(f"pos_by_sid:{signal_id}")
            if pos_id:
                pos_key = f"open_positions:{pos_id}"

            if not pos_key:
                # Scan known prefixes (bounded, not wide)
                for prefix in ("open_positions:", "positions:"):
                    # We cannot scan efficiently here, so rely on the mapping
                    pass
                return None

            h = redis_client.hgetall(pos_key)
            if not h or h.get("status") != "open":
                return None

            tp1_hit = (h.get("tp1_hit", "0")) == "1"
            trailing_active = (h.get("trailing_active", "0")) == "1"
            trailing_started = (h.get("trailing_started", "0")) == "1"

            # Derive break-even state from tp1_hit and trailing logic
            if trailing_active or trailing_started:
                be_state = "activated"
            elif tp1_hit:
                be_state = "armed"
            else:
                be_state = "inactive"

            trail_state = "active" if trailing_active else (
                "armed" if trailing_started else "inactive"
            )

            return {
                "signal_id": signal_id,
                "symbol": (h.get("symbol", "UNKNOWN")).upper(),
                "position_state": "open",
                "break_even_state": be_state,
                "trailing_state": trail_state,
                "tp1_reached": tp1_hit,
                "current_sl": float(h.get("sl", 0)),
                "max_favorable": float(h.get("max_favorable_price", 0)),
                "closeout_state": None,
                "slippage_feedback": None,
                "projection_ver": 0,
            }
        except Exception as exc:
            logger.error(
                "resolve_legacy_from_redis(%s) failed: %s",
                signal_id, exc, exc_info=True,
            )
            return None

    @staticmethod
    def resolve_legacy_closed(
        signal_id: str,
    ) -> dict[str, Any] | None:
        """
        Build legacy protective state for a closed position from
        trades_closed table (truth source for closeout).
        """
        try:
            import psycopg2.extras

            from services.analytics_db import get_conn

            with get_conn() as conn, conn.cursor(
                cursor_factory=psycopg2.extras.RealDictCursor,
            ) as cur:
                # Try trades_closed first
                cur.execute("""
                    SELECT signal_id, symbol, entry_price, exit_price,
                           sl_price, tp1_price, pnl, pnl_bps,
                           max_mae_pct, slippage_bps, close_reason,
                           opened_at, closed_at
                    FROM trades_closed
                    WHERE signal_id = %s
                    ORDER BY closed_at DESC LIMIT 1
                """, (signal_id,))
                row = cur.fetchone()
                if not row:
                    return None

                return {
                    "signal_id": signal_id,
                    "symbol": (row.get("symbol", "UNKNOWN")).upper(),
                    "position_state": "closed",
                    "break_even_state": "closed",
                    "trailing_state": "closed",
                    "tp1_reached": True,  # Cannot determine for closed trades
                    "current_sl": float(row.get("sl_price", 0) or 0),
                    "max_favorable": None,
                    "closeout_state": {
                        "close_reason": row.get("close_reason"),
                        "exit_price": float(row.get("exit_price", 0) or 0),
                        "pnl_bps": float(row.get("pnl_bps", 0) or 0),
                        "max_mae_pct": float(row.get("max_mae_pct", 0) or 0),
                    },
                    "slippage_feedback": {
                        "slippage_bps": float(row.get("slippage_bps", 0) or 0),
                    },
                    "projection_ver": 0,
                }
        except Exception as exc:
            logger.error(
                "resolve_legacy_closed(%s) failed: %s",
                signal_id, exc, exc_info=True,
            )
            return None
