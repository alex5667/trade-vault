"""position_reconcile_loop_v1.py — Periodic reconcile: local positions vs Binance.

P0-2: Detects mismatches between local Redis state and exchange reality.

Mismatch types:
  naked_position    — exchange has open position, no local state / no protection orders
  orphan_order      — exchange has open order, no matching local position
  qty_mismatch      — local qty differs from exchange qty by > threshold
  local_only        — local state says open, exchange says flat

Actions (ENV-gated):
  shadow (default)  — emit metrics + events, no auto-action
  enforce           — also calls emergency flatten on naked_position

ENV:
  RECONCILE_LOOP_ENABLED=1
  RECONCILE_LOOP_INTERVAL_MS=2000
  RECONCILE_LOOP_ENFORCE=0          shadow by default
  RECONCILE_LOOP_AUTO_CLOSE_NAKED=0 shadow by default
  RECONCILE_LOOP_NAKED_GRACE_MS=3000  grace window after ENTRY_FILLED
  RECONCILE_LOOP_QTY_MISMATCH_THRESH=0.05  5% relative tolerance
  RECONCILE_LOOP_SYMBOLS=           optional comma-separated allowlist
  PROMETHEUS_PORT=9942
  REDIS_URL=redis://redis-worker-1:6379/0
  BINANCE_API_KEY / BINANCE_API_SECRET
"""
from __future__ import annotations

import contextlib
import math
import os
import time
from typing import Any

try:
    import redis as _redis_mod
except ImportError:
    _redis_mod = None  # type: ignore[assignment]

try:
    from prometheus_client import Counter, Gauge, Histogram, start_http_server
    _prom_ok = True
except Exception:
    Counter = Gauge = Histogram = start_http_server = None  # type: ignore[assignment]
    _prom_ok = False


# ---------------------------------------------------------------------------
# Prometheus metrics
# ---------------------------------------------------------------------------

def _metric(factory, name, doc, labels=None, **kw):
    if factory is None:
        return None
    try:
        return factory(name, doc, labels or [], **kw)
    except ValueError:
        try:
            from prometheus_client import REGISTRY
            return REGISTRY._names_to_collectors.get(name)
        except Exception:
            return None


RECONCILE_MISMATCH_TOTAL = _metric(
    Counter,
    "reconcile_mismatch_total",
    "Position/order mismatches detected between local Redis state and Binance.",
    ["type", "symbol"],
)
RECONCILE_LOOP_DURATION_MS = _metric(
    Histogram,
    "reconcile_loop_duration_ms",
    "Wall-clock ms per reconcile loop iteration.",
    buckets=[10, 50, 100, 250, 500, 1000, 2000, 5000],
)
RECONCILE_NAKED_POSITION_AGE_MS = _metric(
    Gauge,
    "reconcile_naked_position_age_ms",
    "Age in ms of a detected naked position (no protection orders).",
    ["symbol"],
)
RECONCILE_ORPHAN_ORDERS_COUNT = _metric(
    Gauge,
    "reconcile_orphan_orders_count",
    "Number of open orders on exchange with no matching local position.",
    ["symbol"],
)
RECONCILE_LOOP_LAST_RUN_TS = _metric(
    Gauge,
    "reconcile_loop_last_run_ts",
    "Unix timestamp of last completed reconcile loop cycle.",
)
RECONCILE_AUTO_CLOSE_TOTAL = _metric(
    Counter,
    "reconcile_auto_close_total",
    "Emergency closes triggered by the reconcile loop.",
    ["symbol", "result"],
)


def _ms_now() -> int:
    return int(time.time() * 1000)


def _f(val: Any, default: float = 0.0) -> float:
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


# ---------------------------------------------------------------------------
# Core reconcile logic (testable, no async)
# ---------------------------------------------------------------------------

class PositionReconcileLoop:
    """Periodic reconcile: compares local Redis state vs Binance exchange.

    Designed to be sync and easy to unit-test with fakes.
    """

    def __init__(
        self,
        *,
        r: Any,
        binance_client: Any,
        flatten_service: Any = None,
        filters: Any = None,
        enforce: bool = False,
        auto_close_naked: bool = False,
        naked_grace_ms: int = 3000,
        qty_mismatch_thresh: float = 0.05,
        symbols_allowlist: set[str] | None = None,
        write_event_fn: Any = None,
    ) -> None:
        self.r = r
        self._client = binance_client
        self._flatten = flatten_service
        self._filters = filters
        self.enforce = enforce
        self.auto_close_naked = auto_close_naked
        self.naked_grace_ms = naked_grace_ms
        self.qty_mismatch_thresh = qty_mismatch_thresh
        self.symbols_allowlist = symbols_allowlist
        self._write_event = write_event_fn or (lambda fields: None)

    # ------------------------------------------------------------------
    # Data collection
    # ------------------------------------------------------------------

    def _fetch_exchange_positions(self) -> dict[str, dict[str, Any]]:
        """Return {symbol: position_risk_dict} for non-flat positions."""
        try:
            risks = self._client.get_position_risk() or []
        except Exception:
            return {}
        result: dict[str, dict[str, Any]] = {}
        for pos in risks:
            sym = str((pos or {}).get("symbol") or "").upper()
            if not sym:
                continue
            amt = _f(pos.get("positionAmt"), 0.0)
            if math.isclose(amt, 0.0, abs_tol=1e-9):
                continue
            if self.symbols_allowlist and sym not in self.symbols_allowlist:
                continue
            result[sym] = dict(pos)
        return result

    def _fetch_exchange_open_orders(self) -> dict[str, list[dict[str, Any]]]:
        """Return {symbol: [order_dict, ...]} for open orders."""
        try:
            orders = self._client.get_open_orders() or []
        except Exception:
            return {}
        result: dict[str, list[dict[str, Any]]] = {}
        for order in orders:
            sym = str((order or {}).get("symbol") or "").upper()
            if not sym:
                continue
            if self.symbols_allowlist and sym not in self.symbols_allowlist:
                continue
            result.setdefault(sym, []).append(dict(order))
        return result

    def _fetch_local_positions(self) -> dict[str, dict[str, Any]]:
        """Return {symbol: local_state_dict} from Redis orders:open."""
        try:
            cursor = 0
            local: dict[str, dict[str, Any]] = {}
            while True:
                cursor, batch = self.r.sscan("orders:open", cursor, count=1000)
                for oid in batch or []:
                    sid = oid.decode() if isinstance(oid, bytes) else str(oid)
                    h_raw = self.r.hgetall(f"order:{sid}") or {}
                    h = {
                        (k.decode() if isinstance(k, bytes) else k):
                        (v.decode() if isinstance(v, bytes) else v)
                        for k, v in h_raw.items()
                    }
                    if not h:
                        continue
                    sym = str(h.get("symbol") or "").upper()
                    if not sym:
                        continue
                    if self.symbols_allowlist and sym not in self.symbols_allowlist:
                        continue
                    if h.get("status") == "open":
                        local[sym] = h
                if cursor == 0:
                    break
        except Exception:
            return {}
        return local

    def _has_protection_orders(self, symbol: str, open_orders: list[dict]) -> bool:
        """Return True if there are any open protection (SL/TP) orders for symbol."""
        for order in open_orders:
            order_type = str(order.get("type") or order.get("origType") or "").upper()
            if order_type in {"STOP_MARKET", "TAKE_PROFIT_MARKET", "STOP", "TAKE_PROFIT",
                              "TRAILING_STOP_MARKET"}:
                return True
        return False

    def _is_in_grace_period(self, local_state: dict[str, Any]) -> bool:
        """Return True if position was just filled (within naked_grace_ms)."""
        filled_ts = local_state.get("fill_ts_ms") or local_state.get("filled_at_ms")
        if not filled_ts:
            return False
        try:
            age_ms = _ms_now() - int(filled_ts)
            return age_ms < self.naked_grace_ms
        except Exception:
            return False

    # ------------------------------------------------------------------
    # Mismatch detection
    # ------------------------------------------------------------------

    def _detect_mismatches(
        self,
        *,
        exchange_positions: dict[str, dict[str, Any]],
        exchange_orders: dict[str, list[dict[str, Any]]],
        local_positions: dict[str, dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """Return list of mismatch records."""
        mismatches: list[dict[str, Any]] = []
        now_ms = _ms_now()

        # 1. Exchange positions without local state / without protection
        for sym, ex_pos in exchange_positions.items():
            local = local_positions.get(sym)
            orders_for_sym = exchange_orders.get(sym, [])
            has_protection = self._has_protection_orders(sym, orders_for_sym)

            if local is None:
                # Exchange has position but we have no local state
                mismatches.append({
                    "type": "naked_position",
                    "symbol": sym,
                    "exchange_qty": abs(_f(ex_pos.get("positionAmt"))),
                    "local_qty": 0.0,
                    "has_protection": has_protection,
                    "detected_at_ms": now_ms,
                })
            else:
                # We have local state — check protection and qty
                if not has_protection and not self._is_in_grace_period(local):
                    mismatches.append({
                        "type": "naked_position",
                        "symbol": sym,
                        "exchange_qty": abs(_f(ex_pos.get("positionAmt"))),
                        "local_qty": _f(local.get("qty") or local.get("filled_qty")),
                        "has_protection": False,
                        "detected_at_ms": now_ms,
                    })

                # Qty mismatch check
                exchange_qty = abs(_f(ex_pos.get("positionAmt")))
                local_qty = _f(local.get("qty") or local.get("filled_qty"))
                if local_qty > 0 and exchange_qty > 0:
                    diff_pct = abs(exchange_qty - local_qty) / max(local_qty, 1e-12)
                    if diff_pct > self.qty_mismatch_thresh:
                        mismatches.append({
                            "type": "qty_mismatch",
                            "symbol": sym,
                            "exchange_qty": exchange_qty,
                            "local_qty": local_qty,
                            "diff_pct": round(diff_pct, 4),
                            "detected_at_ms": now_ms,
                        })

        # 2. Local positions with no exchange position (local_only)
        for sym, local in local_positions.items():
            if sym not in exchange_positions:
                mismatches.append({
                    "type": "local_only_position",
                    "symbol": sym,
                    "local_qty": _f(local.get("qty") or local.get("filled_qty")),
                    "detected_at_ms": now_ms,
                })

        # 3. Orphan orders: exchange has orders but no local position
        for sym, orders in exchange_orders.items():
            if sym not in local_positions and sym not in exchange_positions:
                mismatches.append({
                    "type": "orphan_order",
                    "symbol": sym,
                    "order_count": len(orders),
                    "detected_at_ms": now_ms,
                })

        return mismatches

    # ------------------------------------------------------------------
    # Action
    # ------------------------------------------------------------------

    def _handle_mismatch(self, mismatch: dict[str, Any]) -> None:
        sym = mismatch["symbol"]
        mtype = mismatch["type"]

        with contextlib.suppress(Exception):
            if RECONCILE_MISMATCH_TOTAL is not None:
                RECONCILE_MISMATCH_TOTAL.labels(type=mtype, symbol=sym).inc()

        self._write_event({
            "event_type": "RECONCILE_MISMATCH",
            "severity": "warning" if mtype != "naked_position" else "critical",
            **mismatch,
        })

        if mtype == "naked_position":
            with contextlib.suppress(Exception):
                if RECONCILE_NAKED_POSITION_AGE_MS is not None:
                    RECONCILE_NAKED_POSITION_AGE_MS.labels(symbol=sym).set(0)

            if self.enforce and self.auto_close_naked and self._flatten is not None:
                self._auto_close_naked(mismatch)

        elif mtype == "orphan_order":
            with contextlib.suppress(Exception):
                if RECONCILE_ORPHAN_ORDERS_COUNT is not None:
                    RECONCILE_ORPHAN_ORDERS_COUNT.labels(symbol=sym).set(
                        mismatch.get("order_count", 1)
                    )

    def _auto_close_naked(self, mismatch: dict[str, Any]) -> None:
        sym = mismatch["symbol"]
        ex_qty = mismatch.get("exchange_qty", 0.0)
        if ex_qty <= 0:
            return

        # Determine side from exchange position sign
        try:
            risks = self._client.get_position_risk(symbol=sym) or []
            for pos in risks:
                if str(pos.get("symbol") or "").upper() != sym:
                    continue
                amt = _f(pos.get("positionAmt"), 0.0)
                logical_side = "LONG" if amt > 0 else "SHORT"
                filters = self._filters
                result = self._flatten.force_flatten_exact(
                    sid=f"reconcile:{sym}:{_ms_now()}",
                    symbol=sym,
                    logical_side=logical_side,
                    client=self._client,
                    filters=filters,
                    reason="reconcile_naked_position",
                )
                ok = result.get("flatten_ok", False)
                with contextlib.suppress(Exception):
                    if RECONCILE_AUTO_CLOSE_TOTAL is not None:
                        RECONCILE_AUTO_CLOSE_TOTAL.labels(
                            symbol=sym, result="ok" if ok else "failed"
                        ).inc()
                self._write_event({
                    "event_type": "RECONCILE_AUTO_CLOSE",
                    "severity": "critical",
                    "symbol": sym,
                    "flatten_ok": ok,
                    "reason": "reconcile_naked_position",
                })
                return
        except Exception as exc:
            with contextlib.suppress(Exception):
                if RECONCILE_AUTO_CLOSE_TOTAL is not None:
                    RECONCILE_AUTO_CLOSE_TOTAL.labels(symbol=sym, result="error").inc()
            self._write_event({
                "event_type": "RECONCILE_AUTO_CLOSE_ERROR",
                "severity": "critical",
                "symbol": sym,
                "error": str(exc)[:200],
            })

    # ------------------------------------------------------------------
    # Single loop iteration
    # ------------------------------------------------------------------

    def run_once(self) -> list[dict[str, Any]]:
        """Run one reconcile cycle. Returns list of detected mismatches."""
        t0 = time.monotonic()
        try:
            exchange_positions = self._fetch_exchange_positions()
            exchange_orders = self._fetch_exchange_open_orders()
            local_positions = self._fetch_local_positions()

            mismatches = self._detect_mismatches(
                exchange_positions=exchange_positions,
                exchange_orders=exchange_orders,
                local_positions=local_positions,
            )

            for m in mismatches:
                self._handle_mismatch(m)

            return mismatches
        finally:
            elapsed_ms = (time.monotonic() - t0) * 1000
            with contextlib.suppress(Exception):
                if RECONCILE_LOOP_DURATION_MS is not None:
                    RECONCILE_LOOP_DURATION_MS.observe(elapsed_ms)
            with contextlib.suppress(Exception):
                if RECONCILE_LOOP_LAST_RUN_TS is not None:
                    RECONCILE_LOOP_LAST_RUN_TS.set(time.time())

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    def run_forever(self, interval_ms: int = 2000) -> None:
        while True:
            try:
                self.run_once()
            except KeyboardInterrupt:
                break
            except Exception:
                pass
            time.sleep(interval_ms / 1000.0)


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

def main() -> None:
    enabled = os.getenv("RECONCILE_LOOP_ENABLED", "1").strip() not in {"0", "false", "False"}
    if not enabled:
        import sys
        print("RECONCILE_LOOP_ENABLED=0, exiting", flush=True)
        sys.exit(0)

    interval_ms = int(os.getenv("RECONCILE_LOOP_INTERVAL_MS", "2000"))
    enforce = os.getenv("RECONCILE_LOOP_ENFORCE", "0").strip() not in {"0", "false", "False"}
    auto_close = os.getenv("RECONCILE_LOOP_AUTO_CLOSE_NAKED", "0").strip() not in {"0", "false", "False"}
    naked_grace_ms = int(os.getenv("RECONCILE_LOOP_NAKED_GRACE_MS", "3000"))
    qty_thresh = float(os.getenv("RECONCILE_LOOP_QTY_MISMATCH_THRESH", "0.05"))
    symbols_raw = os.getenv("RECONCILE_LOOP_SYMBOLS", "").strip()
    symbols_allowlist = {s.strip().upper() for s in symbols_raw.split(",") if s.strip()} or None

    # Prometheus
    prom_port = int(os.getenv("PROMETHEUS_PORT", "9942"))
    if _prom_ok and start_http_server:
        with contextlib.suppress(Exception):
            start_http_server(prom_port)

    # Redis
    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    if _redis_mod is None:
        raise RuntimeError("redis-py required")
    r = _redis_mod.from_url(redis_url, decode_responses=False)

    # Binance client
    from services.binance_futures_client import BinanceFuturesClient
    client = BinanceFuturesClient.from_env(prefix="BINANCE_")

    # Optional: flatten service for auto-close
    flatten_svc = None
    if auto_close:
        from services.execution.emergency_flatten_service import EmergencyFlattenService
        from services.execution.binance_filters import FiltersCache
        from services.execution_metrics import EXECUTION_EMERGENCY_FLATTEN_TOTAL  # noqa: F401
        filters = FiltersCache(client)
        flatten_svc = EmergencyFlattenService(
            position_mode=os.getenv("BINANCE_POSITION_MODE", "oneway"),
            write_event_fn=None,
        )
    else:
        filters = None

    # Exec stream writer for events
    exec_stream = os.getenv("EXEC_STREAM", "orders:exec")

    def _write_event(fields: dict) -> None:
        with contextlib.suppress(Exception):
            r.xadd(exec_stream, {k: str(v) for k, v in fields.items()}, maxlen=50_000, approximate=True)

    loop = PositionReconcileLoop(
        r=r,
        binance_client=client,
        flatten_service=flatten_svc,
        filters=filters,
        enforce=enforce,
        auto_close_naked=auto_close,
        naked_grace_ms=naked_grace_ms,
        qty_mismatch_thresh=qty_thresh,
        symbols_allowlist=symbols_allowlist,
        write_event_fn=_write_event,
    )

    print(
        f"[reconcile_loop] enforce={enforce} auto_close={auto_close} "
        f"interval={interval_ms}ms grace={naked_grace_ms}ms prom=:{prom_port}",
        flush=True,
    )
    loop.run_forever(interval_ms=interval_ms)


if __name__ == "__main__":
    main()
