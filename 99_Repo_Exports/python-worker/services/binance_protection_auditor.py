#!/usr/bin/env python3
from __future__ import annotations

from utils.time_utils import get_ny_time_millis

"""Independent Binance protection-audit loop.

P0 risk containment:
  * scans live positionRisk + openAlgoOrders
  * alerts on naked / partially protected positions
  * optionally flattens a position when operator enables flatten mode

The auditor is intentionally independent from the main executor.  It provides a
second control plane: even if the executor loses state or a reconcile path
misbehaves, the auditor still sees the exchange truth and can page / flatten.
"""

import json
import os
import time
from collections.abc import Iterable
from typing import Any

from core.redis_keys import RedisStreams as RS
import contextlib

try:
    import redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None  # type: ignore

try:
    from services.binance_futures_client import BinanceFuturesClient
except ImportError:  # pragma: no cover
    from binance_futures_client import BinanceFuturesClient  # type: ignore

try:
    from services.telegram.telegram_client import TelegramClient
except ImportError:  # pragma: no cover
    from telegram.telegram_client import TelegramClient  # type: ignore

try:
    from services.execution_metrics import (
        EXECUTION_PROTECTION_AUDIT_FINDING_TOTAL,
        EXECUTION_PROTECTION_AUDIT_FLATTEN_TOTAL,
        EXECUTION_PROTECTION_AUDIT_LAST_RUN_TS,
        EXECUTION_PROTECTION_AUDIT_OPEN_FINDINGS,
    )
except ImportError:  # pragma: no cover
    try:
        from execution_metrics import (
            EXECUTION_PROTECTION_AUDIT_FINDING_TOTAL,
            EXECUTION_PROTECTION_AUDIT_FLATTEN_TOTAL,
            EXECUTION_PROTECTION_AUDIT_LAST_RUN_TS,
            EXECUTION_PROTECTION_AUDIT_OPEN_FINDINGS,
        )
    except ImportError:  # pragma: no cover
        EXECUTION_PROTECTION_AUDIT_FINDING_TOTAL = None  # type: ignore
        EXECUTION_PROTECTION_AUDIT_FLATTEN_TOTAL = None  # type: ignore
        EXECUTION_PROTECTION_AUDIT_LAST_RUN_TS = None  # type: ignore
        EXECUTION_PROTECTION_AUDIT_OPEN_FINDINGS = None  # type: ignore


def _bool_env(name: str, default: bool = False) -> bool:
    v = (os.getenv(name) or "").strip().lower()
    if not v:
        return bool(default)
    return v in {"1", "true", "yes", "on"}


def _ms_now() -> int:
    return get_ny_time_millis()


def _f(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


class BinanceProtectionAuditor:
    def __init__(
        self,
        *,
        redis_client: Any | None = None,
        prod_client: BinanceFuturesClient | None = None,
        demo_client: BinanceFuturesClient | None = None,
        telegram_client: Any | None = None,
    ) -> None:
        if redis_client is None and redis is None:
            raise RuntimeError("redis-py is required (pip install redis)")
        self.redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
        self.r = redis_client if redis_client is not None else redis.from_url(self.redis_url, decode_responses=True)  # type: ignore
        self.exec_stream = os.getenv("EXEC_STREAM", RS.ORDERS_EXEC)
        _maxlen = int(os.getenv("EXEC_STREAM_MAXLEN", "0") or "0")
        self.exec_stream_maxlen: int | None = _maxlen if _maxlen > 0 else None
        self.mode = (os.getenv("BINANCE_PROTECTION_AUDITOR_MODE") or "alert").strip().lower()
        if self.mode not in {"alert", "flatten"}:
            self.mode = "alert"
        self.interval_ms = int(os.getenv("BINANCE_PROTECTION_AUDITOR_INTERVAL_MS", "5000"))
        self.alert_dedupe_sec = int(os.getenv("BINANCE_PROTECTION_AUDITOR_ALERT_DEDUPE_SEC", "60"))
        self.orphan_cancel_enable = _bool_env("BINANCE_PROTECTION_AUDITOR_CANCEL_ORPHAN_ALGOS", False)
        self.position_eps = float(os.getenv("BINANCE_PROTECTION_AUDITOR_POSITION_EPS", "1e-12"))
        # When true (default), also scan plain openOrders (STOP/TAKE_PROFIT not via Algo Service)
        self.scan_plain_orders = _bool_env("BINANCE_PROTECTION_AUDITOR_SCAN_PLAIN_ORDERS", True)
        self.tg = telegram_client if telegram_client is not None else TelegramClient.from_env()

        self.prod_client = prod_client
        if self.prod_client is None and (os.getenv("BINANCE_API_KEY") or "").strip():
            self.prod_client = BinanceFuturesClient.from_env(prefix="BINANCE_")
        self.demo_client = demo_client
        if self.demo_client is None and (os.getenv("BINANCE_DEMO_API_KEY") or "").strip():
            self.demo_client = BinanceFuturesClient.from_env(prefix="BINANCE_DEMO_")
        if self.prod_client is None and self.demo_client is None:
            raise RuntimeError("At least one of BINANCE_API_KEY or BINANCE_DEMO_API_KEY must be set")

    # ── Client enumeration ────────────────────────────────────────────────
    def _iter_clients(self) -> Iterable[tuple[str, BinanceFuturesClient]]:
        if self.prod_client is not None:
            yield "prod", self.prod_client
        if self.demo_client is not None:
            yield "demo", self.demo_client

    # ── Algo order classification ─────────────────────────────────────────
    def _algo_kind(self, order: dict[str, Any]) -> str:
        """Classify an order (algo or plain) as sl/tp/trail/other based on type and client IDs."""
        typ = str(order.get("type") or order.get("algoType") or "").upper()
        # Check both clientAlgoId (algo orders) and clientOrderId (plain orders)
        cid = str(order.get("clientAlgoId") or order.get("clientOrderId") or "").lower()
        if typ == "TRAILING_STOP_MARKET" or cid.endswith("-trail"):
            return "trail"
        if typ in {"STOP_MARKET", "STOP"} or cid.endswith("-sl"):
            return "sl"
        if typ in {"TAKE_PROFIT_MARKET", "TAKE_PROFIT"} or "-tp" in cid:
            return "tp"
        return "other"

    def _token_from_cid(self, client_algo_id: str) -> str:
        cid = (client_algo_id or "").strip()
        if not cid:
            return ""
        parts = cid.split("-")
        if len(parts) < 3:
            return ""
        return parts[-2]

    # ── Exchange state readers ────────────────────────────────────────────
    def _positions_by_symbol(self, client: BinanceFuturesClient) -> dict[str, dict[str, Any]]:
        out: dict[str, dict[str, Any]] = {}
        for row in list(client.get_position_risk() or []):
            symbol = (row.get("symbol") or "").strip().upper()
            if not symbol:
                continue
            amt = _f(row.get("positionAmt"), 0.0)
            if abs(amt) <= self.position_eps:
                continue
            current = out.get(symbol) or {"qty": 0.0, "amt": 0.0}
            current["qty"] = max(float(current.get("qty") or 0.0), abs(float(amt)))
            current["amt"] = float(amt)
            current["logical_side"] = "LONG" if float(amt) > 0 else "SHORT"
            out[symbol] = current
        return out

    def _algos_by_symbol(self, client: BinanceFuturesClient) -> dict[str, dict[str, Any]]:
        """Aggregate protection orders per symbol from both algo and plain order APIs.

        Algo orders (POST /fapi/v1/algoOrder) → get_open_algo_orders()
        Plain orders (POST /fapi/v1/order)    → get_open_orders()  [if scan_plain_orders=True]

        Some executor paths (e.g., `post_plain_order`) place STOP/TAKE_PROFIT
        as plain orders, not algo orders. Without the plain-order scan, the
        auditor would falsely report those positions as unprotected.
        """
        out: dict[str, dict[str, Any]] = {}

        # 1. Algo orders (primary protection path)
        for row in list(client.get_open_algo_orders() or []):
            symbol = (row.get("symbol") or "").strip().upper()
            if not symbol:
                continue
            entry = out.setdefault(symbol, {"sl": 0, "tp": 0, "trail": 0, "other": 0, "orders": []})
            kind = self._algo_kind(row)
            entry[kind] = int(entry.get(kind, 0)) + 1
            entry["orders"].append(dict(row))

        # 2. Plain open orders — scan for STOP/TAKE_PROFIT types placed via
        #    post_plain_order (not routed through the Algo Service).
        if self.scan_plain_orders:
            _PLAIN_PROTECTION_TYPES = {
                "STOP_MARKET", "STOP",
                "TAKE_PROFIT_MARKET", "TAKE_PROFIT",
                "TRAILING_STOP_MARKET",
            }
            try:
                plain_orders = list(client.get_open_orders() or [])
            except Exception:
                plain_orders = []
            for row in plain_orders:
                symbol = (row.get("symbol") or "").strip().upper()
                if not symbol:
                    continue
                typ = (row.get("type") or "").upper()
                if typ not in _PLAIN_PROTECTION_TYPES:
                    # Skip plain MARKET/LIMIT entry orders — they are not protection
                    continue
                entry = out.setdefault(symbol, {"sl": 0, "tp": 0, "trail": 0, "other": 0, "orders": []})
                kind = self._algo_kind(row)
                entry[kind] = int(entry.get(kind, 0)) + 1
                entry["orders"].append(dict(row))

        return out

    # ── Event / notification helpers ──────────────────────────────────────
    def _emit_event(self, event: dict[str, Any]) -> None:
        try:
            fields = {k: json.dumps(v, ensure_ascii=False, default=str) if isinstance(v, (dict, list)) else str(v) for k, v in dict(event or {}).items()}
            kwargs: dict[str, Any] = {}
            if self.exec_stream_maxlen:
                kwargs = {"maxlen": self.exec_stream_maxlen, "approximate": True}
            self.r.xadd(self.exec_stream, fields, **kwargs, maxlen=50000)
        except Exception:
            return

    def _dedupe_key(self, venue: str, symbol: str, finding: str) -> str:
        return f"orders:protection_audit:dedupe:{venue}:{symbol}:{finding}"

    def _notify_once(self, *, venue: str, symbol: str, finding: str, text: str) -> None:
        if self.tg is None:
            return
        key = self._dedupe_key(venue, symbol, finding)
        try:
            if self.r.get(key):
                return
            self.r.set(key, "1", ex=max(1, self.alert_dedupe_sec))
        except Exception:
            pass
        try:
            self.tg.send_text(text)
        except Exception:
            return

    # ── Remediation actions ───────────────────────────────────────────────
    def _cancel_orphan_algos(self, *, client: BinanceFuturesClient, symbol: str, orders: list[dict[str, Any]]) -> int:
        canceled = 0
        for order in list(orders or []):
            try:
                algo_id = int(order.get("algoId"))  # type: ignore
            except Exception:
                continue
            try:
                client.cancel_algo_order(symbol, algo_id=algo_id)
                canceled += 1
            except Exception:
                continue
        return canceled

    def _flatten_position(self, *, client: BinanceFuturesClient, symbol: str, logical_side: str, qty: float, finding: str, venue: str) -> dict[str, Any]:
        close_side = "SELL" if str(logical_side).upper() == "LONG" else "BUY"
        with contextlib.suppress(Exception):
            client.cancel_all_orders(symbol)
        order = client.post_plain_order({
            "symbol": symbol,
            "side": close_side,
            "type": "MARKET",
            "quantity": str(qty),
            "reduceOnly": True,
            "newClientOrderId": f"audit-{venue}-{symbol.lower()}-{_ms_now()}",
        })
        try:
            if EXECUTION_PROTECTION_AUDIT_FLATTEN_TOTAL is not None:
                EXECUTION_PROTECTION_AUDIT_FLATTEN_TOTAL.labels(venue=venue, finding=finding).inc()
        except Exception:
            pass
        return {
            "flatten_order_id": order.get("orderId"),
            "flatten_client_order_id": order.get("clientOrderId") or order.get("newClientOrderId"),
        }

    # ── Finding event builder ─────────────────────────────────────────────
    def _finding_event(self, *, venue: str, symbol: str, finding: str, details: dict[str, Any]) -> dict[str, Any]:
        return {
            "sid": "",
            "symbol": symbol,
            "action": "protection_audit",
            "event_type": "protection_audit_finding",
            "venue": venue,
            "status": "warning",
            "finding": finding,
            "mode": self.mode,
            "details": details,
            "ts_event_ms": _ms_now(),
        }

    # ── Core scan logic ───────────────────────────────────────────────────
    def scan_once(self) -> list[dict[str, Any]]:
        findings: list[dict[str, Any]] = []
        for venue, client in self._iter_clients():
            positions = self._positions_by_symbol(client)
            algos = self._algos_by_symbol(client)
            symbols = sorted(set(positions.keys()) | set(algos.keys()))
            for symbol in symbols:
                pos = positions.get(symbol) or {}
                algo = algos.get(symbol) or {"sl": 0, "tp": 0, "trail": 0, "other": 0, "orders": []}
                pos_qty = float(pos.get("qty") or 0.0)
                total_protection = int(algo.get("sl", 0)) + int(algo.get("tp", 0)) + int(algo.get("trail", 0))
                if pos_qty > self.position_eps and int(algo.get("sl", 0)) == 0:
                    findings.append(self._finding_event(venue=venue, symbol=symbol, finding="position_without_sl", details={"position_qty": pos_qty, "algo_summary": algo}))
                if pos_qty > self.position_eps and int(algo.get("tp", 0)) == 0:
                    findings.append(self._finding_event(venue=venue, symbol=symbol, finding="position_without_any_tp", details={"position_qty": pos_qty, "algo_summary": algo}))
                if pos_qty > self.position_eps and total_protection == 0:
                    findings.append(self._finding_event(venue=venue, symbol=symbol, finding="position_without_any_protection", details={"position_qty": pos_qty, "algo_summary": algo}))
                if pos_qty <= self.position_eps and list(algo.get("orders") or []):
                    findings.append(self._finding_event(venue=venue, symbol=symbol, finding="orphan_algo_without_position", details={"algo_summary": algo}))
        return findings

    # ── Finding execution (alert / flatten / orphan cancel) ───────────────
    def execute_findings(self, findings: list[dict[str, Any]]) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for finding in list(findings or []):
            venue = (finding.get("venue") or "")
            symbol = (finding.get("symbol") or "")
            finding_name = (finding.get("finding") or "")
            details = dict(finding.get("details") or {})
            client = self.prod_client if venue == "prod" else self.demo_client
            if client is None:
                continue
            # Prometheus metrics
            try:
                if EXECUTION_PROTECTION_AUDIT_FINDING_TOTAL is not None:
                    EXECUTION_PROTECTION_AUDIT_FINDING_TOTAL.labels(venue=venue, finding=finding_name, mode=self.mode).inc()
                if EXECUTION_PROTECTION_AUDIT_OPEN_FINDINGS is not None:
                    EXECUTION_PROTECTION_AUDIT_OPEN_FINDINGS.labels(venue=venue, symbol=symbol, finding=finding_name).set(1.0)
            except Exception:
                pass
            event = dict(finding)
            # Flatten mode: emergency close for dangerous findings
            if self.mode == "flatten" and finding_name in {"position_without_sl", "position_without_any_protection"}:
                position_qty = _f(details.get("position_qty"), 0.0)
                algo_summary = details.get("algo_summary") or {}
                if position_qty > self.position_eps:
                    logical_side = "LONG"
                    try:
                        # Re-read positions to avoid flattening stale state from a previous scan.
                        positions = self._positions_by_symbol(client)
                        pos = positions.get(symbol) or {}
                        position_qty = _f(pos.get("qty"), position_qty)
                        logical_side = str(pos.get("logical_side") or logical_side)
                    except Exception:
                        pass
                    flatten_info = self._flatten_position(client=client, symbol=symbol, logical_side=logical_side, qty=position_qty, finding=finding_name, venue=venue)
                    event["status"] = "flattened"
                    event["flatten"] = flatten_info
            elif self.mode == "flatten" and finding_name == "orphan_algo_without_position" and self.orphan_cancel_enable:
                canceled = self._cancel_orphan_algos(client=client, symbol=symbol, orders=list((details.get("algo_summary") or {}).get("orders") or []))
                event["status"] = "orphan_algos_canceled" if canceled else event.get("status")
                event["canceled_orphan_algos"] = canceled
            self._emit_event(event)
            self._notify_once(venue=venue, symbol=symbol, finding=finding_name, text=(
                f"🛑 Binance protection audit\nvenue={venue} symbol={symbol}\nfinding={finding_name} mode={self.mode}\nstatus={event.get('status')}"
            ))
            out.append(event)
        # Clear gauges for symbols that were not found in this scan is intentionally omitted;
        # Prometheus staleness handles disappeared series and the counter remains authoritative.
        for venue, _ in self._iter_clients():
            try:
                if EXECUTION_PROTECTION_AUDIT_LAST_RUN_TS is not None:
                    EXECUTION_PROTECTION_AUDIT_LAST_RUN_TS.labels(venue=venue).set(time.time())
            except Exception:
                pass
        return out

    def run_once(self) -> list[dict[str, Any]]:
        findings = self.scan_once()
        return self.execute_findings(findings)

    def run_forever(self) -> None:
        print("🚨 BinanceProtectionAuditor starting")
        print(f"   exec_stream={self.exec_stream}")
        print(f"   mode={self.mode}")
        while True:
            self.run_once()
            time.sleep(max(0.25, float(self.interval_ms) / 1000.0))


if __name__ == "__main__":  # pragma: no cover
    BinanceProtectionAuditor().run_forever()
