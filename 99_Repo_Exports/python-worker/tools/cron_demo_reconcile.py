#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations
"""
tools/cron_demo_reconcile.py

Reconciliation report: Project records (orders:exec + orders:state:*)
vs. Binance testnet account reality (REST API).

Shows: open positions diff, fill price slippage, PnL estimated by project
vs. unrealised PnL on testnet, orphaned/missing entries.
Also: closed-trades PnL comparison (project SQL pnl_net vs testnet REALIZED_PNL income)
with per-symbol breakdown: count, win-rate, Δ PnL.

Usage:
    cd python-worker
    BINANCE_DEMO_API_KEY=xxx \\
    BINANCE_DEMO_API_SECRET=xxx \\
    BINANCE_DEMO_FUTURES_BASE_URL=https://testnet.binancefuture.com \\
    REDIS_URL=redis://localhost:6379/0 \\
    TRADES_DB_DSN=postgresql://user:pass@host:5432/scanner_analytics \\
    python -m tools.cron_demo_reconcile

ENV:
    BINANCE_DEMO_API_KEY            Testnet API key         (required)
    BINANCE_DEMO_API_SECRET         Testnet API secret      (required)
    BINANCE_DEMO_FUTURES_BASE_URL   Testnet base URL        (https://testnet.binancefuture.com)
    REDIS_URL                       Redis URL               (redis://localhost:6379/0)
    EXEC_STREAM                     Source exec stream      (orders:exec)
    DEMO_RECONCILE_SINCE_HOURS      Lookback hours          (24)
    TRADES_DB_DSN                   PostgreSQL DSN for closed-trades PnL comparison (optional)
    TELEGRAM_MODE                   redis | direct          (redis)
    TELEGRAM_NOTIFY_STREAM          Redis Telegram stream   (notify:telegram)
    TELEGRAM_BOT_TOKEN              For direct mode
    TELEGRAM_CHAT_ID                For direct mode
"""
from utils.time_utils import get_ny_time_millis

import argparse
import html
import json
import os
import sys
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now_ms() -> int:
    return get_ny_time_millis()


def _f(x: Any, d: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return d


def _i(x: Any, d: int = 0) -> int:
    try:
        return int(float(x))
    except Exception:
        return d


def _truthy(v: Any) -> bool:
    return str(v).strip().lower() in {"1", "true", "yes", "on"}


def _envs(name: str, default: str = "") -> str:
    v = os.getenv(name)
    return default if v is None else str(v)


def _envi(name: str, default: int = 0) -> int:
    try:
        return int(os.getenv(name, default) or default)
    except Exception:
        return int(default)


def _envf(name: str, default: float = 0.0) -> float:
    try:
        return float(os.getenv(name, default) or default)
    except Exception:
        return float(default)


# ---------------------------------------------------------------------------
# Project side: read demo orders from orders:exec + orders:state:*
# ---------------------------------------------------------------------------

@dataclass
class ProjectOrder:
    sid: str
    symbol: str
    side: str
    exec_price: float
    qty: float
    ts_ms: int
    status: str      # from orders:state:{sid}
    pnl_est: float   # from trades:closed or state


def read_exec_stream(r: Any, stream: str, since_ms: int, max_scan: int = 200_000) -> List[Dict[str, str]]:
    """Read demo open events from orders:exec."""
    rows: List[Dict[str, str]] = []
    last_id = "+"
    scanned = 0

    while scanned < max_scan:
        batch = r.xrevrange(stream, max=last_id, min="-", count=2000)
        if not batch:
            break

        stuck = True
        for msg_id, raw in batch:
            if msg_id == last_id and last_id != "+":
                continue
            stuck = False
            scanned += 1
            last_id = msg_id

            fields: Dict[str, str] = {}
            for k, v in (raw or {}).items():
                k2 = k.decode("utf-8") if isinstance(k, bytes) else k
                v2 = v.decode("utf-8") if isinstance(v, bytes) else v
                fields[k2] = v2

            try:
                stream_ts = int(str(msg_id).split("-")[0])
            except Exception:
                stream_ts = 0

            ts = _i(fields.get("ts_ms"), 0) or stream_ts
            if ts and ts < since_ms:
                scanned = max_scan
                break

            # Only virtual open events
            is_virt = _truthy(fields.get("is_virtual")) or str(fields.get("venue", "")).lower() == "binance_demo"
            action = str(fields.get("action", "")).lower()
            if not is_virt:
                continue
            if action not in ("open", ""):
                continue
            if not fields.get("symbol"):
                continue

            fields["_stream_ts_ms"] = str(stream_ts)
            rows.append(fields)

        if stuck:
            break

    rows.sort(key=lambda x: _i(x.get("ts_ms") or x.get("_stream_ts_ms"), 0))
    return rows


def read_order_state(r: Any, sid: str) -> Dict[str, Any]:
    """Read orders:state:{sid} hash."""
    try:
        raw = r.hgetall(f"orders:state:{sid}")
        if not raw:
            return {}
        result: Dict[str, Any] = {}
        for k, v in raw.items():
            k2 = k.decode("utf-8") if isinstance(k, bytes) else k
            v2 = v.decode("utf-8") if isinstance(v, bytes) else v
            result[k2] = v2
        return result
    except Exception:
        return {}


# ---------------------------------------------------------------------------
# Binance testnet side
# ---------------------------------------------------------------------------

def _signed_get(base_url: str, path: str, api_key: str, api_secret: str,
                params: Optional[Dict[str, Any]] = None, timeout: float = 8.0) -> Any:
    """Minimal signed GET to Binance testnet — stdlib only."""
    import hashlib
    import hmac
    import urllib.parse
    import urllib.request

    p: Dict[str, Any] = dict(params or {})
    p["timestamp"] = _now_ms()
    p["recvWindow"] = 5000

    qs = urllib.parse.urlencode(sorted(p.items()))
    sig = hmac.new(api_secret.encode(), qs.encode(), hashlib.sha256).hexdigest()
    qs = qs + "&signature=" + sig

    url = base_url.rstrip("/") + path + "?" + qs
    req = urllib.request.Request(url, method="GET")
    req.add_header("X-MBX-APIKEY", api_key)

    import urllib.error
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        body = ""
        try:
            body = e.read().decode("utf-8", errors="replace")
        except Exception:
            pass
        raise RuntimeError(f"Binance HTTP {e.code}: {body[:400]}")


@dataclass
class TestnetAccount:
    total_wallet_balance: float
    total_unrealized_profit: float
    positions: List[Dict[str, Any]]       # all non-zero positions
    open_orders: List[Dict[str, Any]]     # all open orders


def fetch_testnet_account(base_url: str, api_key: str, api_secret: str) -> TestnetAccount:
    acc = _signed_get(base_url, "/fapi/v2/account", api_key, api_secret)
    pos_risk = _signed_get(base_url, "/fapi/v2/positionRisk", api_key, api_secret)
    open_orders = _signed_get(base_url, "/fapi/v1/openOrders", api_key, api_secret)

    wallet = _f(acc.get("totalWalletBalance"), 0.0)
    unrealised = _f(acc.get("totalUnrealizedProfit"), 0.0)

    open_pos = []
    for p in (pos_risk or []):
        amt = _f(p.get("positionAmt"), 0.0)
        if amt != 0.0:
            open_pos.append({
                "symbol": str(p.get("symbol", "")),
                "positionAmt": amt,
                "entryPrice": _f(p.get("entryPrice"), 0.0),
                "markPrice": _f(p.get("markPrice"), 0.0),
                "unrealizedProfit": _f(p.get("unrealizedProfit"), 0.0),
                "positionSide": str(p.get("positionSide", "BOTH")),
            })

    return TestnetAccount(
        total_wallet_balance=wallet,
        total_unrealized_profit=unrealised,
        positions=open_pos,
        open_orders=list(open_orders or []),
    )


def fetch_testnet_income(base_url: str, api_key: str, api_secret: str,
                         since_ms: int) -> List[Dict[str, Any]]:
    """Fetch REALIZED_PNL income events from testnet."""
    try:
        rows = _signed_get(base_url, "/fapi/v1/income", api_key, api_secret, {
            "incomeType": "REALIZED_PNL",
            "startTime": since_ms,
            "limit": 1000,
        })
        return list(rows or [])
    except Exception:
        return []


# ---------------------------------------------------------------------------
# Closed-trades PnL comparison: SQL (project) vs testnet income events
# ---------------------------------------------------------------------------

@dataclass
class SymbolPnlRow:
    """Per-symbol comparison of closed-trade PnL."""
    symbol: str
    proj_trades: int          # closed virtual trades in window (SQL)
    proj_wins: int            # trades with pnl_net > 0
    proj_pnl_net: float       # SUM(pnl_net) from SQL
    proj_fees: float          # SUM(fees) from SQL
    tn_pnl: float             # SUM(income) from testnet REALIZED_PNL events
    delta_pnl: float          # proj_pnl_net - tn_pnl
    delta_pct: float          # |delta_pnl / tn_pnl| * 100


@dataclass
class ClosedPnlSummary:
    """Aggregated result of closed-trades PnL comparison."""
    rows: List[SymbolPnlRow]          # per-symbol
    proj_total_pnl: float             # sum of proj_pnl_net
    tn_total_pnl: float               # sum of testnet income
    delta_total: float                # proj - tn
    proj_total_trades: int            # total closed trades
    proj_total_wins: int              # total winning trades

    @property
    def proj_win_rate_pct(self) -> float:
        if self.proj_total_trades == 0:
            return 0.0
        return self.proj_total_wins / self.proj_total_trades * 100.0

    @property
    def delta_total_pct(self) -> float:
        if abs(self.tn_total_pnl) < 1e-8:
            return 0.0
        return abs(self.delta_total / self.tn_total_pnl) * 100.0


def read_closed_trades_sql(
    dsn: str,
    since_ms: int,
) -> List[Dict[str, Any]]:
    """
    Query closed virtual trades from SQL, grouped by symbol.

    Returns list of dicts with keys:
        symbol, n_trades, wins, pnl_net_sum, fees_sum

    Graceful: returns [] on any error (DB unavailable, table not found, etc.).
    """
    if not dsn:
        return []
    try:
        import psycopg2
        from psycopg2.extras import RealDictCursor
        conn = psycopg2.connect(dsn, connect_timeout=5)
        conn.autocommit = True
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                # trades_closed — основная таблица закрытых сделок (analytics_db.save_trade_closed)
                # NOTE: ранее здесь было FROM trades (неверно) — сделки пишутся в trades_closed
                cur.execute(
                    """
                    SELECT
                        symbol,
                        COUNT(*)                                       AS n_trades,
                        SUM(CASE WHEN pnl_net > 0 THEN 1 ELSE 0 END)  AS wins,
                        COALESCE(SUM(pnl_net), 0.0)                    AS pnl_net_sum,
                        COALESCE(SUM(fees), 0.0)                       AS fees_sum
                    FROM trades_closed
                    WHERE is_virtual = TRUE
                      AND exit_ts_ms >= %(since_ms)s
                    GROUP BY symbol
                    ORDER BY symbol
                    """,
                    {"since_ms": since_ms},
                )
                rows = cur.fetchall()
                return [dict(r) for r in (rows or [])]
        finally:
            conn.close()
    except Exception as exc:  # noqa: BLE001
        print(f"[reconcile] WARN read_closed_trades_sql failed: {exc}")
        return []


def compare_closed_pnl(
    sql_rows: List[Dict[str, Any]],          # from read_closed_trades_sql
    income_rows: List[Dict[str, Any]],       # from fetch_testnet_income
) -> ClosedPnlSummary:
    """
    Compare project closed-trade PnL (SQL) vs Binance testnet income events.

    sql_rows keys:    symbol, n_trades, wins, pnl_net_sum, fees_sum
    income_rows keys: symbol, income (str), incomeType
    """
    # --- Aggregate testnet income per symbol ---
    tn_by_sym: Dict[str, float] = {}
    for row in income_rows:
        sym = str(row.get("symbol", "") or "")
        inc = _f(row.get("income"), 0.0)
        tn_by_sym[sym] = tn_by_sym.get(sym, 0.0) + inc

    # --- Aggregate project SQL per symbol ---
    sql_by_sym: Dict[str, Dict[str, Any]] = {}
    for row in sql_rows:
        sym = str(row.get("symbol", "") or "")
        sql_by_sym[sym] = row

    all_symbols = sorted(set(sql_by_sym) | set(tn_by_sym))

    result_rows: List[SymbolPnlRow] = []
    for sym in all_symbols:
        sq = sql_by_sym.get(sym)
        tn_pnl = tn_by_sym.get(sym, 0.0)
        proj_trades  = _i(sq["n_trades"],   0)   if sq else 0
        proj_wins    = _i(sq["wins"],        0)   if sq else 0
        proj_pnl_net = _f(sq["pnl_net_sum"], 0.0) if sq else 0.0
        proj_fees    = _f(sq["fees_sum"],    0.0) if sq else 0.0
        delta = proj_pnl_net - tn_pnl
        delta_pct = abs(delta / tn_pnl * 100.0) if abs(tn_pnl) > 1e-8 else 0.0
        result_rows.append(SymbolPnlRow(
            symbol=sym,
            proj_trades=proj_trades,
            proj_wins=proj_wins,
            proj_pnl_net=proj_pnl_net,
            proj_fees=proj_fees,
            tn_pnl=tn_pnl,
            delta_pnl=delta,
            delta_pct=delta_pct,
        ))

    proj_total_pnl    = sum(r.proj_pnl_net   for r in result_rows)
    tn_total_pnl      = sum(r.tn_pnl         for r in result_rows)
    delta_total       = proj_total_pnl - tn_total_pnl
    proj_total_trades = sum(r.proj_trades     for r in result_rows)
    proj_total_wins   = sum(r.proj_wins       for r in result_rows)

    return ClosedPnlSummary(
        rows=result_rows,
        proj_total_pnl=proj_total_pnl,
        tn_total_pnl=tn_total_pnl,
        delta_total=delta_total,
        proj_total_trades=proj_total_trades,
        proj_total_wins=proj_total_wins,
    )


# ---------------------------------------------------------------------------
# SL/TP coverage: check that open positions have protective orders
# ---------------------------------------------------------------------------

@dataclass
class _SymbolCoverage:
    """SL/TP/trailing protection status for one open position."""
    has_sl: bool = False
    has_tp: bool = False
    has_trailing: bool = False
    sl_price: float = 0.0
    tp_price: float = 0.0
    trailing_delta: float = 0.0   # priceRate %
    sl_type: str = ""
    tp_type: str = ""

    @property
    def is_protected(self) -> bool:
        """Position is protected if it has SL AND (TP or trailing)."""
        return self.has_sl and (self.has_tp or self.has_trailing)


_SL_TYPES = {"STOP_MARKET", "STOP"}
_TP_TYPES = {"TAKE_PROFIT_MARKET", "TAKE_PROFIT"}
_TRAILING_TYPES = {"TRAILING_STOP_MARKET"}


def classify_sl_tp_coverage(
    positions: List[Dict[str, Any]],
    open_orders: List[Dict[str, Any]],
) -> Dict[str, _SymbolCoverage]:
    """
    For each open position, determine which protective order types are present.

    Returns: {symbol: _SymbolCoverage}
    Only symbols that have an open position are keyed.
    Orders for symbols without a position are ignored.
    """
    pos_syms = {str(p.get("symbol", "")) for p in positions}
    coverage: Dict[str, _SymbolCoverage] = {sym: _SymbolCoverage() for sym in pos_syms if sym}

    for o in open_orders:
        sym = str(o.get("symbol", "") or "")
        if sym not in coverage:
            continue
        otype = str(o.get("type", "") or "").upper()
        sc = coverage[sym]

        if otype in _SL_TYPES:
            sc.has_sl = True
            sc.sl_price = _f(o.get("stopPrice"), 0.0)
            sc.sl_type = otype
        elif otype in _TP_TYPES:
            sc.has_tp = True
            sc.tp_price = _f(o.get("stopPrice"), 0.0)
            sc.tp_type = otype
        elif otype in _TRAILING_TYPES:
            sc.has_trailing = True
            sc.trailing_delta = _f(o.get("priceRate"), 0.0)

    return coverage


# ---------------------------------------------------------------------------
# Reconciliation logic
# ---------------------------------------------------------------------------

@dataclass
class ReconcileResult:
    # Counts
    project_orders_n: int
    project_unique_symbols: int

    # Project side aggregates
    project_exec_price_avg: float
    project_qty_total: float

    # Testnet side
    testnet_wallet_balance: float
    testnet_unrealized_pnl: float
    testnet_open_positions: List[Dict[str, Any]]
    testnet_open_orders_n: int
    testnet_realized_pnl: float

    # Diff
    position_diffs: List[str]       # human-readable diff lines
    slippage_lines: List[str]       # per-symbol slippage
    orphaned_positions: List[str]   # on testnet but not in project exec stream
    missing_positions: List[str]    # in project stream but not on testnet

    # SL/TP coverage: per-position protection status
    sl_tp_coverage_lines: List[str] = field(default_factory=list)
    unprotected_count: int = 0

    # Closed-trades PnL comparison (None if DB unavailable or no trades)
    closed_pnl: Optional[ClosedPnlSummary] = None


def reconcile(
    project_rows: List[Dict[str, str]],
    account: TestnetAccount,
    income_rows: List[Dict[str, Any]],
    *,
    sql_trade_rows: Optional[List[Dict[str, Any]]] = None,
) -> ReconcileResult:
    # --- Project aggregates ---
    symbols_in_proj = {r.get("symbol", "?") for r in project_rows}
    qty_total = sum(_f(r.get("qty"), 0.0) for r in project_rows)
    prices = [_f(r.get("exec_price"), 0.0) for r in project_rows if _f(r.get("exec_price"), 0.0) > 0]
    exec_price_avg = sum(prices) / len(prices) if prices else 0.0

    # --- Testnet realized PnL ---
    realized_pnl = sum(_f(r.get("income"), 0.0) for r in income_rows)

    # --- Position diff ---
    testnet_pos_map: Dict[str, Dict[str, Any]] = {p["symbol"]: p for p in account.positions}
    proj_sym_set = set(symbols_in_proj)
    testnet_sym_set = set(testnet_pos_map.keys())

    position_diffs: List[str] = []
    slippage_lines: List[str] = []

    # Symbols on both sides
    for sym in sorted(proj_sym_set & testnet_sym_set):
        tp = testnet_pos_map[sym]
        proj_sym_rows = [r for r in project_rows if r.get("symbol") == sym]
        proj_qty = sum(_f(r.get("qty"), 0.0) for r in proj_sym_rows)
        proj_avg = (
            sum(_f(r.get("exec_price"), 0.0) * _f(r.get("qty"), 0.0) for r in proj_sym_rows)
            / proj_qty if proj_qty > 0 else 0.0
        )
        tn_qty = abs(tp["positionAmt"])
        tn_entry = tp["entryPrice"]
        tn_mark = tp["markPrice"]
        tn_upnl = tp["unrealizedProfit"]
        slip = tn_entry - proj_avg if proj_avg > 0 else 0.0
        slip_bps = (slip / proj_avg * 10000) if proj_avg > 0 else 0.0

        diff_qty = tn_qty - proj_qty
        diff_str = f"{diff_qty:+.4f}" if abs(diff_qty) > 1e-8 else "✅ matched"

        position_diffs.append(
            f"<code>{html.escape(sym)}</code>: "
            f"proj_qty={proj_qty:.4f} tn_qty={tn_qty:.4f} Δ={diff_str} | "
            f"proj_avg={proj_avg:.2f} tn_entry={tn_entry:.2f} | "
            f"mark={tn_mark:.2f} uPnL={tn_upnl:+.2f}$"
        )
        if abs(slip) > 1e-4:
            slippage_lines.append(
                f"<code>{html.escape(sym)}</code>: slip={slip:+.4f} ({slip_bps:+.1f} bps)"
            )

    # Orphaned: on testnet but no project record
    orphaned_positions = [
        f"<code>{html.escape(sym)}</code>: amt={testnet_pos_map[sym]['positionAmt']:+.4f} entry={testnet_pos_map[sym]['entryPrice']:.2f}"
        for sym in sorted(testnet_sym_set - proj_sym_set)
    ]

    # Missing: in project exec stream but no testnet position
    missing_positions = [
        f"<code>{html.escape(sym)}</code>"
        for sym in sorted(proj_sym_set - testnet_sym_set)
    ]

    # --- Closed-trades PnL comparison ---
    closed_pnl: Optional[ClosedPnlSummary] = None
    if sql_trade_rows is not None:
        closed_pnl = compare_closed_pnl(sql_trade_rows, income_rows)
        if closed_pnl.proj_total_trades == 0 and abs(closed_pnl.tn_total_pnl) < 1e-8:
            closed_pnl = None  # nothing to show

    # --- SL/TP coverage ---
    sl_tp_coverage = classify_sl_tp_coverage(account.positions, account.open_orders)
    sl_tp_lines: List[str] = []
    unprotected = 0
    for sym, sc in sorted(sl_tp_coverage.items()):
        sl_icon  = "✅ SL" if sc.has_sl       else "❌ SL"
        tp_icon  = "✅ TP" if sc.has_tp       else ("✅ trail" if sc.has_trailing else "❌ TP")
        warn     = " ⚠️" if not sc.is_protected else ""
        if not sc.is_protected:
            unprotected += 1
        # find position for size
        pos_amt = next((p["positionAmt"] for p in account.positions
                        if p["symbol"] == sym), 0.0)
        sl_tp_lines.append(
            f"<code>{html.escape(sym)}</code>: amt={pos_amt:+.4f} | {sl_icon} | {tp_icon}{warn}"
        )

    return ReconcileResult(
        project_orders_n=len(project_rows),
        project_unique_symbols=len(proj_sym_set),
        project_exec_price_avg=exec_price_avg,
        project_qty_total=qty_total,
        testnet_wallet_balance=account.total_wallet_balance,
        testnet_unrealized_pnl=account.total_unrealized_profit,
        testnet_open_positions=account.positions,
        testnet_open_orders_n=len(account.open_orders),
        testnet_realized_pnl=realized_pnl,
        position_diffs=position_diffs,
        slippage_lines=slippage_lines,
        orphaned_positions=orphaned_positions,
        missing_positions=missing_positions,
        sl_tp_coverage_lines=sl_tp_lines,
        unprotected_count=unprotected,
        closed_pnl=closed_pnl,
    )


# ---------------------------------------------------------------------------
# Report formatting
# ---------------------------------------------------------------------------

def build_reconcile_text(result: ReconcileResult, *, since_hours: float, ts: str) -> str:
    lines: List[str] = []

    window_label = f"{int(since_hours)}h" if since_hours == int(since_hours) else f"{since_hours:.1f}h"

    lines.append(f"<b>🔍 Demo Reconcile Report</b>  ts=<code>{html.escape(ts)}</code>  window=<code>{window_label}</code>")
    lines.append("")

    # --- Project side ---
    lines.append("<b>📋 Project (orders:exec)</b>")
    lines.append(
        f"  Orders recorded: <code>{result.project_orders_n}</code>  "
        f"Symbols: <code>{result.project_unique_symbols}</code>  "
        f"Qty total: <code>{result.project_qty_total:.4f}</code>"
    )
    if result.project_exec_price_avg > 0:
        lines.append(f"  Avg fill price (proj): <code>{result.project_exec_price_avg:.4f}</code>")

    lines.append("")

    # --- Testnet side ---
    lines.append("<b>📡 Binance Testnet (API)</b>")
    lines.append(
        f"  Wallet balance: <code>{result.testnet_wallet_balance:.2f} USDT</code>"
    )
    lines.append(
        f"  Unrealised PnL: <code>{result.testnet_unrealized_pnl:+.2f} USDT</code>"
    )
    lines.append(
        f"  Realised PnL (window): <code>{result.testnet_realized_pnl:+.2f} USDT</code>"
    )
    lines.append(
        f"  Open orders: <code>{result.testnet_open_orders_n}</code>  "
        f"Open positions: <code>{len(result.testnet_open_positions)}</code>"
    )

    lines.append("")

    # --- Position diff ---
    has_any_diff = result.position_diffs or result.orphaned_positions or result.missing_positions
    if result.position_diffs:
        lines.append("<b>⚖️ Position Diff (proj ↔ testnet)</b>")
        for d in result.position_diffs[:10]:
            lines.append(f"  {d}")

    if result.slippage_lines:
        lines.append("")
        lines.append("<b>📐 Fill Price Slippage</b>")
        for s in result.slippage_lines[:8]:
            lines.append(f"  {s}")

    if result.orphaned_positions:
        lines.append("")
        lines.append(f"<b>⚠️ Orphaned on testnet</b> (testnet has position, no project record):")
        for o in result.orphaned_positions[:8]:
            lines.append(f"  • {o}")

    if result.missing_positions:
        lines.append("")
        lines.append(f"<b>⚠️ Missing on testnet</b> (project recorded open, testnet position=0):")
        for m in result.missing_positions[:8]:
            lines.append(f"  • {m}")

    if not has_any_diff:
        lines.append("✅ <b>Project records match testnet positions</b>")

    # --- SL/TP coverage ---
    if result.sl_tp_coverage_lines:
        lines.append("")
        warn_hdr = f" — <b>⚠️ {result.unprotected_count} unprotected</b>" if result.unprotected_count else ""
        lines.append(f"<b>🛡️ SL/TP Coverage</b>{warn_hdr}")
        for cov_line in result.sl_tp_coverage_lines[:15]:
            lines.append(f"  {cov_line}")


    cp = result.closed_pnl
    if cp is not None and (cp.proj_total_trades > 0 or abs(cp.tn_total_pnl) > 1e-8):
        lines.append("")
        lines.append("<b>💹 Closed Trades PnL</b>  (proj SQL ↔ testnet income)")

        # Summary line
        delta_sign = "+" if cp.delta_total >= 0 else ""
        tn_part = f"tn={cp.tn_total_pnl:+.2f}" if abs(cp.tn_total_pnl) > 1e-8 else "tn=—"
        delta_pct_str = f"  ({delta_sign}{cp.delta_total_pct:.1f}%)" if abs(cp.tn_total_pnl) > 1e-8 else ""
        lines.append(
            f"  Total: proj=<code>{cp.proj_total_pnl:+.2f}</code>  "
            f"{tn_part}  "
            f"Δ=<code>{delta_sign}{cp.delta_total:.2f}</code>{delta_pct_str}"
        )

        # Win-rate
        if cp.proj_total_trades > 0:
            lines.append(
                f"  Trades: <code>{cp.proj_total_trades}</code>  "
                f"Wins: <code>{cp.proj_total_wins}</code> "
                f"(<code>{cp.proj_win_rate_pct:.0f}%</code>)"
            )

        # Per-symbol breakdown (top 10 by |delta|)
        sorted_rows = sorted(cp.rows, key=lambda r: abs(r.delta_pnl), reverse=True)
        for r in sorted_rows[:10]:
            wr = f"{r.proj_wins}/{r.proj_trades}({r.proj_wins/r.proj_trades*100:.0f}%)"\
                 if r.proj_trades > 0 else "—"
            d_sign = "+" if r.delta_pnl >= 0 else ""
            tn_str = f"{r.tn_pnl:+.2f}" if abs(r.tn_pnl) > 1e-8 else "—"
            lines.append(
                f"  <code>{html.escape(r.symbol)}</code>: "
                f"proj={r.proj_pnl_net:+.2f} tn={tn_str} "
                f"Δ=<code>{d_sign}{r.delta_pnl:.2f}</code> | "
                f"{r.proj_trades}t {wr}"
            )
    elif cp is None:
        # DB not configured — add a soft note only if there ARE testnet income events
        if abs(result.testnet_realized_pnl) > 1e-8:
            lines.append("")
            lines.append("<i>ℹ️ Set TRADES_DB_DSN to enable closed-trades PnL comparison (proj vs testnet)</i>")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Delivery
# ---------------------------------------------------------------------------

def send_redis(redis_url: str, stream: str, text: str) -> None:
    import redis as redis_lib
    r = redis_lib.Redis.from_url(redis_url)
    r.xadd(stream, {"type": "report", "text": text, "parse_mode": "HTML",
                    "ts": str(_now_ms())}, maxlen=200_000, approximate=True)


def send_direct(token: str, chat_id: str, text: str) -> None:
    import urllib.request as _ur
    import urllib.parse as _up
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    body = _up.urlencode({
        "chat_id": chat_id, "text": text,
        "parse_mode": "HTML", "disable_web_page_preview": "true",
    }).encode()
    _ur.urlopen(_ur.Request(url, data=body, method="POST"), timeout=15)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run() -> int:
    redis_url = _envs("REDIS_URL", "redis://localhost:6379/0")
    exec_stream = _envs("EXEC_STREAM", "orders:exec")
    since_hours = _envf("DEMO_RECONCILE_SINCE_HOURS", 24.0)
    since_ms = _now_ms() - int(since_hours * 3_600_000)
    ts = time.strftime("%Y%m%d_%H%M%S")

    # Testnet credentials
    api_key = _envs("BINANCE_DEMO_API_KEY")
    api_secret = _envs("BINANCE_DEMO_API_SECRET")
    base_url = _envs("BINANCE_DEMO_FUTURES_BASE_URL", "https://testnet.binancefuture.com")

    if not api_key or not api_secret:
        print("[reconcile] ERROR: BINANCE_DEMO_API_KEY / BINANCE_DEMO_API_SECRET not set")
        return 1

    import redis as redis_lib
    r = redis_lib.from_url(redis_url, decode_responses=False)

    print(f"[reconcile] Reading {exec_stream} since {since_hours}h …")
    project_rows = read_exec_stream(r, exec_stream, since_ms)
    print(f"[reconcile] Project demo orders: {len(project_rows)}")

    # --- SQL: closed virtual trades PnL ---
    db_dsn = _envs("TRADES_DB_DSN", "")
    sql_trade_rows: Optional[List[Dict[str, Any]]] = None
    if db_dsn:
        print(f"[reconcile] Reading closed trades from SQL since {since_hours}h …")
        sql_trade_rows = read_closed_trades_sql(db_dsn, since_ms)
        print(f"[reconcile] SQL closed demo trades: {len(sql_trade_rows)} symbols")
    else:
        print("[reconcile] TRADES_DB_DSN not set — closed-trades PnL comparison skipped")

    print(f"[reconcile] Fetching testnet account from {base_url} …")
    try:
        account = fetch_testnet_account(base_url, api_key, api_secret)
    except Exception as e:
        print(f"[reconcile] ERROR fetching testnet account: {e}")
        account = TestnetAccount(0.0, 0.0, [], [])

    print("[reconcile] Fetching testnet income …")
    try:
        income_rows = fetch_testnet_income(base_url, api_key, api_secret, since_ms)
    except Exception as e:
        print(f"[reconcile] WARN income fetch failed: {e}")
        income_rows = []

    result = reconcile(project_rows, account, income_rows, sql_trade_rows=sql_trade_rows)
    text = build_reconcile_text(result, since_hours=since_hours, ts=ts)

    tg_mode = _envs("TELEGRAM_MODE", "redis").lower()
    if tg_mode == "direct":
        token = _envs("TELEGRAM_BOT_TOKEN")
        chat_id = _envs("TELEGRAM_CHAT_ID")
        if not token or not chat_id:
            print("[reconcile] TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID required for direct mode")
            return 1
        # send_direct(token, chat_id, text)
        print("[reconcile] sent via Telegram API [DISABLED]")
    else:
        notify_stream = _envs("TELEGRAM_NOTIFY_STREAM", "notify:telegram")
        notify_redis_url = _envs("TELEGRAM_REDIS_URL") or redis_url
        # send_redis(notify_redis_url, notify_stream, text)
        print(f"[reconcile] sent to Redis {notify_stream} [DISABLED]")

    print("\n--- Demo Reconcile Report ---")
    print(text)
    print("-----------------------------\n")

    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="Demo account reconciliation report")
    ap.parse_args()
    return run()


if __name__ == "__main__":
    raise SystemExit(main())
