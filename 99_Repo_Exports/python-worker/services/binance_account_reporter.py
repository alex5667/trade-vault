from __future__ import annotations

from utils.time_utils import get_ny_time_millis

"""Hourly Binance account state reporter -> Telegram (+ Redis snapshot).

Primary goal (P0)
-----------------
You asked to start with a service that:
  - pulls real account state from Binance USDT-M Futures
  - posts a Telegram message 1x/hour (wallet, margin, open exposure, etc.)

Design choices
--------------
* Deterministic timing: optional alignment to hour boundary to avoid drift.
* Fail-open: never crash-loop on transient Binance/Telegram outages.
* Observability:
    - Prometheus metrics (optional, prometheus_client)
    - Redis snapshot for UI / downstream services

Environment
-----------
Required:
  BINANCE_API_KEY
  BINANCE_API_SECRET
  TELEGRAM_BOT_TOKEN + TELEGRAM_CHAT_ID   (or BOT_TOKEN + CHAT_ID - see telegram_client.py)

Optional:
  BINANCE_FUTURES_BASE_URL=https://fapi.binance.com
    - testnet: https://testnet.binancefuture.com

  REDIS_URL=redis://redis-worker-1:6379/0
  ACCOUNT_SNAPSHOT_KEY=account:snapshot:binance_usdtm
  ACCOUNT_SNAPSHOT_TTL_SEC=7200

  REPORT_INTERVAL_SEC=3600
  REPORT_ALIGN_TO_HOUR=1   (align to UTC hour)
  REPORT_TOPN_POSITIONS=5
  REPORT_INCLUDE_OPEN_ORDERS=1

  ACCOUNT_REPORT_METRICS_ENABLE=0|1
  ACCOUNT_REPORT_METRICS_PORT=9133
  ACCOUNT_REPORT_METRICS_ADDR=0.0.0.0

  # History key for Available-balance delta (1h / 24h)
  ACCOUNT_HISTORY_KEY=account:snapshot:binance_usdtm:history
  ACCOUNT_HISTORY_TTL_SEC=90000   (25 h – auto expire via ZADD + ZREMRANGEBYSCORE)
"""

import os
import sys

# Ensure imports work regardless of how the service is launched.
# The repo uses both `services/` and `tick_flow_full/` on PYTHONPATH.
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(CURRENT_DIR)
TICK_ROOT = os.path.join(REPO_ROOT, 'tick_flow_full')
for _p in (REPO_ROOT, TICK_ROOT):
    if _p and _p not in sys.path:
        sys.path.insert(0, _p)

import json
import math
import time
from dataclasses import dataclass
from typing import Any

try:
    import redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None  # type: ignore

try:
    from prometheus_client import Gauge, Histogram, start_http_server  # type: ignore
except Exception:  # pragma: no cover
    Gauge = None  # type: ignore
    Histogram = None  # type: ignore
    start_http_server = None  # type: ignore

try:
    from common.log import setup_logger
except Exception:  # pragma: no cover - standalone bundle
    import logging
    def setup_logger(name: str):
        return logging.getLogger(name)
try:
    from services.binance_futures_client import BinanceFuturesREST, BinanceHTTPError
except Exception:  # pragma: no cover - standalone bundle
    from binance_futures_client import BinanceFuturesREST, BinanceHTTPError

try:
    from services.telegram.telegram_client import TelegramClient
except Exception:  # pragma: no cover - tests / standalone bundle
    class TelegramClient:  # type: ignore[override]
        @staticmethod
        def from_env():
            return None

        def send_text(self, text: str) -> None:
            return None


log = setup_logger("binance_account_reporter")


def _now_ms() -> int:
    return get_ny_time_millis()


def _fmt_usdt(x: float) -> str:
    """Format a USDT value for Telegram, adapting precision to magnitude.

    Examples:
      1 234.567    -> '1,234.57'
      0.5          -> '0.50'
      0.001234     -> '0.001234'
      0.000000033  -> '0.0000000330'
      0.0          -> '0.00'
    """
    a = abs(x)
    if a == 0.0:
        return "0.00"
    if a >= 0.01:
        # Normal range: 2 decimal places + thousands separator
        return f"{x:,.2f}"
    # Very small: find how many leading zeros after decimal point, then show
    # enough significant digits (up to 10 sig-figs, min 4 decimal places).
    # Number of leading zeros after the decimal point
    # e.g. 0.000000033 -> leading_zeros = 8, show 10 decimal places total
    leading_zeros = max(0, -int(math.floor(math.log10(a))) - 1)
    decimals = min(12, leading_zeros + 4)  # at least 4 sig figs after the zeros
    return f"{x:.{decimals}f}"


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        if v is None:
            return default
        return float(v)
    except Exception:
        return default


def _side_from_position_amt(amt: float) -> str:
    if amt > 0:
        return "LONG"
    if amt < 0:
        return "SHORT"
    return "FLAT"


@dataclass
class AccountSnapshot:
    ts_ms: int
    venue: str
    wallet_balance: float
    margin_balance: float
    available_balance: float
    unrealized_pnl: float
    initial_margin: float
    maint_margin: float
    open_positions_n: int
    open_notional_usdt: float
    open_orders_n: int
    positions: list[dict[str, Any]]

    def to_json(self) -> str:
        return json.dumps(
            {
                "ts_ms": int(self.ts_ms),
                "venue": self.venue,
                "wallet_balance": self.wallet_balance,
                "margin_balance": self.margin_balance,
                "available_balance": self.available_balance,
                "unrealized_pnl": self.unrealized_pnl,
                "initial_margin": self.initial_margin,
                "maint_margin": self.maint_margin,
                "open_positions_n": int(self.open_positions_n),
                "open_notional_usdt": self.open_notional_usdt,
                "open_orders_n": int(self.open_orders_n),
                "positions": self.positions,
            },
            ensure_ascii=False,
            separators=(",", ":"),
        )


def build_snapshot(
    *,
    client: BinanceFuturesREST,
    topn_positions: int,
    include_open_orders: bool,
) -> AccountSnapshot:
    ts_ms = _now_ms()
    acct = client.get_account()
    pr = client.get_position_risk()

    wallet = _safe_float(acct.get("totalWalletBalance"))
    margin_bal = _safe_float(acct.get("totalMarginBalance"))
    avail = _safe_float(acct.get("availableBalance"))
    u_pnl = _safe_float(acct.get("totalUnrealizedProfit"))
    init_m = _safe_float(acct.get("totalInitialMargin"))
    maint_m = _safe_float(acct.get("totalMaintMargin"))

    all_positions: list[dict[str, Any]] = []
    open_notional = 0.0
    if isinstance(pr, list):
        for row in pr:
            amt = _safe_float(row.get("positionAmt"))
            if abs(amt) <= 0.0:
                continue
            mark = _safe_float(row.get("markPrice"))
            entry = _safe_float(row.get("entryPrice"))
            upnl = _safe_float(row.get("unRealizedProfit"))
            # notional field preferred; fallback to amt * markPrice
            notional = _safe_float(row.get("notional"), default=amt * mark)
            open_notional += abs(notional)
            all_positions.append(
                {
                    "symbol": (row.get("symbol") or ""),
                    "side": _side_from_position_amt(amt),
                    "position_amt": amt,
                    "entry_price": entry,
                    "mark_price": mark,
                    "unrealized_pnl": upnl,
                    "notional": notional,
                    "liquidation_price": _safe_float(row.get("liquidationPrice")),
                    "initial_margin": _safe_float(row.get("initialMargin")),
                    "maint_margin": _safe_float(row.get("maintMargin")),
                    "isolated_margin": _safe_float(row.get("isolatedMargin")),
                    "margin_type": (row.get("marginType") or ""),
                    "leverage": _safe_float(row.get("leverage")),
                }
            )

    # open_positions_n counts ALL open positions (not only top-N displayed)
    open_positions_total_n = len(all_positions)

    # Sort by absolute notional to show the most impactful risk first.
    all_positions.sort(key=lambda x: abs(_safe_float(x.get("notional"))), reverse=True)
    positions_top = all_positions
    if topn_positions > 0:
        positions_top = all_positions[:topn_positions]

    open_orders_n = 0
    if include_open_orders:
        try:
            oo = client.get_open_orders(symbol=None)
            if isinstance(oo, list):
                open_orders_n = len(oo)
        except Exception:
            # Open orders are not critical for P0 report.
            open_orders_n = 0

    return AccountSnapshot(
        ts_ms=ts_ms,
        venue="binance_usdtm",
        wallet_balance=wallet,
        margin_balance=margin_bal,
        available_balance=avail,
        unrealized_pnl=u_pnl,
        initial_margin=init_m,
        maint_margin=maint_m,
        open_positions_n=open_positions_total_n,
        open_notional_usdt=open_notional,
        open_orders_n=open_orders_n,
        positions=positions_top,
    )


# ---------------------------------------------------------------------------
# History helpers – store / retrieve available_balance for delta computation.
# ---------------------------------------------------------------------------

_MS_1H  = 3_600_000
_MS_24H = 86_400_000


def _store_history(r: Any, history_key: str, ts_ms: int, available: float, ttl_sec: int = 90_000) -> None:
    """Push a (ts_ms → available_balance) entry into a Redis sorted set.

    score = ts_ms, value = "{ts_ms}:{available_balance}".
    We trim entries older than ttl_sec to save memory.
    """
    try:
        member = f"{ts_ms}:{available}"
        r.zadd(history_key, {member: ts_ms})
        cutoff_ms = ts_ms - ttl_sec * 1000
        r.zremrangebyscore(history_key, "-inf", cutoff_ms)
    except Exception as exc:  # pragma: no cover
        log.warning("⚠️ history store failed: %s", exc)


def _read_delta_available(
    r: Any,
    history_key: str,
    now_ts_ms: int,
    current_available: float,
) -> dict[str, float | None]:
    """Return {"1h": delta_float_or_None, "24h": delta_float_or_None}.

    Looks for the closest snapshot to exactly 1 h and 24 h ago
    (within ±10 min window to tolerate drift / restarts).
    """
    result: dict[str, float | None] = {"1h": None, "24h": None}
    try:
        window_ms = 10 * 60 * 1000  # ±10 min tolerance
        for label, target_offset_ms in (("1h", _MS_1H), ("24h", _MS_24H)):
            target_ms = now_ts_ms - target_offset_ms
            lo = target_ms - window_ms
            hi = target_ms + window_ms
            # ZRANGEBYSCORE returns members with scores in [lo, hi]
            entries = r.zrangebyscore(history_key, lo, hi, withscores=True)
            if not entries:
                continue
            # Pick entry closest in time to the exact target
            best_val, best_score = min(
                entries, key=lambda kv: abs(float(kv[1]) - target_ms)
            )

            val_str = best_val
            if isinstance(val_str, bytes):
                val_str = val_str.decode("utf-8")
            else:
                val_str = str(val_str)

            if ":" in val_str:
                _, avail_str = val_str.split(":", 1)
                old_avail = float(avail_str)
            else:
                old_avail = float(val_str)

            result[label] = current_available - old_avail
    except Exception as exc:  # pragma: no cover
        log.warning("⚠️ history read failed: %s", exc)
    return result


def _fmt_delta(delta: float | None) -> str:
    """Format a delta USDT value with sign and arrow, or '—' if unavailable."""
    if delta is None:
        return "—"
    sign = "+" if delta >= 0 else ""
    arrow = "📈" if delta > 0 else ("📉" if delta < 0 else "➡️")
    return f"{arrow} {sign}{_fmt_usdt(delta)} USDT"


def format_report(
    snapshot: AccountSnapshot,
    deltas: dict[str, float | None] | None = None,
) -> str:
    t_utc = time.strftime("%Y-%m-%d %H:%M UTC", time.gmtime(snapshot.ts_ms / 1000))

    used_pct = 0.0
    if snapshot.margin_balance > 0:
        used_pct = 100.0 * (snapshot.initial_margin / snapshot.margin_balance)

    maint_pct = 0.0
    if snapshot.margin_balance > 0:
        maint_pct = 100.0 * (snapshot.maint_margin / snapshot.margin_balance)

    upnl = snapshot.unrealized_pnl
    upnl_sign = "+" if upnl >= 0 else ""

    lines: list[str] = []
    lines.append(f"<b>📊 Binance USDT-M Account</b>  <code>{t_utc}</code>")
    lines.append("")
    lines.append(f"Wallet: <code>{_fmt_usdt(snapshot.wallet_balance)} USDT</code>")
    lines.append(f"Margin: <code>{_fmt_usdt(snapshot.margin_balance)} USDT</code>")
    lines.append(f"Available: <code>{_fmt_usdt(snapshot.available_balance)} USDT</code>")
    lines.append(f"uPnL: <code>{upnl_sign}{_fmt_usdt(upnl)} USDT</code>")
    lines.append(f"Initial margin: <code>{_fmt_usdt(snapshot.initial_margin)} USDT</code>  (<code>{used_pct:.1f}%</code>)")
    lines.append(f"Maint margin: <code>{_fmt_usdt(snapshot.maint_margin)} USDT</code>  (<code>{maint_pct:.2f}%</code>)")
    lines.append("")
    lines.append(
        f"Open positions: <b>{snapshot.open_positions_n}</b> | Exposure: <code>{_fmt_usdt(snapshot.open_notional_usdt)} USDT</code> | Open orders: <b>{snapshot.open_orders_n}</b>"
    )

    # ------------------------------------------------------------------
    # Available balance delta block (1h / 24h)
    # ------------------------------------------------------------------
    if deltas is not None:
        d1h  = deltas.get("1h")
        d24h = deltas.get("24h")
        lines.append("")
        lines.append("<b>📊 Available Δ</b>")
        lines.append(f"  1h:  <code>{_fmt_delta(d1h)}</code>")
        lines.append(f"  24h: <code>{_fmt_delta(d24h)}</code>")

    if snapshot.positions:
        lines.append("")
        lines.append("<b>Top positions</b>")
        for p in snapshot.positions:
            sym = (p.get("symbol") or "")
            side = (p.get("side") or "")
            amt = _safe_float(p.get("position_amt"))
            notional = _safe_float(p.get("notional"))
            pupnl = _safe_float(p.get("unrealized_pnl"))
            pupnl_sign = "+" if pupnl >= 0 else ""
            liq = _safe_float(p.get("liquidation_price"))
            liq_s = f" liq={liq:.2f}" if liq > 0 else ""
            lines.append(
                f"• <b>{sym}</b> {side} <code>{amt:g}</code> | notional <code>{_fmt_usdt(abs(notional))}</code> | uPnL <code>{pupnl_sign}{_fmt_usdt(pupnl)}</code>{liq_s}"
            )

    # Telegram HTML parse mode.
    return "\n".join(lines).strip()


class Metrics:
    """Prometheus metrics for the account reporter (optional)."""

    def __init__(self) -> None:
        if Gauge is None or Histogram is None:
            raise RuntimeError("prometheus_client is not available")
        self.fetch_latency = Histogram(
            "binance_account_report_fetch_latency_ms",
            "Latency of Binance account fetch",
            buckets=[50, 100, 250, 500, 1000, 2000, 5000, 10000],
        )
        self.last_ok_ts = Gauge("binance_account_report_last_ok_ts_seconds", "Last successful report time")
        self.last_err_ts = Gauge("binance_account_report_last_err_ts_seconds", "Last failed report time")
        self.wallet = Gauge("binance_account_wallet_balance_usdt", "Wallet balance")
        self.margin = Gauge("binance_account_margin_balance_usdt", "Margin balance")
        self.available = Gauge("binance_account_available_balance_usdt", "Available balance")
        self.upnl = Gauge("binance_account_unrealized_pnl_usdt", "Unrealized PnL")
        self.init_margin = Gauge("binance_account_initial_margin_usdt", "Initial margin")
        self.maint_margin = Gauge("binance_account_maint_margin_usdt", "Maintenance margin")
        self.open_pos_n = Gauge("binance_account_open_positions", "Number of open positions")
        self.open_notional = Gauge("binance_account_open_notional_usdt", "Total absolute notional exposure")
        self.open_orders_n = Gauge("binance_account_open_orders", "Number of open orders")
        self.snapshot_age = Gauge("binance_account_snapshot_age_ms", "Age of last stored snapshot")


def _sleep_until_next_tick(interval_s: int, align_to_hour: bool) -> None:
    if interval_s <= 0:
        interval_s = 3600
    now = time.time()
    if align_to_hour and interval_s == 3600:
        # Align to UTC hour boundary.
        next_tick = (int(now) // 3600 + 1) * 3600
        sleep_s = max(0.0, float(next_tick) - now)
        time.sleep(sleep_s)
        return

    # Generic fixed-interval sleep.
    time.sleep(float(interval_s))


def main() -> None:
    # Account reporter connects to demo/testnet account via BINANCE_DEMO_ prefix.
    # To report against production, change prefix to "BINANCE_" (and update docker-compose).
    try:
        client = BinanceFuturesREST.from_env(prefix="BINANCE_DEMO_")
    except RuntimeError:
        # Fallback: try legacy BINANCE_ prefix if BINANCE_DEMO_* not set
        try:
            client = BinanceFuturesREST.from_env(prefix="BINANCE_")
        except RuntimeError:
            raise SystemExit(
                "BINANCE_DEMO_API_KEY / BINANCE_DEMO_API_SECRET are required "
                "(or legacy BINANCE_API_KEY / BINANCE_API_SECRET)"
            )

    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0").strip()
    snapshot_key = os.getenv("ACCOUNT_SNAPSHOT_KEY", "account:snapshot:binance_usdtm").strip()
    snapshot_ttl = int(os.getenv("ACCOUNT_SNAPSHOT_TTL_SEC", "7200"))
    history_key = os.getenv("ACCOUNT_HISTORY_KEY", "account:snapshot:binance_usdtm:history").strip()
    history_ttl = int(os.getenv("ACCOUNT_HISTORY_TTL_SEC", "90000"))  # 25 h

    interval_s = int(os.getenv("REPORT_INTERVAL_SEC", "3600"))
    align = os.getenv("REPORT_ALIGN_TO_HOUR", "1").lower() in {"1", "true", "yes"}
    topn = int(os.getenv("REPORT_TOPN_POSITIONS", "5"))
    include_oo = os.getenv("REPORT_INCLUDE_OPEN_ORDERS", "1").lower() in {"1", "true", "yes"}

    metrics_enable = os.getenv("ACCOUNT_REPORT_METRICS_ENABLE", "0").lower() in {"1", "true", "yes"}
    metrics_port = int(os.getenv("ACCOUNT_REPORT_METRICS_PORT", "9133"))
    metrics_addr = os.getenv("ACCOUNT_REPORT_METRICS_ADDR", "0.0.0.0")
    m: Metrics | None = None
    if metrics_enable:
        if start_http_server is None:
            log.warning("⚠️ prometheus_client not installed; metrics disabled")
        else:
            start_http_server(metrics_port, addr=metrics_addr)
            m = Metrics()
            log.info("✅ Prometheus metrics enabled on %s:%d", metrics_addr, metrics_port)

    tg = TelegramClient.from_env()
    if tg is None:
        log.warning("⚠️ Telegram disabled (TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID not set)")

    r = None
    if redis is None:
        log.warning("⚠️ redis-py not installed; Redis snapshot disabled")
    else:
        r = redis.from_url(redis_url, decode_responses=True)

    log.info("🚀 Binance account reporter started")
    log.info("   base_url=%s", client.base_url)

    log.info("   redis=%s", redis_url)
    log.info("   snapshot_key=%s ttl=%ds", snapshot_key, snapshot_ttl)
    log.info("   history_key=%s ttl=%ds", history_key, history_ttl)
    log.info("   interval=%ds align_to_hour=%s", interval_s, align)

    # Initial alignment sleep to avoid immediate spam on restart.
    _sleep_until_next_tick(interval_s, align_to_hour=align)

    last_snapshot_ts_ms = 0
    while True:
        t0 = time.time()
        try:
            snap = build_snapshot(client=client, topn_positions=topn, include_open_orders=include_oo)
            # Store snapshot for UI / downstream services (optional).
            deltas: dict[str, float | None] | None = None
            if r is not None:
                r.set(snapshot_key, snap.to_json(), ex=snapshot_ttl)
                # Read deltas BEFORE storing current point (so we don't compare to self)
                deltas = _read_delta_available(
                    r, history_key, snap.ts_ms, snap.available_balance
                )
                _store_history(r, history_key, snap.ts_ms, snap.available_balance, history_ttl)
            last_snapshot_ts_ms = int(snap.ts_ms)

            msg = format_report(snap, deltas=deltas)
            if tg is not None:
                _ = tg.send_text(msg)

            if m is not None:
                m.last_ok_ts.set_to_current_time()
                m.wallet.set(float(snap.wallet_balance))
                m.margin.set(float(snap.margin_balance))
                m.available.set(float(snap.available_balance))
                m.upnl.set(float(snap.unrealized_pnl))
                m.init_margin.set(float(snap.initial_margin))
                m.maint_margin.set(float(snap.maint_margin))
                m.open_pos_n.set(float(snap.open_positions_n))
                m.open_notional.set(float(snap.open_notional_usdt))
                m.open_orders_n.set(float(snap.open_orders_n))
                m.snapshot_age.set(0.0)

        except BinanceHTTPError as e:
            log.error("❌ Binance API error: %s", str(e))
            if m is not None:
                m.last_err_ts.set_to_current_time()
        except Exception as e:
            log.exception("❌ Unexpected error in reporter: %s", str(e))
            if m is not None:
                m.last_err_ts.set_to_current_time()

        # Metrics: latency + snapshot age
        if m is not None:
            dt_ms = (time.time() - t0) * 1000.0
            m.fetch_latency.observe(float(dt_ms))
            if last_snapshot_ts_ms > 0:
                m.snapshot_age.set(float(_now_ms() - last_snapshot_ts_ms))

        _sleep_until_next_tick(interval_s, align_to_hour=align)


if __name__ == "__main__":
    main()
