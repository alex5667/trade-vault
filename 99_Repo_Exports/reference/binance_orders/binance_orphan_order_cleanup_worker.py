#!/usr/bin/env python3
from __future__ import annotations

"""Periodic Binance orphan-order cleanup worker.

Goal
----
Find open orders (plain + algo) on symbols where there is NO active position
and cancel them.  These "orphans" arise when:

  1. Binance algo-to-plain conversion changes clientOrderId → lifecycle monitor
     can't match by sid token.
  2. Executor restart between position close and order cleanup → watchdog threads
     lost, startup reconcile doesn't pick up already-closed positions.
  3. Manual position close via Binance UI while executor SL/TP orders remain.

This worker also optionally verifies that every open position has at least one
protective order (SL), and sends Telegram alerts if protection is missing.

Safety contract
---------------
* Only cancels orders on symbols with ZERO position (abs qty ≤ tolerance).
* Requires confirm_passes consecutive sweep confirmations before acting.
* Notifies via Telegram on every cleanup action.
* Emits structured events to the orders:exec Redis stream.

ENV — required:
  REDIS_URL, BINANCE_API_KEY, BINANCE_API_SECRET

ENV — worker settings:
  ORPHAN_CLEANUP_ENABLE=1                (default: 1)
  ORPHAN_CLEANUP_INTERVAL_SEC=30         (sweep interval)
  ORPHAN_CLEANUP_CONFIRM_PASSES=2        (consecutive sweeps needed)
  ORPHAN_CLEANUP_PROTECTION_CHECK=1      (check open positions for missing SL)
  ORPHAN_CLEANUP_DRY_RUN=0               (1=log but don't cancel)

ENV — limits:
  BINANCE_SYMBOL_ALLOWLIST=...           (reuse executor's allowlist)

ENV — telegram (optional):
  TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID
"""

import json
import logging
import math
import os
import socket
import sys
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set

try:
    import redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None  # type: ignore

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(CURRENT_DIR)
TICK_ROOT = os.path.join(REPO_ROOT, 'tick_flow_full')
for _p in (REPO_ROOT, TICK_ROOT):
    if _p and _p not in sys.path:
        sys.path.insert(0, _p)

try:
    from services.binance_futures_client import BinanceAPIError, BinanceFuturesClient
except Exception:  # pragma: no cover
    from binance_futures_client import BinanceAPIError, BinanceFuturesClient  # type: ignore

try:
    from services.telegram.telegram_client import TelegramClient
except Exception:  # pragma: no cover
    try:
        from telegram.telegram_client import TelegramClient  # type: ignore
    except Exception:
        TelegramClient = None  # type: ignore

log = logging.getLogger("binance_orphan_order_cleanup")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _bool_env(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return bool(default)
    return str(raw).strip().lower() in {"1", "true", "yes", "on", "y"}


def _f(v: Any, default: float = 0.0) -> float:
    try:
        return float(v) if v is not None else float(default)
    except Exception:
        return float(default)


def _now_ms() -> int:
    return int(time.time() * 1000)


def _load_symbol_set(raw: str) -> Set[str]:
    return {part.strip().upper() for part in str(raw or '').split(',') if part.strip()}


@dataclass
class SymbolOrphanState:
    """Tracks orphan sightings per symbol across sweeps.

    Changed from strict-consecutive to total-sightings model:
    under unstable network conditions (DNS/timestamp failures)
    consecutive sweeps rarely succeed back-to-back, but we can
    still confirm an orphan if it's been seen N times across
    successful sweeps within a time window.
    """
    symbol: str
    sightings: int = 0
    order_count: int = 0
    order_ids: List[str] = field(default_factory=list)
    first_seen_s: float = 0.0  # monotonic time of first sighting


class BinanceOrphanOrderCleanupWorker:
    """Periodic worker that sweeps for orphaned orders and missing protection.

    Workflow per sweep:
    1. Fetch positionRisk → identify symbols with zero position.
    2. Fetch open orders (plain + algo) for all allowlisted symbols.
    3. For symbols with orders but no position → mark as orphan candidate.
    4. After confirm_passes consecutive sweeps → cancel all orders.
    5. (Optional) For symbols WITH position → check if at least 1 protective
       order exists (plain SL/TP or algo). Alert if missing.
    """

    def __init__(
        self,
        *,
        prod_client: Optional[BinanceFuturesClient] = None,
        demo_client: Optional[BinanceFuturesClient] = None,
        redis_client: Any = None,
        telegram_client: Any = None,
    ) -> None:
        # Clients
        if prod_client is not None:
            self.prod_client: Optional[BinanceFuturesClient] = prod_client
        else:
            _prod_key = (os.getenv("BINANCE_API_KEY") or "").strip()
            if _prod_key:
                self.prod_client = BinanceFuturesClient.from_env(prefix="BINANCE_")
            else:
                self.prod_client = None

        if demo_client is not None:
            self.demo_client: Optional[BinanceFuturesClient] = demo_client
        else:
            _demo_key = (os.getenv("BINANCE_DEMO_API_KEY") or "").strip()
            if _demo_key:
                self.demo_client = BinanceFuturesClient.from_env(prefix="BINANCE_DEMO_")
            else:
                self.demo_client = None

        if self.prod_client is None and self.demo_client is None:
            raise RuntimeError("At least one of BINANCE_API_KEY or BINANCE_DEMO_API_KEY must be set")

        # Redis
        if redis_client is not None:
            self.r = redis_client
        elif redis is not None and os.getenv('REDIS_URL'):
            self.r = redis.from_url(
                os.getenv('REDIS_URL', 'redis://localhost:6379/0'),
                decode_responses=True,
            )
        else:
            self.r = None

        # Telegram
        if telegram_client is not None:
            self.tg = telegram_client
        elif TelegramClient is not None:
            self.tg = TelegramClient.from_env()
        else:
            self.tg = None

        # Settings
        self.enabled = _bool_env('ORPHAN_CLEANUP_ENABLE', True)
        self.dry_run = _bool_env('ORPHAN_CLEANUP_DRY_RUN', False)
        self.interval_sec = float(os.getenv('ORPHAN_CLEANUP_INTERVAL_SEC', '30'))
        self.confirm_passes = max(1, int(os.getenv('ORPHAN_CLEANUP_CONFIRM_PASSES', '2')))
        self.protection_check = _bool_env('ORPHAN_CLEANUP_PROTECTION_CHECK', True)
        self.position_mode = (os.getenv('BINANCE_POSITION_MODE') or 'oneway').strip().lower()

        # Allowlist
        self.allowlist = _load_symbol_set(os.getenv('BINANCE_SYMBOL_ALLOWLIST', ''))

        # Stream
        self.exec_stream = os.getenv('EXEC_STREAM', 'orders:exec')
        _maxlen = int(os.getenv('EXEC_STREAM_MAXLEN', '0') or '0')
        self.exec_stream_maxlen: Optional[int] = _maxlen if _maxlen > 0 else None

        # State tracking
        self._orphan_candidates: Dict[str, SymbolOrphanState] = {}
        self._unprotected_alerted: Set[str] = set()

    # -----------------------------------------------------------------------
    # Helpers
    # -----------------------------------------------------------------------

    def _emit_exec_event(self, *, symbol: str, event_type: str, status: str,
                         payload: Dict[str, Any]) -> None:
        if self.r is None:
            return
        fields = {
            'sid': '',
            'symbol': str(symbol),
            'action': 'orphan_cleanup',
            'event_type': str(event_type),
            'status': str(status),
            'ts_event_ms': str(_now_ms()),
            'ts_ms': str(_now_ms()),
            'payload_json': json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
        }
        try:
            kwargs: Dict[str, Any] = {}
            if self.exec_stream_maxlen:
                kwargs = {'maxlen': self.exec_stream_maxlen, 'approximate': True}
            self.r.xadd(self.exec_stream, fields, **kwargs, maxlen=50000)
        except Exception:
            pass

    def _send_telegram(self, msg: str) -> None:
        if self.tg is None:
            return
        try:
            self.tg.send_text(msg)
        except Exception as e:
            log.warning("Telegram send failed: %s", e)

    def _get_positions(self, client: BinanceFuturesClient) -> Optional[Dict[str, Dict[str, Any]]]:
        """Fetch positionRisk and return dict keyed by symbol with position info.

        Returns None on fetch failure (DNS / timeout / API error) so callers
        can distinguish 'no positions' from 'failed to fetch'.
        """
        try:
            risks = client.get_position_risk() or []
        except Exception as e:
            log.error("Failed to fetch positionRisk: %s", e)
            return None  # signal failure to caller

        positions: Dict[str, Dict[str, Any]] = {}
        for p in risks:
            symbol = str(p.get('symbol') or '').upper().strip()
            if not symbol:
                continue
            if self.allowlist and symbol not in self.allowlist:
                continue
            amt = _f(p.get('positionAmt'), 0.0)
            abs_qty = abs(amt)
            logical_side = 'LONG' if amt > 0 else ('SHORT' if amt < 0 else None)
            positions[symbol] = {
                'symbol': symbol,
                'abs_qty': abs_qty,
                'signed_qty': amt,
                'logical_side': logical_side,
                'notional_usdt': abs(_f(p.get('notional'), 0.0)),
                'margin_usdt': abs(_f(p.get('isolatedMargin'), 0.0) or _f(p.get('initialMargin'), 0.0)),
            }
        return positions

    def _get_all_open_orders(
        self, client: BinanceFuturesClient
    ) -> Optional[Dict[str, Dict[str, Any]]]:
        """Fetch all open plain and algo orders, grouped by symbol.

        Returns None if plain-orders fetch fails (DNS / timeout / API error)
        so callers can skip the sweep without wiping candidate state.
        Algo-order fetch failures per-symbol are tolerated (partial result).
        """
        result: Dict[str, Dict[str, Any]] = {}

        # Plain orders (all symbols at once)
        try:
            plain_orders = client.get_open_orders() or []
            for o in plain_orders:
                sym = str(o.get('symbol') or '').upper().strip()
                if not sym:
                    continue
                if self.allowlist and sym not in self.allowlist:
                    continue
                if sym not in result:
                    result[sym] = {'plain': [], 'algo': []}
                result[sym]['plain'].append(o)
        except Exception as e:
            log.error("Failed to fetch plain orders: %s", e)
            return None  # signal failure to caller

        # Algo orders — need to query per-symbol or all
        for symbol in (self.allowlist or result.keys()):
            try:
                algo_orders = client.get_open_algo_orders(symbol) or []
                for o in algo_orders:
                    sym = str(o.get('symbol') or symbol).upper().strip()
                    if sym not in result:
                        result[sym] = {'plain': [], 'algo': []}
                    result[sym]['algo'].append(o)
            except BinanceAPIError as e:
                if e.status not in (400, 404):
                    log.warning("Algo orders fetch for %s: %s", symbol, e)
            except Exception:
                continue

        return result

    def _cancel_all_symbol_orders(
        self, symbol: str, orders: Dict[str, Any], *, client: BinanceFuturesClient
    ) -> Dict[str, Any]:
        """Cancel all plain + algo orders for a symbol."""
        plain_list = list(orders.get('plain') or [])
        algo_list = list(orders.get('algo') or [])
        canceled_plain = 0
        canceled_algo = 0

        # Bulk cancel first
        try:
            client.cancel_all_orders(symbol)
        except Exception:
            pass

        # Individual fallback
        for o in plain_list:
            try:
                oid = o.get('orderId')
                if oid is not None:
                    client.cancel_plain_order(symbol, order_id=int(oid))
                    canceled_plain += 1
            except Exception:
                continue

        for o in algo_list:
            try:
                oid = o.get('algoId')
                if oid is not None:
                    client.cancel_algo_order(symbol, algo_id=int(oid))
                    canceled_algo += 1
            except Exception:
                continue

        return {
            'plain_seen': len(plain_list),
            'algo_seen': len(algo_list),
            'plain_canceled': canceled_plain,
            'algo_canceled': canceled_algo,
        }

    def _check_protection(
        self, symbol: str, position: Dict[str, Any],
        orders: Dict[str, Any], *, client_label: str,
    ) -> Optional[str]:
        """Check if position has at least one protective order. Returns alert msg or None."""
        abs_qty = position.get('abs_qty', 0.0)
        if abs_qty <= 0:
            return None

        logical_side = position.get('logical_side', '')
        if not logical_side:
            return None

        plain_orders = list(orders.get('plain') or [])
        algo_orders = list(orders.get('algo') or [])

        # Check for any SL-like order (STOP_MARKET, STOP, TRAILING_STOP_MARKET)
        has_sl = False
        has_tp = False
        for o in plain_orders:
            otype = str(o.get('origType') or o.get('type') or '').upper()
            if 'STOP' in otype and 'TAKE_PROFIT' not in otype:
                has_sl = True
            if 'TAKE_PROFIT' in otype:
                has_tp = True
        for o in algo_orders:
            otype = str(o.get('type') or '').upper()
            if 'STOP' in otype and 'TAKE_PROFIT' not in otype:
                has_sl = True
            if 'TAKE_PROFIT' in otype:
                has_tp = True

        if not has_sl and not has_tp:
            alert_key = f"{client_label}:{symbol}"
            if alert_key not in self._unprotected_alerted:
                self._unprotected_alerted.add(alert_key)
                notional = position.get('notional_usdt', 0.0)
                margin = position.get('margin_usdt', 0.0)
                msg = (
                    f"⚠️ UNPROTECTED POSITION [{client_label}]\n"
                    f"Symbol: {symbol} {logical_side}\n"
                    f"Qty: {abs_qty}\n"
                    f"Notional: ${notional:.2f}\n"
                    f"Margin: ${margin:.2f}\n"
                    f"Plain orders: {len(plain_orders)}\n"
                    f"Algo orders: {len(algo_orders)}\n"
                    f"⚠️ No SL/TP protection found!"
                )
                return msg
        else:
            # Clear alert if protection was restored
            alert_key = f"{client_label}:{symbol}"
            self._unprotected_alerted.discard(alert_key)

        return None

    # -----------------------------------------------------------------------
    # Main sweep
    # -----------------------------------------------------------------------

    def _sweep_client(self, client: BinanceFuturesClient, label: str) -> Dict[str, Any]:
        """Single sweep for one client (prod or demo).

        If either positions or orders fetch fails (DNS / timeout), the sweep
        returns early WITHOUT modifying orphan candidate state.  This prevents
        the vicious cycle where failed sweeps wipe candidate counters.
        """
        try:
            client.sync_time()
        except Exception:
            pass

        positions = self._get_positions(client)
        if positions is None:
            # Fetching positions failed — skip sweep, preserve candidate state
            return {
                'client': label, 'positions': -1, 'symbols_with_orders': -1,
                'orphan_cleaned': [], 'protection_alerts': 0,
                'pending': [], 'sweep_failed': True,
            }

        all_orders = self._get_all_open_orders(client)
        if all_orders is None:
            # Fetching orders failed — skip sweep, preserve candidate state
            return {
                'client': label, 'positions': len(positions), 'symbols_with_orders': -1,
                'orphan_cleaned': [], 'protection_alerts': 0,
                'pending': [], 'sweep_failed': True,
            }

        orphan_cleaned: List[Dict[str, Any]] = []
        protection_alerts: List[str] = []
        still_pending: List[str] = []
        now_mono = time.monotonic()

        # 1. Find orphan orders (orders with no position)
        for symbol, orders in all_orders.items():
            total_orders = len(orders.get('plain', [])) + len(orders.get('algo', []))
            if total_orders == 0:
                continue

            pos = positions.get(symbol)
            has_position = pos is not None and pos.get('abs_qty', 0.0) > 1e-12

            if has_position:
                # Position exists → check protection instead
                if self.protection_check:
                    alert = self._check_protection(
                        symbol, pos, orders, client_label=label
                    )
                    if alert:
                        protection_alerts.append(alert)
                continue

            # No position but has orders → orphan candidate
            state_key = f"{label}:{symbol}"
            if state_key not in self._orphan_candidates:
                self._orphan_candidates[state_key] = SymbolOrphanState(
                    symbol=symbol, sightings=0, order_count=total_orders,
                    first_seen_s=now_mono,
                )
            state = self._orphan_candidates[state_key]
            state.sightings += 1
            state.order_count = total_orders

            plain_ids = [str(o.get('orderId', '?')) for o in orders.get('plain', [])]
            algo_ids = [str(o.get('algoId', '?')) for o in orders.get('algo', [])]
            state.order_ids = plain_ids + algo_ids

            if state.sightings < self.confirm_passes:
                still_pending.append(
                    f"{symbol}({state.sightings}/{self.confirm_passes})"
                )
                continue

            # Confirmed orphan → cancel
            age_s = now_mono - state.first_seen_s
            log.info(
                "[%s] Orphan confirmed: %s — %d orders, %d sightings, age=%.0fs",
                label, symbol, total_orders, state.sightings, age_s,
            )

            if self.dry_run:
                doc = {
                    'symbol': symbol, 'client': label, 'dry_run': True,
                    'order_count': total_orders,
                    'plain_ids': plain_ids, 'algo_ids': algo_ids,
                }
                orphan_cleaned.append(doc)
                self._emit_exec_event(
                    symbol=symbol, event_type='orphan_cleanup_dry_run',
                    status='would_cancel', payload=doc,
                )
                msg = (
                    f"🔍 ORPHAN DRY-RUN [{label}]\n"
                    f"Symbol: {symbol}\n"
                    f"Would cancel {total_orders} orders\n"
                    f"Plain: {plain_ids}\n"
                    f"Algo: {algo_ids}"
                )
                self._send_telegram(msg)
            else:
                cancel_result = self._cancel_all_symbol_orders(
                    symbol, orders, client=client,
                )
                doc = {
                    'symbol': symbol, 'client': label,
                    'order_count': total_orders,
                    'plain_ids': plain_ids, 'algo_ids': algo_ids,
                    **cancel_result,
                }
                orphan_cleaned.append(doc)
                self._emit_exec_event(
                    symbol=symbol, event_type='orphan_cleanup',
                    status='canceled', payload=doc,
                )
                msg = (
                    f"🧹 ORPHAN CLEANUP [{label}]\n"
                    f"Symbol: {symbol}\n"
                    f"Canceled: {cancel_result.get('plain_canceled', 0)} plain, "
                    f"{cancel_result.get('algo_canceled', 0)} algo\n"
                    f"(was: {total_orders} orders with no position)"
                )
                self._send_telegram(msg)

            # Reset state
            self._orphan_candidates.pop(state_key, None)

        # Clean up stale candidates (symbol no longer has orphan orders)
        # Only clean up if the sweep succeeded — on failure this block is
        # never reached (early return above).
        active_orphan_keys = {
            f"{label}:{sym}" for sym, orders in all_orders.items()
            if (len(orders.get('plain', [])) + len(orders.get('algo', []))) > 0
            and (positions.get(sym) is None or positions.get(sym, {}).get('abs_qty', 0.0) <= 1e-12)
        }
        stale_keys = [
            k for k in self._orphan_candidates
            if k.startswith(f"{label}:") and k not in active_orphan_keys
        ]
        for k in stale_keys:
            self._orphan_candidates.pop(k, None)

        # Send protection alerts
        for alert_msg in protection_alerts:
            self._send_telegram(alert_msg)

        return {
            'client': label,
            'positions': len(positions),
            'symbols_with_orders': len(all_orders),
            'orphan_cleaned': orphan_cleaned,
            'protection_alerts': len(protection_alerts),
            'pending': still_pending,
        }

    def sweep_once(self) -> Dict[str, Any]:
        """Run one full sweep cycle across all configured clients."""
        results: List[Dict[str, Any]] = []

        if self.prod_client is not None:
            try:
                r = self._sweep_client(self.prod_client, "prod")
                results.append(r)
            except Exception as e:
                log.error("Prod sweep error: %s", e)

        if self.demo_client is not None:
            try:
                r = self._sweep_client(self.demo_client, "demo")
                results.append(r)
            except Exception as e:
                log.error("Demo sweep error: %s", e)

        return {
            'ts': _now_ms(),
            'results': results,
        }

    def run_forever(self) -> None:
        """Main loop — sweep and sleep."""
        log.info(
            "🧹 OrphanOrderCleanup starting | interval=%ss confirm_passes=%d "
            "protection_check=%s dry_run=%s allowlist=%s",
            self.interval_sec, self.confirm_passes,
            self.protection_check, self.dry_run,
            sorted(self.allowlist) if self.allowlist else "ALL",
        )

        while True:
            if not self.enabled:
                time.sleep(self.interval_sec)
                continue

            try:
                result = self.sweep_once()
                for r in result.get('results', []):
                    if r.get('sweep_failed'):
                        log.warning(
                            "[%s] sweep failed (positions=%s, orders=%s) — candidate state preserved",
                            r.get('client', '?'),
                            r.get('positions', '?'),
                            r.get('symbols_with_orders', '?'),
                        )
                        continue
                    cleaned = r.get('orphan_cleaned', [])
                    pending = r.get('pending', [])
                    alerts = r.get('protection_alerts', 0)
                    if cleaned or pending or alerts:
                        log.info(
                            "[%s] cleaned=%d pending=%s alerts=%d",
                            r.get('client', '?'),
                            len(cleaned),
                            pending,
                            alerts,
                        )
            except Exception as e:
                log.error("Sweep error: %s", e)

            time.sleep(self.interval_sec)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    worker = BinanceOrphanOrderCleanupWorker()
    worker.run_forever()


if __name__ == "__main__":
    main()
