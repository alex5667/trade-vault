#!/usr/bin/env python3
from __future__ import annotations

from utils.time_utils import get_ny_time_millis
from core.redis_keys import RedisStreams as RS

"""Periodic Binance dust-position cleanup worker.

Goal
----
Find residual futures positions ("tails" / "dust") that remain after
close/resize/reversal paths and proactively flatten them from exchange truth.

Safety contract
---------------
* Only acts on positions whose current notional or isolated margin is below the
  configured dust thresholds.
* Requires N consecutive sweep confirmations before submitting reduce-only exit.
* Exit quantity always comes from live `positionRisk`, not local state.
* Cancels both plain and algo orders before retrying the reduce-only market exit.
* Verifies `flat + no plain orders + no algo orders` after each attempt.

This worker intentionally operates outside the normal order execution queue so it
can clean stuck residuals even when the original close/resize/reconcile path has
already finished.
"""

import json
import logging
import math
import os
import socket
import sys
import time
from dataclasses import dataclass
from typing import Any
import contextlib

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
    from services.execution_metrics import (
        EXECUTION_DUST_CLEANUP_TOTAL,
        EXECUTION_DUST_RESIDUAL_QTY,
        EXECUTION_DUST_SWEEP_CANDIDATES,
        EXECUTION_DUST_SWEEP_COOLDOWN_REMAINING_SEC,
        EXECUTION_DUST_SWEEP_LAST_RUN_TS,
        EXECUTION_DUST_SWEEP_SKIP_TOTAL,
        EXECUTION_DUST_SWEEP_TOTAL,
        EXECUTION_FORCE_FLAT_VERIFY_TOTAL,
    )
except Exception:  # pragma: no cover
    try:
        from execution_metrics import (  # type: ignore
            EXECUTION_DUST_CLEANUP_TOTAL,
            EXECUTION_DUST_RESIDUAL_QTY,
            EXECUTION_DUST_SWEEP_CANDIDATES,
            EXECUTION_DUST_SWEEP_COOLDOWN_REMAINING_SEC,
            EXECUTION_DUST_SWEEP_LAST_RUN_TS,
            EXECUTION_DUST_SWEEP_SKIP_TOTAL,
            EXECUTION_DUST_SWEEP_TOTAL,
            EXECUTION_FORCE_FLAT_VERIFY_TOTAL,
        )
    except Exception:  # pragma: no cover
        EXECUTION_DUST_CLEANUP_TOTAL = None  # type: ignore
        EXECUTION_DUST_RESIDUAL_QTY = None  # type: ignore
        EXECUTION_DUST_SWEEP_CANDIDATES = None  # type: ignore
        EXECUTION_DUST_SWEEP_COOLDOWN_REMAINING_SEC = None  # type: ignore
        EXECUTION_DUST_SWEEP_LAST_RUN_TS = None  # type: ignore
        EXECUTION_DUST_SWEEP_SKIP_TOTAL = None  # type: ignore
        EXECUTION_DUST_SWEEP_TOTAL = None  # type: ignore
        EXECUTION_FORCE_FLAT_VERIFY_TOTAL = None  # type: ignore


log = logging.getLogger("binance_dust_cleanup_worker")


def _is_429_error(exc: Exception) -> bool:
    """Return True if the exception chain contains an HTTP 429 rate-limit error."""
    cur: BaseException | None = exc
    while cur is not None:
        if isinstance(cur, BinanceAPIError) and int(getattr(cur, 'status', 0) or 0) == 429:
            return True
        cur = cur.__cause__ if cur.__cause__ is not cur else None
    return False


def _is_network_error(exc: Exception) -> bool:
    """Return True if the exception chain contains a DNS / connectivity error."""
    cur: BaseException | None = exc
    while cur is not None:
        if isinstance(cur, BinanceAPIError):
            code = (cur.payload or {}).get('code', '') if isinstance(cur.payload, dict) else ''
            if code in ('dns_resolve_failed', 'connection_refused', 'transport_timeout'):
                return True
        if isinstance(cur, (socket.gaierror, ConnectionRefusedError, ConnectionResetError)):
            return True
        cur = cur.__cause__ if cur.__cause__ is not cur else None
    return False


def _is_1021_error(exc: Exception) -> bool:
    """Return True if the exception chain contains a Binance -1021 timestamp error."""
    cur: BaseException | None = exc
    while cur is not None:
        if isinstance(cur, BinanceAPIError) and isinstance(getattr(cur, 'payload', None), dict):
            if int(cur.payload.get('code', 0) or 0) == -1021:
                return True
        cur = cur.__cause__ if cur.__cause__ is not cur else None
    return False


def _bool_env(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return bool(default)
    return str(raw).strip().lower() in {"1", "true", "yes", "on", "y"}


def _f(v: Any, default: float = 0.0) -> float:
    """Safe float cast with fallback."""
    try:
        if v is None:
            return default
        return float(v)
    except Exception:
        return default


def _now_ms() -> int:
    return get_ny_time_millis()


def _load_symbol_set(raw: str) -> set[str]:
    """Parse comma-separated symbol set (allowlist or denylist) from env."""
    return {part.strip().upper() for part in (raw or '').split(',') if part.strip()}


def _step_decimals(step: float) -> int:
    """Number of meaningful decimal places for a given step size."""
    if step <= 0:
        return 8
    s = f"{step:.16f}".rstrip('0')
    if '.' not in s:
        return 0
    return len(s.split('.', 1)[1])


def _position_side_for_mode(position_mode: str, logical_side: str | None) -> str | None:
    """Return positionSide param value for hedge mode; None for one-way mode."""
    if (position_mode or '').strip().lower() != 'hedge':
        return None
    side = (logical_side or '').upper().strip()
    if side in {'LONG', 'SHORT'}:
        return side
    return None


@dataclass
class SymbolFilter:
    """LOT_SIZE constraints cached per symbol."""
    step_size: float = 0.0
    min_qty: float = 0.0


class BinanceDustCleanupWorker:
    """Periodic worker that sweeps positionRisk for dust/tail positions and flattens them.

    Workflow per sweep cycle:
    1. Fetch full positionRisk snapshot.
    2. Tag each row as dust if notional <= dust_notional_usdt OR margin <= dust_margin_usdt.
    3. Require confirm_passes consecutive sweeps before acting (avoids false positives
       from transient fluctuations between update cycles).
    4. For confirmed dust: cancel all orders → market reduceOnly exit → verify flat.
    5. Emit Prometheus metrics + orders:exec stream events.
    """

    def __init__(
        self,
        *,
        client: BinanceFuturesClient | None = None,
        redis_client: Any = None,
        interval_sec: float | None = None,
        confirm_passes: int | None = None,
        dust_notional_usdt: float | None = None,
        dust_margin_usdt: float | None = None,
        close_retries: int | None = None,
        verify_timeout_ms: int | None = None,
        verify_poll_ms: int | None = None,
        allowlist: set[str] | None = None,
        denylist: set[str] | None = None,
        cooldown_sec: int | None = None,
        error_cooldown_sec: int | None = None,
    ) -> None:
        self.client = client or BinanceFuturesClient.from_env()
        if redis_client is not None:
            self.r = redis_client
        elif redis is not None and os.getenv('REDIS_URL'):
            self.r = redis.from_url(
                os.getenv('REDIS_URL', 'redis://localhost:6379/0'),
                decode_responses=True,
            )
        else:
            self.r = None
        self.exec_stream = os.getenv('EXEC_STREAM', RS.ORDERS_EXEC)
        self.exec_stream_maxlen = max(0, int(os.getenv('EXEC_STREAM_MAXLEN', '0') or '0')) or None
        self.enabled = _bool_env('BINANCE_DUST_SWEEP_ENABLE', True)
        self.interval_sec = float(
            interval_sec if interval_sec is not None
            else os.getenv('BINANCE_DUST_SWEEP_INTERVAL_SEC', '15')
        )
        self.confirm_passes = max(
            1,
            int(confirm_passes if confirm_passes is not None
                else os.getenv('BINANCE_DUST_SWEEP_CONFIRM_PASSES', '2')),
        )
        self.dust_notional_usdt = float(
            dust_notional_usdt if dust_notional_usdt is not None
            else os.getenv('BINANCE_DUST_NOTIONAL_USDT', '3.0')
        )
        self.dust_margin_usdt = float(
            dust_margin_usdt if dust_margin_usdt is not None
            else os.getenv('BINANCE_DUST_MARGIN_USDT', '1.0')
        )
        self.close_retries = max(
            1,
            int(close_retries if close_retries is not None
                else os.getenv('BINANCE_DUST_CLOSE_RETRIES', '3')),
        )
        self.verify_timeout_ms = max(
            250,
            int(verify_timeout_ms if verify_timeout_ms is not None
                else os.getenv('BINANCE_DUST_VERIFY_TIMEOUT_MS', '3000')),
        )
        self.verify_poll_ms = max(
            100,
            int(verify_poll_ms if verify_poll_ms is not None
                else os.getenv('BINANCE_DUST_VERIFY_POLL_MS', '250')),
        )
        self.position_mode = (os.getenv('BINANCE_POSITION_MODE') or 'oneway').strip().lower()
        self.allowlist = {s.upper() for s in (allowlist or _load_symbol_set(os.getenv('BINANCE_SYMBOL_ALLOWLIST', '')))}
        # Static denylist from ENV (comma-separated); also supports dynamic Redis set and per-key overrides.
        self.denylist = {s.upper() for s in (denylist or _load_symbol_set(os.getenv('BINANCE_DUST_SWEEP_DENYLIST', '')))}
        # Cooldown duration (seconds) applied after a successful cleanup to avoid hammer-retrying the same symbol.
        self.cooldown_sec = max(0, int(cooldown_sec if cooldown_sec is not None else os.getenv('BINANCE_DUST_SWEEP_COOLDOWN_SEC', '300')) )
        # Shorter cooldown applied after a cleanup error so the worker backs off briefly.
        self.error_cooldown_sec = max(0, int(error_cooldown_sec if error_cooldown_sec is not None else os.getenv('BINANCE_DUST_SWEEP_ERROR_COOLDOWN_SEC', str(self.cooldown_sec or 60))) )
        # Redis key names for dynamic denylist and cooldown entries.
        self.dynamic_denylist_set_key = os.getenv('BINANCE_DUST_SWEEP_DENYLIST_SET_KEY', 'orders:dust_cleanup:denylist')
        self.dynamic_denylist_prefix = os.getenv('BINANCE_DUST_SWEEP_DENYLIST_PREFIX', 'orders:dust_cleanup:denylist:')
        self.cooldown_prefix = os.getenv('BINANCE_DUST_SWEEP_COOLDOWN_PREFIX', 'orders:dust_cleanup:cooldown:')
        # Per-symbol LOT_SIZE cache to avoid repeated exchange info calls.
        self._filters: dict[str, SymbolFilter] = {}
        # Consecutive dust observation counter — key is symbol, value is passes seen.
        self._confirm_seen: dict[str, int] = {}
        # In-process cooldown fallback when Redis is unavailable.
        self._cooldown_local_until_ms: dict[str, int] = {}

    # -----------------------------------------------------------------------
    # Internal metrics helpers
    # -----------------------------------------------------------------------

    def _metric_inc(self, symbol: str, result: str) -> None:
        """Increment the per-symbol sweep outcome counter (safe, never raises)."""
        if EXECUTION_DUST_SWEEP_TOTAL is None:
            return
        with contextlib.suppress(Exception):
            EXECUTION_DUST_SWEEP_TOTAL.labels(symbol=symbol, result=result).inc()

    def _skip_metric_inc(self, symbol: str, reason: str) -> None:
        """Increment the skip counter for denylist/cooldown/error skip reasons."""
        if EXECUTION_DUST_SWEEP_SKIP_TOTAL is None:
            return
        with contextlib.suppress(Exception):
            EXECUTION_DUST_SWEEP_SKIP_TOTAL.labels(symbol=symbol, reason=reason).inc()

    def _set_cooldown_metric(self, symbol: str, seconds: float) -> None:
        """Update the cooldown-remaining gauge for the given symbol."""
        if EXECUTION_DUST_SWEEP_COOLDOWN_REMAINING_SEC is None:
            return
        with contextlib.suppress(Exception):
            EXECUTION_DUST_SWEEP_COOLDOWN_REMAINING_SEC.labels(symbol=symbol).set(max(0.0, float(seconds)))

    def _cooldown_key(self, symbol: str) -> str:
        """Return the Redis key used to store the per-symbol cleanup cooldown."""
        return f"{self.cooldown_prefix}{symbol.upper().strip()}"

    def _dynamic_denylist_key(self, symbol: str) -> str:
        """Return the Redis key for a per-symbol denylist override."""
        return f"{self.dynamic_denylist_prefix}{symbol.upper().strip()}"

    def _is_symbol_denylisted(self, symbol: str) -> bool:
        """Return True if symbol is blocked via static env denylist, Redis set, or per-key override."""
        target = (symbol or '').upper().strip()
        if not target:
            return False
        # 1. Static env denylist (loaded at startup from BINANCE_DUST_SWEEP_DENYLIST).
        if target in self.denylist:
            return True
        if self.r is None:
            return False
        # 2. Dynamic Redis set (SISMEMBER orders:dust_cleanup:denylist <symbol>).
        try:
            if hasattr(self.r, 'sismember') and self.r.sismember(self.dynamic_denylist_set_key, target):
                return True
        except Exception:
            pass
        # 3. Per-key Redis override (orders:dust_cleanup:denylist:<SYMBOL> != falsy).
        try:
            raw = self.r.get(self._dynamic_denylist_key(target))
            if raw not in (None, '', '0', 'false', 'False'):
                return True
        except Exception:
            pass
        return False

    def _cooldown_remaining_sec(self, symbol: str) -> int:
        """Return remaining cooldown seconds for the symbol (0 means no active cooldown)."""
        target = (symbol or '').upper().strip()
        if not target:
            return 0
        # Prefer Redis TTL-based check (most accurate after restarts).
        if self.r is not None:
            key = self._cooldown_key(target)
            try:
                if hasattr(self.r, 'pttl'):
                    ttl_ms = int(self.r.pttl(key) or -2)
                    if ttl_ms > 0:
                        rem = int(math.ceil(ttl_ms / 1000.0))
                        self._set_cooldown_metric(target, rem)
                        return rem
            except Exception:
                pass
            # Fallback: value encodes until_ms JSON payload (for Redis instances lacking TTL).
            try:
                raw = self.r.get(key)
                if raw:
                    data = json.loads(raw) if str(raw).startswith('{') else {'until_ms': int(raw)}
                    until_ms = int(data.get('until_ms') or 0)
                    rem_ms = until_ms - _now_ms()
                    if rem_ms > 0:
                        rem = int(math.ceil(rem_ms / 1000.0))
                        self._set_cooldown_metric(target, rem)
                        return rem
            except Exception:
                pass
        # Final fallback: in-process dict (survives only within same process lifetime).
        until_ms = int(self._cooldown_local_until_ms.get(target, 0) or 0)
        rem_ms = until_ms - _now_ms()
        if rem_ms > 0:
            rem = int(math.ceil(rem_ms / 1000.0))
            self._set_cooldown_metric(target, rem)
            return rem
        self._cooldown_local_until_ms.pop(target, None)
        self._set_cooldown_metric(target, 0.0)
        return 0

    def _set_cooldown(self, symbol: str, *, seconds: int, reason: str) -> None:
        """Activate cleanup cooldown for the symbol; persists to Redis when available."""
        target = (symbol or '').upper().strip()
        if not target or int(seconds or 0) <= 0:
            self._set_cooldown_metric(target, 0.0)
            return
        until_ms = _now_ms() + (int(seconds) * 1000)
        payload = json.dumps({'until_ms': until_ms, 'reason': reason}, ensure_ascii=False, separators=(",", ":"))
        key = self._cooldown_key(target)
        if self.r is not None:
            try:
                if hasattr(self.r, 'setex'):
                    self.r.setex(key, int(seconds), payload)
                else:
                    self.r.set(key, payload)
            except Exception:
                # If Redis write fails, fall back to in-process dict so cooldown is still respected.
                self._cooldown_local_until_ms[target] = until_ms
        else:
            self._cooldown_local_until_ms[target] = until_ms
        self._set_cooldown_metric(target, float(seconds))

    def _clear_cooldown(self, symbol: str) -> None:
        """Remove cooldown when symbol leaves dust set or exits the allowlist."""
        target = (symbol or '').upper().strip()
        self._cooldown_local_until_ms.pop(target, None)
        if self.r is not None:
            with contextlib.suppress(Exception):
                self.r.delete(self._cooldown_key(target))
        self._set_cooldown_metric(target, 0.0)

    def _emit_exec_event(
        self, *, symbol: str, event_type: str, status: str, payload: dict[str, Any]
    ) -> None:
        """Write a structured event to the orders:exec Redis stream."""
        if self.r is None:
            return
        fields = {
            'sid': '',
            'symbol': symbol,
            'action': 'dust_cleanup',
            'event_type': str(event_type),
            'status': str(status),
            'ts_event_ms': str(_now_ms()),
            'ts_ms': str(_now_ms()),
            'payload_json': json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
        }
        try:
            kwargs: dict[str, Any] = {}
            if self.exec_stream_maxlen:
                kwargs = {'maxlen': self.exec_stream_maxlen, 'approximate': True}
            self.r.xadd(self.exec_stream, fields, **kwargs, maxlen=50000)
        except Exception:
            pass

    # -----------------------------------------------------------------------
    # Exchange-info / filter helpers
    # -----------------------------------------------------------------------

    def _load_symbol_filter(self, symbol: str) -> SymbolFilter:
        """Fetch and cache the LOT_SIZE filter for the given symbol."""
        target = (symbol or '').upper().strip()
        cached = self._filters.get(target)
        if cached is not None:
            return cached
        info = self.client.get_exchange_info() or {}
        filt = SymbolFilter(step_size=0.0, min_qty=0.0)
        for row in list(info.get('symbols') or []):
            if (row.get('symbol') or '').upper().strip() != target:
                continue
            for fr in list(row.get('filters') or []):
                ftype = (fr.get('filterType') or '').upper().strip()
                if ftype == 'LOT_SIZE':
                    filt.step_size = _f(fr.get('stepSize'), 0.0)
                    filt.min_qty = _f(fr.get('minQty'), 0.0)
                    break
            break
        self._filters[target] = filt
        return filt

    def _qty_tolerance(self, symbol: str) -> float:
        """Return the 'flat' comparison tolerance — half a step size."""
        sf = self._load_symbol_filter(symbol)
        if sf.step_size > 0:
            return max(sf.step_size / 2.0, 1e-12)
        return 1e-12

    def _quantize_qty(self, symbol: str, qty: float) -> float:
        """Round qty down to the nearest valid step size."""
        qty = abs(_f(qty, 0.0))
        sf = self._load_symbol_filter(symbol)
        step = float(sf.step_size or 0.0)
        if step <= 0.0 or qty <= 0.0:
            return qty
        quant = math.floor((qty / step) + 1e-12) * step
        if quant <= 0.0:
            quant = qty
        decimals = _step_decimals(step)
        return float(f"{quant:.{decimals}f}")

    # -----------------------------------------------------------------------
    # Live exposure snapshot
    # -----------------------------------------------------------------------

    def _build_live_exposure(
        self, symbol: str, row: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        """Fetch current position risk + open order counts for a symbol from the exchange."""
        row = dict(row or self.client.get_symbol_position_risk(symbol) or {})
        signed_qty = _f(row.get('positionAmt'), 0.0)
        abs_qty = abs(signed_qty)
        logical_side: str | None = None
        if signed_qty > 0:
            logical_side = 'LONG'
        elif signed_qty < 0:
            logical_side = 'SHORT'
        margin = abs(_f(row.get('isolatedMargin'), 0.0) or _f(row.get('initialMargin'), 0.0))
        notional = abs(_f(row.get('notional'), 0.0))
        plain_orders = list(self.client.get_open_orders(symbol) or [])
        algo_orders = list(self.client.get_open_algo_orders(symbol) or [])
        tol = self._qty_tolerance(symbol)
        return {
            'symbol': symbol,
            'signed_qty': signed_qty,
            'abs_qty': abs_qty,
            'logical_side': logical_side,
            'notional_usdt': notional,
            'margin_usdt': margin,
            'open_plain_orders': len(plain_orders),
            'open_algo_orders': len(algo_orders),
            'plain_order_refs': plain_orders,
            'algo_order_refs': algo_orders,
            'qty_tolerance': tol,
            # Flat with zero qty
            'is_flat_qty': math.isclose(abs_qty, 0.0, abs_tol=tol),
            # Truly flat: zero qty AND no lingering orders
            'is_flat': (
                math.isclose(abs_qty, 0.0, abs_tol=tol)
                and len(plain_orders) == 0
                and len(algo_orders) == 0
            ),
        }

    def _is_dust_position(self, snapshot: dict[str, Any]) -> bool:
        """Return True if the snapshot has size but is below the configured dust thresholds."""
        qty = abs(_f(snapshot.get('abs_qty'), 0.0))
        if qty <= 0.0:
            return False
        margin = abs(_f(snapshot.get('margin_usdt'), 0.0))
        notional = abs(_f(snapshot.get('notional_usdt'), 0.0))
        return bool(
            (margin > 0.0 and margin <= self.dust_margin_usdt)
            or (notional > 0.0 and notional <= self.dust_notional_usdt)
        )

    # -----------------------------------------------------------------------
    # Order cancellation
    # -----------------------------------------------------------------------

    def _cancel_all_symbol_orders_best_effort(
        self, symbol: str, exposure: dict[str, Any]
    ) -> dict[str, Any]:
        """Cancel all plain + algo orders for the symbol; errors are swallowed."""
        plain_orders = list(exposure.get('plain_order_refs') or [])
        algo_orders = list(exposure.get('algo_order_refs') or [])
        canceled_plain = 0
        canceled_algo = 0
        # Bulk cancel first (best-effort — exchange might reject if already empty)
        with contextlib.suppress(Exception):
            self.client.cancel_all_orders(symbol)
        # Individual fallback to handle partial cancels
        for row in plain_orders:
            try:
                oid = row.get('orderId')
                cid = str(row.get('clientOrderId') or row.get('origClientOrderId') or '').strip() or None
                if oid is not None:
                    self.client.cancel_plain_order(symbol, order_id=int(oid))
                    canceled_plain += 1
                elif cid:
                    self.client.cancel_plain_order(symbol, client_order_id=cid)
                    canceled_plain += 1
            except Exception:
                continue
        for row in algo_orders:
            try:
                oid = row.get('algoId')
                cid = (row.get('clientAlgoId') or '').strip() or None
                if oid is not None:
                    self.client.cancel_algo_order(symbol, algo_id=int(oid))
                    canceled_algo += 1
                elif cid:
                    self.client.cancel_algo_order(symbol, client_algo_id=cid)
                    canceled_algo += 1
            except Exception:
                continue
        return {
            'plain_seen': len(plain_orders),
            'algo_seen': len(algo_orders),
            'plain_canceled': canceled_plain,
            'algo_canceled': canceled_algo,
        }

    # -----------------------------------------------------------------------
    # Order submission / verify
    # -----------------------------------------------------------------------

    def _make_client_order_id(self, symbol: str, attempt: int) -> str:
        """Unique clientOrderId for dust cleanup orders — max 36 chars."""
        base = f"dust_{symbol.lower()[:8]}_{get_ny_time_millis() % 100000000}_{attempt}"
        return base[:36]

    def _submit_reduce_only_market_exit(
        self, *, symbol: str, logical_side: str, qty: float, attempt: int
    ) -> dict[str, Any]:
        """Place a MARKET reduceOnly order for the given quantity and side."""
        exit_side = 'SELL' if str(logical_side).upper() == 'LONG' else 'BUY'
        q_close = self._quantize_qty(symbol, qty)
        params: dict[str, Any] = {
            'symbol': symbol,
            'side': exit_side,
            'type': 'MARKET',
            'quantity': q_close,
            'newClientOrderId': self._make_client_order_id(symbol, attempt),
            'newOrderRespType': 'RESULT',
        }
        pos_side = _position_side_for_mode(self.position_mode, logical_side)
        if self.position_mode == 'oneway':
            params['reduceOnly'] = True
        elif pos_side:
            params['positionSide'] = pos_side
        j = self.client.post_plain_order(params)
        return {
            'close_order_id': j.get('orderId'),
            'close_client_id': params['newClientOrderId'],
            'close_order_status': j.get('status'),
            'qty': q_close,
            'side': exit_side,
        }

    def _verify_symbol_flat(self, symbol: str) -> dict[str, Any]:
        """Poll positionRisk until flat or timeout; updates EXECUTION_DUST_RESIDUAL_QTY metric."""
        deadline = time.time() + (self.verify_timeout_ms / 1000.0)
        last = self._build_live_exposure(symbol)
        while time.time() <= deadline:
            last = self._build_live_exposure(symbol)
            if EXECUTION_DUST_RESIDUAL_QTY is not None:
                with contextlib.suppress(Exception):
                    EXECUTION_DUST_RESIDUAL_QTY.labels(symbol=symbol).set(
                        float(last.get('abs_qty') or 0.0)
                    )
            if last.get('is_flat'):
                if EXECUTION_FORCE_FLAT_VERIFY_TOTAL is not None:
                    with contextlib.suppress(Exception):
                        EXECUTION_FORCE_FLAT_VERIFY_TOTAL.labels(symbol=symbol, result='flat').inc()
                return last
            time.sleep(self.verify_poll_ms / 1000.0)
        # Timed out — classify residual
        if EXECUTION_FORCE_FLAT_VERIFY_TOTAL is not None:
            try:
                result = 'dust' if self._is_dust_position(last) else 'residual'
                EXECUTION_FORCE_FLAT_VERIFY_TOTAL.labels(symbol=symbol, result=result).inc()
            except Exception:
                pass
        return last

    # -----------------------------------------------------------------------
    # Per-symbol cleanup
    # -----------------------------------------------------------------------

    def _cleanup_symbol(self, symbol: str, row: dict[str, Any]) -> dict[str, Any]:
        """Execute the full cancel → exit → verify sequence for one symbol.

        Always reads live qty from exchange before each attempt so we never
        submit a stale quantity. Returns a structured result document.
        """
        attempts: list[dict[str, Any]] = []

        # Quick pre-check from the sweep row — may have already become flat
        verify = self._build_live_exposure(symbol, row=row)
        if verify.get('is_flat'):
            result = {'symbol': symbol, 'status': 'already_flat', 'verify': verify, 'attempts': attempts}
            self._metric_inc(symbol, 'already_flat')
            return result

        for attempt in range(1, self.close_retries + 1):
            # Re-fetch live state before every attempt
            live = self._build_live_exposure(symbol)
            verify = live
            if live.get('is_flat'):
                break
            live_qty = float(live.get('abs_qty') or 0.0)
            live_side = (live.get('logical_side') or '').upper().strip()
            if live_qty <= 0.0 or live_side not in {'LONG', 'SHORT'}:
                # Position vanished or side indeterminate — stop
                break
            # Cancel pending orders so reduceOnly doesn't conflict
            canceled = self._cancel_all_symbol_orders_best_effort(symbol, live)
            close = self._submit_reduce_only_market_exit(
                symbol=symbol, logical_side=live_side, qty=live_qty, attempt=attempt
            )
            verify = self._verify_symbol_flat(symbol)
            attempts.append({
                'attempt': attempt,
                'before_qty': live_qty,
                'before_side': live_side,
                'before_is_dust': self._is_dust_position(live),
                'canceled': canceled,
                'close': close,
                'verify': {
                    'abs_qty': float(verify.get('abs_qty') or 0.0),
                    'notional_usdt': float(verify.get('notional_usdt') or 0.0),
                    'margin_usdt': float(verify.get('margin_usdt') or 0.0),
                    'open_plain_orders': int(verify.get('open_plain_orders') or 0),
                    'open_algo_orders': int(verify.get('open_algo_orders') or 0),
                    'is_flat': bool(verify.get('is_flat')),
                    'is_dust': self._is_dust_position(verify),
                },
            })
            if verify.get('is_flat'):
                break

        status = (
            'closed' if verify.get('is_flat')
            else ('dust_remaining' if self._is_dust_position(verify) else 'residual_position')
        )
        if EXECUTION_DUST_CLEANUP_TOTAL is not None:
            with contextlib.suppress(Exception):
                EXECUTION_DUST_CLEANUP_TOTAL.labels(symbol=symbol, result=status).inc()
        self._metric_inc(symbol, status)
        doc = {
            'symbol': symbol,
            'status': status,
            'attempts': attempts,
            'residual_qty': float(verify.get('abs_qty') or 0.0),
            'residual_notional_usdt': float(verify.get('notional_usdt') or 0.0),
            'residual_margin_usdt': float(verify.get('margin_usdt') or 0.0),
            'verify': verify,
        }
        self._emit_exec_event(
            symbol=symbol, event_type='dust_cleanup_worker', status=status, payload=doc
        )
        return doc

    # -----------------------------------------------------------------------
    # Public sweep API
    # -----------------------------------------------------------------------

    def sweep_once(self) -> dict[str, Any]:
        """Run one full sweep cycle. Returns a summary dict."""
        ts = time.time()
        if EXECUTION_DUST_SWEEP_LAST_RUN_TS is not None:
            with contextlib.suppress(Exception):
                EXECUTION_DUST_SWEEP_LAST_RUN_TS.set(ts)

        rows = list(self.client.get_position_risk() or [])
        current_candidates: set[str] = set()
        pending_symbols: list[str] = []
        acted: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []

        for row in rows:
            symbol = (row.get('symbol') or '').upper().strip()
            if not symbol:
                continue
            # Allowlist filter: if configured, skip symbols not in the list
            if self.allowlist and symbol not in self.allowlist:
                self._confirm_seen.pop(symbol, None)
                self._clear_cooldown(symbol)
                continue
            snapshot = {
                'symbol': symbol,
                'abs_qty': abs(_f(row.get('positionAmt'), 0.0)),
                'notional_usdt': abs(_f(row.get('notional'), 0.0)),
                'margin_usdt': abs(
                    _f(row.get('isolatedMargin'), 0.0) or _f(row.get('initialMargin'), 0.0)
                ),
            }
            if not self._is_dust_position(snapshot):
                # Symbol is no longer dust — reset confirmation counter and cooldown
                self._confirm_seen.pop(symbol, None)
                self._clear_cooldown(symbol)
                continue
            current_candidates.add(symbol)
            # Denylist check: static env denylist, Redis set, or per-key Redis override.
            # Resets confirm counter so the symbol needs fresh confirmations after the pause.
            if self._is_symbol_denylisted(symbol):
                self._confirm_seen.pop(symbol, None)
                self._skip_metric_inc(symbol, 'denylist')
                self._metric_inc(symbol, 'skip_denylist')
                doc = {
                    'symbol': symbol,
                    'skip_reason': 'denylist',
                    **snapshot,
                }
                self._emit_exec_event(symbol=symbol, event_type='dust_cleanup_skip', status='denylist', payload=doc)
                skipped.append(doc)
                continue
            # Per-symbol cooldown: prevents re-acting too soon after a previous cleanup.
            # Resets confirm counter so the symbol needs fresh confirmations after the cooldown.
            cooldown_remaining = self._cooldown_remaining_sec(symbol)
            if cooldown_remaining > 0:
                self._confirm_seen.pop(symbol, None)
                self._skip_metric_inc(symbol, 'cooldown')
                self._metric_inc(symbol, 'skip_cooldown')
                doc = {
                    'symbol': symbol,
                    'skip_reason': 'cooldown',
                    'cooldown_remaining_sec': cooldown_remaining,
                    **snapshot,
                }
                self._emit_exec_event(symbol=symbol, event_type='dust_cleanup_skip', status='cooldown', payload=doc)
                skipped.append(doc)
                continue
            # Accumulate confirmation passes to avoid acting on transient micro-positions
            seen = int(self._confirm_seen.get(symbol, 0)) + 1
            self._confirm_seen[symbol] = seen
            if seen < self.confirm_passes:
                # Not enough confirmations yet — record as pending
                pending_symbols.append(symbol)
                self._metric_inc(symbol, 'candidate_pending')
                self._emit_exec_event(
                    symbol=symbol,
                    event_type='dust_cleanup_candidate',
                    status='pending',
                    payload={
                        'symbol': symbol,
                        'confirm_seen': seen,
                        'confirm_passes': self.confirm_passes,
                        **snapshot,
                    },
                )
                continue
            # Confirmed dust — execute cleanup and apply success cooldown
            try:
                result = self._cleanup_symbol(symbol, dict(row))
                acted.append(result)
                status = (result.get('status') or '').strip() or 'unknown'
                # Arm cooldown after any real cleanup attempt (not for already_flat shortcuts)
                if status != 'already_flat' and self.cooldown_sec > 0:
                    self._set_cooldown(symbol, seconds=self.cooldown_sec, reason=status)
            except Exception as exc:
                self._metric_inc(symbol, 'error')
                self._skip_metric_inc(symbol, 'error')
                # Arm shorter error cooldown to prevent tight retry-storm on persistent errors
                if self.error_cooldown_sec > 0:
                    self._set_cooldown(symbol, seconds=self.error_cooldown_sec, reason='error')
                self._emit_exec_event(
                    symbol=symbol,
                    event_type='dust_cleanup_worker',
                    status='error',
                    payload={'symbol': symbol, 'error': str(exc), **snapshot},
                )
                acted.append({'symbol': symbol, 'status': 'error', 'error': str(exc)})

        # Prune stale confirms for symbols no longer in dust set
        for symbol in list(self._confirm_seen.keys()):
            if symbol not in current_candidates:
                self._confirm_seen.pop(symbol, None)

        if EXECUTION_DUST_SWEEP_CANDIDATES is not None:
            with contextlib.suppress(Exception):
                EXECUTION_DUST_SWEEP_CANDIDATES.set(len(current_candidates))
        return {
            'ts_ms': _now_ms(),
            'candidates': sorted(current_candidates),
            'pending': pending_symbols,
            'skipped': skipped,
            'acted': acted,
        }

    def run_forever(self) -> None:
        """Main loop — sweeps every interval_sec seconds."""
        _rate_limit_backoff_s = float(os.getenv('BINANCE_DUST_SWEEP_429_BACKOFF_SEC', '60'))
        while True:
            try:
                if self.enabled:
                    self.sweep_once()
            except Exception as exc:  # pragma: no cover
                # Extended backoff on 429 rate-limit errors — avoid hammering
                # Binance at the normal interval when we're being throttled.
                if _is_429_error(exc):
                    log.warning(
                        'dust cleanup: 429 rate-limit detected, backing off %.0fs',
                        _rate_limit_backoff_s,
                    )
                    time.sleep(max(_rate_limit_backoff_s, 1.0))
                    continue
                # Network errors (DNS / connection-refused) — back off to avoid
                # noisy 15s retry loops during prolonged outages.
                if _is_network_error(exc):
                    _net_backoff = float(os.getenv('BINANCE_DUST_SWEEP_NETWORK_BACKOFF_SEC', '30'))
                    log.warning(
                        'dust cleanup: network error (DNS/connection), backing off %.0fs',
                        _net_backoff,
                    )
                    time.sleep(max(_net_backoff, 1.0))
                    continue
                # Timestamp drift (-1021) — resync clock and retry after short backoff.
                if _is_1021_error(exc):
                    _ts_backoff = float(os.getenv('BINANCE_DUST_SWEEP_1021_BACKOFF_SEC', '5'))
                    log.warning(
                        'dust cleanup: -1021 timestamp drift, running sync_time() and backing off %.0fs',
                        _ts_backoff,
                    )
                    with contextlib.suppress(Exception):
                        self.client.sync_time()
                    time.sleep(max(_ts_backoff, 1.0))
                    continue

                log.exception('dust cleanup sweep failed: %s', exc)
            time.sleep(max(self.interval_sec, 1.0))


def main() -> int:
    logging.basicConfig(
        level=os.getenv('LOG_LEVEL', 'INFO').upper(),
        format='%(asctime)s %(levelname)s %(name)s: %(message)s',
    )
    worker = BinanceDustCleanupWorker()
    worker.run_forever()
    return 0


if __name__ == '__main__':  # pragma: no cover
    raise SystemExit(main())
