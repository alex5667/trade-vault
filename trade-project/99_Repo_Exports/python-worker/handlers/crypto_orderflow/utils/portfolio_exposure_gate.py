from __future__ import annotations

import asyncio
import inspect
import json
import logging
import os
import time
from typing import Any

import redis
from core.gates.decision import GateDecisionV1
from utils.time_utils import get_ny_time_millis

try:
    from prometheus_client import Counter
    _ORDER_DUPLICATE_BLOCKS = Counter(
        "order_intent_duplicate_block_total",
        "Number of orders blocked due to duplicate guard",
        ["symbol"]
    )
    _PORTFOLIO_GATE_DECISIONS = Counter(
        "portfolio_gate_decisions_total",
        "Portfolio gate decisions",
        ["decision", "reason_code", "symbol", "mode"]
    )
except ImportError:
    _ORDER_DUPLICATE_BLOCKS = None
    _PORTFOLIO_GATE_DECISIONS = None

log = logging.getLogger("crypto_orderflow.portfolio_gate")

_STAGE = "portfolio"
_GATE_NAME = "portfolio_exposure"


def _detect_async_redis(r: Any) -> bool:
    """Robust async Redis client detection.

    redis.asyncio.Redis methods are dynamically proxied via __getattr__,
    so ``asyncio.iscoroutinefunction(r.get)`` returns False — a known
    pitfall.  We check multiple signals:
      1. Module path of the client class contains "asyncio"
      2. asyncio.iscoroutinefunction on the .get method (works for some subclasses)
      3. Presence of ``redis.asyncio`` package and isinstance check
    """
    if r is None:
        return False

    # 1. Module-based detection (most reliable)
    mod_name = getattr(type(r), "__module__", "") or ""
    if "asyncio" in mod_name:
        return True

    # 2. iscoroutinefunction check (works for explicit async subclasses)
    get_fn = getattr(r, "get", None)
    if get_fn is not None and inspect.iscoroutinefunction(get_fn):
        return True

    # 3. isinstance check against redis.asyncio.Redis (if available)
    try:
        import redis.asyncio as aioredis
        if isinstance(r, aioredis.Redis):
            return True
    except (ImportError, AttributeError):
        pass

    return False



def _make_decision(
    decision: Any,
    reason_code: str,
    severity: Any,
    mode: str,
    ts_event_ms: int,
    latency_us: int,
    notes: dict[str, Any] | None = None,
) -> GateDecisionV1:
    return GateDecisionV1(
        stage=_STAGE,
        gate=_GATE_NAME,
        decision=decision,
        reason_code=reason_code,
        severity=severity,
        profile="portfolio",
        fail_policy="OPEN" if mode != "ENFORCE" else "CLOSED",
        ts_event_ms=ts_event_ms,
        ts_decision_ms=int(time.time() * 1000),
        latency_us=latency_us,
        inputs_hash="",
        notes=notes or {},
    )


class PortfolioExposureGate:
    """
    Evaluates real-time portfolio constraints before executing an order.
    Checks open positions, notional exposure, and duplicate orders.
    """

    def __init__(self, r: redis.Redis | None = None):
        self.r = r
        self.enabled = os.getenv("PORTFOLIO_GATE_ENABLED", "0") == "1"
        self.mode = os.getenv("PORTFOLIO_GATE_MODE", "SHADOW").upper()
        self.fail_policy = os.getenv("PORTFOLIO_GATE_FAIL_POLICY", "OPEN").upper()

        self.max_open_positions = int(os.getenv("MAX_OPEN_POSITIONS", "3"))
        self.max_total_notional = float(os.getenv("MAX_TOTAL_NOTIONAL_USD", "750.0"))
        self.max_daily_loss = float(os.getenv("MAX_DAILY_LOSS_USD", "50.0"))
        self.duplicate_window_ms = int(os.getenv("DUPLICATE_ORDER_WINDOW_MS", "60000"))
        self.beta_group_max = float(os.getenv("MAX_CORRELATED_EXPOSURE_GROUP_CRYPTO_BETA_USD", "500.0"))

        self.account_snapshot_key = os.getenv("ACCOUNT_SNAPSHOT_KEY", "account:snapshot:binance_usdtm")

        self._is_async = _detect_async_redis(r)

    def _get_max_symbol_notional(self, symbol: str) -> float:
        return float(os.getenv(f"MAX_SYMBOL_NOTIONAL_USD__{symbol}", "250.0"))

    def _record(self, dec: GateDecisionV1, symbol: str) -> None:
        if _PORTFOLIO_GATE_DECISIONS is not None:
            try:
                _PORTFOLIO_GATE_DECISIONS.labels(
                    decision=dec.decision,
                    reason_code=dec.reason_code,
                    symbol=symbol,
                    mode=self.mode,
                ).inc()
            except Exception:
                pass

    async def evaluate(
        self,
        symbol: str,
        source: str,
        side: str,
        intent_notional: float,
        ts_event_ms: int = 0,
    ) -> GateDecisionV1:
        t0 = time.monotonic()
        _ts = ts_event_ms or get_ny_time_millis()

        def _allow(reason_code: str, notes: dict | None = None) -> GateDecisionV1:
            latency = int((time.monotonic() - t0) * 1_000_000)
            dec = _make_decision("ALLOW", reason_code, "INFO", self.mode, _ts, latency, notes)
            self._record(dec, symbol)
            return dec

        def _deny(reason_code: str, notes: dict | None = None) -> GateDecisionV1:
            latency = int((time.monotonic() - t0) * 1_000_000)
            decision = "DENY" if self.mode == "ENFORCE" else "SHADOW_DENY"
            sev = "RISK" if self.mode == "ENFORCE" else "WARN"
            dec = _make_decision(decision, reason_code, sev, self.mode, _ts, latency, notes)
            self._record(dec, symbol)
            return dec

        if not self.enabled:
            return _allow("PORTFOLIO_GATE_DISABLED")

        if not self.r:
            log.warning("PortfolioExposureGate has no Redis client; failing open.")
            return _allow("PORTFOLIO_REDIS_UNAVAILABLE")

        now_ms = get_ny_time_millis()

        # 1. Duplicate Order Check
        dup_key = f"portfolio:duplicate_guard:{symbol}:{source}:{side}"
        try:
            if self._is_async:
                is_new = await self.r.set(dup_key, str(now_ms), px=self.duplicate_window_ms, nx=True)
            else:
                is_new = await asyncio.to_thread(
                    self.r.set, dup_key, str(now_ms), px=self.duplicate_window_ms, nx=True
                )
        except Exception as e:
            log.warning("PortfolioExposureGate duplicate check failed: %s", e)
            is_new = True  # fail-open on duplicate check errors
        if not is_new:
            if _ORDER_DUPLICATE_BLOCKS is not None:
                try:
                    _ORDER_DUPLICATE_BLOCKS.labels(symbol=symbol).inc()
                except Exception:
                    pass
            if self.mode == "ENFORCE":
                return _deny("PORTFOLIO_DUPLICATE_ORDER", {"symbol": symbol, "source": source})
            log.debug("[SHADOW] Would reject duplicate order for %s", symbol)

        # 2. Portfolio Snapshot check
        try:
            if self._is_async:
                snap_raw = await self.r.get(self.account_snapshot_key)
            else:
                snap_raw = await asyncio.to_thread(self.r.get, self.account_snapshot_key)
            if snap_raw:
                snap_str = snap_raw.decode("utf-8") if isinstance(snap_raw, bytes) else str(snap_raw)
                snap_str = snap_str.strip()
                if snap_str:
                    snap = json.loads(snap_str)
                    open_pos_n = snap.get("open_positions_n", 0)
                    open_notional = snap.get("open_notional_usdt", 0.0)

                    if open_pos_n >= self.max_open_positions:
                        if self.mode == "ENFORCE":
                            return _deny("PORTFOLIO_MAX_POSITIONS_EXCEEDED",
                                         {"open_positions": open_pos_n, "limit": self.max_open_positions})
                        log.debug("[SHADOW] Would reject MAX_POSITIONS: %d >= %d", open_pos_n, self.max_open_positions)

                    if (open_notional + intent_notional) > self.max_total_notional:
                        if self.mode == "ENFORCE":
                            return _deny("PORTFOLIO_TOTAL_NOTIONAL_EXCEEDED",
                                         {"total": open_notional + intent_notional, "limit": self.max_total_notional})
                        log.debug("[SHADOW] Would reject MAX_TOTAL_NOTIONAL: %.2f > %.2f",
                                  open_notional + intent_notional, self.max_total_notional)

                    positions = snap.get("positions", [])
                    symbol_notional = sum(p.get("notional", 0.0) for p in positions if p.get("symbol") == symbol)
                    max_sym_notional = self._get_max_symbol_notional(symbol)
                    if (abs(symbol_notional) + intent_notional) > max_sym_notional:
                        if self.mode == "ENFORCE":
                            return _deny("PORTFOLIO_SYMBOL_NOTIONAL_EXCEEDED",
                                         {"symbol_total": abs(symbol_notional) + intent_notional, "limit": max_sym_notional})
                        log.debug("[SHADOW] Would reject SYMBOL_NOTIONAL: %.2f > %.2f",
                                  abs(symbol_notional) + intent_notional, max_sym_notional)

                    upnl = snap.get("unrealized_pnl", 0.0)
                    if upnl < -self.max_daily_loss:
                        if self.mode == "ENFORCE":
                            return _deny("PORTFOLIO_DAILY_LOSS_EXCEEDED",
                                         {"upnl": upnl, "limit": -self.max_daily_loss})
                        log.debug("[SHADOW] Would reject DAILY_LOSS: %.2f < -%.2f", upnl, self.max_daily_loss)

        except Exception as e:
            log.error("PortfolioExposureGate failed to read snapshot: %s", e)
            # fail-closed only in ENFORCE mode regardless of fail_policy
            if self.fail_policy == "CLOSED" and self.mode == "ENFORCE":
                return _deny("PORTFOLIO_GATE_ERROR", {"error": str(e)})

        return _allow("PORTFOLIO_APPROVED")
