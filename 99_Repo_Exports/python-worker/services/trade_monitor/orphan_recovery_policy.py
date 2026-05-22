# services/trade_monitor/orphan_recovery_policy.py
"""
ORPHAN_RECOVERY policy — SRE/infrastructure emergency layer.

Handles ORPHAN_TIMEOUT / ORPHAN_TIMEOUT_STALE_PRICE / ORPHAN_TIMEOUT_NO_PRICE
events.  Runs in a background housekeep thread (not on every tick).

**Critical design invariant** (enforced here, not in CloseDetector):

    TIME_EXIT_*       ← trading logic layer   (on_tick, current price)
    ORPHAN_RECOVERY_* ← SRE/infra layer       (housekeep, stale price OK)

Extracted from TradeMonitorService methods:
  _collect_orphan_closures     (monolith lines 3380-3534)
  _finalize_orphan_closures    (3536-3625)
  _housekeep_expired_positions (3663-3899)
  _housekeep_loop              (3645-3661)
  _cleanup_stale_prices        (3628-3643)
  _is_orphan_expired           (inline in collect, + helper _resolve_orphan_ttl_ms)
  _resolve_orphan_ttl_ms       (1237-1279)
  _is_plausible_epoch_ms       (inline validation)
  _is_grace_period_active      (inline validation)

Design:
  - OrphanRecoveryPolicy is a self-contained policy engine.
  - It receives all external state via injected callables (callbacks) so it
    has no direct coupling to TradeMonitorService internals.
  - All methods are fail-open.
  - I/O (repo writes, analytics DB, stats) happens OUTSIDE any global lock —
    callbacks are invoked after positions are evicted from memory.
"""
from __future__ import annotations

import contextlib
import logging
import os
import threading
from dataclasses import asdict
from typing import Any, Callable

from utils.time_utils import get_ny_time_millis

logger = logging.getLogger(__name__)

# Reasonable epoch-ms range: 2020-01-01 .. 2038-01-01
_MIN_PLAUSIBLE_MS = 1_577_836_800_000
_MAX_PLAUSIBLE_MS = 2_145_916_800_000


def _is_plausible_epoch_ms(ts_ms: int) -> bool:
    return _MIN_PLAUSIBLE_MS <= ts_ms <= _MAX_PLAUSIBLE_MS


class OrphanRecoveryPolicy:
    """
    SRE-layer orphan detection and forced closure.

    Injected callbacks (all optional — methods become no-ops if missing):
      get_open_positions_fn   — () -> dict[str, PositionState]
      get_shards_fn           — () -> dict[str, dict[str, PositionState]]
      pop_pos_fn              — (pos_id) — removes position from memory/indexes.
      fsm_transition_fn       — (pos, state, *, trigger, reason, price?, ts_ms?)
      get_spec_fn             — (symbol) -> SymbolSpec
      get_price_fn            — (symbol) -> tuple[int, float] | None
      commission_adj_exit_fn  — (entry_px, direction, spec) -> float
      finalize_trade_fn       — (pos, spec, exit_price, exit_ts_ms, close_reason_raw, tp_ratios)
      persist_closed_fn       — (closed, pos_dict, closed_dict) — repo + DB + stats
      append_event_fn         — (event) — Redis stream write
      emit_ab_closed_fn       — (pos, closed, reason) — AB event logging
      stamp_meta_fn           — (pos, closed, reason) — metadata stamping
      trigger_report_fn       — (source, symbol, counter_type, order_id)
      orphans_metric_fn       — (symbol) -> Counter label — calls .inc()
      cleanup_duration_fn     — (ms) — gauge setter
      tp_ratios               — list[float] for finalize_trade
      housekeep_interval_ms   — polling interval (default 30 000 ms)
      orphan_max_price_age_ms — max age for "fresh" last_price (default 120 000 ms)
    """

    def __init__(
        self,
        *,
        get_shards_fn: Callable | None = None,
        pop_pos_fn: Callable | None = None,
        global_lock: Any = None,
        get_symbol_lock_fn: Callable | None = None,
        fsm_transition_fn: Callable | None = None,
        get_spec_fn: Callable | None = None,
        get_price_fn: Callable | None = None,
        commission_adj_exit_fn: Callable | None = None,
        finalize_trade_fn: Callable | None = None,
        persist_closed_fn: Callable | None = None,
        append_event_fn: Callable | None = None,
        emit_ab_closed_fn: Callable | None = None,
        stamp_meta_fn: Callable | None = None,
        trigger_report_fn: Callable | None = None,
        get_last_housekeep_by_symbol_fn: Callable | None = None,
        set_last_housekeep_by_symbol_fn: Callable | None = None,
        get_last_housekeep_ms_fn: Callable | None = None,
        set_last_housekeep_ms_fn: Callable | None = None,
        cleanup_stale_prices_fn: Callable | None = None,
        orphans_metric_fn: Callable | None = None,
        cleanup_duration_fn: Callable | None = None,
        is_grace_period_active_fn: Callable | None = None,
        max_hold_scan_fn: Callable | None = None,
        tp_ratios: list[float] | None = None,
        housekeep_interval_ms: int = 30_000,
        orphan_max_price_age_ms: int = 120_000,
        smart_timeout_enabled: bool = True,
        log: logging.Logger | None = None,
    ) -> None:
        self._get_shards = get_shards_fn
        self._pop_pos = pop_pos_fn
        self._global_lock = global_lock
        self._get_symbol_lock = get_symbol_lock_fn
        self._fsm_transition = fsm_transition_fn
        self._get_spec = get_spec_fn
        self._get_price = get_price_fn
        self._commission_adj_exit = commission_adj_exit_fn
        self._finalize_trade = finalize_trade_fn
        self._persist_closed = persist_closed_fn
        self._append_event = append_event_fn
        self._emit_ab_closed = emit_ab_closed_fn
        self._stamp_meta = stamp_meta_fn
        self._trigger_report = trigger_report_fn
        self._get_last_hk_by_sym = get_last_housekeep_by_symbol_fn
        self._set_last_hk_by_sym = set_last_housekeep_by_symbol_fn
        self._get_last_hk_ms = get_last_housekeep_ms_fn
        self._set_last_hk_ms = set_last_housekeep_ms_fn
        self._cleanup_stale_prices = cleanup_stale_prices_fn
        self._orphans_metric = orphans_metric_fn
        self._cleanup_duration = cleanup_duration_fn
        self._is_grace_period_active = is_grace_period_active_fn
        self._run_max_hold_timeout_scan = max_hold_scan_fn
        self._tp_ratios = tp_ratios or [1.0]
        self._housekeep_interval_ms = housekeep_interval_ms
        self._orphan_max_price_age_ms = orphan_max_price_age_ms
        self._smart_timeout_enabled = smart_timeout_enabled
        self._logger = log or logger

        # Background thread management
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    # ------------------------------------------------------------------
    # Background thread lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Start the housekeep background thread."""
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._housekeep_loop,
            daemon=True,
            name="TMOrphanHousekeep",
        )
        self._thread.start()
        self._logger.info(
            "▶ OrphanRecoveryPolicy housekeep started (interval=%ds)",
            self._housekeep_interval_ms // 1000,
        )

    def stop(self, timeout: float = 5.0) -> None:
        """Signal background thread to stop and wait for it."""
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=timeout)

    # ------------------------------------------------------------------
    # Housekeep loop (background)
    # ------------------------------------------------------------------

    def _housekeep_loop(self) -> None:
        interval_sec = max(1.0, self._housekeep_interval_ms / 1000.0)
        self._logger.info("OrphanHousekeep thread: interval=%.1fs", interval_sec)
        while not self._stop_event.is_set():
            now_ms = get_ny_time_millis()
            start_ms = now_ms
            try:
                self.run_housekeep(now_ms)
            except Exception as e:
                self._logger.error("Housekeep loop error: %s", e)
            finally:
                duration_ms = get_ny_time_millis() - start_ms
                if callable(self._cleanup_duration):
                    with contextlib.suppress(Exception):
                        self._cleanup_duration(duration_ms)
            self._stop_event.wait(interval_sec)

    # ------------------------------------------------------------------
    # Main housekeep entry (can be called from tests directly)
    # ------------------------------------------------------------------

    def run_housekeep(
        self, now_ms: int, *, current_symbol: str | None = None
    ) -> None:
        """
        Scan all shards (or just current_symbol shard) for expired positions
        and force-close them.
        """
        # Grace period: do not run while price cache is warming up
        if callable(self._is_grace_period_active) and self._is_grace_period_active(now_ms):
            return

        if not _is_plausible_epoch_ms(int(now_ms)):
            return

        shards = self._get_shards() if callable(self._get_shards) else {}
        if not shards:
            return

        by_sym: dict[str, list[str]] = {}

        if current_symbol:
            # Sharded mode: throttle per symbol
            if callable(self._get_last_hk_by_sym):
                last_sh = self._get_last_hk_by_sym(current_symbol)
                if (now_ms - last_sh) < self._housekeep_interval_ms:
                    return
            if callable(self._set_last_hk_by_sym):
                self._set_last_hk_by_sym(current_symbol, now_ms)

            shard = shards.get(current_symbol, {})
            candidates = [
                pid for pid, pos in shard.items()
                if self._is_orphan_expired(pos, now_ms)
            ]
            if candidates:
                by_sym[current_symbol] = candidates
        else:
            # Global mode: throttle globally
            if callable(self._get_last_hk_ms):
                last_global = self._get_last_hk_ms()
                if (now_ms - last_global) < self._housekeep_interval_ms:
                    return
            if callable(self._set_last_hk_ms):
                self._set_last_hk_ms(now_ms)

            for sym, shard in shards.items():
                for pid, pos in shard.items():
                    if self._is_orphan_expired(pos, now_ms):
                        by_sym.setdefault(sym, []).append(pid)

            if callable(self._cleanup_stale_prices):
                with contextlib.suppress(Exception):
                    self._cleanup_stale_prices()

        if not by_sym:
            return

        report_triggers: list[tuple[str, str, str, bool]] = []

        for sym in sorted(by_sym.keys()):
            # Try to acquire symbol lock non-blocking (skip if busy)
            lk = None
            if callable(self._get_symbol_lock):
                lk = self._get_symbol_lock(sym)

            if lk is not None and sym != current_symbol:
                acquired = lk.acquire(blocking=False)
                if not acquired:
                    continue
                @contextlib.contextmanager
                def _manual_ctx():
                    try:
                        yield
                    finally:
                        lk.release()  # type: ignore
                ctx = _manual_ctx()  # type: ignore
            elif lk is not None:
                ctx = lk
            else:
                ctx = contextlib.nullcontext()

            with ctx:
                local_triggers = self._process_symbol_orphans(
                    sym=sym,
                    pos_ids=by_sym[sym],
                    shards=shards,
                    now_ms=now_ms,
                )
                report_triggers.extend(local_triggers)

        # Trigger reports outside all locks
        for src, sym, oid, is_virtual in report_triggers:
            if callable(self._trigger_report) and not is_virtual:
                with contextlib.suppress(Exception):
                    self._trigger_report(src, sym, "trades", oid)

        # ── Mechanism B: max-hold timeout scan ────────────────────────────
        if callable(self._run_max_hold_timeout_scan):
            with contextlib.suppress(Exception):
                self._run_max_hold_timeout_scan(now_ms)

    # ------------------------------------------------------------------
    # Per-symbol orphan processing
    # ------------------------------------------------------------------

    def _process_symbol_orphans(
        self,
        sym: str,
        pos_ids: list[str],
        shards: dict,
        now_ms: int,
    ) -> list[tuple[str, str, str, bool]]:
        """
        Force-close all expired positions for one symbol.

        Returns list of (source, symbol, order_id, is_virtual) report triggers.
        """
        from domain.models import TradeEvent
        from domain.normalizers import source_from_strategy

        io_tasks: list[Any] = []
        local_triggers: list[tuple[str, str, str, bool]] = []

        # Resolve last price for the whole symbol batch
        lp = self._get_price(sym) if callable(self._get_price) else None
        if lp and float(lp[1]) > 0:
            exit_ts_ms_default = int(lp[0])
            exit_price_default = float(lp[1])
            # Stale price check
            if (now_ms - exit_ts_ms_default) > self._orphan_max_price_age_ms:
                self._logger.info(
                    "⚠️ Stale price for %s (%dms old), using entry_price for orphan closure",
                    sym, now_ms - exit_ts_ms_default,
                )
                exit_price_default = 0.0
        else:
            exit_ts_ms_default = now_ms
            exit_price_default = 0.0

        for pos_id in pos_ids:
            shard = shards.get(sym, {})
            pos = shard.get(pos_id)
            if not pos or getattr(pos, "closed", False):
                continue
            if not self._is_orphan_expired(pos, now_ms):
                continue

            try:
                # Per-position exit price (fallback to commission-adjusted)
                pos_exit_price = exit_price_default
                pos_raw = "ORPHAN_CLEANUP_STALE_MONITOR_STATE"

                if pos_exit_price <= 0:
                    pos_raw = "ORPHAN_CLEANUP_NO_PRICE"
                    if callable(self._commission_adj_exit) and callable(self._get_spec):
                        _entry_px = float(getattr(pos, "entry_price", 0.0) or 0.0)
                        _spec = self._get_spec(str(getattr(pos, "symbol", "") or ""))
                        pos_exit_price = self._commission_adj_exit(
                            _entry_px,
                            str(getattr(pos, "direction", "LONG") or "LONG"),
                            _spec,
                        )

                # Mark closed in-memory
                pos.closed = True
                pos.exit_ts_ms = int(exit_ts_ms_default or now_ms)
                pos.exit_price = float(pos_exit_price)

                if callable(self._fsm_transition):
                    with contextlib.suppress(Exception):
                        self._fsm_transition(
                            pos, "ORPHAN_CLOSED",
                            trigger="orphan_housekeep_close",
                            reason=pos_raw,
                            price=float(pos_exit_price),
                            ts_ms=int(exit_ts_ms_default or now_ms),
                        )

                # Prometheus
                if callable(self._orphans_metric):
                    with contextlib.suppress(Exception):
                        self._orphans_metric(sym).inc()

                # Finalize trade
                spec = self._get_spec(sym) if callable(self._get_spec) else None
                if spec is None:
                    from services.pnl_math import SymbolSpec
                    spec = SymbolSpec()

                closed = None
                if callable(self._finalize_trade):
                    closed = self._finalize_trade(
                        pos,
                        spec,
                        exit_price=float(pos_exit_price),
                        exit_ts_ms=int(exit_ts_ms_default or now_ms),
                        close_reason_raw=pos_raw,
                        tp_ratios=self._tp_ratios,
                    )

                if closed is not None:
                    # Mark orphan cleanup: excluded from ML labels
                    with contextlib.suppress(Exception):
                        object.__setattr__(closed, "is_orphan_cleanup", True)
                        object.__setattr__(closed, "exclude_from_ml_labels", True)
                    if callable(self._emit_ab_closed):
                        with contextlib.suppress(Exception):
                            self._emit_ab_closed(pos, closed, pos_raw)
                    if callable(self._stamp_meta):
                        with contextlib.suppress(Exception):
                            self._stamp_meta(pos, closed, pos_raw)

                # Prometheus orphan cleanup counter
                with contextlib.suppress(Exception):
                    from services.trade_monitor._monolith import TM_ORPHAN_CLEANUP_TOTAL
                    TM_ORPHAN_CLEANUP_TOTAL.labels(symbol=sym, reason=pos_raw).inc()

                # Build events
                orphan_ev = TradeEvent(
                    event_type="ORPHAN_CLOSE",
                    order_id=pos.id,
                    sid=getattr(pos, "sid", ""),
                    strategy=getattr(pos, "strategy", ""),
                    source=getattr(pos, "source", ""),
                    symbol=getattr(pos, "symbol", ""),
                    tf=getattr(pos, "tf", ""),
                    direction=getattr(pos, "direction", ""),  # type: ignore
                    ts_ms=int(exit_ts_ms_default or now_ms),  # type: ignore
                    payload={
                        "exit_price": float(pos_exit_price),
                        "exit_ts_ms": int(exit_ts_ms_default or now_ms),
                        "reason_raw": pos_raw,
                        "close_reason_detail": str(
                            getattr(closed, "close_reason_detail", "") or ""
                        ) if closed else pos_raw,
                        "orphan_now_ms": int(now_ms),
                    },
                )
                close_ev = TradeEvent(
                    event_type="CLOSE",
                    order_id=pos.id,
                    sid=getattr(pos, "sid", ""),
                    strategy=getattr(pos, "strategy", ""),
                    source=getattr(pos, "source", ""),
                    symbol=getattr(pos, "symbol", ""),
                    tf=getattr(pos, "tf", ""),
                    direction=getattr(pos, "direction", ""),  # type: ignore
                    ts_ms=int(exit_ts_ms_default or now_ms),  # type: ignore
                    payload={
                        "reason": str(getattr(closed, "close_reason", "") or "") if closed else pos_raw,
                        "reason_raw": str(getattr(closed, "close_reason_raw", "") or pos_raw) if closed else pos_raw,
                        "close_reason_detail": str(getattr(closed, "close_reason_detail", "") or "") if closed else pos_raw,
                    },
                )

                pos_dict = (
                    asdict(pos) if hasattr(pos, "__dataclass_fields__")
                    else dict(getattr(pos, "__dict__", {}) or {})
                )
                closed_dict = (
                    asdict(closed) if (closed and hasattr(closed, "__dataclass_fields__"))
                    else dict(getattr(closed, "__dict__", {}) or {}) if closed else {}
                )

                mapped_src = source_from_strategy(
                    getattr(pos, "strategy", ""),
                    str(getattr(pos, "source", "")),
                )
                local_triggers.append((
                    mapped_src,
                    str(getattr(pos, "symbol", "")),
                    str(pos.id),
                    getattr(pos, "is_virtual", False),
                ))

                # Evict from memory
                if callable(self._pop_pos):
                    with contextlib.suppress(Exception):
                        if self._global_lock:
                            with self._global_lock:
                                self._pop_pos(pos.id)
                        else:
                            self._pop_pos(pos.id)

                # Capture for deferred IO
                def _make_io(o_ev, c_ev, cl, pd, cd):
                    tasks = []
                    if callable(self._append_event):
                        tasks.append(lambda: self._append_event(o_ev))  # type: ignore
                        tasks.append(lambda: self._append_event(c_ev))  # type: ignore
                    if cl is not None and callable(self._persist_closed):  # type: ignore
                        tasks.append(lambda: self._persist_closed(cl, pd, cd))  # type: ignore
                    return tasks  # type: ignore

                io_tasks.extend(_make_io(orphan_ev, close_ev, closed, pos_dict, closed_dict))

            except Exception as e:
                self._logger.warning("⚠️ Orphan forced-close failed for %s/%s: %s", sym, pos_id, e)

        # Execute IO outside all locks
        for task in io_tasks:
            try:
                task()
            except Exception as e:
                self._logger.warning("⚠️ Orphan IO task failed: %s", e)

        return local_triggers

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _is_orphan_expired(self, pos: Any, now_ms: int) -> bool:
        """Return True if position has exceeded its TTL."""
        if not pos or getattr(pos, "closed", False):
            return False
        # Never timeout trailing positions
        if getattr(pos, "trailing_active", False):
            return False

        entry_ts_ms = int(getattr(pos, "entry_ts_ms", 0) or 0)
        if not _is_plausible_epoch_ms(entry_ts_ms):
            return False

        age_ms = int(now_ms) - entry_ts_ms
        if age_ms < 0:
            return False

        ttl_ms = self._resolve_orphan_ttl_ms(pos)
        if ttl_ms <= 0:
            return False

        if age_ms < ttl_ms:
            return False

        # Smart timeout: skip if not profitable and not risky (HOLD)
        if self._smart_timeout_enabled and callable(self._get_price):
            sym = str(getattr(pos, "symbol", "") or "")
            lp = self._get_price(sym)
            if lp and float(lp[1]) > 0:
                last_px = float(lp[1])
                entry_px = float(getattr(pos, "entry_price", 0.0) or 0.0)
                if entry_px > 0:
                    direction = getattr(pos, "direction", "LONG")
                    pnl_bps = (
                        (last_px - entry_px) / entry_px * 10_000.0
                        if direction == "LONG"
                        else (entry_px - last_px) / entry_px * 10_000.0
                    )
                    param_min_pnl = float(os.getenv("TM_SMART_TIMEOUT_PNL_BPS", "4.0"))
                    param_max_mae_atr = float(os.getenv("TM_SMART_TIMEOUT_MAE_ATR", "1.0"))
                    atr = float(getattr(pos, "atr", 0.0) or 0.0)

                    is_profitable = pnl_bps >= param_min_pnl
                    is_risky = False
                    if atr > 0:
                        adverse = (entry_px - last_px) if direction == "LONG" else (last_px - entry_px)
                        is_risky = adverse > (atr * param_max_mae_atr)
                        if not is_profitable and not is_risky:
                            return False  # HOLD: not expired yet per smart policy

        return True

    def _resolve_orphan_ttl_ms(self, pos: Any) -> int:
        """
        Determine position TTL in ms.

        Priority:
          1. signal_payload.meta.horizon.stop_ttl_ms
          2. pos.stop_ttl_ms (if set at position creation)
          3. ENV TM_ORPHAN_TTL_<SYMBOL>
          4. ENV TM_ORPHAN_TTL_MS (global default)
        """
        # 1. Meta horizon
        with contextlib.suppress(Exception):
            sp = getattr(pos, "signal_payload", None) or {}
            meta = (sp.get("meta") or {}) if isinstance(sp, dict) else {}
            horizon = (meta.get("horizon") or {}) if isinstance(meta, dict) else {}
            v = horizon.get("stop_ttl_ms")
            if v is not None:
                ttl = int(float(v))
                if ttl > 0:
                    return ttl

        # 2. Explicit pos attribute
        with contextlib.suppress(Exception):
            v = getattr(pos, "stop_ttl_ms", None)
            if v is not None:
                ttl = int(float(v))
                if ttl > 0:
                    return ttl

        # 3. Per-symbol ENV
        sym = str(getattr(pos, "symbol", "") or "").strip().upper()
        if sym:
            env_val = os.getenv(f"TM_ORPHAN_TTL_{sym}")
            if env_val:
                with contextlib.suppress(Exception):
                    ttl = int(float(env_val))
                    if ttl > 0:
                        return ttl * 1000  # assume seconds if small

        # 4. Global default
        default_str = os.getenv("TM_ORPHAN_TTL_MS", "0")
        with contextlib.suppress(Exception):
            return int(float(default_str))
        return 0
