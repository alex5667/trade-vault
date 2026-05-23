# services/trade_monitor/trade_close_writer.py
"""
Persistence of closed trades: repo, analytics DB, signal outcome pipeline.

Extracted from TradeMonitorService methods:
  _persist_closed_trade_io  (monolith lines 2123-2164)
  _io_save_closed            (1950-1962)
  _io_save_tp_hit            (1915-1922)
  _io_save_trailing_sync     (1924-1939)
  _io_save_trailing_move     (1941-1948)
  _stamp_closed_trade_meta   (1971-2070)
  _safe_save_trade_to_db     (973-988)
  _attach_health_snapshot    (1818-1827, 1862-1873)
  _get_health_snapshot_*     (1785-1860)

Design:
  - All public methods are fail-open.
  - No locks inside — all calls must happen outside the caller's global lock.
  - I/O to analytics DB is offloaded to db_executor (ThreadPoolExecutor).
"""
from __future__ import annotations

import contextlib
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable

from services.horizon_contract import stamp_closed_trade_horizon_from_position
from utils.time_utils import get_ny_time_millis

logger = logging.getLogger(__name__)


def _log_future_exception(fut):  # noqa: ANN001
    """Callback for futures: log unhandled background exceptions."""
    try:
        exc = fut.exception()
        if exc:
            import traceback
            tb_str = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
            logger.error("Async DB task failed: %s\nTraceback:\n%s", exc, tb_str)
    except Exception:
        pass


class TradeCloseWriter:
    """
    Unified writer for trade-close persistence.

    Args:
        redis         — Redis client (for health snapshot reads).
        repo          — RedisTradeRepository.
        db_executor   — ThreadPoolExecutor for async analytics writes.
        batch_writer  — BatchTradeWriter instance (optional).
        analytics_db  — module with save_trade_closed(closed) function.
        pnl_calc      — PnlCalculator (for update_stats).
        attach_health_on_close — if True, attach health snapshot to closed.
        health_cache_ttl_ms    — TTL for health snapshot cache (ms).
    """

    def __init__(
        self,
        redis: Any,
        repo: Any,
        db_executor: ThreadPoolExecutor,
        *,
        batch_writer: Any = None,
        analytics_db: Any = None,
        pnl_calc: Any = None,
        submit_persist_task_fn: Callable | None = None,
        attach_health_on_close: bool = True,
        health_cache_ttl_ms: int = 5_000,
        protective_mirror: Any = None,
        log: logging.Logger | None = None,
    ) -> None:
        self._redis = redis
        self._repo = repo
        self._db_executor = db_executor
        self._batch_writer = batch_writer
        self._analytics_db = analytics_db
        self._pnl_calc = pnl_calc
        self._submit_persist_task_fn = submit_persist_task_fn
        self._attach_health_on_close = attach_health_on_close
        self._health_cache_ttl_ms = health_cache_ttl_ms
        self._protective_mirror = protective_mirror
        self._logger = log or logger

        # Small TTL cache: symbol -> (ts_ms, snap)
        self._health_cache: dict[str, tuple[int, dict[str, str]]] = {}

    # ------------------------------------------------------------------
    # TP hit persistence
    # ------------------------------------------------------------------

    def save_tp_hit(
        self,
        pos: Any,
        tp_level: int,
        fill_price: float,
        closed_qty: float,
        pnl_part: float,
        ts_ms: int,
    ) -> None:
        """Persist TP hit event to repo; notify protective mirror if present."""
        try:
            self._repo.save_tp_hit(
                pos,
                tp_level=tp_level,
                fill_price=fill_price,
                closed_qty=closed_qty,
                pnl_part=pnl_part,
                ts_ms=ts_ms,
            )
            if (
                self._protective_mirror is not None
                and tp_level == 1
                and not getattr(pos, "tp1_mirrored", False)
            ):
                with contextlib.suppress(Exception):
                    self._protective_mirror.on_tp1_reached(
                        str(getattr(pos, "sid", "")),
                        str(getattr(pos, "symbol", "")),
                        float(fill_price),
                        int(ts_ms),
                    )
                    pos.tp1_mirrored = True
        except Exception as e:
            self._logger.warning("save_tp_hit failed: %s", e)

    # ------------------------------------------------------------------
    # Trailing persistence
    # ------------------------------------------------------------------

    def save_trailing_sync(self, pos: Any, ts: int) -> None:
        """Persist trailing sync to repo; notify mirror."""
        try:
            self._repo.save_trailing_sync(pos, ts)
            if self._protective_mirror is not None:
                with contextlib.suppress(Exception):
                    m = self._protective_mirror
                    sid = str(getattr(pos, "sid", ""))
                    sym = str(getattr(pos, "symbol", ""))
                    ts_int = int(ts)
                    if getattr(pos, "trailing_active", False) and not getattr(pos, "be_mirrored", False):
                        m.on_break_even_activated(sid, sym, float(getattr(pos, "sl", 0.0) or 0.0), ts_int)
                        pos.be_mirrored = True
                    if getattr(pos, "trailing_active", False) and not getattr(pos, "trailing_mirrored", False):
                        m.on_trailing_activated(sid, sym, ts_int)
                        pos.trailing_mirrored = True
        except Exception as e:
            self._logger.warning("save_trailing_sync failed: %s", e)

    def save_trailing_move(
        self,
        pos: Any,
        previous_sl: float,
        new_sl: float,
        ts_ms: int,
    ) -> None:
        """Persist SL move to repo; notify mirror."""
        try:
            self._repo.save_trailing_move(pos, previous_sl, new_sl, ts_ms)
            if self._protective_mirror is not None and abs(float(previous_sl) - float(new_sl)) > 1e-9:
                with contextlib.suppress(Exception):
                    self._protective_mirror.on_sl_moved(
                        str(getattr(pos, "sid", "")),
                        str(getattr(pos, "symbol", "")),
                        str(getattr(pos, "direction", "")),
                        float(previous_sl),
                        float(new_sl),
                        float(getattr(pos, "max_favorable_price", 0.0) or 0.0),
                        int(ts_ms),
                    )
        except Exception as e:
            self._logger.warning("save_trailing_move failed: %s", e)

    # ------------------------------------------------------------------
    # Closed trade IO (single entry point)
    # ------------------------------------------------------------------

    def persist_closed(
        self,
        closed: Any,
        pos_dict: dict[str, Any],
        closed_dict: dict[str, Any],
    ) -> None:
        """
        Unified close persistence:
          1. Attach health snapshot (opt-in).
          2. repo.save_closed (Redis stream).
          3. Analytics DB (background thread).
          4. Signal outcome pipeline (background thread).
          5. StatsAggregator + RegimeGuard.

        IMPORTANT: must be called OUTSIDE any global lock.
        """
        symbol = str(getattr(closed, "symbol", "") or "")
        now_ms = get_ny_time_millis()

        # --- Health snapshot attachment (opt-in) ---
        if self._attach_health_on_close:
            with contextlib.suppress(Exception):
                snap = self._get_health_snapshot_prefixed(symbol, now_ms)
                if snap:
                    closed._health_snapshot = snap

        hs: dict[str, str] = {}
        with contextlib.suppress(Exception):
            hs = self._get_health_snapshot_for_trade(symbol)

        # --- Repo (Redis stream) ---
        self._save_closed_to_repo(closed, hs)

        # --- Analytics DB (async) ---
        if self._analytics_db is not None:
            try:
                fut = self._db_executor.submit(self._analytics_db.save_trade_closed, closed)
                fut.add_done_callback(_log_future_exception)
            except Exception as e:
                self._logger.warning("Failed to submit trade to analytics DB: %s", e)

        # --- Signal outcome pipeline (async, fail-open) ---
        try:
            from domain.signal_outcome import from_trade_closed as _build_outcome
            from services.signal_outcome_writer import get_signal_outcome_writer
            _outcome = _build_outcome(closed)
            if _outcome is not None:
                fut_o = self._db_executor.submit(get_signal_outcome_writer().emit, _outcome)
                fut_o.add_done_callback(_log_future_exception)
        except Exception as e:
            self._logger.warning(
                "⚠️ signal_outcome emit failed (fail-open): %s", e
            )

        # --- Stats + RegimeGuard ---
        if self._pnl_calc is not None:
            self._pnl_calc.update_stats(
                pos_dict,
                closed_dict,
                submit_persist_task_fn=self._submit_persist_task_fn,
            )

    # ------------------------------------------------------------------
    # Stamp closed trade metadata
    # ------------------------------------------------------------------

    def stamp_closed_meta(
        self,
        pos: Any,
        closed: Any,
        close_reason_raw: str,
    ) -> None:
        """
        Enrich TradeClosed with:
          - trailing detail (TRAILING_PROFIT / TRAILING_STOP)
          - policy provenance (ATR policy version, scenario, regime, bucket …)
          - live-surface baseline vs selected snapshot
          - trailing-surface A/B snapshot
          - horizon/ATR scalars

        Fail-open throughout.
        """
        # --- Trailing close detail ---
        try:
            if getattr(pos, "trailing_started", False) or getattr(pos, "trailing_active", False):
                with contextlib.suppress(Exception):
                    closed.trailing_active = True
                    closed.trailing_started = True
                try:
                    closed.close_reason_detail = (
                        "TRAILING_PROFIT"
                        if float(getattr(closed, "pnl_net", 0.0) or 0.0) > 1e-8
                        else "TRAILING_STOP"
                    )
                except Exception:
                    closed.close_reason_detail = "TRAILING_STOP"
            else:
                closed.close_reason_detail = str(close_reason_raw)
        except Exception:
            pass

        # --- Phase 5: policy provenance ---
        try:
            sp = getattr(pos, "signal_payload", {}) or {}
            _cs_sm0 = (sp.get("config_snapshot") or {}) if isinstance(sp, dict) else {}
            meta = (sp.get("meta") or _cs_sm0.get("meta") or {}) if isinstance(sp, dict) else {}
            prov = meta.get("policy_provenance", {}) if isinstance(meta, dict) else {}

            # Primary source: meta.policy_provenance (populated by signal_preprocess.py Phase 5)
            # Fallback: top-level atr_policy_* fields also written by signal_preprocess.py
            def _prov_get(prov_key: str, sp_key: str | None = None, default: str = "") -> str:
                v = prov.get(prov_key)
                if v and str(v) not in ("", "None", "0"):
                    return str(v)
                if sp_key:
                    v2 = sp.get(sp_key)
                    if v2 and str(v2) not in ("", "None", "0"):
                        return str(v2)
                return default

            closed.atr_policy_ver = int(prov.get("policy_ver", 0) or sp.get("atr_policy_ver", 0) or 0)
            closed.atr_policy_tag = _prov_get("policy_tag", "atr_policy_tag")
            closed.atr_policy_source = _prov_get("policy_source")
            closed.atr_policy_scenario = _prov_get("scenario", "kind")
            closed.atr_policy_regime = _prov_get("regime")
            closed.atr_policy_bucket = _prov_get("risk_horizon_bucket")
            closed.atr_stop_ttl_mode = _prov_get("stop_ttl_mode")
            closed.atr_trailing_mode = _prov_get("trailing_mode")
            closed.atr_recovery_run_id = _prov_get("recovery_run_id", "atr_recovery_run_id")
            closed.atr_restore_cert_id = _prov_get("restore_cert_id")
            closed.atr_restore_cert_status = _prov_get("restore_cert_status", "atr_restore_cert_status")
            # Store full provenance dict; merge top-level fallbacks if prov is empty
            if prov:
                closed.atr_policy_snapshot_json = prov
            else:
                closed.atr_policy_snapshot_json = {
                    "policy_ver": int(sp.get("atr_policy_ver", 0) or 0),
                    "policy_tag": sp.get("atr_policy_tag", ""),
                    "policy_level": sp.get("atr_policy_level", ""),
                    "active_key": sp.get("atr_policy_key", ""),
                    "reason_code": sp.get("atr_policy_reason_code", ""),
                    "recovery_run_id": sp.get("atr_recovery_run_id", ""),
                    "restore_cert_status": sp.get("atr_restore_cert_status", ""),
                    "_fallback": True,
                }
        except Exception:
            pass

        # --- Phase 2.5: live-surface baseline vs selected ---
        try:
            sp = getattr(pos, "signal_payload", {}) or {}
            _cs_sm = (sp.get("config_snapshot") or {}) if isinstance(sp, dict) else {}
            meta = (sp.get("meta") or _cs_sm.get("meta") or {}) if isinstance(sp, dict) else {}
            baseline = (meta.get("live_surface_baseline") or {}) if isinstance(meta, dict) else {}
            applied = (meta.get("live_surface_applied") or {}) if isinstance(meta, dict) else {}
            candidate = (meta.get("risk_surface_live_candidate") or {}) if isinstance(meta, dict) else {}

            closed.live_surface_applied = bool(applied.get("applied", False))
            closed.live_surface_reason_code = applied.get("reason_code") or ""
            closed.baseline_sl_price = float(baseline.get("sl_price") or 0.0)
            closed.baseline_tp1_price = float(baseline.get("tp1_price") or 0.0)
            closed.selected_sl_price = float(candidate.get("selected_sl_price") or 0.0)
            closed.selected_tp1_price = float(candidate.get("selected_tp1_price") or 0.0)
            closed.live_surface_policy_level = applied.get("policy_level", "")

            if closed.selected_tp1_price == 0.0:
                tp_levels = getattr(pos, "tp_levels", None)
                if tp_levels and len(tp_levels) > 0 and float(tp_levels[0]) > 0:
                    closed.selected_tp1_price = float(tp_levels[0])
            if closed.selected_sl_price == 0.0:
                pos_sl = float(getattr(pos, "sl", 0.0) or 0.0)
                if pos_sl > 0:
                    closed.selected_sl_price = pos_sl
            # baseline = selected when live surface not applied
            if closed.baseline_tp1_price == 0.0:
                closed.baseline_tp1_price = closed.selected_tp1_price
            if closed.baseline_sl_price == 0.0:
                closed.baseline_sl_price = closed.selected_sl_price
        except Exception:
            pass

        # --- Phase 2.6: trailing-surface A/B snapshot ---
        try:
            sp = getattr(pos, "signal_payload", {}) or {}
            _cs_sm2 = (sp.get("config_snapshot") or {}) if isinstance(sp, dict) else {}
            meta = (sp.get("meta") or _cs_sm2.get("meta") or {}) if isinstance(sp, dict) else {}
            canary_decision = (meta.get("trailing_canary_decision") or {}) if isinstance(meta, dict) else {}
            surface_diag = (meta.get("trailing_surface_diagnostic") or {}) if isinstance(meta, dict) else {}

            closed.trailing_surface_applied = bool(canary_decision.get("should_apply", False))
            closed.trailing_surface_reason_code = canary_decision.get("reason_code") or ""
            closed.baseline_trailing_offset_atr = float(surface_diag.get("baseline_offset_distance_px") or 0.0)
            closed.selected_trailing_offset_atr = float(surface_diag.get("selected_offset_distance_px") or 0.0)
            closed.trailing_policy_level = str(getattr(pos, "trailing_policy_level", ""))
        except Exception:
            pass

        # --- Phase 0.3: horizon/ATR scalars ---
        with contextlib.suppress(Exception):
            stamp_closed_trade_horizon_from_position(pos, closed)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _save_closed_to_repo(self, closed: Any, health_snapshot: dict) -> None:
        """repo.save_closed + protective mirror notification. Fail-open."""
        try:
            self._repo.save_closed(closed, health_snapshot=health_snapshot)
            if self._protective_mirror is not None:
                with contextlib.suppress(Exception):
                    self._protective_mirror.on_position_closed(
                        signal_id=str(getattr(closed, "sid", "") or ""),
                        symbol=str(getattr(closed, "symbol", "") or ""),
                        exit_price=float(getattr(closed, "exit_price", 0.0) or 0.0),
                        pnl_bps=float(getattr(closed, "pnl_bps", 0.0) or 0.0),
                        close_reason=str(
                            getattr(closed, "close_reason_raw", "")
                            or getattr(closed, "close_reason", "")
                        ),
                        max_mae_pct=float(getattr(closed, "max_mae_pct", 0.0) or 0.0),
                        ts_ms=int(
                            getattr(closed, "exit_ts_ms", None)
                            or getattr(closed, "closed_at_ms", None)
                            or get_ny_time_millis()
                        ),
                    )
        except Exception as e:
            self._logger.warning("save_closed_to_repo failed: %s", e)

    def _get_health_snapshot_prefixed(self, symbol: str, now_ms: int) -> dict[str, str]:
        """Fetch health snapshot with prefix, TTL-cached."""
        sym = symbol or "UNKNOWN"
        cached = self._health_cache.get(sym)
        if cached:
            ts_ms, snap = cached
            if (now_ms - ts_ms) <= self._health_cache_ttl_ms:
                return snap
        try:
            raw = self._redis.hgetall(f"orderflow:{sym}:health_snapshot") or {}
            snap = self._build_health_snap(raw)
            self._health_cache[sym] = (now_ms, snap)
            return snap
        except Exception:
            return {}

    def _get_health_snapshot_for_trade(self, symbol: str) -> dict[str, str]:
        """Alias for legacy callers."""
        return self._get_health_snapshot_prefixed(symbol, get_ny_time_millis())

    @staticmethod
    def _build_health_snap(raw: dict) -> dict[str, str]:
        return {
            "health_l2_stale_ratio_tick": raw.get("l2_stale_ratio_tick", "0.0"),
            "health_l2_stale_ratio_now": raw.get("l2_stale_ratio_now", "0.0"),
            "health_avg_l2_age_ms": raw.get("avg_l2_age_ms", "0.0"),
            "health_avg_l2_age_tick_ms": raw.get("avg_l2_age_tick_ms", "0.0"),
            "health_signal_emit_rate": raw.get("signal_emit_rate", "0.0"),
            "health_dlq_rate": raw.get("dlq_rate", "0.0"),
            "health_pending_len": raw.get("pending_len", "0"),
            "health_snapshot_ts": raw.get("ts", "0"),
            "health_window_sec": raw.get("window_sec", "0"),
        }
