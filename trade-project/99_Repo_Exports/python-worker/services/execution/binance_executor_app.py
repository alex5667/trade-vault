"""binance_executor_app.py — BinanceExecutor facade (orchestrator only).

Extracted from binance_executor.py (god-class decomposition, Stage 11).

BinanceExecutor is now a thin facade:
- __init__: reads ENV, creates all service objects (DI)
- run_forever / run_once / process_one: main loop
- _resolve_client: demo vs prod routing
- Delegates ALL domain logic to service modules

Services:
    event_writer    → ExecutionEventWriter
    state_store     → ExecutionStateStore
    guard           → ActiveSymbolGuard
    flatten         → EmergencyFlattenService
    protection      → ProtectionService
    reconcile       → ReconcileService
    trailing        → TrailingService
    maker_tp        → MakerTpWatchdog
    open_svc        → OrderOpenService
    modify_svc      → OrderModifyService
    cancel_svc      → OrderCancelService
    filters         → FiltersCache  (prod / demo)
"""
from __future__ import annotations

import contextlib
import json
import os
import time
from typing import Any

try:
    import redis
except ImportError:
    redis = None  # type: ignore[assignment]

try:
    from services.binance_futures_client import BinanceFuturesClient
except Exception:
    from binance_futures_client import BinanceFuturesClient  # type: ignore[no-redef]

try:
    from services.telegram_client import TelegramClient
except Exception:
    TelegramClient = None  # type: ignore[assignment]

try:
    from services.execution_journal import ExecutionJournalSink
except Exception:
    ExecutionJournalSink = None  # type: ignore[assignment]

try:
    from services.rollout_flags import RolloutFlags
except Exception:
    RolloutFlags = None  # type: ignore[assignment]

# ── Sub-services ──────────────────────────────────────────────────────────
from services.execution.binance_filters import FiltersCache
from services.execution.binance_order_mapper import _bool_env, _truthy
from services.execution.execution_event_writer import ExecutionEventWriter
from services.execution.execution_state_store import ExecutionStateStore
from services.execution.active_symbol_guard import ActiveSymbolGuard
from services.execution.emergency_flatten_service import EmergencyFlattenService
from services.execution.protection_service import ProtectionService
from services.execution.reconcile_service import ReconcileService
from services.execution.trailing_service import TrailingService
from services.execution.maker_tp_watchdog import MakerTpWatchdog
from services.execution.order_open_service import OrderOpenService
from services.execution.order_modify_service import OrderModifyService
from services.execution.order_cancel_service import OrderCancelService

# ── Metrics (optional) ───────────────────────────────────────────────────
try:
    from services.execution_metrics import (
        EXECUTION_ORDERS_PROCESSED_TOTAL,  # type: ignore
        EXECUTION_ORDERS_FAILED_TOTAL,  # type: ignore
        EXECUTION_PROCESSING_LATENCY,  # type: ignore
    )
except Exception:
    EXECUTION_ORDERS_PROCESSED_TOTAL = EXECUTION_ORDERS_FAILED_TOTAL = None  # type: ignore
    EXECUTION_PROCESSING_LATENCY = None  # type: ignore


def _ms_now() -> int:
    try:
        from utils.time_utils import get_ny_time_millis
        return get_ny_time_millis()
    except Exception:
        return int(time.time() * 1000)


class BinanceExecutor:
    r: Any  # sync redis.Redis client

    """Facade: consumes orders:queue:binance and delegates to service modules.

    All domain logic lives in the services/execution/ sub-modules.
    This class is responsible only for:
    - ENV parsing and service wiring (__init__)
    - Main event loop (run_forever / run_once / process_one)
    - Client routing (demo vs prod)
    - Quarantine guard

    PositionSizer Contract:
    - BinanceExecutor is an execution gateway, NOT a risk/sizing engine.
    - qty MUST be pre-calculated by the upstream risk layer.
    """

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
        self.r = (
            redis_client
            if redis_client is not None
            else redis.from_url(self.redis_url, decode_responses=True)  # type: ignore
        )

        # ── Queue keys ───────────────────────────────────────────────────
        try:
            from core.redis_keys import RedisStreams as RS
            _default_queue = RS.ORDERS_QUEUE_BINANCE
            _default_exec_stream = RS.ORDERS_EXEC
        except Exception:
            _default_queue = "orders:queue:binance"
            _default_exec_stream = "orders:exec"

        self.queue = os.getenv("ORDERS_QUEUE_BINANCE") or os.getenv("ORDERS_QUEUE") or _default_queue
        self.queue_processing = os.getenv("ORDERS_QUEUE_BINANCE_PROCESSING") or f"{self.queue}:processing"
        self.queue_dlq = os.getenv("ORDERS_QUEUE_BINANCE_DLQ") or f"{self.queue}:dlq"
        self.exec_stream = os.getenv("EXEC_STREAM", _default_exec_stream)
        _maxlen_raw = int(os.getenv("EXEC_STREAM_MAXLEN", "0"))
        self.exec_stream_maxlen: int | None = _maxlen_raw if _maxlen_raw > 0 else None

        # ── Position mode ─────────────────────────────────────────────────
        self.position_mode = (os.getenv("BINANCE_POSITION_MODE") or "oneway").strip().lower()
        if self.position_mode not in {"oneway", "hedge"}:
            self.position_mode = "oneway"

        # ── Basic config ──────────────────────────────────────────────────
        self.assume_lot_is_qty = _bool_env("BINANCE_ASSUME_LOT_IS_QTY", True)
        self.max_retry = int(os.getenv("BINANCE_MAX_RETRY", "3"))
        self.fill_timeout_s = float(os.getenv("BINANCE_FILL_TIMEOUT_S", "8.0"))
        self.fill_poll_s = float(os.getenv("BINANCE_FILL_POLL_S", "0.25"))
        self.default_leverage = int(os.getenv("BINANCE_DEFAULT_LEVERAGE", "10"))
        self.allowlist: set[str] = set()
        allow_raw = (os.getenv("BINANCE_SYMBOL_ALLOWLIST") or "").strip()
        if allow_raw:
            self.allowlist = {s.strip().upper() for s in allow_raw.split(",") if s.strip()}

        # ── Rollout flags ─────────────────────────────────────────────────
        self.rollout_flags = RolloutFlags.from_env() if RolloutFlags else None

        # ── Telegram ──────────────────────────────────────────────────────
        if TelegramClient:
            self.tg = telegram_client if telegram_client is not None else TelegramClient.from_env()
        else:
            self.tg = telegram_client

        # ── Dual-client architecture ──────────────────────────────────────
        self._client_mode = (os.getenv("BINANCE_CLIENT_MODE") or "auto").strip().lower()

        if demo_client is not None:
            self.demo_client: BinanceFuturesClient | None = demo_client
            self.demo_filters: FiltersCache | None = FiltersCache(demo_client)
        else:
            _demo_key = (os.getenv("BINANCE_DEMO_API_KEY") or "").strip()
            if _demo_key:
                self.demo_client = BinanceFuturesClient.from_env(prefix="BINANCE_DEMO_")
                self.demo_filters = FiltersCache(self.demo_client)
            else:
                self.demo_client = None
                self.demo_filters = None

        if prod_client is not None:
            self.client: BinanceFuturesClient | None = prod_client
            self.filters: FiltersCache | None = FiltersCache(prod_client)
        else:
            _prod_key = (os.getenv("BINANCE_API_KEY") or "").strip()
            if _prod_key:
                self.client = BinanceFuturesClient.from_env(prefix="BINANCE_")
                self.filters = FiltersCache(self.client)
            else:
                self.client = None
                self.filters = None

        if self.demo_client is None and self.client is None:
            raise RuntimeError("At least one of BINANCE_DEMO_API_KEY or BINANCE_API_KEY must be set")

        # ── Service config (read from ENV) ────────────────────────────────
        _sl_wt = (os.getenv("SL_WORKING_TYPE") or "MARK_PRICE").strip().upper()
        _state_prefix = (os.getenv("ORDERS_STATE_KEY_PREFIX") or "orders:state:").rstrip(":") + ":"
        _state_ttl = int(os.getenv("ORDERS_STATE_TTL_SEC", "86400"))
        _exec_reconcile = _bool_env("EXEC_RECONCILE_ENABLE", True)
        _exec_single = _bool_env("EXEC_SINGLE_ACTIVE_POSITION_PER_SYMBOL", False)
        _exec_exchange_truth = _bool_env("EXEC_SINGLE_ACTIVE_POSITION_EXCHANGE_TRUTH_RELEASE", True)
        _active_prefix = (os.getenv("ORDERS_ACTIVE_SYMBOL_KEY_PREFIX") or "orders:active_symbol_sid:").rstrip(":") + ":"

        # ── SQL journal ───────────────────────────────────────────────────
        if ExecutionJournalSink:
            _sql_enable = getattr(self.rollout_flags, "exec_journal_sql_enable", True) if self.rollout_flags else True
            self.execution_journal = ExecutionJournalSink() if _sql_enable else ExecutionJournalSink(dsn="")
        else:
            self.execution_journal = None

        # ── Quarantine ────────────────────────────────────────────────────
        self.orders_quarantine_sids_key = (os.getenv("ORDERS_QUARANTINE_SIDS_KEY") or "orders:quarantine:state:sids").strip()
        self.exec_quarantine_resume_guard_enable = _bool_env("EXEC_QUARANTINE_RESUME_GUARD_ENABLE", True)

        # ── Wire all services ─────────────────────────────────────────────
        self.event_writer = ExecutionEventWriter(
            r=self.r,
            exec_stream=self.exec_stream,
            exec_stream_maxlen=self.exec_stream_maxlen,
            queue=self.queue,
            queue_processing=self.queue_processing,
            queue_dlq=self.queue_dlq,
            exec_inline_state_projection=_bool_env("EXEC_INLINE_STATE_PROJECTION", False),
            execution_journal=self.execution_journal,
        )

        self.state_store = ExecutionStateStore(
            r=self.r,
            state_key_prefix=_state_prefix,
            state_ttl=_state_ttl,
            exec_stream=self.exec_stream,
            exec_replay_scan_count=int(os.getenv("EXEC_REPLAY_SCAN_COUNT", "20000")),
            exec_rehydrate_on_state_miss=_bool_env("EXEC_REHYDRATE_ON_STATE_MISS", True),
            exec_journal_primary=_bool_env("EXEC_JOURNAL_PRIMARY", True),
            exec_state_derived_view=_bool_env("EXEC_STATE_DERIVED_VIEW", True),
            exec_inline_state_projection=_bool_env("EXEC_INLINE_STATE_PROJECTION", False),
            write_event_fn=self.event_writer.write,
            execution_journal=self.execution_journal,
            exec_single_active_position_per_symbol=_exec_single,
            exec_single_active_position_exchange_truth_release=_exec_exchange_truth,
            exec_single_active_position_release_on_terminal=_bool_env("EXEC_SINGLE_ACTIVE_POSITION_RELEASE_ON_TERMINAL", True),
        )

        # Inject project_fn back into event_writer after state_store is created
        self.event_writer._project_fn = self.state_store.project_from_event  # type: ignore

        self.active_guard = ActiveSymbolGuard(
            r=self.r,
            active_symbol_key_prefix=_active_prefix,
            tombstone_ttl_sec=int(os.getenv("ACTIVE_SYMBOL_GUARD_TOMBSTONE_TTL_SEC", "120")),
            state_ttl=_state_ttl,
            user_stream_status_key=os.getenv("USER_STREAM_STATUS_KEY", "orders:user_stream:status"),
            exec_active_symbol_user_stream_stale_ms=int(os.getenv("EXEC_ACTIVE_SYMBOL_USER_STREAM_STALE_MS", "30000")),
            exec_single_active_position_per_symbol=_exec_single,
            exec_single_active_position_exchange_truth_release=_exec_exchange_truth,
            exec_single_active_position_release_on_terminal=_bool_env("EXEC_SINGLE_ACTIVE_POSITION_RELEASE_ON_TERMINAL", True),
            exec_single_active_position_require_flat_no_orders=_bool_env("EXEC_SINGLE_ACTIVE_POSITION_REQUIRE_FLAT_NO_ORDERS", True),
            exec_single_active_position_stale_timeout_ms=int(os.getenv("EXEC_SINGLE_ACTIVE_POSITION_STALE_TIMEOUT_MS", "900000")),
            exec_single_active_position_guard_repair_enable=_bool_env("EXEC_SINGLE_ACTIVE_POSITION_GUARD_REPAIR_ENABLE", True),
            write_event_fn=self.event_writer.write,
        )

        # Wire guard callbacks into state_store
        self.state_store._guard_acquire_fn = self.active_guard.acquire_or_refresh
        self.state_store._guard_release_fn = lambda sym, sid: self.active_guard.mark_released(sym, expected_sid=sid)
        self.state_store._guard_cas_metric_fn = self.active_guard._record_cas
        self.state_store._state_is_terminalish_fn = self.active_guard._state_is_terminalish

        self.flatten_svc = EmergencyFlattenService(
            position_mode=self.position_mode,
            dust_notional_usdt=float(os.getenv("BINANCE_DUST_NOTIONAL_USDT", "3.0")),
            dust_margin_usdt=float(os.getenv("BINANCE_DUST_MARGIN_USDT", "1.0")),
            dust_close_retries=int(os.getenv("BINANCE_DUST_CLOSE_RETRIES", "3")),
            dust_verify_timeout_ms=int(os.getenv("BINANCE_DUST_VERIFY_TIMEOUT_MS", "3000")),
            dust_verify_poll_ms=int(os.getenv("BINANCE_DUST_VERIFY_POLL_MS", "250")),
            sl_working_type=_sl_wt,
            write_event_fn=self.event_writer.write,
        )

        self.protection_svc = ProtectionService(
            position_mode=self.position_mode,
            sl_working_type=_sl_wt,
            tp_market_working_type=(os.getenv("TP_MARKET_WORKING_TYPE") or "MARK_PRICE").strip().upper(),
            tp_limit_trigger_working_type=(os.getenv("TP_LIMIT_TRIGGER_WORKING_TYPE") or "MARK_PRICE").strip().upper(),
            tp_limit_time_in_force=(os.getenv("TP_LIMIT_TIME_IN_FORCE") or "GTX").strip().upper(),
            tp_limit_price_offset_bps=float(os.getenv("TP_LIMIT_PRICE_OFFSET_BPS", "0.0")),
            protection_arm_timeout_ms=int(os.getenv("PROTECTION_ARM_TIMEOUT_MS", "2500")),
            protection_fee_buffer_bps=float(os.getenv("PROTECTION_FEE_BUFFER_BPS", "8.0")),
            protection_replace_max_naked_ms=int(os.getenv("PROTECTION_REPLACE_MAX_NAKED_MS", "3000")),
            exec_strict_protection_verify=_bool_env("EXEC_STRICT_PROTECTION_VERIFY", True),
            exec_reconcile_require_protection_complete=_bool_env("EXEC_RECONCILE_REQUIRE_PROTECTION_COMPLETE", True),
            exec_modify_resize_strict_replace=_bool_env("EXEC_MODIFY_RESIZE_STRICT_REPLACE", True),
            write_event_fn=self.event_writer.write,
            telegram_fn=getattr(self.tg, "send_message", None) if self.tg else None,
        )

        self.reconcile_svc = ReconcileService(
            r=self.r,
            user_stream_cache_prefix=(os.getenv("USER_STREAM_CACHE_PREFIX") or "orders:user_stream:").rstrip(":") + ":",
            reconcile_enable=_exec_reconcile,
            exec_reconcile_on_503_unknown=_bool_env("EXEC_RECONCILE_ON_503_UNKNOWN", True),
            exec_reconcile_prefer_user_stream=_bool_env("EXEC_RECONCILE_PREFER_USER_STREAM", True),
            write_event_fn=self.event_writer.write,
            mark_pending_reconcile_fn=self.state_store.mark_pending_reconcile,
        )

        self.trailing_svc = TrailingService(
            trail_mode=(os.getenv("BINANCE_TRAIL_MODE") or "orchestrator").strip().lower(),
            trail_profile_name=(os.getenv("BINANCE_TRAIL_PROFILE") or "rocket_v1").strip(),
            trail_cb_min=float(os.getenv("BINANCE_TRAIL_CALLBACK_MIN", "0.1")),
            trail_cb_max=float(os.getenv("BINANCE_TRAIL_CALLBACK_MAX", "5.0")),
            trail_cb_default=float(os.getenv("BINANCE_TRAIL_CALLBACK_DEFAULT", "0.3")),
            trail_arm_poll_s=float(os.getenv("BINANCE_TRAIL_ARM_POLL_S", "1.0")),
            trail_arm_timeout_s=float(os.getenv("BINANCE_TRAIL_ARM_TIMEOUT_S", "7200")),
            trail_notify=_bool_env("BINANCE_TRAIL_NOTIFY", True),
            trail_activate_price_bps=float(os.getenv("TRAIL_ACTIVATE_PRICE_BPS", "5.0")),
            trail_sl_move_min_delta_pct=float(os.getenv("BINANCE_TRAIL_SL_MOVE_MIN_DELTA_PCT", "0.05")),
            trail_loop_poll_s=float(os.getenv("BINANCE_TRAIL_LOOP_POLL_S", "2.0")),
            trail_loop_timeout_s=float(os.getenv("BINANCE_TRAIL_LOOP_TIMEOUT_S", "14400")),
            trail_working_type=(os.getenv("TRAIL_WORKING_TYPE") or "MARK_PRICE").strip().upper(),
            position_mode=self.position_mode,
            write_event_fn=self.event_writer.write,
            telegram_fn=getattr(self.tg, "send_message", None) if self.tg else None,
            r=self.r,
        )

        self.maker_tp_svc = MakerTpWatchdog(
            tp_limit_poll_s=float(os.getenv("TP_LIMIT_WATCHDOG_POLL_S", "2.0")),
            tp_limit_timeout_s=float(os.getenv("TP_TRIGGER_MONITOR_TIMEOUT_S", "7200")),
            tp_limit_spread_warn_bps=float(os.getenv("TP_LIMIT_SPREAD_WARN_BPS", "20.0")),
            write_event_fn=self.event_writer.write,
            lookup_user_stream_fn=self.reconcile_svc.lookup_user_stream_event,
        )

        self.open_svc = OrderOpenService(
            position_mode=self.position_mode,
            sl_working_type=_sl_wt,
            max_retry=self.max_retry,
            default_leverage=self.default_leverage,
            exec_set_leverage=_bool_env("BINANCE_INIT_SYMBOL_SETTINGS", False),
            # P0-1: Emergency close for naked positions (SHADOW by default)
            emergency_close_if_unprotected=_bool_env("EXEC_EMERGENCY_CLOSE_IF_UNPROTECTED", False),
            block_symbol_on_protection_fail=_bool_env("EXEC_BLOCK_SYMBOL_ON_PROTECTION_FAIL", False),
            cooldown_after_protection_fail_ms=int(os.getenv("COOLDOWN_AFTER_PROTECTION_FAIL_MS", "900000")),
            state_store=self.state_store,
            event_writer=self.event_writer,
            protection_service=self.protection_svc,
            reconcile_service=self.reconcile_svc,
            active_symbol_guard=self.active_guard,
            flatten_service=self.flatten_svc,
            r=self.r,
        )

        self.modify_svc = OrderModifyService(
            position_mode=self.position_mode,
            sl_working_type=_sl_wt,
            exec_modify_resize_strict_replace=_bool_env("EXEC_MODIFY_RESIZE_STRICT_REPLACE", True),
            state_store=self.state_store,
            event_writer=self.event_writer,
            protection_service=self.protection_svc,
            r=self.r,
        )

        self.cancel_svc = OrderCancelService(
            position_mode=self.position_mode,
            sl_working_type=_sl_wt,
            state_store=self.state_store,
            event_writer=self.event_writer,
            protection_service=self.protection_svc,
            flatten_service=self.flatten_svc,
            r=self.r,
        )

    # ── Client routing ────────────────────────────────────────────────────

    def _resolve_client(
        self, payload: dict[str, Any]
    ) -> tuple[BinanceFuturesClient, FiltersCache]:
        """Return (client, filters_cache) based on is_virtual flag and client mode."""
        use_demo: bool
        if self._client_mode == "demo":
            use_demo = True
        elif self._client_mode == "real":
            use_demo = False
        else:
            use_demo = _truthy(payload.get("is_virtual")) or _truthy(payload.get("virtual"))

        if use_demo:
            if self.demo_client is None:
                raise RuntimeError("Virtual order requested but BINANCE_DEMO_API_KEY not configured")
            return self.demo_client, self.demo_filters  # type: ignore[return-value]

        if self.client is None:
            if self.demo_client is not None:
                return self.demo_client, self.demo_filters  # type: ignore[return-value]
            raise RuntimeError("BINANCE_API_KEY not configured and no demo client available")
        return self.client, self.filters  # type: ignore[return-value]

    # ── Quarantine guard ──────────────────────────────────────────────────

    def _is_sid_quarantined(self, sid: str) -> bool:
        if not self.exec_quarantine_resume_guard_enable or not sid:
            return False
        try:
            return bool(self.r.sismember(self.orders_quarantine_sids_key, sid))
        except Exception:
            return False

    # ── Main processing loop ──────────────────────────────────────────────

    def process_one(self, raw: str) -> None:
        """Process a single JSON message from the orders queue."""
        ts_exec_start_ms = _ms_now()
        try:
            payload: dict[str, Any] = json.loads(raw)
        except Exception:
            self.event_writer.dlq(raw, "json_parse_error")
            self.event_writer.ack_processing(raw)
            return

        action = (payload.get("action") or "open").strip().lower()
        sid = (payload.get("sid") or payload.get("id") or "").strip()
        symbol = (payload.get("symbol") or "").strip().upper()
        ts_queue_ms: int = int(payload.get("ts_ms") or payload.get("ts_queue_ms") or ts_exec_start_ms)

        if not sid or not symbol:
            self.event_writer.dlq(raw, "missing_sid_or_symbol")
            self.event_writer.ack_processing(raw)
            return

        if self.allowlist and symbol not in self.allowlist:
            self.event_writer.dlq(raw, f"symbol_not_in_allowlist:{symbol}")
            self.event_writer.ack_processing(raw)
            return

        if self._is_sid_quarantined(sid):
            self.event_writer.write({
                "sid": sid, "symbol": symbol, "action": action,
                "event_type": "RESUME_GUARD_BLOCKED", "severity": "warning",
                "msg": "sid is quarantined",
            })
            self.event_writer.ack_processing(raw)
            return

        try:
            client, filters = self._resolve_client(payload)
        except RuntimeError as exc:
            self.event_writer.dlq(raw, f"client_resolve_failed:{exc}")
            self.event_writer.ack_processing(raw)
            return

        try:
            if action == "open":
                self.open_svc.handle_open(
                    payload=payload, client=client, filters=filters,
                    sid=sid, ts_queue_ms=ts_queue_ms, ts_exec_start_ms=ts_exec_start_ms,
                )
            elif action in {"cancel", "close"}:
                self.cancel_svc.handle_cancel(
                    payload=payload, client=client, filters=filters,
                    sid=sid, ts_queue_ms=ts_queue_ms, ts_exec_start_ms=ts_exec_start_ms,
                )
            elif action == "modify":
                self.modify_svc.handle_modify(
                    payload=payload, client=client, filters=filters,
                    sid=sid, ts_queue_ms=ts_queue_ms, ts_exec_start_ms=ts_exec_start_ms,
                )
            elif action == "resize":
                self.cancel_svc.handle_resize(
                    payload=payload, client=client, filters=filters,
                    sid=sid, ts_queue_ms=ts_queue_ms, ts_exec_start_ms=ts_exec_start_ms,
                )
            elif action == "timeout_close":
                self.cancel_svc.handle_timeout_close(
                    payload=payload, client=client, filters=filters,
                    sid=sid, ts_queue_ms=ts_queue_ms, ts_exec_start_ms=ts_exec_start_ms,
                )
            else:
                self.event_writer.dlq(raw, f"unknown_action:{action}")

            with contextlib.suppress(Exception):
                if EXECUTION_ORDERS_PROCESSED_TOTAL is not None:
                    EXECUTION_ORDERS_PROCESSED_TOTAL.labels(action=action, symbol=symbol).inc()
        except Exception as exc:
            with contextlib.suppress(Exception):
                if EXECUTION_ORDERS_FAILED_TOTAL is not None:
                    EXECUTION_ORDERS_FAILED_TOTAL.labels(action=action, symbol=symbol).inc()
            self.event_writer.write({
                "sid": sid, "symbol": symbol, "action": action,
                "event_type": "PROCESS_ONE_UNHANDLED_ERROR",
                "severity": "critical",
                "error": str(exc)[:300],
            })
            retry_n = payload.get("retry_n") or 0
            if retry_n < self.max_retry:
                self.event_writer.requeue(payload, raw, str(exc)[:100])
            else:
                self.event_writer.dlq(raw, f"max_retry_exceeded:{exc}")
        finally:
            self.event_writer.ack_processing(raw)

    def run_once(self, timeout_s: float = 2.0) -> bool:
        """Pop and process one message. Returns True if a message was processed."""
        try:
            result = self.r.brpoplpush(self.queue, self.queue_processing, timeout=int(timeout_s))
            if not result:
                return False
            self.process_one(result)
            return True
        except Exception:
            return False

    def run_forever(self) -> None:
        """Main blocking loop — polls queue until process terminates."""
        while True:
            try:
                self.run_once(timeout_s=2.0)
            except KeyboardInterrupt:
                break
            except Exception:
                time.sleep(0.5)


def main() -> None:
    executor = BinanceExecutor()
    executor.run_forever()


if __name__ == "__main__":
    main()
