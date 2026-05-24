"""Phase B: TrailingCommandConsumer — reads events:trailing:commands and
forwards SL-modify commands to OrderTrailingDispatcher (go-gateway).

Producer side: services.trailing_state_worker.TrailingStateWorker._emit_command
publishes XADD payloads to events:trailing:commands when SHADOW=0. Without a
consumer those commands accumulate but never reach the gateway, so SL doesn't
actually move on the exchange. This service plugs that gap.

ENV:
  TCC_ENABLED=0                       — force-on flag (overrides autocal).
                                        1 = always active, ignore autocal.
                                        0 = follow autocal (default).
  TCC_FOLLOW_AUTOCAL=1                — auto-activate when autocal:trailing_state:state
                                        publishes shadow=false. Telegram notification
                                        sent on each transition.
                                        Default 1 → fully automatic shadow→live.
                                        0 + TCC_ENABLED=0 = permanently idle.
  TCC_NOTIFY_STREAM=notify:telegram   — Redis stream for transition notifications
  TCC_STREAM=events:trailing:commands
  TCC_GROUP=trailing-cmd-consumer
  TCC_CONSUMER=tcc-1
  TCC_BATCH_SIZE=20
  TCC_BLOCK_MS=5000
  TCC_DLQ_STREAM=events:trailing:dlq
  TCC_MAX_RETRIES=5
  TCC_RETRY_TTL_SEC=3600
  TCC_PEL_STALE_MS=60000
  TCC_PEL_RECLAIM_COUNT=50
  TCC_PEL_RECLAIM_INTERVAL_S=30
  TCC_STATS_INTERVAL_SEC=300
  TCC_IDLE_SLEEP_SEC=60               — when TCC_ENABLED=0
  TCC_METRICS_PORT=9923
  GATEWAY_URL=http://scanner-go-gateway:8090
  GATEWAY_TIMEOUT=3.0
  REDIS_URL=redis://redis-worker-1:6379/0
"""

from __future__ import annotations

import json
import os
import signal
import sys
import time
from pathlib import Path
from typing import Any

import redis

_worker_path = Path(__file__).parent.parent
if str(_worker_path) not in sys.path:
    sys.path.insert(0, str(_worker_path))

from common.log import setup_logger
from services.order_trailing_dispatcher import OrderTrailingDispatcher

log = setup_logger("trailing_command_consumer")

# ── Prometheus metrics (fail-open) ────────────────────────────────────────────
try:
    from prometheus_client import Counter as _Counter, Gauge as _Gauge, Histogram as _Histogram

    _tcc_received_total = _Counter(
        "trailing_cmd_consumer_received_total",
        "Messages read from events:trailing:commands",
    )
    _tcc_dispatched_total = _Counter(
        "trailing_cmd_consumer_dispatched_total",
        "Dispatch attempts to go-gateway by result",
        ["result"],
    )
    _tcc_dlq_total = _Counter(
        "trailing_cmd_consumer_dlq_total",
        "Messages pushed to events:trailing:dlq by consumer",
        ["reason"],
    )
    _tcc_dlq_write_failed_total = _Counter(
        "trailing_cmd_consumer_dlq_write_failed_total",
        "DLQ write failures — message left in PEL",
    )
    _tcc_poison_total = _Counter(
        "trailing_cmd_consumer_poison_total",
        "Messages force-ACKed after exceeding retry cap",
    )
    _tcc_pel_pending = _Gauge(
        "trailing_cmd_consumer_pel_pending",
        "Messages currently in PEL (unacknowledged pending list)",
    )
    _tcc_dispatch_latency_ms = _Histogram(
        "trailing_cmd_consumer_dispatch_latency_ms",
        "send_trailing_modify call latency (ms)",
        buckets=(5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000),
    )
except Exception:  # pragma: no cover
    _tcc_received_total = None  # type: ignore[assignment]
    _tcc_dispatched_total = None  # type: ignore[assignment]
    _tcc_dlq_total = None  # type: ignore[assignment]
    _tcc_dlq_write_failed_total = None  # type: ignore[assignment]
    _tcc_poison_total = None  # type: ignore[assignment]
    _tcc_pel_pending = None  # type: ignore[assignment]
    _tcc_dispatch_latency_ms = None  # type: ignore[assignment]


def _inc(metric, *labels):
    """Fail-open Counter.inc()."""
    if metric is None:
        return
    try:
        if labels:
            metric.labels(*labels).inc()
        else:
            metric.inc()
    except Exception:
        pass


def _observe(metric, value: float):
    """Fail-open Histogram.observe()."""
    if metric is None:
        return
    try:
        metric.observe(value)
    except Exception:
        pass


_REQUIRED_FIELDS = ("sid", "symbol", "side", "new_sl", "position_id")


class TrailingCommandConsumer:
    """Reads events:trailing:commands and forwards SL-modify to go-gateway."""

    _AUTOCAL_REFRESH_S: float = 60.0

    def __init__(self, redis_client: redis.Redis | None = None) -> None:
        # ── Config ───────────────────────────────────────────────────────────
        # TCC_ENABLED=1 force-on (ignores autocal). TCC_ENABLED=0 + TCC_FOLLOW_AUTOCAL=1
        # → driven by autocal:trailing_state:state.shadow (default).
        # TCC_ENABLED=0 + TCC_FOLLOW_AUTOCAL=0 → permanently disabled.
        self.force_enabled = os.getenv("TCC_ENABLED", "0") == "1"
        self.follow_autocal = os.getenv("TCC_FOLLOW_AUTOCAL", "1") == "1"
        self.enabled = self.force_enabled  # back-compat: tests may set TCC_ENABLED=1
        self.autocal_active = False  # set by _refresh_autocal_state()
        self.stream = os.getenv("TCC_STREAM", "events:trailing:commands")
        self.group = os.getenv("TCC_GROUP", "trailing-cmd-consumer")
        self.consumer = os.getenv("TCC_CONSUMER", f"tcc-{int(time.time())}")
        self.batch_size = int(os.getenv("TCC_BATCH_SIZE", "20"))
        self.block_ms = int(os.getenv("TCC_BLOCK_MS", "5000"))
        self.dlq_stream = os.getenv("TCC_DLQ_STREAM", "events:trailing:dlq")
        self.max_retries = int(os.getenv("TCC_MAX_RETRIES", "5"))
        self.retry_ttl_sec = int(os.getenv("TCC_RETRY_TTL_SEC", "3600"))
        self.pel_stale_ms = int(os.getenv("TCC_PEL_STALE_MS", "60000"))
        self.pel_reclaim_count = int(os.getenv("TCC_PEL_RECLAIM_COUNT", "50"))
        self.pel_reclaim_interval_s = int(os.getenv("TCC_PEL_RECLAIM_INTERVAL_S", "30"))
        self.stats_interval_s = int(os.getenv("TCC_STATS_INTERVAL_SEC", "300"))
        self.idle_sleep_s = int(os.getenv("TCC_IDLE_SLEEP_SEC", "60"))
        self.redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")

        # ── Redis ────────────────────────────────────────────────────────────
        if redis_client is not None:
            self.r = redis_client
        else:
            self.r = redis.from_url(self.redis_url, decode_responses=True)
            log.info("✅ Connected to Redis: %s", self.redis_url)

        # ── Gateway dispatcher ───────────────────────────────────────────────
        # share Redis client to avoid second connection
        try:
            self.dispatcher = OrderTrailingDispatcher(redis_client=self.r)
        except Exception as exc:  # pragma: no cover
            log.error("OrderTrailingDispatcher init failed: %s", exc)
            raise

        # ── Stats ────────────────────────────────────────────────────────────
        self.stats: dict[str, int] = {
            "messages_read": 0,
            "messages_processed": 0,
            "messages_acked": 0,
            "dispatched_ok": 0,
            "dispatched_fail": 0,
            "dlq_pushed": 0,
            "dlq_write_failed": 0,
            "poison_acked": 0,
            "errors": 0,
            "last_message_ts": 0,
        }

        # Run flag
        self.running = False

        log.info(
            "✅ TrailingCommandConsumer initialized | enabled=%s stream=%s group=%s consumer=%s",
            self.enabled, self.stream, self.group, self.consumer,
        )

    # ── Group bootstrap ──────────────────────────────────────────────────────

    def _ensure_group(self) -> None:
        """XGROUP CREATE on the stream (idempotent, MKSTREAM, id='0')."""
        max_attempts = 30
        for attempt in range(1, max_attempts + 1):
            try:
                self.r.xgroup_create(self.stream, self.group, id="0", mkstream=True)
                log.info("✅ Consumer group created: %s", self.group)
                return
            except redis.ResponseError as e:
                msg = str(e)
                if "BUSYGROUP" in msg:
                    log.debug("Consumer group already exists: %s", self.group)
                    return
                if "Redis is loading the dataset in memory" in msg:
                    wait = min(5 * attempt, 30)
                    log.warning("⚠️ Redis loading (%d/%d), sleep %ds", attempt, max_attempts, wait)
                    time.sleep(wait)
                    continue
                log.error("Failed to create consumer group: %s", e)
                raise
            except (redis.ConnectionError, redis.TimeoutError) as e:
                wait = min(2 * attempt, 10)
                log.warning("⚠️ Redis connection error (%d/%d), sleep %ds: %s",
                            attempt, max_attempts, wait, e)
                time.sleep(wait)
                continue
        raise RuntimeError(f"Failed to create consumer group after {max_attempts} attempts")

    # ── Signal handler ───────────────────────────────────────────────────────

    def _signal_handler(self, signum, _frame: Any) -> None:
        log.info("⛔ Received signal %d, shutting down...", signum)
        self.running = False

    # ── Autocal reader (auto-activate on shadow=false) ───────────────────────

    def _refresh_autocal_state(self) -> None:
        """Read autocal:trailing_state:state.shadow and update self.autocal_active.

        Notifies Telegram on transition. Fail-open on Redis errors (no state change).
        """
        if not self.follow_autocal:
            return
        try:
            from core.redis_keys import RK
            raw = self.r.get(RK.AUTOCAL_TRAILING_STATE)
            new_active = False
            if raw:
                snap = json.loads(raw if isinstance(raw, str) else raw.decode())  # type: ignore[union-attr]
                new_active = not bool(snap.get("shadow", True))
            if new_active != self.autocal_active:
                log.warning(
                    "TCC autocal transition: active=%s → %s",
                    self.autocal_active, new_active,
                )
                self._notify_telegram_transition(new_active)
            self.autocal_active = new_active
        except Exception as exc:
            log.debug("_refresh_autocal_state: %s", exc)
        finally:
            self._autocal_last_refresh = time.time()

    def _maybe_refresh_autocal(self) -> None:
        if time.time() - getattr(self, "_autocal_last_refresh", 0.0) >= self._AUTOCAL_REFRESH_S:
            self._refresh_autocal_state()

    @property
    def is_active(self) -> bool:
        """Whether the consumer should be reading + dispatching right now."""
        if self.force_enabled:
            return True
        if self.follow_autocal:
            return self.autocal_active
        return False

    def _notify_telegram_transition(self, active: bool) -> None:
        try:
            notify_stream = os.getenv("TCC_NOTIFY_STREAM", "notify:telegram")
            if active:
                text = (
                    "<b>🟢 TrailingCommandConsumer — ACTIVATED (live SL execution)</b>\n\n"
                    "Autocal промотил <code>shadow=false</code>. Consumer теперь читает "
                    "<code>events:trailing:commands</code> и шлёт SL-modify в gateway.\n\n"
                    "<b>Rollback:</b>\n"
                    "<code>docker exec redis-worker-1 redis-cli SET "
                    "autocal:trailing_state:state '{\"shadow\":true}'</code>\n"
                    "Эффект ≤60 с — consumer вернётся в idle, новые SL-модификации остановятся."
                )
            else:
                text = (
                    "<b>🔴 TrailingCommandConsumer — SUSPENDED (rolled back to shadow)</b>\n\n"
                    "Autocal вернул <code>shadow=true</code>. Consumer прекратил dispatch.\n"
                    "Команды продолжат копиться в <code>events:trailing:commands</code>, "
                    "но реальные SL-модификации не идут."
                )
            self.r.xadd(
                notify_stream,
                {
                    "type": "report",
                    "subtype": "trailing_cmd_consumer",
                    "ts": str(int(time.time() * 1000)),
                    "text": text,
                    "parse_mode": "HTML",
                },
                maxlen=50_000,
            )
            log.info("Telegram notification sent: active=%s", active)
        except Exception as exc:
            log.warning("Telegram notify failed: %s", exc)

    # ── Parse ────────────────────────────────────────────────────────────────

    def _parse_command(self, fields: dict[str, Any]) -> dict[str, Any] | None:
        """Validate and parse fields → typed dict.

        Returns None on:
          - missing required keys
          - shadow="1" (defensive — producer should not write these here)
          - new_sl not float-parseable
        """
        if not fields:
            return None

        # Defensive: skip shadow=1 messages (they shouldn't reach this stream)
        shadow = str(fields.get("shadow", "0"))
        if shadow == "1":
            log.debug("skipping shadow=1 command (sid=%s)", fields.get("sid"))
            return None

        # Required fields
        for k in _REQUIRED_FIELDS:
            v = fields.get(k)
            if v is None or v == "":
                log.warning("parse_command: missing required field '%s' in %s", k, fields)
                return None

        # Parse new_sl as float
        try:
            new_sl = float(fields["new_sl"])
        except (TypeError, ValueError):
            log.warning("parse_command: invalid new_sl=%r", fields.get("new_sl"))
            return None

        if new_sl <= 0:
            log.warning("parse_command: non-positive new_sl=%s", new_sl)
            return None

        side = str(fields["side"]).upper()
        if side not in ("LONG", "SHORT", "BUY", "SELL"):
            log.warning("parse_command: invalid side=%r", side)
            return None

        return {
            "sid": str(fields["sid"]),
            "position_id": str(fields["position_id"]),
            "symbol": str(fields["symbol"]),
            "side": side,
            "new_sl": new_sl,
            "reason_code": str(fields.get("reason_code", "")),
            "profile": str(fields.get("profile", "")),
            "ts_ms": str(fields.get("ts_ms", "")),
        }

    # ── Dispatch to gateway ──────────────────────────────────────────────────

    def _dispatch(self, cmd: dict[str, Any]) -> tuple[bool, str]:
        """Call OrderTrailingDispatcher.send_trailing_modify.

        Returns (success, error_msg). Wraps exceptions.
        """
        t0 = time.time()
        try:
            ok = self.dispatcher.send_trailing_modify(
                sid=cmd["sid"],
                symbol=cmd["symbol"],
                side=cmd["side"],
                position_id=cmd["position_id"],
                new_sl=cmd["new_sl"],
                metadata={
                    "reason_code": cmd.get("reason_code", ""),
                    "profile": cmd.get("profile", ""),
                    "source": "trailing_command_consumer",
                },
            )
        except Exception as exc:
            elapsed_ms = (time.time() - t0) * 1000.0
            _observe(_tcc_dispatch_latency_ms, elapsed_ms)
            _inc(_tcc_dispatched_total, "failure")
            self.stats["dispatched_fail"] += 1
            err = f"{type(exc).__name__}:{exc}"
            log.error("dispatch exception sid=%s: %s", cmd.get("sid"), err)
            return False, err

        elapsed_ms = (time.time() - t0) * 1000.0
        _observe(_tcc_dispatch_latency_ms, elapsed_ms)

        if ok:
            _inc(_tcc_dispatched_total, "success")
            self.stats["dispatched_ok"] += 1
            return True, ""

        _inc(_tcc_dispatched_total, "failure")
        self.stats["dispatched_fail"] += 1
        return False, "gateway_returned_false"

    # ── DLQ ──────────────────────────────────────────────────────────────────

    def _push_dlq(self, msg_id: str, fields: dict[str, Any], reason: str) -> bool:
        """XADD to events:trailing:dlq. Returns True on success."""
        try:
            entry = {
                "kind": "trailing_cmd",
                "reason": reason[:256],
                "original_msg_id": str(msg_id),
                "ts_ms": str(int(time.time() * 1000)),
                "source": "trailing_command_consumer",
                "stream": self.stream,
                "fields_json": json.dumps(fields, ensure_ascii=False, default=str)[:4000],
            }
            self.r.xadd(self.dlq_stream, entry, maxlen=5000, approximate=True)  # type: ignore[arg-type]
            self.stats["dlq_pushed"] += 1
            _inc(_tcc_dlq_total, reason.split(":")[0])
            log.warning("⚠️ DLQ push: msg_id=%s reason=%s", msg_id, reason)
            return True
        except Exception as exc:
            self.stats["dlq_write_failed"] += 1
            _inc(_tcc_dlq_write_failed_total)
            log.warning("⚠️ DLQ write failed for msg_id=%s: %s", msg_id, exc)
            return False

    # ── Retry cap ────────────────────────────────────────────────────────────

    def _check_retry_cap(self, msg_id: str) -> bool:
        """Increment retry counter for msg_id. Return True if exceeded cap."""
        key = f"tcc:retries:{msg_id}"
        try:
            count = int(self.r.incr(key) or 0)  # type: ignore[arg-type]
            self.r.expire(key, self.retry_ttl_sec)
            if count > self.max_retries:
                self.stats["poison_acked"] += 1
                _inc(_tcc_poison_total)
                log.warning(
                    "⚠️ Poison cap: msg_id=%s retries=%d > max=%d → force DLQ+ACK",
                    msg_id, count, self.max_retries,
                )
                return True
        except Exception as exc:
            log.debug("retry-counter error (ignored): %s", exc)
        return False

    # ── ACK ──────────────────────────────────────────────────────────────────

    def _xack(self, msg_id: str) -> None:
        try:
            self.r.xack(self.stream, self.group, msg_id)
            self.stats["messages_acked"] += 1
        except Exception as exc:
            log.warning("XACK failed for %s: %s", msg_id, exc)

    # ── Process one message ──────────────────────────────────────────────────

    def _process_one_message(self, msg_id: str, fields: dict[str, Any]) -> None:
        """parse → dispatch → DLQ-on-failure → ACK semantics.

        Rules:
          - retry-cap exceeded → DLQ (force ACK regardless of DLQ result)
          - parse error        → DLQ; ACK if DLQ ok, else no-ACK (PEL retry)
          - dispatch failure   → DLQ; ACK if DLQ ok, else no-ACK
          - success            → ACK
        """
        self.stats["messages_read"] += 1
        _inc(_tcc_received_total)

        # Poison guard
        if self._check_retry_cap(msg_id):
            self._push_dlq(msg_id, fields, "max_retries_exceeded")
            self._xack(msg_id)  # force ACK
            return

        # Parse
        cmd = self._parse_command(fields)
        if cmd is None:
            ok = self._push_dlq(msg_id, fields, "parse_error")
            if ok:
                self._xack(msg_id)
            self.stats["errors"] += 1
            return

        # Dispatch
        try:
            success, err_msg = self._dispatch(cmd)
        except Exception as exc:
            # _dispatch already wraps, but be defensive
            success = False
            err_msg = f"unexpected:{type(exc).__name__}:{exc}"
            log.error("unexpected dispatch error sid=%s: %s", cmd.get("sid"), err_msg)

        if not success:
            ok = self._push_dlq(msg_id, fields, f"dispatch_failed:{err_msg}")
            if not ok:
                return  # leave in PEL
            self._xack(msg_id)
            self.stats["errors"] += 1
            return

        # Happy path
        self._xack(msg_id)
        self.stats["messages_processed"] += 1
        self.stats["last_message_ts"] = int(time.time())

    # ── PEL reclaim ──────────────────────────────────────────────────────────

    def _reclaim_pel(self) -> None:
        """XAUTOCLAIM stale messages from PEL → re-process."""
        try:
            result = self.r.xautoclaim(
                self.stream,
                self.group,
                self.consumer,
                self.pel_stale_ms,
                "0-0",
                count=self.pel_reclaim_count,
            )
            claimed = result[1] if isinstance(result, (list, tuple)) and len(result) > 1 else []
            if claimed:
                log.info("♻️ PEL reclaim: %d messages", len(claimed))
            for msg_id, fields in claimed:
                self._process_one_message(msg_id, fields)
            # Update pending gauge
            try:
                pending = self.r.xpending(self.stream, self.group)
                if pending and isinstance(pending, dict):
                    n = int(pending.get("pending", 0) or 0)
                    if _tcc_pel_pending is not None:
                        _tcc_pel_pending.set(n)
            except Exception:
                pass
        except AttributeError:
            pass  # redis-py < 4.3
        except Exception as exc:
            log.debug("PEL reclaim error (ignored): %s", exc)

    # ── Read messages ────────────────────────────────────────────────────────

    def _read_messages(self) -> list[tuple[str, dict[str, str]]]:
        try:
            resp = self.r.xreadgroup(
                self.group,
                self.consumer,
                streams={self.stream: ">"},
                count=self.batch_size,
                block=self.block_ms,
            )
            if not resp:
                return []
            for stream_key, msgs in resp:
                if stream_key == self.stream:
                    return msgs
            return []
        except Exception as exc:
            msg = str(exc)
            if "NOGROUP" in msg:
                log.error("NOGROUP — recreating: %s", msg)
                try:
                    self._ensure_group()
                except Exception as create_err:
                    log.error("Failed to recreate group: %s", create_err)
                return []
            log.error("xreadgroup error: %s", msg)
            return []

    # ── Stats ────────────────────────────────────────────────────────────────

    def _log_stats(self) -> None:
        log.info(
            "📊 TCC Stats: read=%d processed=%d acked=%d dispatched_ok=%d "
            "dispatched_fail=%d dlq=%d dlq_fail=%d poison=%d errors=%d",
            self.stats["messages_read"],
            self.stats["messages_processed"],
            self.stats["messages_acked"],
            self.stats["dispatched_ok"],
            self.stats["dispatched_fail"],
            self.stats["dlq_pushed"],
            self.stats["dlq_write_failed"],
            self.stats["poison_acked"],
            self.stats["errors"],
        )

    # ── Main loop ────────────────────────────────────────────────────────────

    def run(self) -> None:
        """Main XREADGROUP loop. _ensure_group is called lazily (first active iter)."""
        log.info(
            "🚀 TrailingCommandConsumer.run() starting | force=%s follow_autocal=%s",
            self.force_enabled, self.follow_autocal,
        )
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        self.running = True
        self._group_ensured = False
        self._autocal_last_refresh = 0.0
        self._refresh_autocal_state()  # initial autocal read

        last_stats = time.time()
        last_reclaim = time.time()

        log.info(
            "📊 batch_size=%d block_ms=%d initial_active=%s",
            self.batch_size, self.block_ms, self.is_active,
        )

        while self.running:
            try:
                # Auto-activate/deactivate via autocal state
                self._maybe_refresh_autocal()

                if not self.is_active:
                    # Idle: re-check autocal each idle_sleep_s
                    time.sleep(min(float(self.idle_sleep_s), self._AUTOCAL_REFRESH_S))
                    continue

                # Lazy group creation — only when active for the first time
                if not self._group_ensured:
                    try:
                        self._ensure_group()
                        self._group_ensured = True
                        log.info("Consumer group ensured (first activation)")
                    except Exception as exc:
                        log.error("ensure_group failed: %s — retrying in 5s", exc)
                        time.sleep(5)
                        continue

                msgs = self._read_messages()
                if not msgs:
                    if time.time() - last_stats >= self.stats_interval_s:
                        self._log_stats()
                        last_stats = time.time()
                    if time.time() - last_reclaim >= self.pel_reclaim_interval_s:
                        self._reclaim_pel()
                        last_reclaim = time.time()
                    continue

                for msg_id, fields in msgs:
                    self._process_one_message(msg_id, fields)

                if time.time() - last_reclaim >= self.pel_reclaim_interval_s:
                    self._reclaim_pel()
                    last_reclaim = time.time()

                if time.time() - last_stats >= self.stats_interval_s:
                    self._log_stats()
                    last_stats = time.time()

                time.sleep(0.05)

            except KeyboardInterrupt:
                log.info("⛔ Keyboard interrupt")
                self.running = False
                break
            except redis.ConnectionError as e:
                self.stats["errors"] += 1
                log.error("Redis connection error: %s", e)
                time.sleep(5.0)
            except Exception as e:
                self.stats["errors"] += 1
                log.error("Loop error: %s", e, exc_info=True)
                time.sleep(1.0)

        log.info("🛑 TrailingCommandConsumer stopped")
        self._log_stats()


# ── Entrypoint ────────────────────────────────────────────────────────────────


def main() -> None:
    log.info("=" * 80)
    log.info("TrailingCommandConsumer Service")
    log.info("=" * 80)

    metrics_port = int(os.getenv("TCC_METRICS_PORT", "9923"))
    try:
        from prometheus_client import start_http_server
        start_http_server(metrics_port)
        log.info("Prometheus metrics: :%d/metrics", metrics_port)
    except Exception as exc:
        log.warning("Prometheus HTTP server not started: %s", exc)

    force_enabled = os.getenv("TCC_ENABLED", "0") == "1"
    follow_autocal = os.getenv("TCC_FOLLOW_AUTOCAL", "1") == "1"

    if not force_enabled and not follow_autocal:
        log.warning(
            "TCC_ENABLED=0 AND TCC_FOLLOW_AUTOCAL=0 — consumer is permanently disabled; "
            "idle-loop (no Redis reads)."
        )
        _stop = {"flag": False}

        def _stop_handler(signum, _frame):
            log.info("⛔ Signal %d received, stopping idle loop", signum)
            _stop["flag"] = True

        signal.signal(signal.SIGINT, _stop_handler)
        signal.signal(signal.SIGTERM, _stop_handler)
        idle = int(os.getenv("TCC_IDLE_SLEEP_SEC", "60"))
        while not _stop["flag"]:
            time.sleep(idle)
        log.info("Idle loop exited")
        return

    if force_enabled:
        log.info("TCC_ENABLED=1 — force-on (autocal ignored)")
    else:
        log.info(
            "TCC_ENABLED=0 + TCC_FOLLOW_AUTOCAL=1 — autocal-driven "
            "(idle until autocal:trailing_state:state.shadow=false)"
        )

    consumer = TrailingCommandConsumer()
    log.info("Configuration:")
    log.info("  Redis URL: %s", consumer.redis_url)
    log.info("  Stream:    %s", consumer.stream)
    log.info("  Group:     %s", consumer.group)
    log.info("  Consumer:  %s", consumer.consumer)
    log.info("  DLQ:       %s", consumer.dlq_stream)
    log.info("=" * 80)

    try:
        consumer.run()
    except Exception as e:
        log.error("❌ Fatal: %s", e, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
