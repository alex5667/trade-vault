from utils.time_utils import get_ny_time_millis

"""
ExecutionGateService  (v2 — dual-buffer, no MT5)

Implements "Second Opinion" logic for high-reliability execution.
Acts as a bridge between Signal Generators and the Order Executor.

Inputs:
  - signals:crypto:raw (Monolith): The "Proposal". Contains full trade parameters.
  - signals:of:confirm (OFConfirmService): The "Validation". Contains independent confirmation.

Output:
  - orders:queue:binance (verified orders for Binance executor).

Logic (ENFORCE mode):
  1. Buffer BOTH proposals AND confirmations (dual-buffer).
  2. On each new event — try to match across buffers.
     Match criteria: symbol + direction + |ts_diff| <= EXEC_GATE_MATCH_MS.
  3. If matched and ok=1 -> publish to orders:queue:binance.
  4. Cleanup loop prunes expired entries from both buffers.

Logic (PASS-THROUGH mode, default):
  - Proposals pass immediately with validation_status="bypassed".
"""

import asyncio
import json
import logging
import os
import signal
import time
from dataclasses import dataclass, field
from typing import Any

import redis.asyncio as aioredis
from prometheus_client import Counter, Gauge, start_http_server

from core.redis_keys import RedisStreams as RS
from utils.task_manager import safe_create_task
import contextlib

# ---------------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------------
orders_published_total = Counter(
    "exec_gate_orders_published_total",
    "Total verified orders published",
    ["symbol", "direction"],
)
proposals_received_total = Counter(
    "exec_gate_proposals_received_total",
    "Total signal proposals received",
    ["symbol"],
)
proposals_bypassed_total = Counter(
    "exec_gate_proposals_bypassed_total",
    "Total proposals bypassed (PASS-THROUGH mode)",
    ["symbol"],
)
proposals_expired_total = Counter(
    "exec_gate_proposals_expired_total",
    "Total proposals expired by TTL",
    ["symbol"],
)
confirmations_received_total = Counter(
    "exec_gate_confirmations_received_total",
    "Total confirmations received",
    ["symbol"],
)
confirmations_expired_total = Counter(
    "exec_gate_confirmations_expired_total",
    "Total confirmations expired by TTL (no matching proposal)",
    ["symbol"],
)
confirmations_matched_total = Counter(
    "exec_gate_confirmations_matched_total",
    "Confirmations that matched a proposal",
    ["symbol", "direction"],
)
confirmations_orphan_total = Counter(
    "exec_gate_confirmations_orphan_total",
    "Confirmations buffered because no proposal was available yet",
    ["symbol"],
)
virtual_enforce_total = Counter(
    "exec_gate_virtual_enforce_total",
    "Virtual proposals processed through ENFORCE gate",
    ["symbol", "direction", "result"],  # result: passed / rejected
)
incomplete_dropped_total = Counter(
    "exec_gate_incomplete_dropped_total",
    "Proposals dropped due to missing qty/sl/tp_levels",
    ["symbol"],
)
pending_proposals_gauge = Gauge(
    "exec_gate_pending_proposals",
    "Current number of pending proposals",
)
pending_confirmations_gauge = Gauge(
    "exec_gate_pending_confirmations",
    "Current number of pending confirmations",
)
mode_info = Gauge(
    "exec_gate_enforce_mode",
    "1 if ENFORCE mode, 0 if PASS-THROUGH",
)
virtual_enforce_mode_info = Gauge(
    "exec_gate_virtual_enforce_mode",
    "1 if virtual proposals also go through ENFORCE gate",
)
exec_gate_rejection_total = Counter(
    "exec_gate_rejection_total",
    "Orders rejected by SAFEGUARD (ok=0)",
    ["symbol", "direction", "reason", "virtual"],
)
exec_gate_orphan_confirmation_buffered = Counter(
    "exec_gate_orphan_confirmation_buffered",
    "Confirmations buffered with no matching proposal (orphan)",
    ["symbol"],
)
exec_gate_orphan_expired_total = Counter(
    "exec_gate_orphan_expired_total",
    "Orphan confirmations expired without proposal match",
    ["symbol"],
)
exec_gate_qty_mismatch = Counter(
    "exec_gate_qty_mismatch",
    "qty/lot disagreement (>10% delta)",
    ["symbol"],
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("execution_gate_service")


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------
@dataclass
class Proposal:
    symbol: str
    direction: str          # "long" or "short"
    ts_ms: int              # generated_at from the signal
    payload: dict[str, Any]
    received_at: float = field(default_factory=time.time)


@dataclass
class Confirmation:
    symbol: str
    direction: str          # "long" or "short"
    ts_ms: int              # ts_ms from OFConfirm
    data: dict[str, Any]    # full confirmation payload (ok, score, reason …)
    received_at: float = field(default_factory=time.time)


# ---------------------------------------------------------------------------
# Service
# ---------------------------------------------------------------------------
class ExecutionGateService:
    def __init__(self):
        self.redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
        self.redis: aioredis.Redis | None = None

        # --- Streams ---
        self.stream_raw = os.getenv("CRYPTO_RAW_STREAM", RS.CRYPTO_RAW)
        self.stream_confirm = os.getenv("OF_CONFIRM_STREAM", RS.OF_CONFIRM)

        # Output queue — Binance executor (MT5 output disabled)
        self.queue_out = os.getenv("ORDERS_QUEUE_BINANCE", RS.ORDERS_QUEUE_BINANCE)

        # --- Config ---
        self.proposal_ttl_s = float(os.getenv("EXEC_GATE_TTL_S", "5.0"))
        self.match_tolerance_ms = int(os.getenv("EXEC_GATE_MATCH_MS", "2000"))
        self.require_of_confirm = os.getenv(
            "EXEC_GATE_REQUIRE_OF_CONFIRM", "false"
        ).lower() in {"1", "true", "yes", "on"}
        self.enforce_virtual = os.getenv(
            "EXEC_GATE_ENFORCE_VIRTUAL", "false"
        ).lower() in {"1", "true", "yes", "on"}
        self.shadow_only = os.getenv(
            "BINANCE_VIRTUAL_ORDERS_ENABLED", "0"
        ).lower() in {"1", "true", "yes", "on"}

        self.running = True
        self._loop: asyncio.AbstractEventLoop | None = None

        # --- Dual buffer ---
        # symbol -> List[Proposal]
        self.proposals: dict[str, list[Proposal]] = {}
        # symbol -> List[Confirmation]  (F3 fix: buffer confirms too)
        self.confirmations: dict[str, list[Confirmation]] = {}

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    async def start(self):
        logger.info(
            f"Starting ExecutionGateService "
            f"(mode={'ENFORCE' if self.require_of_confirm else 'PASS-THROUGH'}"
            f", virtual={'ENFORCE' if self.enforce_virtual else 'SHADOW'}) "
            f"streams: {self.stream_raw} + {self.stream_confirm} -> {self.queue_out}"
        )
        # Bounded pool: prevents connection flood to redis-worker-1 when
        # EXEC_GATE_ENFORCE_VIRTUAL=true triggers concurrent RPUSH bursts.
        # Unbounded from_url() was creating O(signals) connections under load.
        from redis.asyncio import BlockingConnectionPool
        _max_conn = int(os.getenv("EXEC_GATE_MAX_REDIS_CONN", "10"))
        _pool = BlockingConnectionPool.from_url(
            self.redis_url,
            max_connections=_max_conn,
            decode_responses=True,
            socket_timeout=float(os.getenv("EXEC_GATE_SOCKET_TIMEOUT", "2.0")),
            socket_connect_timeout=float(os.getenv("EXEC_GATE_CONN_TIMEOUT", "5.0")),
            timeout=2,
        )
        self.redis = aioredis.Redis(connection_pool=_pool)
        self._loop = asyncio.get_running_loop()

        # Metrics
        start_http_server(int(os.getenv("PROMETHEUS_PORT", 8004)))
        mode_info.set(1 if self.require_of_confirm else 0)
        virtual_enforce_mode_info.set(1 if self.enforce_virtual else 0)

        tasks = [
            safe_create_task(self._consume_raw_signals()),
            safe_create_task(self._consume_confirmations()),
            safe_create_task(self._cleanup_loop()),
        ]
        await asyncio.gather(*tasks)  # type: ignore

    @staticmethod
    def _is_redis_loading(exc: Exception) -> bool:
        """True when Redis is still loading its snapshot (transient startup state)."""
        return "loading" in str(exc).lower()

    # ------------------------------------------------------------------
    # Proposal consumer
    # ------------------------------------------------------------------
    async def _consume_raw_signals(self):
        """Consumes proposals from the monolith (signals:crypto:raw)."""
        logger.info(f"Listening for proposals on {self.stream_raw}")
        last_id = "$"
        _backoff = 1.0

        while self.running:
            try:
                results = await self.redis.xread(  # type: ignore
                    {self.stream_raw: last_id}, count=10, block=1000
                )
                _backoff = 1.0

                for _stream_name, messages in results:
                    for msg_id, fields in messages:
                        last_id = msg_id
                        await self._handle_proposal(fields)

            except Exception as e:
                if self._is_redis_loading(e):
                    logger.warning(
                        f"Redis loading snapshot – raw signals paused (retry in {_backoff:.0f}s)"
                    )
                    await asyncio.sleep(_backoff)
                    _backoff = min(_backoff * 2, 30.0)
                else:
                    logger.error(f"Error consuming raw signals: {e}")
                    await asyncio.sleep(1)

    async def _handle_proposal(self, fields: dict[str, Any]):
        try:
            payload_str = fields.get("payload")
            if not payload_str:
                return

            data = json.loads(payload_str)
            symbol = data.get("symbol")
            direction = data.get("direction", "").lower()

            if not symbol or direction not in ("long", "short"):
                return

            # Use ts_ms (canonical) or generated_at or fallback to now
            ts_ms = int(
                data.get("ts_ms")
                or data.get("generated_at")
                or get_ny_time_millis()
            )

            proposal = Proposal(
                symbol=symbol,
                direction=direction,
                ts_ms=ts_ms,
                payload=data,
            )

            # Always count
            proposals_received_total.labels(symbol=symbol).inc()

            is_virtual = bool(data.get("is_virtual", 0) or 0)

            # --- VIRTUAL SHADOW mode (legacy): always proceed, match if possible ---
            if is_virtual and not self.enforce_virtual:
                matched_confirm = self._try_match_confirm_for_proposal(proposal)
                if matched_confirm is not None:
                    proposal.payload["validation_status"] = (
                        "passed" if matched_confirm.data.get("ok") == 1 else "failed"
                    )
                    proposal.payload["validation_reason"] = matched_confirm.data.get(
                        "reason", "confirmed"
                    )
                    await self._publish_execution(proposal, matched_confirm.data)
                else:
                    proposal.payload["validation_status"] = "passed"
                    proposal.payload["validation_reason"] = "shadow_pass"
                    await self._publish_execution(proposal, {"ok": 1, "score": 1.0, "reason": "shadow_pass"})
                return

            # --- REAL: check for PASS-THROUGH vs ENFORCE ---
            # (also applies to VIRTUAL when enforce_virtual=True)
            if not self.require_of_confirm:
                data["validation_status"] = "bypassed"
                data["validation_reason"] = "OFConfirm validation disabled"
                proposals_bypassed_total.labels(symbol=symbol).inc()
                await self._publish_execution(proposal, {"ok": 1, "score": 1.0})
                return

            # --- REAL (ENFORCE): buffer proposal, then try to match ---
            self.proposals.setdefault(symbol, []).append(proposal)
            logger.info(
                f"Received REAL PROPOSAL: {symbol} {direction} (buffered, waiting for confirm)"
            )

            # F3 fix: check if a confirmation already arrived first (for real signals)
            matched_confirm = self._try_match_confirm_for_proposal(proposal)
            if matched_confirm is not None:
                # Remove proposal from buffer (just appended)
                self.proposals[symbol].remove(proposal)
                proposal.payload["validation_status"] = (
                    "passed" if matched_confirm.data.get("ok") == 1 else "failed"
                )
                proposal.payload["validation_reason"] = matched_confirm.data.get(
                    "reason", "confirmed"
                )
                confirmations_matched_total.labels(
                    symbol=symbol, direction=direction
                ).inc()
                await self._publish_execution(proposal, matched_confirm.data)

        except Exception as e:
            logger.error(f"Failed to parse proposal: {e}")

    def _try_match_confirm_for_proposal(self, proposal: Proposal) -> Confirmation | None:
        """Check buffered confirmations for a match. Returns & removes matched Confirmation."""
        confirms = self.confirmations.get(proposal.symbol, [])
        for i, conf in enumerate(confirms):
            if conf.direction == proposal.direction:
                if abs(conf.ts_ms - proposal.ts_ms) <= self.match_tolerance_ms:
                    confirms.pop(i)
                    if not confirms:
                        self.confirmations.pop(proposal.symbol, None)
                    return conf
        return None

    # ------------------------------------------------------------------
    # Confirmation consumer
    # ------------------------------------------------------------------
    async def _consume_confirmations(self):
        """Consumes validations from OFConfirmService (signals:of:confirm)."""
        logger.info(f"Listening for confirmations on {self.stream_confirm}")
        last_id = "$"
        _backoff = 1.0

        while self.running:
            try:
                results = await self.redis.xread(  # type: ignore
                    {self.stream_confirm: last_id}, count=10, block=1000
                )
                _backoff = 1.0

                for _stream_name, messages in results:
                    for msg_id, fields in messages:
                        last_id = msg_id
                        await self._handle_confirmation(fields)

            except Exception as e:
                if self._is_redis_loading(e):
                    logger.warning(
                        f"Redis loading snapshot – confirmations paused (retry in {_backoff:.0f}s)"
                    )
                    await asyncio.sleep(_backoff)
                    _backoff = min(_backoff * 2, 30.0)
                else:
                    logger.error(f"Error consuming confirmations: {e}")
                    await asyncio.sleep(1)

    async def _handle_confirmation(self, fields: dict[str, Any]):
        try:
            payload_str = fields.get("payload")
            if not payload_str:
                return

            data = json.loads(payload_str)
            symbol = data.get("symbol")
            direction = data.get("direction", "").lower()

            if not symbol or not direction:
                return

            confirmations_received_total.labels(symbol=symbol).inc()
            ts_ms = int(data.get("ts_ms", get_ny_time_millis()))

            # 1. Try to match against pending proposals
            matched_prop = self._try_match_proposal_for_confirm(
                symbol, direction, ts_ms
            )
            if matched_prop is not None:
                ok = data.get("ok", 0)
                matched_prop.payload["validation_status"] = (
                    "passed" if ok == 1 else "failed"
                )
                matched_prop.payload["validation_reason"] = data.get(
                    "reason", "confirmed"
                )
                confirmations_matched_total.labels(
                    symbol=symbol, direction=direction
                ).inc()
                await self._publish_execution(matched_prop, data)
                return

            # 2. F3 fix: no proposal yet — buffer the confirmation
            confirm_obj = Confirmation(
                symbol=symbol,
                direction=direction,
                ts_ms=ts_ms,
                data=data,
            )
            self.confirmations.setdefault(symbol, []).append(confirm_obj)
            confirmations_orphan_total.labels(symbol=symbol).inc()
            exec_gate_orphan_confirmation_buffered.labels(symbol=symbol).inc()
            logger.info(
                f"⏳ Confirmation for {symbol} {direction} buffered (orphan, no proposal yet) "
                f"ts_ms={ts_ms} ok={data.get('ok', 0)} score={data.get('score', '?')}. "
                f"Waiting for proposal (TTL={self.proposal_ttl_s}s)."
            )

        except Exception as e:
            logger.error(f"Failed to handle confirmation: {e}")

    def _try_match_proposal_for_confirm(
        self, symbol: str, direction: str, ts_ms: int
    ) -> Proposal | None:
        """Check buffered proposals for a match. Returns & removes matched Proposal."""
        proposals = self.proposals.get(symbol, [])
        for i, prop in enumerate(proposals):
            if prop.direction == direction:
                if abs(prop.ts_ms - ts_ms) <= self.match_tolerance_ms:
                    proposals.pop(i)
                    if not proposals:
                        self.proposals.pop(symbol, None)
                    return prop
        return None

    # ------------------------------------------------------------------
    # Publish
    # ------------------------------------------------------------------
    async def _publish_execution(
        self, proposal: Proposal, confirmation: dict[str, Any]
    ):
        """Publish the verified order to the execution queue."""
        try:
            is_virtual = bool(proposal.payload.get("is_virtual", 0) or 0)

            # --- OVERRIDE FOR GLOBAL VIRTUAL MODE ---
            if self.shadow_only:
                is_virtual = True
                proposal.payload["is_virtual"] = 1

            is_ok = int(confirmation.get("ok", 0)) == 1

            # Virtual shadow mode (legacy, enforce_virtual=False): always pass
            if is_virtual and not self.enforce_virtual:
                is_ok = True

            # SAFEGUARD: Do not execute if validation failed
            if not is_ok:
                reason = confirmation.get("reason", "UNKNOWN")
                score = confirmation.get("score", 0.0)

                logger.warning(
                    f"🚫 EXEC GATE REJECTED: {proposal.symbol} {proposal.direction} "
                    f"reason={reason} score={score:.2f} virtual={is_virtual}"
                )

                exec_gate_rejection_total.labels(
                    symbol=proposal.symbol,
                    direction=proposal.direction,
                    reason=reason[:32],
                    virtual="true" if is_virtual else "false"
                ).inc()

                if is_virtual:
                    virtual_enforce_total.labels(
                        symbol=proposal.symbol, direction=proposal.direction, result="rejected"
                    ).inc()
                return

            if is_virtual:
                virtual_enforce_total.labels(
                    symbol=proposal.symbol, direction=proposal.direction, result="passed"
                ).inc()
                logger.info(
                    f"👻 VIRTUAL EXECUTION GATE: {'ENFORCE' if self.enforce_virtual else 'SHADOW'} "
                    f"ok=1 {proposal.symbol} {proposal.direction}. "
                    f"Event tracked by TradeMonitor — skipping Binance queue."
                )
                # DO NOT push virtual trades to binance executor queue. TradeMonitor handles their lifecycle.
                return
            else:
                logger.info(
                    f"✅ EXECUTION GATE: Validated {proposal.symbol} "
                    f"{proposal.direction}. Publishing order."
                )

            order_payload = proposal.payload.copy()

            # --- Ensure executor-required fields are present ---
            # 1. action: open (required by BinanceExecutor.process_one)
            order_payload.setdefault("action", "open")

            # 2. side: LONG/SHORT (executor expects uppercase)
            raw_side = str(order_payload.get("side") or proposal.direction or "").upper()
            if raw_side in ("LONG", "SHORT"):
                order_payload["side"] = raw_side
            elif proposal.direction == "long":
                order_payload["side"] = "LONG"
            elif proposal.direction == "short":
                order_payload["side"] = "SHORT"

            # 3. sid: canonical execution ID (required by process_one)
            if not order_payload.get("sid"):
                sym = str(order_payload.get("symbol") or proposal.symbol or "").upper()
                ts = int(order_payload.get("ts_ms") or proposal.ts_ms or get_ny_time_millis())
                order_payload["sid"] = f"crypto-of:{sym}:{ts}"

            # 4. Guard: reject payloads missing critical execution fields
            # Raw signals from signals:crypto:raw lack qty/sl/tp_levels — pushing them
            # to the executor queue would only DLQ them. Skip and let the signal_pipeline
            # push complete order payloads via _push_virtual_to_binance_queue.

            # Critical Fix: Map the properly sized 'lot' to 'qty' because raw signals
            # often contain dummy qty=0.01 from candidate generation.
            if order_payload.get("lot") is not None:
                with contextlib.suppress(ValueError):
                    new_qty = float(order_payload["lot"])
                    old_qty = float(order_payload.get("qty") or 0.0)
                    if old_qty > 0 and abs(new_qty - old_qty) > old_qty * 0.1:
                        logger.warning(
                            f"⚠️ EXEC GATE: qty/lot mismatch {proposal.symbol} "
                            f"qty={old_qty} vs lot={new_qty} (delta={abs(new_qty-old_qty):.4f}). "
                            f"Using lot (newer sizing)."
                        )
                        exec_gate_qty_mismatch.labels(symbol=proposal.symbol).inc()
                    order_payload["qty"] = new_qty

            has_qty = order_payload.get("qty") is not None or order_payload.get("quantity") is not None or order_payload.get("lot") is not None
            has_sl = order_payload.get("sl") is not None
            has_tp = bool(order_payload.get("tp_levels"))
            if not has_qty or not has_sl or not has_tp:
                logger.warning(
                    "⚠️ EXEC GATE: Skipping %s %s — missing execution fields "
                    "(qty=%s sl=%s tp=%s). Signal pipeline should push complete orders directly.",
                    proposal.symbol, proposal.direction, has_qty, has_sl, has_tp,
                )
                incomplete_dropped_total.labels(symbol=proposal.symbol).inc()
                return

            order_payload["gate_verified"] = True
            order_payload["gate_ts_ms"] = get_ny_time_millis()
            order_payload["confirm_score"] = confirmation.get("score", 1.0)

            await self.redis.rpush(self.queue_out, json.dumps(order_payload))  # type: ignore
            orders_published_total.labels(
                symbol=proposal.symbol, direction=proposal.direction
            ).inc()

        except Exception as e:
            logger.error(f"Failed to publish execution order: {e}")

    # ------------------------------------------------------------------
    # Cleanup (dual-buffer)
    # ------------------------------------------------------------------
    async def _cleanup_loop(self):
        """Remove stale proposals AND confirmations from both buffers."""
        while self.running:
            try:
                now = time.time()
                prop_count = 0
                confirm_count = 0

                # --- Prune proposals ---
                for sym in list(self.proposals.keys()):
                    fresh = [
                        p
                        for p in self.proposals[sym]
                        if (now - p.received_at) < self.proposal_ttl_s
                    ]
                    if len(fresh) != len(self.proposals[sym]):
                        removed = len(self.proposals[sym]) - len(fresh)
                        proposals_expired_total.labels(symbol=sym).inc(removed)
                        logger.debug(f"Pruned {removed} stale proposals for {sym}")

                    if not fresh:
                        del self.proposals[sym]
                    else:
                        self.proposals[sym] = fresh
                        prop_count += len(fresh)

                # --- Prune confirmations ---
                for sym in list(self.confirmations.keys()):
                    fresh = [
                        c
                        for c in self.confirmations[sym]
                        if (now - c.received_at) < self.proposal_ttl_s
                    ]
                    if len(fresh) != len(self.confirmations[sym]):
                        removed = len(self.confirmations[sym]) - len(fresh)
                        expired_confirms = [
                            c for c in self.confirmations[sym]
                            if (now - c.received_at) >= self.proposal_ttl_s
                        ]
                        confirmations_expired_total.labels(symbol=sym).inc(removed)
                        exec_gate_orphan_expired_total.labels(symbol=sym).inc(removed)

                        if expired_confirms:
                            ages = [(now - c.received_at) for c in expired_confirms]
                            avg_age = sum(ages) / len(ages)
                            logger.warning(
                                f"⏳ {removed} orphan confirmations expired for {sym} "
                                f"(avg age={avg_age:.1f}s, max={max(ages):.1f}s). "
                                f"Indicates proposals not arriving from signal_pipeline."
                            )

                    if not fresh:
                        del self.confirmations[sym]
                    else:
                        self.confirmations[sym] = fresh
                        confirm_count += len(fresh)

                pending_proposals_gauge.set(prop_count)
                pending_confirmations_gauge.set(confirm_count)
                await asyncio.sleep(1)

            except Exception as e:
                logger.error(f"Cleanup error: {e}")
                await asyncio.sleep(5)

    # ------------------------------------------------------------------
    # Shutdown
    # ------------------------------------------------------------------
    async def shutdown(self):
        self.running = False
        if self.redis:
            await self.redis.aclose()


# ======================================================================
# Entrypoint
# ======================================================================
if __name__ == "__main__":
    _service = ExecutionGateService()
    _main_loop: asyncio.AbstractEventLoop | None = None

    def _handle_sigterm(*_args):
        """F5 fix: thread-safe shutdown on Python 3.10+."""
        if _main_loop is not None and _main_loop.is_running():
            _main_loop.call_soon_threadsafe(
                lambda: _main_loop.create_task(_service.shutdown())  # type: ignore
            )

    signal.signal(signal.SIGTERM, _handle_sigterm)
    signal.signal(signal.SIGINT, _handle_sigterm)

    try:
        _main_loop = asyncio.new_event_loop()
        _main_loop.run_until_complete(_service.start())
    except KeyboardInterrupt:
        pass
    finally:
        if _main_loop and not _main_loop.is_closed():
            _main_loop.run_until_complete(_service.shutdown())
            _main_loop.close()
