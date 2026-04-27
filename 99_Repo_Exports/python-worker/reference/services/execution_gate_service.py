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
from utils.task_manager import safe_create_task

import json
import logging
import os
import signal
import time
from dataclasses import dataclass, field
from typing import Dict, Any, Optional, List

import redis.asyncio as aioredis
from prometheus_client import Counter, Gauge, start_http_server

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
    payload: Dict[str, Any]
    received_at: float = field(default_factory=time.time)


@dataclass
class Confirmation:
    symbol: str
    direction: str          # "long" or "short"
    ts_ms: int              # ts_ms from OFConfirm
    data: Dict[str, Any]    # full confirmation payload (ok, score, reason …)
    received_at: float = field(default_factory=time.time)


# ---------------------------------------------------------------------------
# Service
# ---------------------------------------------------------------------------
class ExecutionGateService:
    def __init__(self):
        self.redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
        self.redis: Optional[aioredis.Redis] = None

        # --- Streams ---
        self.stream_raw = os.getenv("CRYPTO_RAW_STREAM", "signals:crypto:raw")
        self.stream_confirm = os.getenv("OF_CONFIRM_STREAM", "signals:of:confirm")

        # Output queue — Binance executor (MT5 output disabled)
        self.queue_out = os.getenv("ORDERS_QUEUE_BINANCE", "orders:queue:binance")

        # --- Config ---
        self.proposal_ttl_s = float(os.getenv("EXEC_GATE_TTL_S", "5.0"))
        self.match_tolerance_ms = int(os.getenv("EXEC_GATE_MATCH_MS", "2000"))
        self.require_of_confirm = os.getenv(
            "EXEC_GATE_REQUIRE_OF_CONFIRM", "false"
        ).lower() in {"1", "true", "yes", "on"}

        self.running = True
        self._loop: Optional[asyncio.AbstractEventLoop] = None

        # --- Dual buffer ---
        # symbol -> List[Proposal]
        self.proposals: Dict[str, List[Proposal]] = {}
        # symbol -> List[Confirmation]  (F3 fix: buffer confirms too)
        self.confirmations: Dict[str, List[Confirmation]] = {}

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    async def start(self):
        logger.info(
            f"Starting ExecutionGateService (mode={'ENFORCE' if self.require_of_confirm else 'PASS-THROUGH'}) "
            f"streams: {self.stream_raw} + {self.stream_confirm} -> {self.queue_out}"
        )
        self.redis = aioredis.from_url(self.redis_url, decode_responses=True)
        self._loop = asyncio.get_running_loop()

        # Metrics
        start_http_server(int(os.getenv("PROMETHEUS_PORT", 8004)))
        mode_info.set(1 if self.require_of_confirm else 0)

        tasks = [
            safe_create_task(self._consume_raw_signals()),
            safe_create_task(self._consume_confirmations()),
            safe_create_task(self._cleanup_loop()),
        ]
        await asyncio.gather(*tasks)

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
                results = await self.redis.xread(
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

    async def _handle_proposal(self, fields: Dict[str, Any]):
        try:
            payload_str = fields.get("payload")
            if not payload_str:
                return

            data = json.loads(payload_str)
            symbol = data.get("symbol")
            direction = data.get("direction", "").lower()

            if not symbol or direction not in ("long", "short"):
                return

            ts_ms = int(data.get("generated_at", get_ny_time_millis()))

            proposal = Proposal(
                symbol=symbol,
                direction=direction,
                ts_ms=ts_ms,
                payload=data,
            )

            # Always count
            proposals_received_total.labels(symbol=symbol).inc()

            # --- PASS-THROUGH: execute immediately ---
            if not self.require_of_confirm:
                data["validation_status"] = "bypassed"
                data["validation_reason"] = "OFConfirm validation disabled"
                proposals_bypassed_total.labels(symbol=symbol).inc()
                await self._publish_execution(proposal, {"ok": 1, "score": 1.0})
                return

            # --- ENFORCE: buffer proposal, then try to match ---
            self.proposals.setdefault(symbol, []).append(proposal)
            logger.info(
                f"Received PROPOSAL: {symbol} {direction} (buffered, waiting for confirm)"
            )

            # F3 fix: check if a confirmation already arrived first
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

    def _try_match_confirm_for_proposal(self, proposal: Proposal) -> Optional[Confirmation]:
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
                results = await self.redis.xread(
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

    async def _handle_confirmation(self, fields: Dict[str, Any]):
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
            logger.debug(
                f"Confirmation for {symbol} {direction} buffered (no proposal yet)"
            )

        except Exception as e:
            logger.error(f"Failed to handle confirmation: {e}")

    def _try_match_proposal_for_confirm(
        self, symbol: str, direction: str, ts_ms: int
    ) -> Optional[Proposal]:
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
        self, proposal: Proposal, confirmation: Dict[str, Any]
    ):
        """Publish the verified order to the execution queue."""
        try:
            is_virtual = bool(proposal.payload.get("is_virtual", 0) or 0)
            is_ok = int(confirmation.get("ok", 0)) == 1

            # SAFEGUARD: Do not execute if validation failed
            if not is_ok:
                logger.info(
                    f"🚫 EXECUTION SKIPPED (Validation Failed): "
                    f"{proposal.symbol} {proposal.direction} ok={is_ok} virtual={is_virtual}"
                )
                return

            if is_virtual:
                logger.info(
                    f"👻 VIRTUAL EXECUTION GATE: Validated {proposal.symbol} "
                    f"{proposal.direction}. Publishing virtual order."
                )
            else:
                logger.info(
                    f"✅ EXECUTION GATE: Validated {proposal.symbol} "
                    f"{proposal.direction}. Publishing order."
                )

            order_payload = proposal.payload.copy()
            order_payload["gate_verified"] = True
            order_payload["gate_ts_ms"] = get_ny_time_millis()
            order_payload["confirm_score"] = confirmation.get("score", 1.0)

            await self.redis.rpush(self.queue_out, json.dumps(order_payload))
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
                        confirmations_expired_total.labels(symbol=sym).inc(removed)
                        logger.debug(
                            f"Pruned {removed} stale confirmations for {sym}"
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
    _main_loop: Optional[asyncio.AbstractEventLoop] = None

    def _handle_sigterm(*_args):
        """F5 fix: thread-safe shutdown on Python 3.10+."""
        if _main_loop is not None and _main_loop.is_running():
            _main_loop.call_soon_threadsafe(
                lambda: _main_loop.create_task(_service.shutdown())
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
