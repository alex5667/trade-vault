from __future__ import annotations

import orjson
import os
from dataclasses import dataclass
from typing import Any

import redis
import redis.exceptions
from prometheus_client import Counter

from core.redis_keys import RedisStreams as RS
from services.signal_preprocess import preprocess_signal_for_publish

# Create In-Memory Prometheus counters
PUB_OK_TOTAL = Counter("signals_publish_ok_total", "Successful signal publishes", ["source", "stream"])
PUB_ERR_TOTAL = Counter("signals_publish_errors_total", "Failed signal publishes", ["source", "stream"])
PUB_BUSY_TOTAL = Counter("signals_publish_busy_total", "BusyLoading Redis errors", ["source", "stream"])
PUB_RETRIES_ENQUEUED_TOTAL = Counter("signals_publish_retries_enqueued_total", "Signals queued for retry", ["source", "symbol"])
PUB_RETRIES_SUCCESS_TOTAL = Counter("signals_publish_retries_success_total", "Successful retries", ["source", "symbol"])
PUB_DROPPED_TOTAL = Counter("signals_publish_dropped_total", "Signals dropped after max retries or overflow", ["source", "symbol"])


def _json_dumps_safe(obj: Any) -> str:
    """
    Async hot-path JSON: MUST NOT raise.
    default=str handles Enums/Decimals/np types.
    OPT_NON_STR_KEYS handles int dict keys (e.g. meta["atr_candidates"] uses tf_ms ints as keys).
    """
    try:
        return orjson.dumps(obj, default=str, option=orjson.OPT_NON_STR_KEYS).decode("utf-8")
    except Exception as exc:
        return f'{{"error":"json_dumps_failed", "details": "{str(exc)}"}}'




@dataclass(frozen=True)
class StreamSink:
    """
    A single XADD sink.
    field: which field name to write ("payload" vs "data") – different streams use different conventions.
    """
    name: str
    field: str = "payload"
    maxlen: int = 10000


@dataclass(frozen=True)
class AsyncPublishResult:
    ok: bool
    raw_written: bool
    busy_loading: bool
    errors: int
    retryable: bool = True
    status: str = ""


import asyncio
import logging
import time as _wall_time
from concurrent.futures import ThreadPoolExecutor

from utils.task_manager import safe_create_task

_COMPARISON_EXECUTOR = ThreadPoolExecutor(max_workers=2, thread_name_prefix="async_pub_cmp")

# Dedicated pool for InvariantEngine + GraphGate validation.
# Previously used None (default executor), competing with preprocess_signal_for_publish
# threads and causing contention during signal bursts.
_INVARIANT_EXECUTOR = ThreadPoolExecutor(max_workers=2, thread_name_prefix="async_pub_inv")

# Lazy pre-xadd overhead histogram (registered once at import time)
_signal_emit_pre_xadd_us = None

def _get_pre_xadd_hist():
    global _signal_emit_pre_xadd_us
    if _signal_emit_pre_xadd_us is None:
        try:
            from services.orderflow.metrics import _get_or_create_prom_histogram
            _signal_emit_pre_xadd_us = _get_or_create_prom_histogram(
                "signal_emit_pre_xadd_us",
                "Pre-XADD overhead in AsyncSignalPublisher (invariant/gate/mget) (us). SLO budget: < 2ms.",
                ["symbol", "stream"],
                buckets=[50, 100, 250, 500, 1_000, 2_000, 5_000, 8_000, 15_000, 30_000],
            )
        except Exception:
            pass
    return _signal_emit_pre_xadd_us

class AsyncSignalPublisher:
    """
    Shared publisher for async producers (aioredis / redis.asyncio).

    Goals:
      - At-Least-Once delivery: local in-memory buffer for retries during network outages.
      - contract normalization (ts_ms / ids / side_int / entry_price / mirrors)
      - structured result for deterministic unit tests
    """

    def __init__(
        self,
        *,
        redis_client: Any,
        source: str,
        metrics_prefix: str = "signals_publish_async",
        logger: Any = None,
        max_retries: int = 5,
        retry_queue_maxsize: int = 1000,
    ) -> None:
        self.r = redis_client
        self.source = source or "na"
        self.metrics_prefix = metrics_prefix or "signals_publish_async"
        self.logger = logger or logging.getLogger(__name__)
        self.max_retries = max_retries

        # Buffer for failed signals (in-memory, volatile)
        self._retry_queue: asyncio.Queue = asyncio.Queue(maxsize=retry_queue_maxsize)
        # Background worker task — created lazily via start() to avoid
        # RuntimeError: no running event loop when __init__ is called
        # outside an asyncio context (e.g. from a plain Thread).
        self._worker_task: asyncio.Task | None = None

        # ── Hot-path ENV cache ──────────────────────────────────────────────
        # Read ENV flags ONCE at construction (and refresh every 30s) to avoid
        # os.getenv() overhead on every XADD call.
        self._env_cache_ts: float = 0.0
        self._env_cache_ttl: float = 30.0
        self._env: dict = {}
        self._refresh_env_cache()

        # Cached enforcement router singleton (avoid factory call per XADD)
        self._enforcement_router: Any = None
        try:
            from services.atr_policy_enforcement_router import get_enforcement_router
            self._enforcement_router = get_enforcement_router()
        except Exception:
            pass

        # ── Degrade state cache (Signal Emit P99 fix) ──────────────────────
        # Root cause of Signal Emit P99 = 30ms:
        # The pre-XADD gate performs 3 MGET keys per signal (degrade:symbol,
        # degrade:venue, degrade:global). With 3660 Redis connections the pool
        # is saturated → each MGET waits 10-25ms for a slot.
        # Fix: cache degrade state in-process with TTL=500ms.
        # degrade state is slow-changing (minutes), 500ms cache is safe.
        # Key: (symbol, venue) -> (highest_precedence, legacy_advisory, cached_at)
        self._degrade_cache: dict = {}
        self._degrade_cache_ttl: float = float(
            os.getenv("ASYNC_PUB_DEGRADE_CACHE_TTL_MS", "500")
        ) / 1000.0

        # ── Enforce state cache (Signal Emit P99 fix) ────────────────────────
        # Fixes synchronous router.get_runtime_decision call blocking the event loop
        self._enforce_cache: dict = {}
        self._enforce_cache_ttl: float = float(
            os.getenv("ASYNC_PUB_ENFORCE_CACHE_TTL_MS", "500")
        ) / 1000.0

    def _refresh_env_cache(self) -> None:
        """Re-read ENV flags into in-process cache. Called at init and every 30s."""
        self._env = {
            "raw_fast_xadd_only": os.getenv("ASYNC_PUB_RAW_FAST_XADD_ONLY", "0").lower() in ("1", "true"),
            "freeze_matrix_enable": os.getenv("ATR_FREEZE_MATRIX_RUNTIME_ENABLE", "1").lower() in ("1", "true"),
            "graph_gate_enable": os.getenv("ATR_GRAPH_RUNTIME_GATE_ENABLE", "0").lower() in ("1", "true"),
            "graph_gate_compare": os.getenv("ATR_GRAPH_RUNTIME_GATE_COMPARE", "0").lower() in ("1", "true"),
            "policy_enforce_enable": os.getenv("ATR_POLICY_ENFORCEMENT_ENABLE", "1").lower() in ("1", "true"),
            "bp_enable": os.getenv("ASYNC_PUB_BACKPRESSURE", "1").strip().lower() in ("1", "true", "yes", "on"),
            "bp_timeout": float(os.getenv("ASYNC_PUB_BP_TIMEOUT_SEC", "1.0") or 1.0),
            "xadd_timeout": float(os.getenv("ASYNC_PUB_TIMEOUT_SEC", "15.0") or 15.0),
        }
        self._env_cache_ts = _wall_time.monotonic()

    def start(self) -> None:
        """
        Start the background retry worker.
        MUST be called from within a running asyncio event loop
        (i.e. after the loop is started, e.g. inside an async entrypoint or
        via asyncio.get_event_loop().run_until_complete(...)).

        Safe to call multiple times — subsequent calls are no-ops if the task
        is already running.
        """
        if self._worker_task is None or self._worker_task.done():
            self._worker_task = safe_create_task(self._retry_worker())
            self.logger.debug("AsyncSignalPublisher: retry worker task started")

    async def _retry_worker(self) -> None:
        """Background worker to rescue signals during network drops."""
        while True:
            try:
                # 1. Сначала пытаемся вычитать из Redis stream
                try:
                    items = await self.r.xrange(RS.PUBLISHER_RETRY, min="-", max="+", count=10)
                except Exception:
                    items = []

                if items:
                    for _id, fields in items:
                        # Decode
                        data = fields.get(b"data") or fields.get("data")
                        if isinstance(data, bytes):
                            data = data.decode("utf-8")
                        try:
                            rec = orjson.loads(data)
                        except Exception:
                            # Bad payload
                            await self.r.xdel(RS.PUBLISHER_RETRY, _id)
                            continue

                        sink_d = rec.get("sink", {})
                        sink = StreamSink(name=sink_d.get("name", ""), field=sink_d.get("field", "payload"), maxlen=sink_d.get("maxlen", 10000))
                        payload = rec.get("payload")
                        symbol = rec.get("symbol")
                        attempt = rec.get("attempt", 1)
                        approx = rec.get("approximate", True)

                        res = await self.xadd_json_internal(sink=sink, payload=payload, symbol=symbol, approximate=approx)

                        if res.ok:
                            PUB_RETRIES_SUCCESS_TOTAL.labels(source=self.source, symbol=symbol).inc()
                            await self.r.xdel(RS.PUBLISHER_RETRY, _id)
                        else:
                            if attempt < self.max_retries:
                                rec["attempt"] = attempt + 1
                                try:
                                    pipeline = self.r.pipeline()
                                    pipeline.xdel(RS.PUBLISHER_RETRY, _id)
                                    pipeline.xadd(RS.PUBLISHER_RETRY, fields={"data": _json_dumps_safe(rec)}, maxlen=10000, approximate=True)
                                    await pipeline.execute()
                                except Exception:
                                    pass
                            else:
                                PUB_DROPPED_TOTAL.labels(source=self.source, symbol=symbol).inc()
                                self.logger.error("🚨 SIGNAL LOST PERMANENTLY (Redis limit): %s", symbol)
                                await self.r.xdel(RS.PUBLISHER_RETRY, _id)
                    # Simple backoff
                    await asyncio.sleep(1.0)
                else:
                    # 2. Если Redis пуст или недоступен – проверяем локальную очередь (network outage cache)
                    try:
                        sink, payload, symbol, attempt, approximate = self._retry_queue.get_nowait()
                        res = await self.xadd_json_internal(
                            sink=sink, payload=payload, symbol=symbol, approximate=approximate
                        )
                        if res.ok:
                            PUB_RETRIES_SUCCESS_TOTAL.labels(source=self.source, symbol=symbol).inc()
                        else:
                            if attempt < self.max_retries:
                                try:
                                    # Пытаемся скинуть в Redis-очередь, если он ожил
                                    retry_rec = _json_dumps_safe({
                                        "sink": {"name": sink.name, "field": sink.field, "maxlen": sink.maxlen},
                                        "payload": payload,
                                        "symbol": symbol,
                                        "attempt": attempt + 1,
                                        "approximate": approximate
                                    })
                                    await self.r.xadd(RS.PUBLISHER_RETRY, fields={"data": retry_rec}, maxlen=10000, approximate=True)
                                except Exception:
                                    # Если Redis все еще мертв, держим в локальной очереди
                                    wait_sec = min(10.0, 0.5 * (2**attempt))
                                    await asyncio.sleep(wait_sec)
                                    self._retry_queue.put_nowait((sink, payload, symbol, attempt + 1, approximate))
                            else:
                                PUB_DROPPED_TOTAL.labels(source=self.source, symbol=symbol).inc()
                                self.logger.error("🚨 SIGNAL LOST PERMANENTLY (Local limit): %s", symbol)
                        self._retry_queue.task_done()
                    except asyncio.QueueEmpty:
                        await asyncio.sleep(0.5)

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.exception("Retry worker crashed, restarting in 1s: %r", e)
                await asyncio.sleep(1)

    async def xadd_json(
        self,
        *,
        sink: StreamSink,
        payload: dict[str, Any],
        symbol: str,
        approximate: bool = True,
        no_retry: bool = False,
        timeout_sec: float | None = None,
    ) -> AsyncPublishResult:
        """
        Public method that guarantees At-Least-Once delivery (via Redis stream or local buffer).
        FAIL-OPEN: never raises.
        """
        if (_wall_time.monotonic() - self._env_cache_ts) > self._env_cache_ttl:
            self._refresh_env_cache()

        res = await self.xadd_json_internal(
            sink=sink, payload=payload, symbol=symbol, approximate=approximate, timeout_sec=timeout_sec
        )

        if not res.ok and res.retryable and not no_retry:
            try:
                # 1. P0: Попытка атомарного сохранения в Redis (выживает при падении worker-а)
                retry_rec = _json_dumps_safe({
                    "sink": {"name": sink.name, "field": sink.field, "maxlen": sink.maxlen},
                    "payload": payload,
                    "symbol": symbol,
                    "attempt": 1,
                    "approximate": approximate
                })
                await self.r.xadd(RS.PUBLISHER_RETRY, fields={"data": retry_rec}, maxlen=10000, approximate=True)
                PUB_RETRIES_ENQUEUED_TOTAL.labels(source=self.source, symbol=symbol).inc()
            except Exception:
                # 2. Redis недоступен -> локальная in-memory очередь
                try:
                    if self._env["bp_enable"]:
                        await asyncio.wait_for(
                            self._retry_queue.put((sink, payload, symbol, 1, approximate)),
                            timeout=self._env["bp_timeout"]
                        )
                    else:
                        self._retry_queue.put_nowait((sink, payload, symbol, 1, approximate))
                    self.logger.warning("⚠️ Signal %s queued LOCALLY for retry (Redis fail)", symbol)
                    PUB_RETRIES_ENQUEUED_TOTAL.labels(source=self.source, symbol=symbol).inc()
                except (TimeoutError, asyncio.QueueFull):
                    PUB_DROPPED_TOTAL.labels(source=self.source, symbol=symbol).inc()
                    self.logger.critical("🚨 RETRY QUEUE OVERFLOW! Signal lost: %s", symbol)

        return res

    async def xadd_json_internal(
        self,
        *,
        sink: StreamSink,
        payload: dict[str, Any],
        symbol: str,
        approximate: bool = True,
        timeout_sec: float | None = None,
    ) -> AsyncPublishResult:
        """
        Internal raw XADD logic.
        """
        import time as _time
        # ── Full-path timer (pre-xadd + xadd combined) ────────────────────
        # Measures ALL overhead before and including the XADD command so that
        # the pre-xadd gate/mget/invariant cost is visible in Grafana.
        _t_entry = _time.monotonic_ns()
        errors = 0
        busy = False
        raw_written = False

        # Refresh ENV cache if stale (cheap monotonic check — no syscall)
        if (_wall_time.monotonic() - self._env_cache_ts) > self._env_cache_ttl:
            self._refresh_env_cache()

        raw_fast_xadd_only = (
            self._env["raw_fast_xadd_only"]
            and sink.name == RS.CRYPTO_RAW
        )

        # 1) normalize contract (hot-path optimization)
        if not raw_fast_xadd_only:
            try:
                # Detect if this is a high-frequency telemetry stream (BBO, CVD, etc)
                # or a mission-critical trade signal (orders:queue).
                is_trade_signal = sink.name.startswith(RS.ORDERS_QUEUE) or sink.name.startswith("events:signals")
                fast_path = not is_trade_signal

                if fast_path:
                    # Fast path: Synchronous block is minimal (basic normalization)
                    preprocess_signal_for_publish(
                        payload,
                        symbol=symbol,
                        source=self.source,
                        logger=self.logger,
                        fast_path=True
                    )
                else:
                    # Signal path: Offload heavy ATR/Risk resolution to background thread
                    # to prevent blocking the event loop during signal bursts.
                    await asyncio.to_thread(
                        preprocess_signal_for_publish,
                        payload,
                        symbol=symbol,
                        source=self.source,
                        logger=self.logger,
                        fast_path=False
                    )
            except Exception:
                pass

        # Phase 2: Inject explicit Python-calculated volume into the payload for MT5 to consume
        if "qty" in payload and "volume" not in payload:
            import copy
            payload = copy.deepcopy(payload)
            payload["volume"] = float(payload.get("qty", 0.0) or 0.0)

        # 1.5) Invariant Firewall (Phase 7.1) — only for orders:queue streams
        if sink.name.startswith(RS.ORDERS_QUEUE):
            try:
                from services.atr_invariant_runtime_engine import get_runtime_engine
                engine = get_runtime_engine()
                loop = asyncio.get_running_loop()
                allow, violations = await loop.run_in_executor(_INVARIANT_EXECUTOR, engine.validate_signal, payload)

                if violations:
                    try:
                        # Asynchronous violation log to prevent DB writes in hot paths
                        v_payload = _json_dumps_safe({"signal": payload, "violations": violations})
                        await self.r.xadd(
                            "events:invariant_violations",
                            fields={"payload": v_payload},
                            maxlen=10000,
                            approximate=True
                        )
                    except Exception as ve:
                        if self.logger:
                            self.logger.error("Failed to publish invariant violations to Redis: %r", ve)

                if not allow:
                    # Signal blocked by InvariantRuntimeEngine — terminal deny, never retry
                    errors += 1
                    return AsyncPublishResult(
                        ok=False,
                        raw_written=False,
                        busy_loading=False,
                        errors=errors,
                        retryable=False,
                        status="invariant_denied",
                    )
            except Exception as iv_err:
                if self.logger:
                    self.logger.error("InvariantRuntimeEngine crashed: %r", iv_err)
                # Fail-open internally if engine crashes

        # 1.6) Runtime Gating (Phase 7.6 Legacy + Phase 8.5 Graph) — only for orders:queue
        if sink.name.startswith(RS.ORDERS_QUEUE):
            try:
                from services.atr_graph_backed_runtime_gate import ATRGraphBackedRuntimeGateService
                action = payload.get("action", "")
                kind = payload.get("kind", "")

                # Sacred Protective Exits mapping (MUST bypass freezes)
                is_protective_exit = (action != "OPEN") and (
                    kind.lower() not in ["new_entry", "signal", ""]
                )
                if any(x in kind.lower() for x in ["tp", "sl", "be", "close", "trail", "ratchet"]):
                    is_protective_exit = True

                if not is_protective_exit:
                    venue = payload.get("venue", "binance")

                    # --- [Phase 7.6] Legacy Freeze Overlay (MGET, O(3), fast) ---
                    # P-EMIT-FIX: cache degrade state TTL=500ms to skip per-signal MGET.
                    # degrade state is slow-changing (operator writes, minutes apart).
                    _now_mono = _wall_time.monotonic()
                    _cache_key = (symbol, venue)
                    _cached = self._degrade_cache.get(_cache_key)
                    if _cached is not None and (_now_mono - _cached[2]) < self._degrade_cache_ttl:
                        highest_precedence, legacy_advisory = _cached[0], _cached[1]
                    else:
                        degrade_keys = [
                            f"cfg:atr_degrade:symbol:{symbol}",
                            f"cfg:atr_degrade:venue:{venue}",
                            "cfg:atr_degrade:global:all"
                        ]
                        degrade_vals = await self.r.mget(degrade_keys)

                        from services.atr_constants import PRECEDENCE_MAP

                        highest_precedence = 0
                        legacy_advisory = True

                        for v in degrade_vals:
                            if v:
                                try:
                                    d = orjson.loads(v)
                                    state = d.get("state")
                                    adv = d.get("advisory", True)
                                    prec = PRECEDENCE_MAP.get(state, 0)
                                    if prec > highest_precedence:
                                        highest_precedence = prec
                                        legacy_advisory = adv
                                except Exception:
                                    pass

                        self._degrade_cache[_cache_key] = (highest_precedence, legacy_advisory, _now_mono)

                    legacy_decision = ATRGraphBackedRuntimeGateService.decide_legacy_runtime(highest_precedence)
                    if legacy_advisory and legacy_decision != "allow":
                        legacy_decision = "allow"

                    # P0.2: Use cached ENV flag — no os.getenv() on hot path
                    if not self._env["freeze_matrix_enable"]:
                        legacy_decision = "allow"

                    effective = legacy_decision

                    # --- [Phase 8.5] Graph-Backed Runtime Gating Overlay ---
                    # Use cached flags — avoids os.getenv() per call
                    is_graph_enabled = self._env["graph_gate_enable"]
                    compare_enabled = self._env["graph_gate_compare"]

                    graph_decision = "allow"
                    # Only enter executor if at least one graph flag is on (default: both off)
                    if is_graph_enabled or compare_enabled:
                        scope_value = symbol
                        loop = asyncio.get_running_loop()
                        graph_decision = await loop.run_in_executor(
                            _INVARIANT_EXECUTOR,
                            ATRGraphBackedRuntimeGateService.decide_runtime_from_graph,
                            payload, scope_value
                        )

                        if compare_enabled:
                            try:
                                loop.run_in_executor(
                                    _COMPARISON_EXECUTOR,
                                    ATRGraphBackedRuntimeGateService.compare_with_legacy_runtime,
                                    legacy_decision, graph_decision, scope_value
                                )
                            except Exception as ce:
                                if self.logger:
                                    self.logger.error("Failed to enqueue graph compare task: %r", ce)

                    # --- [Phase 10.2] Policy-driven Enforcement Layer (L2) ---
                    enforcement_decision = "allow"
                    if self._env["policy_enforce_enable"]:
                        try:
                            _now_mono = _wall_time.monotonic()
                            _enf_cached = self._enforce_cache.get(symbol)
                            if _enf_cached is not None and (_now_mono - _enf_cached[1]) < self._enforce_cache_ttl:
                                enforcement_decision = _enf_cached[0]
                            else:
                                enf_keys = [
                                    f"cache:atr:enforcement:runtime:{symbol}",
                                    "cache:atr:enforcement:runtime:global"
                                ]
                                enf_vals = await self.r.mget(enf_keys)
                                enforcement_decision = "allow"
                                if enf_vals[0]:
                                    enforcement_decision = orjson.loads(enf_vals[0]).get("overall_action", "allow").lower()
                                elif enf_vals[1]:
                                    enforcement_decision = orjson.loads(enf_vals[1]).get("overall_action", "allow").lower()
                                
                                self._enforce_cache[symbol] = (enforcement_decision, _now_mono)

                            if self.logger and enforcement_decision != "allow":
                                self.logger.info(
                                    "Policy Router decision for %s: %s", symbol, enforcement_decision
                                )
                        except Exception as pe:
                            if self.logger:
                                self.logger.error("Policy Enforcement Router error: %r", pe)

                    # --- [Resolution] Determine Final Effective State ---
                    if enforcement_decision in ["deny_new_risk", "block_release"]:
                        effective = "deny"
                    elif enforcement_decision == "clip_new_risk":
                        if effective != "deny":
                            effective = "clip"

                    # --- [Enforcement] Apply Priority Decision ---
                    if effective == "deny":
                        if self.logger:
                            self.logger.warning(
                                "🚫 SIGNAL BLOCKED by Runtime/Policy Gate "
                                "(effective=%s, policy=%s), symbol=%s",
                                effective, enforcement_decision, symbol
                            )
                        errors += 1
                        return AsyncPublishResult(
                            ok=False,
                            raw_written=False,
                            busy_loading=False,
                            errors=errors,
                            retryable=False,
                            status="runtime_policy_denied",
                        )
                    elif effective == "clip":
                        import copy
                        current_risk = float(payload.get("risk_pct", 1.0))
                        payload = copy.deepcopy(payload)
                        payload.setdefault("dq_flags", []).append("RISK_CLIPPED_BY_ENFORCEMENT")
                        clip_factor = 0.5
                        payload["risk_pct_original"] = current_risk
                        payload["risk_pct"] = current_risk * clip_factor
                        payload["_risk_clip"] = {
                            "applied": True,
                            "original_risk": current_risk,
                            "reason": "policy_enforcement"
                        }
                        if self.logger:
                            self.logger.info(
                                "✂️ SIGNAL RISK CLIPPED by Policy Gate "
                                "(risk_pct from %s to %s) symbol=%s",
                                current_risk, payload["risk_pct"], symbol
                            )

            except Exception as fe:
                if self.logger:
                    self.logger.error("Runtime Gate overlay error: %r", fe)
                # Fail-open to avoid halting execution if redis or database fails

        # 2) serialize
        ser = _json_dumps_safe(payload)

        # 3) xadd — measure ONLY the Redis round-trip (H4 budget: p99 < 8ms)
        # NOTE: asyncio.wait_for() is intentionally NOT used here.
        # For a ~1ms XADD, the 15s timeout creates a Task per call (~0.2-1ms scheduling
        # overhead) that inflates p99. Genuine socket timeouts surface via
        # redis.exceptions.TimeoutError (configured at connection pool level via
        # socket_timeout). If a per-call timeout is needed, pass timeout_sec explicitly.
        _t0 = _time.monotonic_ns()
        try:
            await self.r.xadd(
                sink.name,
                fields={sink.field or "payload": ser},
                maxlen=sink.maxlen,
                approximate=approximate,
            )
            raw_written = True
            PUB_OK_TOTAL.labels(source=self.source, stream=sink.name).inc()
        except redis.exceptions.BusyLoadingError:
            busy = True
            PUB_BUSY_TOTAL.labels(source=self.source, stream=sink.name).inc()
        except Exception as e:
            errors += 1
            PUB_ERR_TOTAL.labels(source=self.source, stream=sink.name).inc()
            if self.logger is not None:
                try:
                    is_timeout = (
                        isinstance(
                            e,
                            (asyncio.TimeoutError, TimeoutError,
                             getattr(redis.exceptions, "TimeoutError", type(None)))
                        )
                        or "TimeoutError" in type(e).__name__
                    )
                    is_conn_error = (
                        "ConnectionError" in type(e).__name__ or
                        "No connection available" in str(e)
                    )
                    if (is_timeout or is_conn_error) and "bbo_ts" in sink.name:
                        self.logger.debug("async_publish.xadd skipped (timeout/pool-full) stream=%s", sink.name)
                    else:
                        self.logger.warning("async_publish.xadd failed stream=%s err=%r", sink.name, e)
                except Exception:
                    pass

        # 4) Latency audit — two sub-metrics:
        #    a) signal_emit_latency_us  : pure Redis XADD round-trip (H4 SLO)
        #    b) signal_emit_pre_xadd_us : pre-xadd overhead (invariant/gate/mget)
        try:
            _now_ns = _time.monotonic_ns()
            _xadd_us = (_now_ns - _t0) / 1_000
            _pre_us = (_t0 - _t_entry) / 1_000
            from services.orderflow.metrics import signal_emit_latency_us
            signal_emit_latency_us.labels(
                symbol=symbol, stream=sink.name
            ).observe(_xadd_us)
            _h = _get_pre_xadd_hist()
            if _h is not None:
                _h.labels(symbol=symbol, stream=sink.name).observe(_pre_us)
        except Exception:
            pass

        return AsyncPublishResult(ok=raw_written, raw_written=raw_written, busy_loading=busy, errors=errors)
