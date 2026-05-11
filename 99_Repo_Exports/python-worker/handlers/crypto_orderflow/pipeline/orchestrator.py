from __future__ import annotations

import json
import logging

# Import our components
import math
import os
import random
import time
from collections.abc import Callable
from typing import Any

from prometheus_client import Counter, Histogram

from common.dq_flags import append_dq_flag, ensure_dq_flags
from common.enums import VetoReason
from common.math_safe import safe_float
from common.metrics_stage import emit_ok_total, stage_ms_hist
from domain.reasons import normalize_reason
from services.observability.metrics_registry import (
    dlq_xadd_errors_total as _DLQ_XADD_ERRORS_TOTAL,
)
from services.observability.metrics_registry import (
    schema_version_fallback_total as _SCHEMA_VERSION_FALLBACK_TOTAL,
)
from services.observability.metrics_registry import (
    signal_dq_flag_total as _DQ_FLAG_TOTAL,
)
from services.observability.metrics_registry import (
    ts_rejected_total as _TS_REJECTED_TOTAL,
)
from utils.time_utils import get_epoch_ms
import contextlib
from core.redis_keys import RedisStreams as RS

logger = logging.getLogger(__name__)
SIGNAL_BUILD_FAILED_TOTAL = Counter(
    "signal_build_failed_total",
    "Total signal payload build failures",
    ["symbol"],
)

SIGNAL_EMIT_ERROR_TOTAL = Counter(
    "signal_emit_error_total",
    "Total signal emit failures",
    ["symbol"],
)

PAYLOAD_TS_ANOMALY_TOTAL = Counter(
    "payload_ts_anomaly_total",
    "Trade payloads with anomalous timestamp normalized to 0 (seconds instead of ms / future-skew / zero)",
    ["symbol"],
)

ORCHESTRATOR_SWALLOWED_EXCEPTIONS_TOTAL = Counter(
    "orchestrator_swallowed_exceptions_total",
    "Exceptions swallowed in fail-open orchestrator hot path",
    ["phase"],
)

# ── SLO Histogram: full pipeline latency (tick → emit) ────────────────────────────
# Use histogram_quantile(0.99, ...) in PromQL for p99.
# prometheus_client Python does NOT support Summary(quantiles=...).
ORCHESTRATOR_SLO_SECONDS = Histogram(
    "orchestrator_slo_seconds",
    "Full pipeline SLO: end-to-end latency per candidate from detect to emit (seconds)",
    ["kind"],
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
)



# ── #24: ENV-overridable histogram buckets ───────────────────────────────────────────
# Set ORCHESTRATOR_STAGE_MS_BUCKETS=0.5,1,2,5,10,25,50,100,250,500,1000,2500,5000
def _parse_buckets(env_var: str, default: tuple) -> tuple:
    """Parse comma-separated float bucket edges from an ENV var.

    Falls back to default on any parse error — guaranteed never to raise.
    Ensures buckets are sorted, unique, all positive.
    """
    raw = os.getenv(env_var, "")
    if not raw:
        return default
    try:
        parsed = tuple(sorted({float(x.strip()) for x in raw.split(",") if x.strip()}))
        return parsed if parsed else default
    except Exception:
        logger.warning(
            "orchestrator: invalid histogram buckets in %s=%r; using defaults",
            env_var, raw,
        )
        return default

_DEFAULT_STAGE_BUCKETS = (0.5, 1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000)

# GAP-3: Stage-level latency histogram (closes V2 parity gap).
# Labels: stage = quality | regime | smt | consistency | edge_cost | confirmations | emit | full_process | build_payload
#         kind  = breakout | reversal | custom | ...
ORCHESTRATOR_STAGE_MS = Histogram(
    "orchestrator_stage_ms",
    "Orchestrator per-stage processing latency in milliseconds",
    ["stage", "kind"],
    buckets=_parse_buckets("ORCHESTRATOR_STAGE_MS_BUCKETS", _DEFAULT_STAGE_BUCKETS),
)

def _get_lookback_ms() -> int:
    """Return timestamp lookback window in ms. Controlled by TIMESTAMP_VALID_LOOKBACK_DAYS (default=7).

    Replaces hardcoded 7-day constant so historical replay > 7 days is supported.
    Set TIMESTAMP_VALID_LOOKBACK_DAYS=30 for 30-day replay windows.
    """
    try:
        days = int(os.getenv("TIMESTAMP_VALID_LOOKBACK_DAYS", "7") or "7")
        days = max(1, min(days, 365))  # clamp: 1..365
        return days * 24 * 3_600 * 1_000
    except Exception:
        return 7 * 24 * 3_600 * 1_000


_ONE_MINUTE_MS = 60 * 1_000


def _resolve_side(cand: Any) -> str:
    """Resolve side string from Candidate, normalising int direction (1/-1) → 'LONG'/'SHORT'.

    Priority: cand.side (str) → cand.direction (int) → empty-string fallback.
    All unknown/0 int directions return '' so downstream gates stay fail-open.
    """
    side = getattr(cand, "side", None)
    if isinstance(side, str) and side:
        return side
    direction = getattr(cand, "direction", None)
    if isinstance(direction, int):
        if direction == 1:
            return "LONG"
        if direction == -1:
            return "SHORT"
    # fall back to whatever str() gives — may be empty
    return str(side or "").strip()


def _emit_dq_flag(ctx: Any, flag: str, symbol="") -> None:
    """Append DQ flag to ctx AND increment signal_dq_flag_total{flag, symbol}.

    This is the canonical callsite for all DQ flag appends in the orchestrator.
    Two invariants guaranteed:
      1. Flag is deduplicated on ctx (via append_dq_flag).
      2. Counter fires exactly once per unique (flag, ctx) combinator —
         guarded by the dedup inside append_dq_flag: we check presence
         before incrementing so rate() reflects unique events, not retries.
    Fail-open: never raises.
    """
    try:
        # Dedup-aware: only count if flag is actually new on this ctx
        _existing = ensure_dq_flags(ctx)
        _f = (flag or "").strip()
        if not _f:
            return
        _is_new = _f not in _existing
        append_dq_flag(ctx, _f)
        if _is_new:
            try:
                _sym = str(symbol or getattr(ctx, "symbol", "") or "unknown")
                _DQ_FLAG_TOTAL.labels(flag=_f, symbol=_sym).inc()  # type: ignore
            except Exception:
                ORCHESTRATOR_SWALLOWED_EXCEPTIONS_TOTAL.labels(phase="dq_flag_counter").inc()
    except Exception:
        ORCHESTRATOR_SWALLOWED_EXCEPTIONS_TOTAL.labels(phase="emit_dq_flag").inc()


def _normalize_ts_ms(raw_ts: Any, now_ms: int, source: str = "orchestrator") -> int:
    """Convert raw timestamp to epoch_ms with range validation.

    Accepts seconds (<10_000_000_000) or milliseconds. Validates result
    against [now - TIMESTAMP_VALID_LOOKBACK_DAYS days, now+1m]; falls back to 0 on any anomaly.

    Side-effects (non-blocking):
    - Increments ts_rejected_total{source, reason} Prometheus counter on rejection.
    Callers must add dq_flags='ts_invalid' to any DLQ payload when this returns 0.
    """
    try:
        ts_val = int(raw_ts or 0)
        # P2: boundary for s vs ms. 2e9 (~2033) or 3e9 is safer than 10e9.
        if 0 < ts_val < 3_000_000_000:
            ts_val *= 1_000
        _lookback_ms = _get_lookback_ms()
        if ts_val <= 0 or ts_val < now_ms - _lookback_ms or ts_val > now_ms + _ONE_MINUTE_MS:
            if ts_val <= 0:  # noqa: E501
                reason = "zero_or_negative"
            elif ts_val > now_ms + _ONE_MINUTE_MS:
                reason = "future"
            else:
                reason = "too_old"
            logger.warning(
                "orchestrator ts out of valid range raw=%r normalized=%d source=%s reason=%s; returning 0",
                raw_ts, ts_val, source, reason,
            )
            with contextlib.suppress(Exception):
                _TS_REJECTED_TOTAL.labels(source=source, reason=reason).inc()  # type: ignore
            return 0
        return ts_val
    except Exception:
        logger.warning("orchestrator ts parse failed raw=%r source=%s", raw_ts, source)
        with contextlib.suppress(Exception):
            _TS_REJECTED_TOTAL.labels(source=source, reason="parse_error").inc()  # type: ignore
        return 0

from core.redis_keys import RS
from core.redis_keys import STREAM_RETENTION as _STREAM_RETENTION
from core.retention import MAXLEN_GLOBAL
from handlers.crypto_orderflow.components.gates import CryptoSignalGates
from handlers.crypto_orderflow.components.liquidity import CryptoLiquidity
from handlers.crypto_orderflow.components.observability import CryptoObservability
from handlers.crypto_orderflow.config.handler_config import CryptoOrderFlowConfigManager
import contextlib

_MAXLEN_DLQ: int = _STREAM_RETENTION.get(RS.SIGNAL_DLQ, 2_000)


class PipelineAuditWriter:
    """Extracts observability counters from hot-path try/except blocks to preserve veto reason boundaries."""
    def __init__(self, obs, sym_root, kind_key, cand_start_ts):
        self.obs = obs
        self.sym_root = sym_root
        self.kind_key = kind_key
        self.t_start = cand_start_ts
        self._metrics = ORCHESTRATOR_STAGE_MS

    def record_stage(self, stage_name: str, t_since: float) -> None:
        try:
            _ms = (time.monotonic() - t_since) * 1000
            self._metrics.labels(stage=stage_name, kind=self.kind_key).observe(_ms)
            stage_ms_hist(self.obs, stage=stage_name, ms=_ms, kind=self.kind_key, symbol=self.sym_root)
        except Exception:
            pass

    def record_full(self) -> None:
        try:
            _ms = (time.monotonic() - self.t_start) * 1000
            self._metrics.labels(stage="full_process", kind=self.kind_key).observe(_ms)
            ORCHESTRATOR_SLO_SECONDS.labels(kind=self.kind_key).observe(_ms / 1000.0)
            stage_ms_hist(self.obs, stage="full_process", ms=_ms, kind=self.kind_key, symbol=self.sym_root)
        except Exception:
            pass

    @staticmethod
    def safe_inc(counter, **labels):
        with contextlib.suppress(Exception):
            counter.labels(**labels).inc()

class SignalOrchestrator:
    """
    Orchestrates the signal generation pipeline:
    Detect -> Enrich -> Validate -> Score -> Gates -> Emit.
    """
    def __init__(
        self,
        config: CryptoOrderFlowConfigManager,
        gates: CryptoSignalGates,
        liquidity: CryptoLiquidity,
        observability: CryptoObservability,
        confirmations_engine: Any,
        emitter: Any,
    ):
        self.cfg = config
        self.gates = gates
        self.liquidity = liquidity
        self.observability = observability
        self.confirmations = confirmations_engine
        self.emitter = emitter
        # Bounded LRU: evict oldest when over capacity to prevent OOM on long-lived workers
        from collections import OrderedDict
        self._last_ts_ms: OrderedDict = OrderedDict()
        self._last_ts_ms_maxsize = 1024

    def _emit_build_failed(self, kind: str, e: Exception, symbol: str = "unknown") -> None:
        logger.warning("orchestrator build failed kind=%s: %r", kind, e)
        with contextlib.suppress(Exception):
            SIGNAL_BUILD_FAILED_TOTAL.labels(symbol=symbol).inc()
        try:
            m = getattr(self.observability, "_metrics", None)
            if m:
                m.inc("signal_build_failed_total", 1, tags={"kind": str(kind)})
        except Exception:
            pass

    def _handle_veto(self, ctx: Any, cand: Any, kind: str, reason_code: str | VetoReason) -> None:
        """Emits metrics and publishes vetoed signal to DLQ for audit trails and replay."""
        rc_norm = normalize_reason(reason_code)
        self.observability.emit_veto_metric(kind=kind, ctx=ctx, reason_code=rc_norm)
        try:
            redis_client = getattr(ctx, "redis", None)
            if redis_client:
                from core.redis_keys import RS
                ts_raw = getattr(ctx, "ts_ms", None) or getattr(ctx, "ts", None) or 0
                ts_val = _normalize_ts_ms(ts_raw, get_epoch_ms(), source="orchestrator_veto")

                dlq_payload = {
                    "signal_id": str(getattr(cand, "signal_id", "") or ""),
                    "kind": str(kind),
                    "symbol": str(getattr(ctx, "symbol", getattr(self.cfg, "symbol", "")) or ""),
                    "side": str(getattr(cand, "side", "") or ""),
                    "ts_ms": ts_val,
                    "price": float(getattr(ctx, "price", getattr(cand, "price", 0.0)) or 0.0),
                    "raw_score": float(getattr(cand, "raw_score", 0.0) or 0.0),
                    "veto_reason": str(rc_norm),
                }

                # DQ marker: replay and Grafana can filter ts_ms=0 rows via dq_flags
                if ts_val == 0:
                    dlq_payload["dq_flags"] = "ts_invalid"
                    _sym_veto = str(getattr(ctx, "symbol", getattr(self.cfg, "symbol", "unknown")) or "unknown")
                    _emit_dq_flag(ctx, "ts_invalid", symbol=_sym_veto)

                reasons = getattr(cand, "reasons", None)
                if reasons:
                    dlq_payload["cand_reasons"] = json.dumps([str(x) for x in reasons][:16])

                clean_payload = {k: str(v) if v is not None else "" for k, v in dlq_payload.items()}
                try:
                    redis_client.xadd(RS.SIGNAL_DLQ, clean_payload, maxlen=_MAXLEN_DLQ, approximate=True)
                except Exception as e:
                    _sym = (dlq_payload.get("symbol", "unknown"))
                    logger.error("❌ orchestrator dlq xadd failed symbol=%s kind=%s: %r", _sym, kind, e)
                    with contextlib.suppress(Exception):  # type: ignore
                        _DLQ_XADD_ERRORS_TOTAL.labels(symbol=_sym, kind=kind).inc()  # type: ignore
        except Exception as e:
            logger.warning("orchestrator dlq publish payload build failed kind=%s: %r", kind, e)

    def process(
        self,
        ctx: Any,
        detect_fn: Callable[[Any], list[Any]],
    ) -> bool:
        """
        Main processing loop.
        Returns True if any signal was emitted.
        """
        # 1. Detect
        candidates = detect_fn(ctx)
        if not candidates:
            return False

        any_sent = False

        # Snapshot config (hot-path)
        rt = self.cfg.get_runtime_snapshot()

        # Phase 1: Unified TS Normalization and Monotonicity
        _now_ms = get_epoch_ms()
        _ts_raw = getattr(ctx, "ts_ms", None) or getattr(ctx, "ts", None) or 0
        _ts_ms = _normalize_ts_ms(_ts_raw, _now_ms, source="orchestrator")
        try:
            ctx.ts_ms = _ts_ms
            ctx.ts = _ts_ms
        except Exception:
            pass

        _sym_root = str(getattr(self.cfg, "symbol", getattr(ctx, "symbol", "unknown")))
        if _ts_ms > 0:
            if _ts_ms < self._last_ts_ms.get(_sym_root, 0):
                _policy = os.environ.get("OOO_TICK_POLICY", "flag").lower()
                if _policy in ("flag", "drop"):
                    _emit_dq_flag(ctx, "out_of_order", symbol=_sym_root)
                    try:
                        from services.observability.metrics_registry import ts_rejected_total  # type: ignore
                        ts_rejected_total.labels(source="orchestrator", reason="non_monotonic").inc()  # type: ignore
                    except Exception:
                        pass
                if _policy == "drop":
                    return False
            elif _ts_ms > self._last_ts_ms.get(_sym_root, 0):
                self._last_ts_ms[_sym_root] = _ts_ms
                # evict oldest entry if over capacity
                if len(self._last_ts_ms) > self._last_ts_ms_maxsize:
                    self._last_ts_ms.popitem(last=False)

        for cand in candidates:
            # Stage ordering tracking

            # We assume cand has 'kind' and 'side' attributes
            # safe_lower logic
            kind_raw = getattr(cand, "kind", "custom")
            try:
                kind_key = str(kind_raw).lower()
            except Exception:
                kind_key = "custom"

            # GAP-3: per-candidate full latency span
            _t_cand_start = time.monotonic()
            audit = PipelineAuditWriter(self.observability, _sym_root, kind_key, _t_cand_start)

            # 1.0 Data Quality & Integrity Gate (Early Pipeline)
            _t_dq = time.monotonic()
            dq_integrity = self.gates.check_dq_integrity(ctx, kind_key)
            audit.record_stage("dq_integrity", _t_dq)
            if dq_integrity and dq_integrity.decision in ("DENY", "SHADOW_DENY"):
                self._handle_veto(ctx, cand, kind_key, dq_integrity.reason_code)
                continue

            # 1.5 Quality Gate (Detector Check)
            _t_q = time.monotonic()
            qa = self.gates.check_quality(ctx, kind_key, side=_resolve_side(cand))
            audit.record_stage("quality", _t_q)
            _qa_decision = getattr(qa, "decision", "DENY" if getattr(qa, "veto", False) else "ALLOW")
            if qa and _qa_decision in ("DENY", "SHADOW_DENY"):
                rc = getattr(qa, "reason_code", getattr(qa, "reason", VetoReason.VETO_QUALITY))
                self._handle_veto(ctx, cand, kind_key, rc)
                continue


            # 2. Regime Gate (Component)
            _t_r = time.monotonic()
            rg_decision = self.gates.check_regime_gate(ctx=ctx, kind=kind_key)
            audit.record_stage("regime", _t_r)
            if isinstance(rg_decision, tuple):
                _rg_dec = "ALLOW" if rg_decision[0] else "DENY"
                rc = rg_decision[1]
            else:
                _rg_dec = getattr(rg_decision, "decision", "DENY" if getattr(rg_decision, "veto", False) else "ALLOW")
                rc = getattr(rg_decision, "reason_code", getattr(rg_decision, "reason", "VETO_REGIME"))

            if rg_decision is not None and _rg_dec in ("DENY", "SHADOW_DENY"):
                self._handle_veto(ctx, cand, kind_key, rc)
                continue


            # 2.5 SMT Coherence Gate (Following)
            side_val = getattr(cand, "side", 0)
            _t_smt = time.monotonic()
            smt_decision = self.gates.check_smt(ctx=ctx, kind=kind_key, side=side_val)
            audit.record_stage("smt", _t_smt)
            _smt_dec = getattr(smt_decision, "decision", "DENY" if getattr(smt_decision, "veto", False) else "ALLOW")
            if smt_decision and _smt_dec in ("DENY", "SHADOW_DENY"):
                rc = getattr(smt_decision, "reason_code", getattr(smt_decision, "reason", VetoReason.VETO_SMT))
                self._handle_veto(ctx, cand, kind_key, rc)
                continue


            # 2.6 Consistency Gate (Microstructure Coherence)
            _t_const = time.monotonic()
            consistency_decision = self.gates.consistency_once(ctx=ctx, symbol=self.cfg.symbol, kind=kind_key, side=_resolve_side(cand))
            audit.record_stage("consistency", _t_const)
            _c_dec = getattr(consistency_decision, "decision", "DENY" if getattr(consistency_decision, "veto", False) else "ALLOW")
            if consistency_decision and _c_dec in ("DENY", "SHADOW_DENY"):
                rc = getattr(consistency_decision, "reason_code", getattr(consistency_decision, "reason", VetoReason.VETO_CONSISTENCY))
                self._handle_veto(ctx, cand, kind_key, rc)
                continue


            # 3. Level Enrichment
            try:
                # Resolve risk config
                risk_cfg = self.cfg.resolve_risk_cfg()

                # SLQ dynamic stop override (fail-open, idempotent)
                try:
                    from services.slq_risk_adjust import maybe_apply_slq_to_risk_cfg
                    # FIX: ctx.redis is None in production (ctx has no redis attr).
                    # Use the sync Redis client from handler_config as fallback.
                    redis_client = getattr(ctx, "redis", None)
                    if redis_client is None:
                        from handlers.crypto_orderflow.config.handler_config import _get_sync_redis
                        redis_client = _get_sync_redis()
                    risk_cfg = maybe_apply_slq_to_risk_cfg(
                        redis=redis_client,
                        ctx=ctx,
                        symbol=str(self.cfg.symbol),
                        side=side_val,
                        cfg=dict(risk_cfg or {}),
                    )
                    with contextlib.suppress(Exception):
                        ctx.risk_cfg = dict(risk_cfg or {})
                except Exception:
                    logger.exception(
                        "orchestrator slq_risk_adjust failed symbol=%s kind=%s",
                        getattr(ctx, "symbol", "?"), kind_key,
                    )

                # Using liquidity component to ensure levels.
                # GAP-2 fix: side_val already captured from cand at L214;
                # do NOT re-read here — any mutation of cand.side between
                # SMT gate and levels would silently diverge.
                self.liquidity.ensure_trade_levels_once(
                    ctx=ctx,
                    symbol=ctx.symbol,
                    side=cand.side,
                    kind=cand.kind,
                    cfg=risk_cfg,
                    overwrite=False,
                )
                # 3.5 Level Metrics
                tm = str(getattr(ctx, "tp_mode_used", "ATR_LEGACY")).upper()
                self.observability.emit_level_mode_metric(tm, ctx)
            except Exception:
                logger.exception(
                    "orchestrator levels_attach_failed symbol=%s kind=%s",
                    getattr(ctx, "symbol", "?"), kind_key,
                )
                _emit_dq_flag(ctx, "levels_attach_failed", symbol=_sym_root)


            # 4. Cost Edge Gate
            _t_edge = time.monotonic()
            cost_decision = self.gates.edge_cost_cached(
                ctx=ctx, kind=kind_key, symbol=self.cfg.symbol, side=getattr(cand, "side", 0), cfg=None
            )
            audit.record_stage("edge_cost", _t_edge)

            # Publish Edge Gate diagnostics (async/fire-and-forget)
            try:
                self._maybe_publish_edge_event(ctx, cand, cost_decision, kind_key)
            except Exception as e:
                sym = str(getattr(ctx, "symbol", getattr(self.cfg, "symbol", "unknown")))
                self._emit_build_failed(kind_key, e, symbol=sym)

            _cost_dec = getattr(cost_decision, "decision", "DENY" if getattr(cost_decision, "veto", False) else "ALLOW")
            if cost_decision and _cost_dec in ("DENY", "SHADOW_DENY"):
                rc = getattr(cost_decision, "reason_code", getattr(cost_decision, "reason", VetoReason.VETO_COST))
                self._handle_veto(ctx, cand, kind_key, rc)
                continue
            elif cost_decision and _cost_dec == "TIGHTEN":
                # ARCH-3 fix: propagate slippage tighten into ctx.indicators so downstream sizing
                # and risk calculations see the updated cost. Mirror logic from signal_pipeline.py.
                tadd = float(getattr(cost_decision, "notes", {}).get("tighten_add_bps", 0.0) or 0.0)
                if tadd > 0:
                    inds = getattr(ctx, "indicators", None)
                    if isinstance(inds, dict):
                        inds["expected_slippage_bps"] = float(inds.get("expected_slippage_bps", 0.0) or 0.0) + tadd
                    logger.info(
                        "⚡ [ORCH] edge_cost TIGHTEN +%.2f bps | reason=%s sym=%s kind=%s",
                        tadd, getattr(cost_decision, "reason_code", "?"), _sym_root, kind_key,
                    )


            # 5. Validation & Scoring (ConfirmationsEngine)
            _t_confirm = time.monotonic()
            res = self.confirmations.validate(kind=kind_key, ctx=ctx)
            audit.record_stage("confirmations", _t_confirm)

            if getattr(res, "veto", getattr(res, "ok", True) is False):
                 rc = getattr(res, "reason_code", getattr(res, "code", VetoReason.VETO_CONFIRM))
                 self._handle_veto(ctx, cand, kind_key, rc)
                 continue


            # 6. Payload & Entry Policy
            if not getattr(ctx, "sizing_ok", False):
                _emit_dq_flag(ctx, "veto_sizing_failed", symbol=_sym_root)
                self._handle_veto(ctx, cand, kind_key, VetoReason.VETO_SIZING)
                continue

            _t_build = time.monotonic()
            try:
                payload, parts, envelope_kwargs = self._build_payload(ctx, cand, res, _now_ms)
            except Exception as e:
                sym = str(getattr(ctx, "symbol", getattr(self.cfg, "symbol", "unknown")))
                self._emit_build_failed(kind_key, e, symbol=sym)
                _emit_dq_flag(ctx, "payload_build_failed", symbol=sym)
                continue
            finally:
                audit.record_stage("build_payload", _t_build)

            # Entry Policy
            # Fix: GateDecisionV1 uses 'decision' ("DENY", "SHADOW_DENY")
            ep_decision = self.gates.check_entry_policy(ctx, payload)
            _ep_dec = getattr(ep_decision, "decision", "DENY" if getattr(ep_decision, "veto", False) else "ALLOW")
            if ep_decision and _ep_dec in ("DENY", "SHADOW_DENY"):
                rc = getattr(ep_decision, "reason_code", getattr(ep_decision, "reason", VetoReason.VETO_ENTRY_POLICY))
                self._handle_veto(ctx, cand, kind_key, rc)
                continue

            # 7. Emission
            _t_emit = time.monotonic()
            try:
                emit_res = self.emitter.emit(
                    signal_id=envelope_kwargs["signal_id"],
                    kind=envelope_kwargs["kind"],
                    symbol=envelope_kwargs["symbol"],
                    side=envelope_kwargs["side"],
                    ts_event_ms=envelope_kwargs["ts_event_ms"],
                    ingest_time_ms=envelope_kwargs["ingest_time_ms"],
                    trace_id=envelope_kwargs["trace_id"],
                    quality_flags=envelope_kwargs["quality_flags"],
                    source=envelope_kwargs["source"],
                    meta_schema_version=envelope_kwargs["schema_version"],
                    raw_score=envelope_kwargs["raw_score"],
                    final_score=envelope_kwargs["final_score"],
                    confidence_pct=envelope_kwargs["confidence_pct"],
                    payload=payload,
                )
                ok = getattr(emit_res, "ok", bool(emit_res))
                if ok:
                    any_sent = True
                    PipelineAuditWriter.safe_inc(emit_ok_total, kind=kind_key, symbol=_sym_root) if hasattr(emit_ok_total, "labels") else None
            except Exception:
                sym = str(getattr(ctx, "symbol", getattr(self.cfg, "symbol", "unknown")))
                logger.exception(
                    "orchestrator emit failed symbol=%s kind=%s signal_id=%s",
                    sym, kind_key, getattr(cand, "signal_id", "?"),
                )
                SIGNAL_EMIT_ERROR_TOTAL.labels(symbol=sym).inc()
            finally:
                # GAP-3: emit latency
                audit.record_stage("emit", _t_emit)

            # GAP-3: full per-candidate latency + SLO Summary
            audit.record_full()

        return any_sent

    def _build_payload(self, ctx: Any, cand: Any, res: Any, _now_ms: int = 0) -> Any:
        """
        Builds the signal payload dict and extracts contract envelope fields.

        Returns (payload_dict, parts, envelope_kwargs) where envelope_kwargs carries
        all mandatory CLAUDE.md §Data Contracts fields:
          schema_version, source, event_time_ms (= ts_ms), ingest_time_ms,
          trace_id, quality_flags.
        event_id is generated automatically by OutboxEnvelope.make_envelope().
        """
        # Safe string helpers
        def _ss(v): return (v or "")

        reasons = list(getattr(cand, "reasons", None) or [])
        reasons = [_ss(x) for x in reasons][:16]

        # ── Timestamp (event_time_ms) ─────────────────────────────────────────
        # Note: ts_ms is normalized at the start of process()
        _ts_ms = getattr(ctx, "ts_ms", 0)

        if _ts_ms == 0:
            _sym_for_metric = str(
                getattr(ctx, "symbol", getattr(self.cfg, "symbol", "unknown")) or "unknown"
            )
            with contextlib.suppress(Exception):
                PAYLOAD_TS_ANOMALY_TOTAL.labels(symbol=_sym_for_metric).inc()
            _emit_dq_flag(ctx, "payload_ts_anomaly", symbol=_sym_for_metric)
            logger.warning(
                "orchestrator _build_payload: ts anomaly symbol=%s ts_ms=%r; payload.ts=0",
                _sym_for_metric, _ts_ms,
            )

        # ── ingest_time_ms ────────────────────────────────────────────────────
        # Prefer ctx.redis_read_time_ms (set by Go→Redis ingest path), else now.
        _ingest_ms: int = int(
            getattr(ctx, "redis_read_time_ms", None)
            or getattr(ctx, "ingest_time_ms", None)
            or _now_ms
            or get_epoch_ms()
        )

        # ── trace_id ──────────────────────────────────────────────────────────
        # Propagate from ctx (set by Go worker or upstream handler), else None
        # (OutboxEnvelope will auto-generate from event_id).
        _trace_id: str | None = getattr(ctx, "trace_id", None) or None

        # ── quality_flags ─────────────────────────────────────────────────────
        # Merge dq_flags and data_quality_flags accumulated on ctx during gate processing.
        _qf_raw = getattr(ctx, "dq_flags", None)
        _quality_flags: list[str] = list(_qf_raw) if _qf_raw else []
        _dqf_raw = getattr(ctx, "data_quality_flags", None)
        if _dqf_raw:
            _quality_flags.extend(list(_dqf_raw))
        _quality_flags = list(dict.fromkeys(_quality_flags))

        # ── schema_version ────────────────────────────────────────────────────
        # Propagated from ConfirmationsEngine (meta_schema_version field).
        _raw_schema_v = getattr(res, "meta_schema_version", None) or getattr(res, "schema_version", None)
        _schema_version: int = int(_raw_schema_v) if _raw_schema_v else 1

        if not _raw_schema_v:
            _sym_sch = _ss(getattr(ctx, "symbol", ""))
            _kind = _ss(getattr(cand, "kind", ""))
            logger.warning("⚠️ schema_version fallback to 1 for %s (kind=%s)", _sym_sch, _kind)
            with contextlib.suppress(Exception):  # type: ignore
                _SCHEMA_VERSION_FALLBACK_TOTAL.labels(symbol=_sym_sch, kind=_kind).inc()  # type: ignore

        # ── source ───────────────────────────────────────────────────────────
        _source: str = (
            getattr(ctx, "source", None)
            or getattr(self.cfg, "source", None)
            or "python-worker"
        )

        _confidence_pct = float(getattr(res, "confidence", 0.0) or 0.0)
        if not math.isfinite(_confidence_pct):
            _confidence_pct = 0.5  # NaN/Inf must never reach outbox
            if "confidence_nan" not in _quality_flags:
                _quality_flags.append("confidence_nan")
        if _confidence_pct <= 0:
            try:  # type: ignore
                from services.observability.metrics_registry import metrics_registry  # type: ignore
                metrics_registry.get_or_create_counter(
                    "missing_confidence_total",
                    "Signals missing confidence pct",
                    ["symbol"],
                ).labels(symbol=_ss(getattr(ctx, "symbol", ""))).inc()
            except Exception: pass
            if "confidence_missing" not in _quality_flags:
                _quality_flags.append("confidence_missing")
                _sym_conf = str(getattr(ctx, "symbol", getattr(self.cfg, "symbol", "unknown")) or "unknown")
                _emit_dq_flag(ctx, "confidence_missing", symbol=_sym_conf)

        # ── entry_regime: stamp market regime at signal emission time ─────────
        # Canonical extraction order (mirrors pre_publish_gates._get_regime):
        #   ctx.regime (str or object with .name/.label)
        #   ctx.of.regime   (OrderflowContext sub-object)
        #   ctx.regime_label / ctx.market_regime (alternative attr names)
        # Normalised via contexts.normalize_regime_label → canonical lowercase string.
        # Falls back to "na" if no regime is available (trade_monitor will show as "unknown").
        _of_ctx = getattr(ctx, "of", None)
        _raw_regime = (
            getattr(ctx, "regime", None)
            or (_of_ctx is not None and getattr(_of_ctx, "regime", None))
            or getattr(ctx, "regime_label", None)
            or getattr(ctx, "market_regime", None)
        )
        if _raw_regime is not None and not isinstance(_raw_regime, str):
            # Handle enum/object regime (e.g. MarketRegime.TREND → "TREND")
            _raw_regime = str(
                getattr(_raw_regime, "name", None)
                or getattr(_raw_regime, "label", None)
                or getattr(_raw_regime, "value", None)
                or _raw_regime
            )
        try:
            from contexts import normalize_regime_label as _nrl
            _entry_regime: str = _nrl(str(_raw_regime or ""))
        except Exception:
            _entry_regime = str(_raw_regime or "").strip().lower() or "na"

        payload = {
            "kind": _ss(getattr(cand, "kind", "")),
            "side": _ss(getattr(cand, "side", "")),
            "symbol": _ss(getattr(ctx, "symbol", "")),
            # Keep "ts" as backward-compat alias; canonical = event_time_ms in envelope.
            "ts": _ts_ms,
            "price": safe_float(getattr(ctx, "price", None), 0.0),
            "raw_score": safe_float(getattr(cand, "raw_score", None), 0.0),
            "final_score": safe_float(getattr(res, "final_score", 0.0), 0.0),
            "confidence": _confidence_pct,
            "reasons": reasons,
            "signal_id": _ss(getattr(cand, "signal_id", "")),
            "venue": _ss(getattr(ctx, "venue", None)),
            "timeframe": _ss(getattr(ctx, "timeframe", None)),

            # Market regime at entry — consumed by trade_monitor._extract_regime_from_signal
            # and stamped onto PositionState.entry_regime for performance attribution.
            # Value: normalize_regime_label output (e.g. "trending_bull", "range", "na").
            "entry_regime": _entry_regime,
            "regime": _entry_regime,  # backward-compat alias (trade_monitor checks both)

            # -----------------------------------------------------------------
            # Trade levels and ATR for metrics downstream
            # -----------------------------------------------------------------
            "atr": safe_float(getattr(ctx, "atr", None), 0.0),
            "sl_price": safe_float(getattr(ctx, "sl_price", None), 0.0),
            "tp1_price": safe_float(getattr(ctx, "tp1_price", None), 0.0),
            "tp_mode": str(getattr(ctx, "tp_mode_used", "ATR_LEGACY") or "ATR_LEGACY"),
            "risk_usd_target": safe_float(getattr(ctx, "risk_usd_target", None), 0.0),
            "risk_usd_actual": safe_float(getattr(ctx, "risk_usd", None), 0.0),
            "qty": safe_float(getattr(ctx, "qty", None), 0.0),

            # Trailing & Execution params
            "trail_profile": str(getattr(ctx, "trail_profile", "") or ""),
            "trailing_min_lock_r": safe_float(getattr(ctx, "trailing_min_lock_r", None), 0.0),
            "slq_used": int(getattr(ctx, "risk_cfg", {}).get("slq_used", 0) or 0),
        }

        # ⚡ LAST-RESORT NOTIONAL CAP: clamp qty before emission
        # Fail-closed! If limit calculation fails, zero-out quantity to prevent unbounded risk.
        try:
            _entry = float(payload.get("price", 0.0) or 0.0)
            _qty = float(payload.get("qty", 0.0) or 0.0)
            if _entry > 0 and _qty > 0:
                _max_notional = float(os.getenv("RISK_MAX_NOTIONAL_USD", "0") or "0")
                if _max_notional <= 0:
                    _dep = float(os.getenv("ACCOUNT_DEPOSIT_USD", "100") or "100")
                    _rp = float(os.getenv("RISK_PERCENT", "5.0") or "5.0")
                    if 0 < _rp < 0.5:
                        logger.warning(
                            "orchestrator RISK_PERCENT=%.4f выглядит как доля (< 0.5); "
                            "автоматически масштабируется в %.1f%%. "
                            "Задайте значение явно в процентах (например RISK_PERCENT=5.0) "
                            "для устранения неоднозначности.",
                            _rp, _rp * 100.0,
                        )
                        _rp *= 100.0
                    _nc = float(os.getenv("NOTIONAL_LEVERAGE_CAP", "100") or "100")
                    _max_notional = _dep * (_rp / 100.0) * _nc  # e.g. 100*5%*100=500
                _max_qty = float(os.getenv("RISK_MAX_QTY", "0") or "0")
                _max_qty_by_notional = _max_notional / _entry if _max_notional > 0 else float("inf")
                _cap = min(
                    _max_qty_by_notional,
                    _max_qty if _max_qty > 0 else float("inf"),
                )
                if _qty > _cap:
                    payload["qty"] = _cap
                    with contextlib.suppress(Exception):
                        ctx.qty = _cap
                    try:
                        from services.observability.metrics_registry import notional_clamped_total  # type: ignore
                        notional_clamped_total.labels(symbol=str(getattr(ctx, "symbol", getattr(self.cfg, "symbol", "unknown")))).inc()  # type: ignore
                    except Exception as metric_err:
                        logger.warning("Failed to emit notional_clamped_total metric: %r", metric_err)
        except Exception as e:
            logger.error("orchestrator notional clamp failed! Zeroing qty. Error: %r", e)
            payload["qty"] = 0.0
            with contextlib.suppress(Exception): ctx.qty = 0.0

        # ── Phase 0: Horizon-aware contract enrichment ────────────────────────
        # Attach ATRProfileV1 / HorizonProfileV1 to ctx and enrich payload meta.
        # ALL USE_FOR_* flags are OFF → trading logic (SL/TP/trailing) unchanged.
        # Controlled by ATR_HORIZON_EMIT_PAYLOAD_META / ATR_HORIZON_EMIT_METRICS.
        _hz_risk_profile = None
        try:
            from core.horizon_contract import (
                attach_phase0_profiles_to_ctx,
                build_horizon_meta_for_payload,
            )
            from core.horizon_metrics import emit_horizon_contract_metrics

            _kind_key_str = _ss(getattr(cand, "kind", ""))
            _sym_str = _ss(getattr(ctx, "symbol", getattr(self.cfg, "symbol", "")))
            # Reuse already-normalised regime computed above for Horizon contract.
            _regime_str = _entry_regime or "unknown"
            _now_ts_ms_hz = _ts_ms or _now_ms or int(time.time() * 1000)

            # Risk config for ATR mult fields
            _rc = getattr(ctx, "risk_cfg", None) or {}
            _sl_atr_mult = float(_rc.get("sl_atr_mult") or getattr(ctx, "sl_atr_mult", 0.0) or 0.0)
            _tp1_atr_mult = float(_rc.get("tp1_atr_mult") or getattr(ctx, "tp1_atr_mult", 0.0) or 0.0)
            _tp2_atr_mult = float(_rc.get("tp2_atr_mult") or getattr(ctx, "tp2_atr_mult", 0.0) or 0.0)

            # 1. Attach profiles to ctx (fail-open)
            _hz_risk_profile = attach_phase0_profiles_to_ctx(
                ctx,
                symbol=_sym_str,
                kind=_kind_key_str,
                regime=_regime_str,
                now_ts_ms=_now_ts_ms_hz,
                sl_atr_mult=_sl_atr_mult,
                tp1_atr_mult=_tp1_atr_mult,
                tp2_atr_mult=_tp2_atr_mult,
            )

            if _hz_risk_profile is not None:
                # 2. Enrich payload meta (additive only — no existing keys overridden)
                _existing_meta = dict(payload.get("meta") or {})
                # Merge in existing risk_cfg meta fields
                if not _existing_meta.get("sl_mode"):
                    _existing_meta["sl_mode"] = (_rc.get("sl_mode") or "ATR")
                if not _existing_meta.get("sl_atr_mult"):
                    _existing_meta["sl_atr_mult"] = _sl_atr_mult
                _enriched_meta = build_horizon_meta_for_payload(
                    _hz_risk_profile,
                    existing_meta=_existing_meta,
                )
                payload["meta"] = _enriched_meta

                # 3. Emit Prometheus metrics (fail-open)
                emit_horizon_contract_metrics(
                    symbol=_sym_str,
                    kind=_kind_key_str,
                    risk_profile=_hz_risk_profile,
                )

                # 4. Enrich diagnostics trace fragment (fail-open)
                try:
                    from core.horizon_contract import build_horizon_trace_fragment as _build_hz_frag
                    _hz_frag = _build_hz_frag(_hz_risk_profile)
                    # Attach to trace if available
                    _tr = getattr(ctx, "trace", None)
                    if _tr is not None and hasattr(_tr, "__dict__"):
                        _tr_extra = getattr(_tr, "extra", None)
                        if isinstance(_tr_extra, dict):
                            _tr_extra.setdefault("horizon", _hz_frag.get("horizon"))
                            _tr_extra.setdefault("atr_profile", _hz_frag.get("atr_profile"))
                except Exception:
                    pass

        except Exception as _hz_err:
            try:  # type: ignore
                from services.observability.metrics_registry import metrics_registry  # type: ignore
                _sym_err = str(getattr(ctx, "symbol", getattr(self.cfg, "symbol", "unknown")) or "unknown")
                metrics_registry.get_or_create_counter(
                    "horizon_attach_errors_total",
                    "Failures during Horizon profile attachment in Orchestrator",
                    ["symbol", "error_type"]
                ).labels(symbol=_sym_err, error_type=_hz_err.__class__.__name__).inc()
                logger.warning(
                    "orchestrator Phase 0 Horizon attach failed symbol=%s: %r",
                    _sym_err, _hz_err
                )
            except Exception:
                pass  # absolute fail-open: Phase 0 enrichment must never block emission

        parts = getattr(res, "parts", {})

        # ── Envelope kwargs (mandatory contract fields) ────────────────────────
        envelope_kwargs = {
            "signal_id": _ss(getattr(cand, "signal_id", "")),
            "kind": _ss(getattr(cand, "kind", "")),
            "symbol": _ss(getattr(ctx, "symbol", "")),
            "side": _resolve_side(cand) or None,
            "ts_event_ms": _ts_ms,
            "ingest_time_ms": _ingest_ms,
            "trace_id": _trace_id,
            "quality_flags": _quality_flags if _quality_flags else None,
            "source": _source,
            "schema_version": _schema_version,
            "raw_score": safe_float(getattr(cand, "raw_score", None), 0.0),
            "final_score": safe_float(getattr(res, "final_score", None), 0.0),
            "confidence_pct": _confidence_pct,
        }

        # ── GAP-5: Shadow payload contract validation ─────────────────────────
        # #16: Shadow→enforce flip now requires explicit percentage-based rollout.
        # ENV vars:
        #   PAYLOAD_CONTRACT_MODE = off | shadow | enforce  (default: shadow)
        #   PAYLOAD_CONTRACT_ROLLOUT_PCT = 0..100  (default: 100 in shadow, required in enforce)
        #     e.g. 10 → enforce fires for ~10% of payloads only.
        # Switching from shadow to enforce at 100% in one shot is dangerous;
        # ramp up: 1% → 10% → 50% → 100% with monitoring between steps.
        try:
            from common.contracts.tradeable_contracts import assert_tradeable_dict
            _contract_mode = os.getenv("PAYLOAD_CONTRACT_MODE", "shadow").lower()
            if _contract_mode not in ("shadow", "enforce", "on", "1", "true"):
                pass  # off / disabled
            else:
                # Percentage rollout gate (applies to both shadow and enforce)
                _rollout_pct = float(os.getenv("PAYLOAD_CONTRACT_ROLLOUT_PCT", "100") or "100")
                _rollout_pct = max(0.0, min(100.0, _rollout_pct))
                _in_rollout = (_rollout_pct >= 100.0) or (random.random() * 100.0 < _rollout_pct)
                if _in_rollout:
                    try:
                        assert_tradeable_dict(payload, where="orchestrator._build_payload")
                    except AssertionError as _contract_err:
                        _sym_for_metric = str(
                            getattr(ctx, "symbol", getattr(self.cfg, "symbol", "unknown")) or "unknown"
                        )
                        logger.warning(
                            "orchestrator _build_payload contract violation symbol=%s kind=%s pct=%.1f: %s",
                            _sym_for_metric,
                            _ss(getattr(cand, "kind", "")),
                            _rollout_pct,
                            _contract_err,
                        )
                        try:  # type: ignore
                            from services.observability.metrics_registry import metrics_registry  # type: ignore
                            metrics_registry.get_or_create_counter(
                                "signal_payload_contract_violation_total",
                                "Payload contract assertions that fired in Orchestrator",
                                ["symbol"],
                            ).labels(symbol=_sym_for_metric).inc()
                        except Exception:
                            pass
                        # enforce mode: re-raise (blocks emit for bad payloads)
                        if _contract_mode == "enforce":
                            raise _contract_err
        except AssertionError as e:
            if os.getenv("PAYLOAD_CONTRACT_MODE", "shadow").lower() == "enforce":
                raise e
        except Exception:
            pass  # import error or unexpected — never block prod path

        return payload, parts, envelope_kwargs

    def _maybe_publish_edge_event(self, ctx: Any, cand: Any, cost_decision: Any, kind: str) -> None:
        """
        Publishes EdgeGateEvent to Redis Stream for ingestion into Postgres.
        Ref: D1-A Architecture.
        """
        if not cost_decision:
            return

        mode = os.getenv("EDGE_GATE_EVENTS_MODE", "off").lower()
        if mode not in {"redis_stream", "stream", "on", "1", "true"}:
            return

        # EdgeCostGateDecision: veto + passed property
        veto = bool(getattr(cost_decision, "veto", False))
        passed = not veto

        # Sampling
        if not passed:
            # VETO: 100% sample by default
            sample_rate = float(os.getenv("EDGE_GATE_SAMPLE_VETO", "1.0"))
        else:
            # PASS: 1-5% sample by default
            sample_rate = float(os.getenv("EDGE_GATE_SAMPLE_PASS", "0.02"))

        if sample_rate < 1.0 and random.random() > sample_rate:
            return

        redis_client = getattr(ctx, "redis", None)
        if not redis_client:
            return

        stream_key = os.getenv("EDGE_GATE_EVENTS_STREAM", RS.EDGE_GATE_EVENTS)

        # Build event with robust field mapping
        try:
            # Normalize ts_ms (handle seconds, missing values)
            ts_ms = getattr(ctx, "ts_ms", 0)

            # Direct field mapping from EdgeCostGateDecision
            exp_bps = float(getattr(cost_decision, "expected_move_bps", 0.0))
            req_bps = float(getattr(cost_decision, "threshold_bps", 0.0))
            k = float(getattr(cost_decision, "k", 0.0))

            fees_bps = float(getattr(cost_decision, "fees_bps", 0.0))
            slip_bps = float(getattr(cost_decision, "slippage_bps", 0.0))
            buf_bps = float(getattr(cost_decision, "buffer_bps", 0.0))

            # #17: Use total_costs_bps from cost_decision directly to avoid drift.
            # Recompute only as fallback if the field is missing (e.g. older gate version).
            _tcd = getattr(cost_decision, "total_costs_bps", None)
            total_costs_bps = float(_tcd) if _tcd is not None else fees_bps + slip_bps + buf_bps

            edge_source = str(getattr(cost_decision, "edge_source",
                            getattr(cost_decision, "mode", "none")) or "none")

            # Short veto code (reason_code)
            veto_code = None
            if not passed:
                veto_code = str(getattr(cost_decision, "reason_code", "edge_cost:veto") or "edge_cost:veto")

            # Compute margin and edge_ratio
            margin_bps = exp_bps - req_bps

            # Edge ratio with safe division
            if req_bps > 0:
                edge_ratio = exp_bps / req_bps
            else:
                edge_ratio = float("inf") if exp_bps > 0 else 0.0

            evt = {
                "signal_id": str(getattr(cand, "signal_id", "") or ""),
                "symbol": str(getattr(ctx, "symbol", "") or self.cfg.symbol or "").upper(),
                "ts_ms": int(ts_ms),
                "gate_name": "edge_cost",
                "gate_version": 3,
                "stage": "pre_emit",
                "passed": 1 if passed else 0,
                "veto_code": veto_code,
                "edge_source": edge_source,

                # Metrics
                "exp_bps": exp_bps,
                "req_bps": req_bps,
                "margin_bps": margin_bps,
                "edge_ratio": edge_ratio,

                "k": k,
                "fees_bps": fees_bps,
                "slip_bps": slip_bps,
                "buf_bps": buf_bps,
                "total_costs_bps": total_costs_bps,

                "ctx": json.dumps({"kind": kind})
            }

            # Fire and forget
            try:
                redis_client.xadd(stream_key, {k: str(v) if v is not None else "" for k, v in evt.items()}, maxlen=MAXLEN_GLOBAL, approximate=True)
            except Exception as e:
                sym = str(getattr(ctx, "symbol", getattr(self.cfg, "symbol", "unknown")))
                logger.warning("orchestrator edge gate event xadd failed symbol=%s kind=%s: %r", sym, kind, e)

        except Exception as e:
            sym = str(getattr(ctx, "symbol", getattr(self.cfg, "symbol", "unknown")))
            self._emit_build_failed(kind, e, symbol=sym)

