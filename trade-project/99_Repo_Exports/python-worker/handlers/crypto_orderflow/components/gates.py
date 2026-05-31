from __future__ import annotations

import logging
import os
import time
import hashlib
import json
from typing import Any

from common.ctx_cache import cached_on_ctx
from common.dq_flags import append_dq_flag
from handlers.crypto_orderflow.utils.edge_cost_gate import EdgeCostGate
from handlers.crypto_orderflow.utils.portfolio_exposure_gate import PortfolioExposureGate
from handlers.crypto_orderflow.utils.entry_policy_gate import EntryPolicyGate
import contextlib
from core.gates.decision import GateDecisionV1
from handlers.crypto_orderflow.utils.pre_publish_gates import HardDataQualityGate, AtrFloorGate, BreadthGate
from services.orderflow.book_sanity_gate import BookSanityGate
from services.orderflow.stream_integrity_gate import StreamIntegrityGate
from core.atr_floor_policy import compute_atr_bps_threshold
from core.fees_aware_policy import fees_aware_min_atr_bps
import core.instrument_config as _ic
from dataclasses import replace
from services.orderflow.derivatives_context import aread_derivatives_context
from services.orderflow.derivatives_context_gate import evaluate_derivatives_context_v2
from services.orderflow.defillama_context import aread_defillama_context
from services.orderflow.defillama_context_gate import evaluate_defillama_context
from services.orderflow.sentiment_context import aread_sentiment_context
from services.orderflow.sentiment_context_gate import evaluate_sentiment_context
from services.orderflow.crossvenue_context import aread_crossvenue_context
from services.orderflow.crossvenue_context_gate import evaluate_crossvenue_context
from services.orderflow.liquidity_geom_policy import evaluate_liq_geom
from services.orderflow.flow_toxicity import evaluate_flow_toxicity
from services.orderflow.liquidation_context_worker import aread_liq_context
from services.orderflow.breadth_context import aread_breadth_context
from services.orderflow.exec_health_freeze_hook import aread_exec_health_auto_freeze, build_exec_health_auto_freeze_decision
from core.funding_basis_calibrator import FundingBasisCalibrator

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Optional Prometheus metrics (fail-open if registry unavailable)
# ---------------------------------------------------------------------------
try:
    from prometheus_client import Counter, Histogram  # type: ignore

    _GATES_ERROR = Counter(
        "gates_error_total",
        "Number of gate errors (fail-open activations) per gate",
        ["gate", "reason"],
    )
    _GATE_DECISIONS_TOTAL = Counter(
        "gate_decisions_total",
        "Total gate decisions",
        ["stage", "gate", "decision", "reason_code", "symbol", "kind", "profile"],
    )
    _GATE_SHADOW_DENY_TOTAL = Counter(
        "gate_shadow_deny_total",
        "Total gate shadow deny",
        ["gate", "reason_code"],
    )
    _GATE_TIGHTEN_TOTAL = Counter(
        "gate_tighten_total",
        "Total gate tighten",
        ["gate", "reason_code"],
    )
    _GATE_LATENCY_US = Histogram(
        "gate_latency_us",
        "Gate latency in microseconds",
        ["gate"],
        buckets=[100, 500, 1000, 5000, 10000, 20000, 50000]
    )
    _GATES_EVAL_TOTAL = Counter(
        "gates_eval_total",
        "Total gate evaluations per gate",
        ["gate"],
    )
    _FUNDING_BASIS_REGIME_TOTAL = Counter(
        "funding_basis_calib_regime_total",
        "Funding/basis calibrator regime tag observations per symbol",
        ["symbol", "regime_tag"],
    )
    _GATES_METRICS = True
except Exception:  # pragma: no cover
    _GATES_METRICS = False
    _GATE_DECISIONS_TOTAL = None
    _GATE_SHADOW_DENY_TOTAL = None
    _GATE_TIGHTEN_TOTAL = None
    _GATE_LATENCY_US = None
    _GATES_EVAL_TOTAL = None
    _FUNDING_BASIS_REGIME_TOTAL = None

def _record_gate_error(gate: str, reason: str) -> None:
    """Increment gates_error_total counter, fail-open if metrics unavailable."""
    if _GATES_METRICS:
        with contextlib.suppress(Exception):
            _GATES_ERROR.labels(gate=gate, reason=reason).inc()

def _record_gate_decision(dec, symbol: str, kind: str, profile: str) -> None:
    if not _GATES_METRICS:
        return
    with contextlib.suppress(Exception):
        if _GATE_DECISIONS_TOTAL is not None:
            _GATE_DECISIONS_TOTAL.labels(
                stage=getattr(dec, "stage", "UNKNOWN"),
                gate=dec.gate,
                decision=dec.decision,
                reason_code=dec.reason_code,
                symbol=symbol,
                kind=kind,
                profile=profile
            ).inc()
        if dec.decision == "SHADOW_DENY" and _GATE_SHADOW_DENY_TOTAL is not None:
            _GATE_SHADOW_DENY_TOTAL.labels(gate=dec.gate, reason_code=dec.reason_code).inc()
        elif dec.decision == "TIGHTEN" and _GATE_TIGHTEN_TOTAL is not None:
            _GATE_TIGHTEN_TOTAL.labels(gate=dec.gate, reason_code=dec.reason_code).inc()
        
        if _GATE_LATENCY_US is not None and getattr(dec, "latency_us", None) is not None:
            _GATE_LATENCY_US.labels(gate=dec.gate).observe(dec.latency_us)

def _record_gate_eval(gate: str) -> None:
    """Increment gates_eval_total counter."""
    if _GATES_EVAL_TOTAL is not None:
        with contextlib.suppress(Exception):
            _GATES_EVAL_TOTAL.labels(gate=gate).inc()  # type: ignore

def _fast_hash(**kwargs) -> str:
    try:
        j = json.dumps(kwargs, sort_keys=True, default=str)
        return hashlib.md5(j.encode("utf-8")).hexdigest()[:8]
    except Exception:
        return "err_hash"

def _get_ts_ms(ctx: Any) -> int:
    try:
        ts = getattr(ctx, "ts_ms", getattr(ctx, "ts", 0))
        return int(ts) if ts else 0
    except Exception:
        return 0


# Resolved sync Redis client cache (module-level, lazy init).
# orchestrator.py:726 documents ctx.redis is None in prod; use _get_sync_redis().
_EDGE_EVT_REDIS = None
_EDGE_EVT_STREAM_KEY = None
_EDGE_EVT_INIT_FAILED = False


def _maybe_publish_edge_event_from_gate(
    ctx: Any, res: Any, *, kind: str, symbol: str, side: Any
) -> None:
    """
    Publish EdgeCostGate decision to Redis Stream for ingest into TimescaleDB.
    Fire-and-forget. Wired into the active SignalPipeline path
    (services/orderflow/signal_pipeline.py:2069 -> edge_cost_cached).

    Twin of orchestrator._maybe_publish_edge_event (which lives in the
    legacy SignalOrchestrator path that prod CryptoOrderflowService does
    not exercise). Keep producer fields aligned with edge_gate_ingestor.py
    field parser.
    """
    global _EDGE_EVT_REDIS, _EDGE_EVT_STREAM_KEY, _EDGE_EVT_INIT_FAILED

    if res is None:
        return

    mode = os.getenv("EDGE_GATE_EVENTS_MODE", "off").lower()
    if mode not in {"redis_stream", "stream", "on", "1", "true"}:
        return

    veto = bool(getattr(res, "veto", False))
    passed = not veto

    # Sampling: 100% VETO, low rate for PASS.
    import random as _rand
    if passed:
        rate = float(os.getenv("EDGE_GATE_SAMPLE_PASS", "0.05") or 0.05)
    else:
        rate = float(os.getenv("EDGE_GATE_SAMPLE_VETO", "1.0") or 1.0)
    if rate < 1.0 and _rand.random() > rate:
        return

    if _EDGE_EVT_INIT_FAILED:
        return

    redis_client = _EDGE_EVT_REDIS
    if redis_client is None:
        try:
            from handlers.crypto_orderflow.config.handler_config import _get_sync_redis
            redis_client = _get_sync_redis()
            _EDGE_EVT_REDIS = redis_client
            _EDGE_EVT_STREAM_KEY = os.getenv(
                "EDGE_GATE_EVENTS_STREAM", "stream:diag:edge_gate_events"
            )
        except Exception:
            _EDGE_EVT_INIT_FAILED = True
            return
    if redis_client is None:
        _EDGE_EVT_INIT_FAILED = True
        return

    try:
        ts_ms = _get_ts_ms(ctx) or int(time.time() * 1000)

        exp_bps = float(getattr(res, "expected_move_bps", 0.0) or 0.0)
        req_bps = float(getattr(res, "threshold_bps", 0.0) or 0.0)
        k_val = float(getattr(res, "k", 0.0) or 0.0)
        fees_bps = float(getattr(res, "fees_bps", 0.0) or 0.0)
        slip_bps = float(getattr(res, "slippage_bps", 0.0) or 0.0)
        buf_bps = float(getattr(res, "buffer_bps", 0.0) or 0.0)
        tcd = getattr(res, "total_costs_bps", None)
        total_costs_bps = float(tcd) if tcd is not None else (fees_bps + slip_bps + buf_bps)
        edge_source = str(getattr(res, "edge_source", getattr(res, "mode", "none")) or "none")
        margin_bps = exp_bps - req_bps
        if req_bps > 0:
            ratio = exp_bps / req_bps
        else:
            ratio = float("inf") if exp_bps > 0 else 0.0
        ratio_cap = float(os.getenv("EDGE_GATE_RATIO_CAP", "1000000.0") or 1000000.0)
        if ratio == float("inf") or ratio > ratio_cap:
            ratio = ratio_cap

        side_str = str(getattr(side, "value", side) or "").upper()
        sid = (
            getattr(ctx, "signal_id", None)
            or getattr(ctx, "sid", None)
            or f"{symbol}:{kind}:{ts_ms}:{side_str}"
        )

        veto_code = None
        if veto:
            veto_code = str(getattr(res, "reason_code", "edge_cost:veto") or "edge_cost:veto")

        fields = {
            "signal_id": str(sid),
            "symbol": str(symbol or "").upper(),
            "ts_ms": int(ts_ms),
            "gate_name": "edge_cost",
            "gate_version": 3,
            "stage": "pre_emit",
            "passed": 1 if passed else 0,
            "veto_code": veto_code or "",
            "edge_source": edge_source,
            "exp_bps": exp_bps,
            "req_bps": req_bps,
            "margin_bps": margin_bps,
            "edge_ratio": ratio,
            "k": k_val,
            "fees_bps": fees_bps,
            "slip_bps": slip_bps,
            "buf_bps": buf_bps,
            "total_costs_bps": total_costs_bps,
        }

        stream_key = _EDGE_EVT_STREAM_KEY or "stream:diag:edge_gate_events"
        maxlen = int(os.getenv("EDGE_GATE_EVENTS_MAXLEN", "500000") or 500000)
        redis_client.xadd(stream_key, fields, maxlen=maxlen, approximate=True)
    except Exception as exc:
        log.debug("edge_gate_event publish failed: %s", exc)


class GateOrchestrator:
    """
    Manages signal validation gates and returns unified GateDecisionV1.
    """

    def __init__(
        self,
        entry_policy: EntryPolicyGate | None,
        cost_gate: EdgeCostGate | None,
        portfolio_gate: PortfolioExposureGate | None = None,
        consistency_gate: Any = None,
        regime_liquidity_gate: Any = None,
        smt_gate: Any = None,
        dq_gate: HardDataQualityGate | None = None,
        book_sanity_gate: BookSanityGate | None = None,
        stream_integrity_gate: StreamIntegrityGate | None = None,
        atr_floor_gate: AtrFloorGate | None = None,
        breadth_gate: BreadthGate | None = None,
    ):
        self._entry_policy = entry_policy
        self._cost_gate = cost_gate
        self.portfolio_gate = portfolio_gate
        self._consistency_gate = consistency_gate
        self._regime_liquidity_gate = regime_liquidity_gate
        self._smt_gate = smt_gate
        self._dq_gate = dq_gate
        self._book_sanity_gate = book_sanity_gate
        self._stream_integrity_gate = stream_integrity_gate
        self._atr_floor_gate = atr_floor_gate
        self._breadth_gate = breadth_gate

        # Adaptive funding/basis calibrator: ENV-controlled, shadow by default.
        _fb_enforce = (os.getenv("FUNDING_BASIS_CALIB_ENFORCE", "0") or "0").strip().lower() in {"1", "true", "yes", "on"}
        _fb_min = int(os.getenv("FUNDING_BASIS_CALIB_MIN_SAMPLES", "500") or "500")
        self._funding_basis_cal = FundingBasisCalibrator(
            min_samples=_fb_min,
            enforce=_fb_enforce,
            auto_enforce=True,
        )
        self._fb_loaded: bool = False
        self._fb_last_snapshot_ms: int = 0
        _snap_sec = int(os.getenv("FUNDING_BASIS_CALIB_SNAPSHOT_SEC", "120") or "120")
        self._fb_snapshot_interval_ms: int = _snap_sec * 1000

        self._regime_strict = (os.getenv("REGIME_GATE_STRICT", "0") or "0").strip().lower() in {"1", "true", "yes", "on"}
        def _csv(name: str, default: str = "") -> set[str]:
            v = (os.getenv(name, default) or default).strip().lower()
            return {x.strip() for x in v.split(",") if x.strip()}

        self._regime_breakout_block = _csv("REGIME_GATE_BREAKOUT_BLOCK")
        self._regime_extreme_block = _csv("REGIME_GATE_EXTREME_BLOCK")

        # Direction-aware regime filter: block counter-trend trades.
        # Mode: off | shadow | enforce  (default: off — explicit opt-in).
        self._regime_dir_mode = (os.getenv("REGIME_DIRECTION_GATE_MODE", "off") or "off").strip().lower()
        self._regime_bear_labels = _csv(
            "REGIME_BEAR_LABELS",
            "trending_bear,bear_trend,strong_bear,bear",
        )
        self._regime_bull_labels = _csv(
            "REGIME_BULL_LABELS",
            "trending_bull,bull_trend,strong_bull,bull",
        )
        # Fallback to Redis `regime:{SYMBOL}` when context regime is empty.
        self._regime_redis_fallback = (os.getenv("REGIME_GATE_REDIS_FALLBACK", "1") or "1").strip().lower() in {"1", "true", "yes", "on"}
        # In-process TTL cache for Redis fallback to avoid hot-path hammering.
        self._regime_redis_cache: dict[str, tuple[float, str]] = {}
        try:
            self._regime_redis_cache_ttl_s = float(os.getenv("REGIME_GATE_REDIS_CACHE_TTL_S", "2.0"))
        except (ValueError, TypeError):
            self._regime_redis_cache_ttl_s = 2.0

    # ── FundingBasisCalibrator persistence ───────────────────────────────────

    async def _snapshot_funding_basis_to_redis(self, redis: Any, now_ms: int) -> None:
        if now_ms - self._fb_last_snapshot_ms < self._fb_snapshot_interval_ms:
            return
        from core.redis_keys import RK
        try:
            import json as _json
            for regime_key in list(self._funding_basis_cal._n.keys()):
                state = self._funding_basis_cal.dump_regime_state(
                    symbol=regime_key.upper(),
                    regime=regime_key,
                    updated_ts_ms=now_ms,
                )
                await redis.hset(RK.AUTOCAL_FUNDING_BASIS, regime_key, _json.dumps(state))
            self._fb_last_snapshot_ms = now_ms
        except Exception:
            pass

    async def _load_funding_basis_from_redis(self, redis: Any) -> None:
        from core.redis_keys import RK
        try:
            import json as _json
            raw_map = await redis.hgetall(RK.AUTOCAL_FUNDING_BASIS)
            if not raw_map:
                return
            for raw_val in raw_map.values():
                if isinstance(raw_val, (bytes, bytearray)):
                    raw_val = raw_val.decode("utf-8", "ignore")
                try:
                    state = _json.loads(raw_val)
                    self._funding_basis_cal.load_regime_state(state)
                except Exception:
                    pass
        except Exception:
            pass

    def _mark_dq(self, ctx: Any, flag: str, exc: Exception | None = None) -> None:
        if exc is not None:
            log.debug("_mark_dq %s: %s", flag, exc)
        append_dq_flag(ctx, flag)

    def check_quality(self, ctx: Any, kind: str, side: str = "") -> GateDecisionV1:
        t0 = time.monotonic()
        ts_dec_ms = int(time.time() * 1000)
        ts_ev_ms = _get_ts_ms(ctx)
        sym = str(getattr(ctx, "symbol", "") or "")
        inp_hash = _fast_hash(kind=kind, side=side, sym=sym)
        gate_name = "RegimeSessionLiquidityGate"
        
        if self._regime_liquidity_gate is None:
            latency_us = int((time.monotonic() - t0) * 1_000_000)
            return GateDecisionV1(
                stage="quality", gate=gate_name, decision="ABSTAIN", reason_code="OK", severity="INFO",
                profile="default", fail_policy="OPEN", ts_event_ms=ts_ev_ms, ts_decision_ms=ts_dec_ms,
                latency_us=latency_us, inputs_hash=inp_hash, notes={"msg": "gate_not_configured"}
            )
        try:
            res = self._regime_liquidity_gate.evaluate(ctx=ctx, symbol=sym, kind=kind)
            veto = (getattr(res, "decision", "ALLOW") == "DENY")
            rc = getattr(res, "reason_code", getattr(res, "reason", "OK"))
            notes = {"msg": getattr(res, "notes", {})}
            latency_us = int((time.monotonic() - t0) * 1_000_000)
            return GateDecisionV1(
                stage="quality", gate=gate_name, decision="DENY" if veto else "ALLOW", reason_code=rc,
                severity="WARN" if veto else "INFO", profile="default", fail_policy="OPEN",
                ts_event_ms=ts_ev_ms, ts_decision_ms=ts_dec_ms, latency_us=latency_us,
                inputs_hash=inp_hash, notes=notes
            )
        except Exception as exc:
            log.exception("check_quality failed, fail-open: %s", exc)
            self._mark_dq(ctx, "quality_error", exc=exc)
            _record_gate_error("check_quality", "quality_error")
            latency_us = int((time.monotonic() - t0) * 1_000_000)
            return GateDecisionV1(
                stage="quality", gate=gate_name, decision="ALLOW", reason_code="FAIL_OPEN_QUALITY",
                severity="CRITICAL", profile="default", fail_policy="OPEN", ts_event_ms=ts_ev_ms,
                ts_decision_ms=ts_dec_ms, latency_us=latency_us, inputs_hash=inp_hash, notes={"error": str(exc)}
            )

    def check_liquidity_integrity(self, ctx: Any) -> GateDecisionV1:
        t0 = time.monotonic()
        ts_dec_ms = int(time.time() * 1000)
        ts_ev_ms = _get_ts_ms(ctx)
        sym = str(getattr(ctx, "symbol", "") or "")
        inp_hash = _fast_hash(sym=sym)
        gate_name = "LiquidityIntegrityGate"

        try:
            indicators = getattr(ctx, "indicators", {})
            cfg = getattr(ctx, "config", {})
            if not cfg:
                # Try to get from ctx.runtime.config
                runtime = getattr(ctx, "runtime", None)
                cfg = getattr(runtime, "config", {}) if runtime else {}

            # 1. Spread BPS
            max_spread_bps = cfg.get("gate_spread_max_bps", 0.0) or 0.0
            curr_spread_bps = indicators.get("liq_spread_bps", indicators.get("spread_bps", 0.0) or 0.0)

            # 2. Spread Z-Score
            max_spread_z = cfg.get("gate_spread_max_z", 0.0) or 0.0
            curr_spread_z = indicators.get("spread_z", 0.0) or 0.0

            # 3. Book Staleness
            max_stale_ms = cfg.get("gate_book_stale_ms", 0) or 0
            curr_stale_ms = indicators.get("book_ts_gap_ms", indicators.get("liq_book_stale_ms", 0) or 0)

            veto = False
            rc = "OK"
            notes = {}

            if max_spread_bps > 0 and curr_spread_bps > max_spread_bps:
                veto = True
                rc = "VETO_SPREAD_BPS"
                notes = {"val": curr_spread_bps, "thr": max_spread_bps}
            elif max_spread_z > 0 and curr_spread_z > max_spread_z:
                veto = True
                rc = "VETO_SPREAD_Z"
                notes = {"val": curr_spread_z, "thr": max_spread_z}
            elif max_stale_ms > 0 and curr_stale_ms > max_stale_ms:
                veto = True
                rc = "VETO_BOOK_STALE"
                notes = {"val": curr_stale_ms, "thr": max_stale_ms}

            latency_us = int((time.monotonic() - t0) * 1_000_000)
            return GateDecisionV1(
                stage="integrity", gate=gate_name, decision="DENY" if veto else "ALLOW", reason_code=rc,
                severity="WARN" if veto else "INFO", profile="default", fail_policy="OPEN",
                ts_event_ms=ts_ev_ms, ts_decision_ms=ts_dec_ms, latency_us=latency_us,
                inputs_hash=inp_hash, notes=notes
            )
        except Exception as exc:
            log.exception("check_liquidity_integrity failed, fail-open: %s", exc)
            self._mark_dq(ctx, "liquidity_integrity_error", exc=exc)
            latency_us = int((time.monotonic() - t0) * 1_000_000)
            return GateDecisionV1(
                stage="integrity", gate=gate_name, decision="ALLOW", reason_code="FAIL_OPEN_LIQ",
                severity="CRITICAL", profile="default", fail_policy="OPEN", ts_event_ms=ts_ev_ms,
                ts_decision_ms=ts_dec_ms, latency_us=latency_us, inputs_hash=inp_hash, notes={"error": str(exc)}
            )

    def check_entry_policy(self, ctx: Any, kind: str, side: Any = "") -> GateDecisionV1:
        t0 = time.monotonic()
        ts_dec_ms = int(time.time() * 1000)
        ts_ev_ms = _get_ts_ms(ctx)
        sym = str(getattr(ctx, "symbol", "") or "").strip().upper()
        kind = (kind or "custom").strip() or "custom"
        inp_hash = _fast_hash(kind=kind, sym=sym)
        gate_name = "EntryPolicyGate"

        if self._entry_policy is None:
            latency_us = int((time.monotonic() - t0) * 1_000_000)
            return GateDecisionV1(
                stage="entry_policy", gate=gate_name, decision="ABSTAIN", reason_code="OK", severity="INFO",
                profile="default", fail_policy="OPEN", ts_event_ms=ts_ev_ms, ts_decision_ms=ts_dec_ms,
                latency_us=latency_us, inputs_hash=inp_hash, notes={"msg": "no_gate"}
            )
        try:
            res = self._entry_policy.evaluate(ctx=ctx, symbol=sym, kind=kind, side=side)
            veto = getattr(res, "veto", False)
            rc = str(getattr(res, "reason_code", getattr(res, "reason", "OK")))
            notes = {"msg": str(getattr(res, "notes", ""))}
            latency_us = int((time.monotonic() - t0) * 1_000_000)
            return GateDecisionV1(
                stage="entry_policy", gate=gate_name, decision="DENY" if veto else "ALLOW", reason_code=rc,
                severity="WARN" if veto else "INFO", profile="default", fail_policy="OPEN",
                ts_event_ms=ts_ev_ms, ts_decision_ms=ts_dec_ms, latency_us=latency_us,
                inputs_hash=inp_hash, notes=notes
            )
        except Exception as e:
            log.exception("check_entry_policy failed, fail-open: %s", e)
            latency_us = int((time.monotonic() - t0) * 1_000_000)
            return GateDecisionV1(
                stage="entry_policy", gate=gate_name, decision="ALLOW", reason_code="ERROR",
                severity="CRITICAL", profile="default", fail_policy="OPEN", ts_event_ms=ts_ev_ms,
                ts_decision_ms=ts_dec_ms, latency_us=latency_us, inputs_hash=inp_hash, notes={"error": str(e)}
            )

    def consistency_once(self, *, ctx: Any, symbol: str, kind: str, side: str) -> GateDecisionV1:
        key = (symbol, kind, side)
        
        def _compute() -> GateDecisionV1:
            t0 = time.monotonic()
            ts_dec_ms = int(time.time() * 1000)
            ts_ev_ms = _get_ts_ms(ctx)
            inp_hash = _fast_hash(kind=kind, side=side, sym=symbol)
            gate_name = "SignalConsistencyGate"

            fn = getattr(self._consistency_gate, "evaluate", None) if self._consistency_gate is not None else None
            if not callable(fn):
                latency_us = int((time.monotonic() - t0) * 1_000_000)
                return GateDecisionV1(
                    stage="consistency", gate=gate_name, decision="ABSTAIN", reason_code="OK", severity="INFO",
                    profile="default", fail_policy="OPEN", ts_event_ms=ts_ev_ms, ts_decision_ms=ts_dec_ms,
                    latency_us=latency_us, inputs_hash=inp_hash, notes={"msg": "no_gate"}
                )

            try:
                res = fn(ctx=ctx, symbol=symbol, kind=kind, side=side)
                veto = (getattr(res, "decision", "ALLOW") == "DENY")
                rc = getattr(res, "reason_code", getattr(res, "reason", "OK"))
                notes = {"msg": getattr(res, "notes", {})}
                latency_us = int((time.monotonic() - t0) * 1_000_000)
                return GateDecisionV1(
                    stage="consistency", gate=gate_name, decision="DENY" if veto else "ALLOW", reason_code=rc,
                    severity="WARN" if veto else "INFO", profile="default", fail_policy="OPEN",
                    ts_event_ms=ts_ev_ms, ts_decision_ms=ts_dec_ms, latency_us=latency_us,
                    inputs_hash=inp_hash, notes=notes
                )
            except Exception as exc:
                log.exception("consistency_once failed, fail-open: %s", exc)
                self._mark_dq(ctx, "consistency_error", exc=exc)
                _record_gate_error("consistency_once", "FAIL_OPEN_CONSISTENCY")
                latency_us = int((time.monotonic() - t0) * 1_000_000)
                return GateDecisionV1(
                    stage="consistency", gate=gate_name, decision="ALLOW", reason_code="FAIL_OPEN_CONSISTENCY",
                    severity="CRITICAL", profile="default", fail_policy="OPEN", ts_event_ms=ts_ev_ms,
                    ts_decision_ms=ts_dec_ms, latency_us=latency_us, inputs_hash=inp_hash, notes={"error": str(exc)}
                )

        return cached_on_ctx(ctx, slot="_cache_consistency_decision_v1", key=key, compute=_compute)

    def edge_cost_cached(self, *, ctx: Any, kind: str, symbol: str, side: Any, cfg: Any = None) -> GateDecisionV1:
        key = (kind, symbol, side)

        def _compute() -> GateDecisionV1:
            t0 = time.monotonic()
            ts_dec_ms = int(time.time() * 1000)
            ts_ev_ms = _get_ts_ms(ctx)
            inp_hash = _fast_hash(kind=kind, side=side, sym=symbol)
            gate_name = "EdgeCostGate"

            if self._cost_gate is None:
                latency_us = int((time.monotonic() - t0) * 1_000_000)
                return GateDecisionV1(
                    stage="edge_cost", gate=gate_name, decision="ABSTAIN", reason_code="OK", severity="INFO",
                    profile="default", fail_policy="OPEN", ts_event_ms=ts_ev_ms, ts_decision_ms=ts_dec_ms,
                    latency_us=latency_us, inputs_hash=inp_hash, notes={"msg": "no_gate"}
                )

            try:
                res = self._cost_gate.evaluate(ctx=ctx, kind=kind, symbol=symbol)
                # Fire-and-forget telemetry → stream:diag:edge_gate_events
                # → edge-gate-ingestor → Timescale edge_gate_events table.
                with contextlib.suppress(Exception):
                    _maybe_publish_edge_event_from_gate(
                        ctx, res, kind=kind, symbol=symbol, side=side
                    )
                veto = getattr(res, "veto", False)
                rc = getattr(res, "reason_code", getattr(res, "reason", "OK"))
                notes = {"msg": getattr(res, "notes", "")}
                latency_us = int((time.monotonic() - t0) * 1_000_000)
                return GateDecisionV1(
                    stage="edge_cost", gate=gate_name, decision="DENY" if veto else "ALLOW", reason_code=rc,
                    severity="WARN" if veto else "INFO", profile="default", fail_policy="OPEN",
                    ts_event_ms=ts_ev_ms, ts_decision_ms=ts_dec_ms, latency_us=latency_us,
                    inputs_hash=inp_hash, notes=notes
                )
            except Exception as exc:
                log.exception("edge_cost_cached failed, fail-open: %s", exc)
                self._mark_dq(ctx, "edge_cost_error", exc=exc)
                _record_gate_error("edge_cost_cached", "FAIL_OPEN_EDGE_COST")
                latency_us = int((time.monotonic() - t0) * 1_000_000)
                return GateDecisionV1(
                    stage="edge_cost", gate=gate_name, decision="ALLOW", reason_code="FAIL_OPEN_EDGE_COST",
                    severity="CRITICAL", profile="default", fail_policy="OPEN", ts_event_ms=ts_ev_ms,
                    ts_decision_ms=ts_dec_ms, latency_us=latency_us, inputs_hash=inp_hash, notes={"error": str(exc)}
                )

        return cached_on_ctx(ctx, slot="_cache_edge_cost_v1", key=key, compute=_compute)

    def _read_regime_from_redis(self, symbol: str) -> str:
        """Best-effort read of `regime:{SYMBOL}` from Redis with small TTL cache.

        Returns lowercase regime label or "" on miss / error. Fail-open semantics.
        """
        if not symbol:
            return ""
        sym = symbol.upper()
        now = time.monotonic()
        cached = self._regime_redis_cache.get(sym)
        if cached and (now - cached[0]) < self._regime_redis_cache_ttl_s:
            return cached[1]
        redis_client = getattr(self.portfolio_gate, "r", None) if self.portfolio_gate else None
        if redis_client is None:
            return ""
        try:
            raw = redis_client.get(f"regime:{sym}")
        except Exception:
            return ""
        if raw is None:
            self._regime_redis_cache[sym] = (now, "")
            return ""
        try:
            label = raw.decode("utf-8") if isinstance(raw, (bytes, bytearray)) else str(raw)
        except Exception:
            label = ""
        label = label.strip().lower()
        self._regime_redis_cache[sym] = (now, label)
        return label

    def check_regime_gate(self, ctx: Any, kind: str, side: Any = "") -> GateDecisionV1:
        t0 = time.monotonic()
        ts_dec_ms = int(time.time() * 1000)
        ts_ev_ms = _get_ts_ms(ctx)
        sym = str(getattr(ctx, "symbol", "") or "")
        regime = str(getattr(ctx, "market_regime", None) or getattr(ctx, "regime", None) or getattr(ctx, "regime_label", None) or "")
        # Normalize incoming side: accept "LONG"/"SHORT" strings or +1/-1 ints.
        side_str = ""
        if isinstance(side, str):
            side_str = side.strip().upper()
        elif isinstance(side, (int, float)):
            if side > 0:
                side_str = "LONG"
            elif side < 0:
                side_str = "SHORT"
        if side_str not in ("LONG", "SHORT"):
            side_str = ""

        # Redis fallback for regime label when context is empty (Phase D).
        if not regime and self._regime_redis_fallback:
            regime = self._read_regime_from_redis(sym)

        inp_hash = _fast_hash(kind=kind, regime=regime, sym=sym, side=side_str)
        gate_name = "StrictRegimeGate"

        dir_mode = self._regime_dir_mode  # off | shadow | enforce
        dir_active = dir_mode in {"shadow", "enforce"}

        # Fast-path ABSTAIN only when BOTH the legacy strict gate AND the new
        # direction gate are disabled — otherwise we must evaluate.
        if not self._regime_strict and not dir_active:
            latency_us = int((time.monotonic() - t0) * 1_000_000)
            return GateDecisionV1(
                stage="regime", gate=gate_name, decision="ABSTAIN", reason_code="OK", severity="INFO",
                profile="default", fail_policy="OPEN", ts_event_ms=ts_ev_ms, ts_decision_ms=ts_dec_ms,
                latency_us=latency_us, inputs_hash=inp_hash, notes={"msg": "not_strict"}
            )

        def _sl(v: Any) -> str:
            try:
                return str(v).lower() if v else ""
            except Exception:
                return ""

        k = _sl(kind)
        r = _sl(regime)

        veto = False
        rc = "OK"
        notes: dict[str, Any] = {"regime": r, "side": side_str}

        # Policy mode handling (legacy strict gate)
        mode = os.getenv("REGIME_POLICY_MODE", "SHADOW").upper()

        # Direction-vs-regime check uses its own mode override.
        dir_enforce = (dir_mode == "enforce")

        # Check staleness if redis is available (legacy strict gate only)
        redis_client = getattr(self.portfolio_gate, "r", None) if self.portfolio_gate else None
        if self._regime_strict and redis_client and sym:
            try:
                snap_json = redis_client.get(f"regime_snapshot:{sym}")
                if snap_json:
                    import json
                    snap = json.loads(snap_json.decode("utf-8") if isinstance(snap_json, bytes) else snap_json)
                    snap_ts = int(snap.get("ts_event_ms", 0))
                    max_stale_ms = int(os.getenv("REGIME_MAX_STALE_MS", "300000"))
                    if ts_ev_ms - snap_ts > max_stale_ms:
                        veto = True
                        rc = "VETO_REGIME_STALE"
                        notes["msg"] = f"stale regime snapshot: {ts_ev_ms - snap_ts}ms old"
            except Exception as e:
                log.warning("Failed to fetch regime snapshot for staleness check: %s", e)
                # Fail-open if we can't read the snapshot
                pass

        if not veto and self._regime_strict and r:
            if k == "breakout" and any(b in r for b in self._regime_breakout_block):
                veto = True
                rc = "VETO_REGIME_BREAKOUT_BLOCK"
            if k == "extreme" and any(b in r for b in self._regime_extreme_block):
                veto = True
                rc = "VETO_REGIME_EXTREME_BLOCK"

        # Direction-aware counter-trend block.
        # We block LONG when the active regime is in the bear set, and SHORT
        # when it is in the bull set. Skip when side/regime are unknown
        # (fail-open) so we don't double-veto missing data.
        dir_counter_veto = False
        dir_rc = "OK"
        if dir_active and side_str and r:
            if side_str == "LONG" and r in self._regime_bear_labels:
                dir_counter_veto = True
                dir_rc = "VETO_REGIME_COUNTER_TREND_LONG"
            elif side_str == "SHORT" and r in self._regime_bull_labels:
                dir_counter_veto = True
                dir_rc = "VETO_REGIME_COUNTER_TREND_SHORT"

        decision = "ALLOW"
        sev = "INFO"
        if veto:
            decision = "DENY" if mode == "ENFORCE" else "SHADOW_DENY"
            sev = "WARN"
        elif dir_counter_veto:
            rc = dir_rc
            decision = "DENY" if dir_enforce else "SHADOW_DENY"
            sev = "WARN"
            notes["dir_gate_mode"] = dir_mode

        latency_us = int((time.monotonic() - t0) * 1_000_000)
        return GateDecisionV1(
            stage="regime", gate=gate_name, decision=decision, reason_code=rc,
            severity=sev, profile="default", fail_policy="OPEN",
            ts_event_ms=ts_ev_ms, ts_decision_ms=ts_dec_ms, latency_us=latency_us,
            inputs_hash=inp_hash, notes=notes
        )
    
    def check_atr_floor(self, ctx: Any, kind: str) -> GateDecisionV1:
        t0 = time.monotonic()
        ts_dec_ms = int(time.time() * 1000)
        ts_ev_ms = _get_ts_ms(ctx)
        sym = str(getattr(ctx, "symbol", "") or "")
        inp_hash = _fast_hash(kind=kind, sym=sym)
        gate_name = "AtrFloorGate"

        if self._atr_floor_gate is None:
            latency_us = int((time.monotonic() - t0) * 1_000_000)
            return GateDecisionV1(
                stage="atr_floor", gate=gate_name, decision="ABSTAIN", reason_code="OK", severity="INFO",
                profile="default", fail_policy="OPEN", ts_event_ms=ts_ev_ms, ts_decision_ms=ts_dec_ms,
                latency_us=latency_us, inputs_hash=inp_hash, notes={"msg": "no_gate"}
            )

        try:
            # 1. Base ATR Floor (Tiers)
            res = self._atr_floor_gate.evaluate(ctx=ctx, symbol=sym, kind=kind)
            veto = (getattr(res, "decision", "ALLOW") == "DENY")
            rc = getattr(res, "reason_code", getattr(res, "reason", "OK"))
            notes = dict(getattr(res, "notes", {}))

            # 2. Fees-aware unified threshold — applied to ALL trail profiles
            # (was rocket_v1-only; protective_only/range setups were bypassing
            # "SL covers fees+spread+TP buffer" check entirely).
            indicators = getattr(ctx, "indicators", {})
            trail_profile = str(indicators.get("trail_profile", "") or "")
            is_rocket = trail_profile in ("rocket_v1", "expansion_v1")

            # Master + non-rocket shadow flags (rocket_v1 stays enforce-by-default).
            unified_enabled = (os.getenv("ATR_UNIFIED_GATE_ALL_PROFILES_ENABLED", "1") or "1") == "1"
            non_rocket_shadow = (os.getenv("ATR_UNIFIED_GATE_NON_ROCKET_SHADOW", "1") or "1") == "1"

            if unified_enabled:
                fees_bps_rt = float(os.getenv("FEES_BPS_RT", "10.0"))
                tp_bps_buffer = float(os.getenv("TP_BPS_BUFFER", "5.0"))
                tp1_share = 0.5  # Default

                cfg = getattr(ctx, "config", {})
                if cfg and "tp_ratio" in cfg:
                    from services.tp_config import parse_tp_ratio
                    tp_ratios = parse_tp_ratio(cfg["tp_ratio"])
                    if tp_ratios:
                        tp1_share = tp_ratios[0]

                # Per-profile TP1 ATR multiplier (= tp1_atr / atr).
                # rocket_v1/expansion_v1: TP1 = atr * rocket_mult.
                # else: TP1 = stop_dist * rr1 = atr * sl_atr_mult * rr1.
                if is_rocket:
                    tp1_atr_mult = float(os.getenv("ROCKET_TP1_ATR_MULT", "1.5") or 1.5)
                else:
                    # signal_pipeline floors sl_atr_mult at SL_ATR_MULT_FLOOR (default 0.78)
                    sl_atr_mult_floor = float(os.getenv("SL_ATR_MULT_FLOOR", "0.78") or 0.78)
                    rr1 = 1.3  # default first TP RR
                    if cfg and "tp_rr" in cfg:
                        try:
                            rr1_str = str(cfg["tp_rr"]).split(",")[0].strip()
                            if rr1_str:
                                rr1 = float(rr1_str)
                        except Exception:
                            pass
                    tp1_atr_mult = sl_atr_mult_floor * rr1

                fees_th, _ = fees_aware_min_atr_bps(
                    fees_bps_rt=fees_bps_rt,
                    tp_bps_buffer=tp_bps_buffer,
                    tp1_share=tp1_share,
                    rocket_mult=tp1_atr_mult,
                )

                floor_th = notes.get("thr", 0.0)
                unified_th = max(floor_th, fees_th)

                atr_bps = notes.get("atr", 0.0)
                if atr_bps == 0:
                    atr_bps = indicators.get("atr_bps_exec", 0.0)

                # Meme relaxation (×0.05) PLUS absolute bps floor — ensures SL still
                # covers roundtrip fees even after relax (Fix #3).
                is_meme = _ic.symbol_env_prefix(sym) in ("PEPE", "SHIB", "DOGE", "BONK", "FLOKI", "WIF")  # type: ignore
                meme_abs_floor_bps = float(os.getenv("ATR_UNIFIED_MEME_ABS_FLOOR_BPS", "20.0") or 20.0)
                meme_abs_shadow = (os.getenv("ATR_UNIFIED_MEME_ABS_FLOOR_SHADOW", "1") or "1") == "1"

                effective_th = unified_th
                if is_meme:
                    relaxed_th = unified_th * 0.05
                    effective_th = relaxed_th
                    if meme_abs_floor_bps > 0:
                        notes["meme_abs_floor_bps"] = meme_abs_floor_bps
                        notes["meme_relaxed_th"] = relaxed_th
                        if not meme_abs_shadow and meme_abs_floor_bps > effective_th:
                            effective_th = meme_abs_floor_bps
                            notes["meme_abs_floor_applied"] = 1
                        elif meme_abs_floor_bps > relaxed_th:
                            notes["meme_abs_floor_would_apply"] = 1

                notes["unified_th"] = unified_th
                notes["fees_th"] = fees_th
                notes["effective_th"] = effective_th
                notes["tp1_atr_mult_used"] = tp1_atr_mult
                notes["unified_gate_profile"] = "rocket" if is_rocket else "non_rocket"

                if effective_th > 0 and atr_bps < effective_th:
                    if is_rocket or not non_rocket_shadow:
                        veto = True
                        rc = "VETO_ATR_UNIFIED"
                    else:
                        notes["unified_th_would_veto"] = 1
                        notes["unified_th_shadow"] = 1
                elif is_meme and atr_bps < unified_th:
                    notes["relaxed_pass"] = True

            latency_us = int((time.monotonic() - t0) * 1_000_000)
            return GateDecisionV1(
                stage="atr_floor", gate=gate_name, decision="DENY" if veto else "ALLOW", reason_code=rc,
                severity="WARN" if veto else "INFO", profile="default", fail_policy="OPEN",
                ts_event_ms=ts_ev_ms, ts_decision_ms=ts_dec_ms, latency_us=latency_us,
                inputs_hash=inp_hash, notes=notes
            )
        except Exception as e:
            log.exception("check_atr_floor failed, fail-open: %s", e)
            latency_us = int((time.monotonic() - t0) * 1_000_000)
            return GateDecisionV1(
                stage="atr_floor", gate=gate_name, decision="ALLOW", reason_code="ERROR",
                severity="CRITICAL", profile="default", fail_policy="OPEN", ts_event_ms=ts_ev_ms,
                ts_decision_ms=ts_dec_ms, latency_us=latency_us, inputs_hash=inp_hash, notes={"error": str(e)}
            )

    def check_breadth(self, ctx: Any, kind: str, side: str) -> GateDecisionV1:
        t0 = time.monotonic()
        ts_dec_ms = int(time.time() * 1000)
        ts_ev_ms = _get_ts_ms(ctx)
        sym = str(getattr(ctx, "symbol", "") or "")
        inp_hash = _fast_hash(kind=kind, side=side, sym=sym)
        gate_name = "BreadthGate"

        if self._breadth_gate is None:
            latency_us = int((time.monotonic() - t0) * 1_000_000)
            return GateDecisionV1(
                stage="breadth", gate=gate_name, decision="ABSTAIN", reason_code="OK", severity="INFO",
                profile="default", fail_policy="OPEN", ts_event_ms=ts_ev_ms, ts_decision_ms=ts_dec_ms,
                latency_us=latency_us, inputs_hash=inp_hash, notes={"msg": "no_gate"}
            )

        try:
            res = self._breadth_gate.evaluate(ctx=ctx, symbol=sym, kind=kind, side=side)
            veto = getattr(res, "veto", False)
            rc = str(getattr(res, "reason_code", getattr(res, "reason", "OK")))
            notes = getattr(res, "notes", {})
            latency_us = int((time.monotonic() - t0) * 1_000_000)
            return GateDecisionV1(
                stage="breadth", gate=gate_name, decision="DENY" if veto else "ALLOW", reason_code=rc,
                severity="WARN" if veto else "INFO", profile="default", fail_policy="OPEN",
                ts_event_ms=ts_ev_ms, ts_decision_ms=ts_dec_ms, latency_us=latency_us,
                inputs_hash=inp_hash, notes=notes
            )
        except Exception as e:
            log.exception("check_breadth failed, fail-open: %s", e)
            latency_us = int((time.monotonic() - t0) * 1_000_000)
            return GateDecisionV1(
                stage="breadth", gate=gate_name, decision="ALLOW", reason_code="ERROR",
                severity="CRITICAL", profile="default", fail_policy="OPEN", ts_event_ms=ts_ev_ms,
                ts_decision_ms=ts_dec_ms, latency_us=latency_us, inputs_hash=inp_hash, notes={"error": str(e)}
            )

    def check_smt(self, ctx: Any, kind: str, side: Any) -> GateDecisionV1:
        t0 = time.monotonic()
        ts_dec_ms = int(time.time() * 1000)
        ts_ev_ms = _get_ts_ms(ctx)
        sym = str(getattr(ctx, "symbol", "") or "")
        inp_hash = _fast_hash(kind=kind, side=side, sym=sym)
        gate_name = "SmtCoherenceGate"

        if self._smt_gate is None:
            latency_us = int((time.monotonic() - t0) * 1_000_000)
            return GateDecisionV1(
                stage="smt", gate=gate_name, decision="ABSTAIN", reason_code="SMT_DISABLED", severity="INFO",
                profile="default", fail_policy="OPEN", ts_event_ms=ts_ev_ms, ts_decision_ms=ts_dec_ms,
                latency_us=latency_us, inputs_hash=inp_hash, notes={"msg": "no_gate"}
            )

        try:
            s = str(side).upper()
            if s in {"1", "BUY", "LONG"}:
                direction = "UP"
            elif s in {"-1", "SELL", "SHORT"}:
                direction = "DOWN"
            else:
                direction = "NA"
        except Exception:
            direction = "NA"

        if direction == "NA":
            latency_us = int((time.monotonic() - t0) * 1_000_000)
            return GateDecisionV1(
                stage="smt", gate=gate_name, decision="ALLOW", reason_code="OK", severity="INFO",
                profile="default", fail_policy="OPEN", ts_event_ms=ts_ev_ms, ts_decision_ms=ts_dec_ms,
                latency_us=latency_us, inputs_hash=inp_hash, notes={"msg": "VETO_SMT_NA_SIDE (shadow)"}
            )

        try:
            res = self._smt_gate.evaluate(ctx=ctx, redis_client=getattr(ctx, "redis", None), symbol=sym, kind=kind, side=direction)
            veto = (getattr(res, "decision", "ALLOW") == "DENY")
            rc = getattr(res, "reason_code", getattr(res, "reason", "OK"))
            notes = {"msg": getattr(res, "notes", {})}
            latency_us = int((time.monotonic() - t0) * 1_000_000)
            return GateDecisionV1(
                stage="smt", gate=gate_name, decision="DENY" if veto else "ALLOW", reason_code=rc,
                severity="WARN" if veto else "INFO", profile="default", fail_policy="OPEN",
                ts_event_ms=ts_ev_ms, ts_decision_ms=ts_dec_ms, latency_us=latency_us,
                inputs_hash=inp_hash, notes=notes
            )
        except Exception as exc:
            log.exception("check_smt failed, fail-open: %s", exc)
            self._mark_dq(ctx, "smt_error", exc=exc)
            _record_gate_error("check_smt", "smt_error")
            latency_us = int((time.monotonic() - t0) * 1_000_000)
            return GateDecisionV1(
                stage="smt", gate=gate_name, decision="ALLOW", reason_code="FAIL_OPEN_SMT",
                severity="CRITICAL", profile="default", fail_policy="OPEN", ts_event_ms=ts_ev_ms,
                ts_decision_ms=ts_dec_ms, latency_us=latency_us, inputs_hash=inp_hash, notes={"error": str(exc)}
            )

    def check_dq_integrity(self, ctx: Any, kind: str) -> GateDecisionV1:
        """
        Unified Hard DQ + Book Sanity + Stream Integrity gate.
        Executes at the absolute start of the pipeline.
        """
        t0 = time.monotonic()
        ts_dec_ms = int(time.time() * 1000)
        ts_ev_ms = _get_ts_ms(ctx)
        sym = str(getattr(ctx, "symbol", "") or "")
        inp_hash = _fast_hash(kind=kind, sym=sym)
        
        # 1. Hard Data Quality Gate
        if self._dq_gate is not None:
            try:
                dq_res = self._dq_gate.evaluate(ctx=ctx, symbol=sym, kind=kind)
                if (getattr(dq_res, "decision", "ALLOW") == "DENY"):
                    latency_us = int((time.monotonic() - t0) * 1_000_000)
                    return GateDecisionV1(
                        stage="dq_integrity", gate="HardDataQualityGate", decision="DENY",
                        reason_code=getattr(dq_res, "reason_code", "VETO_DQ"),
                        severity="CRITICAL", profile="hard", fail_policy="CLOSED",
                        ts_event_ms=ts_ev_ms, ts_decision_ms=ts_dec_ms,
                        latency_us=latency_us, inputs_hash=inp_hash,
                        notes={"msg": getattr(dq_res, "notes", {})}
                    )
            except Exception as e:
                log.warning("dq_gate failed: %s", e)
                self._mark_dq(ctx, "dq_gate_error")

        # 2. Book Sanity Gate
        if self._book_sanity_gate is not None:
            try:
                # indicators are often in ctx.indicators or ctx.of (for OrderflowContext)
                ind = getattr(ctx, "indicators", {}) or {}
                if not ind:
                    of = getattr(ctx, "of", None)
                    if of is not None:
                        ind = getattr(of, "indicators", {}) or {}
                
                bs_res = self._book_sanity_gate.evaluate(indicators=ind, symbol=sym)
                if (getattr(bs_res, "decision", "ALLOW") == "DENY"):
                    latency_us = int((time.monotonic() - t0) * 1_000_000)
                    return GateDecisionV1(
                        stage="dq_integrity", gate="BookSanityGate", decision="DENY",
                        reason_code=getattr(bs_res, "reason_code", "VETO_BOOK_SANITY"),
                        severity="CRITICAL", profile="hard", fail_policy="CLOSED",
                        ts_event_ms=ts_ev_ms, ts_decision_ms=ts_dec_ms,
                        latency_us=latency_us, inputs_hash=inp_hash,
                        notes={"flags": getattr(bs_res, "flags", []), "notes": getattr(bs_res, "notes", {})}
                    )
            except Exception as e:
                log.warning("book_sanity_gate failed: %s", e)
                self._mark_dq(ctx, "book_sanity_error")

        # 3. Stream Integrity Gate
        if self._stream_integrity_gate is not None:
            try:
                ind = getattr(ctx, "indicators", {}) or {}
                if not ind:
                    of = getattr(ctx, "of", None)
                    if of is not None:
                        ind = getattr(of, "indicators", {}) or {}
                        
                si_res = self._stream_integrity_gate.evaluate(indicators=ind, symbol=sym)
                if (getattr(si_res, "decision", "ALLOW") == "DENY"):
                    latency_us = int((time.monotonic() - t0) * 1_000_000)
                    return GateDecisionV1(
                        stage="dq_integrity", gate="StreamIntegrityGate", decision="DENY",
                        reason_code=getattr(si_res, "reason_code", "VETO_STREAM_INTEGRITY"),
                        severity="CRITICAL", profile="hard", fail_policy="CLOSED",
                        ts_event_ms=ts_ev_ms, ts_decision_ms=ts_dec_ms,
                        latency_us=latency_us, inputs_hash=inp_hash,
                        notes={"flags": getattr(si_res, "flags", []), "notes": getattr(si_res, "notes", {})}
                    )
            except Exception as e:
                log.warning("stream_integrity_gate failed: %s", e)
                self._mark_dq(ctx, "stream_integrity_error")

        latency_us = int((time.monotonic() - t0) * 1_000_000)
        return GateDecisionV1(
            stage="dq_integrity", gate="DQIntegrityOrchestrator", decision="ALLOW", reason_code="OK",
            severity="INFO", profile="default", fail_policy="OPEN", ts_event_ms=ts_ev_ms,
            ts_decision_ms=ts_dec_ms, latency_us=latency_us, inputs_hash=inp_hash, notes={}
        )

    async def check_derivatives_context(
        self,
        ctx: Any,
        kind: str,
        side: str,
        profile: str = "default",
        thr_funding_z: float = 3.0,
        thr_basis_bps: float = 10.0,
        require_oi_for_veto: bool = True,
        tighten_mult: float = 1.0,
        tighten_cap_bps: float = 8.0,
    ) -> GateDecisionV1:
        t0 = time.monotonic()
        ts_dec_ms = int(time.time() * 1000)
        ts_ev_ms = _get_ts_ms(ctx)
        sym = str(getattr(ctx, "symbol", "") or "")
        inp_hash = _fast_hash(kind=kind, side=side, sym=sym)
        gate_name = "DerivativesContextGate"

        try:
            redis = getattr(ctx, "redis", None)
            snap = await aread_derivatives_context(redis, symbol=sym)
            if snap is None:
                latency_us = int((time.monotonic() - t0) * 1_000_000)
                return GateDecisionV1(
                    stage="context", gate=gate_name, decision="ABSTAIN", reason_code="MISSING_DATA",
                    severity="INFO", profile=profile, fail_policy="OPEN", ts_event_ms=ts_ev_ms,
                    ts_decision_ms=ts_dec_ms, latency_us=latency_us, inputs_hash=inp_hash, notes={"msg": "no_snapshot"}
                )

            # Context enrichment (breadth + liq imbalance)
            breadth_ctx = await aread_breadth_context(redis)
            liq_ctx = await aread_liq_context(redis, symbol=sym)

            if breadth_ctx:
                snap = replace(
                    snap,
                    market_breadth_ret_24h=breadth_ctx.get("ret_24h", 0.0),
                    leader_btc_eth_confirm=breadth_ctx.get("leader_confirm", 0.0)
                )
            if liq_ctx:
                snap = replace(
                    snap,
                    liq_imbalance_z=liq_ctx.get("liq_imbalance_z", 0.0)
                )

            # Lazy-load calibrator state from Redis on first call per symbol.
            if not self._fb_loaded and redis is not None:
                await self._load_funding_basis_from_redis(redis)
                self._fb_loaded = True

            # Adaptive thresholds: detect regime tag, observe, get calibrated thresholds.
            _abs_fz = abs(snap.funding_rate_z or 0.0)
            _abs_bb = abs(snap.basis_bps or 0.0)
            _regime_tag = self._funding_basis_cal.observe(
                regime=sym.lower(),
                abs_funding_z=_abs_fz,
                abs_basis_bps=_abs_bb,
            )
            if _GATES_METRICS and _FUNDING_BASIS_REGIME_TOTAL is not None:
                with contextlib.suppress(Exception):
                    _FUNDING_BASIS_REGIME_TOTAL.labels(symbol=sym, regime_tag=_regime_tag).inc()

            _fb_th = self._funding_basis_cal.thresholds(
                regime=sym.lower(),
                current_regime_tag=_regime_tag,
                default_funding_z=thr_funding_z,
                default_basis_bps=thr_basis_bps,
            )

            # Periodic Redis snapshot.
            if redis is not None:
                await self._snapshot_funding_basis_to_redis(redis, ts_dec_ms)

            res = evaluate_derivatives_context_v2(
                profile=profile,
                side=side,
                funding_rate_z=snap.funding_rate_z,
                basis_bps=snap.basis_bps,
                oi_accel=snap.oi_accel,
                long_short_ratio_z=snap.long_short_ratio_z,
                taker_buy_sell_imbalance=snap.taker_buy_sell_imbalance,
                liq_imbalance_z=snap.liq_imbalance_z,
                market_breadth_ret_24h=snap.market_breadth_ret_24h,
                leader_btc_eth_confirm=snap.leader_btc_eth_confirm,
                thr_funding_z=_fb_th.funding_z,
                thr_basis_bps=_fb_th.basis_bps,
                require_oi_for_veto=require_oi_for_veto,
                tighten_mult=tighten_mult,
                tighten_cap_bps=tighten_cap_bps,
            )

            decision = "ALLOW"
            if res.veto:
                decision = "DENY"
            elif res.tighten_add_bps > 0.0:
                decision = "TIGHTEN"

            latency_us = int((time.monotonic() - t0) * 1_000_000)
            return GateDecisionV1(
                stage="context", gate=gate_name, decision=decision,
                reason_code=res.veto_reason or "OK",
                severity="WARN" if res.veto else "INFO",
                profile=profile, fail_policy="OPEN",
                ts_event_ms=ts_ev_ms, ts_decision_ms=ts_dec_ms,
                latency_us=latency_us, inputs_hash=inp_hash,
                notes={
                    "flags": res.flags,
                    "tighten_add_bps": res.tighten_add_bps,
                    "crowding_score": res.crowding_score,
                    "age_ms": max(0, ts_dec_ms - snap.ts_ms)
                }
            )
        except Exception as e:
            log.exception("check_derivatives_context failed: %s", e)
            _record_gate_error(gate_name, "error")
            latency_us = int((time.monotonic() - t0) * 1_000_000)
            return GateDecisionV1(
                stage="context", gate=gate_name, decision="ALLOW", reason_code="ERROR",
                severity="CRITICAL", profile=profile, fail_policy="OPEN",
                ts_event_ms=ts_ev_ms, ts_decision_ms=ts_dec_ms,
                latency_us=latency_us, inputs_hash=inp_hash, notes={"error": str(e)}
            )

    async def check_defillama_context(
        self,
        ctx: Any,
        kind: str,
        side: str,
        profile: str = "default",
        max_age_ms: int = 3600000,
        tighten_mult: float = 1.0,
        tighten_cap_bps: float = 8.0,
    ) -> GateDecisionV1:
        t0 = time.monotonic()
        ts_dec_ms = int(time.time() * 1000)
        ts_ev_ms = _get_ts_ms(ctx)
        sym = str(getattr(ctx, "symbol", "") or "")
        inp_hash = _fast_hash(kind=kind, side=side, sym=sym)
        gate_name = "DefiLlamaContextGate"

        try:
            redis = getattr(ctx, "redis", None)
            snap = await aread_defillama_context(redis, symbol=sym)
            if snap is None:
                latency_us = int((time.monotonic() - t0) * 1_000_000)
                return GateDecisionV1(
                    stage="macro", gate=gate_name, decision="ABSTAIN", reason_code="MISSING_DATA",
                    severity="INFO", profile=profile, fail_policy="OPEN", ts_event_ms=ts_ev_ms,
                    ts_decision_ms=ts_dec_ms, latency_us=latency_us, inputs_hash=inp_hash, notes={"msg": "no_snapshot"}
                )

            age_ms = max(0, ts_dec_ms - snap.ts_ms)
            if age_ms > max_age_ms:
                latency_us = int((time.monotonic() - t0) * 1_000_000)
                return GateDecisionV1(
                    stage="macro", gate=gate_name, decision="ABSTAIN", reason_code="STALE_DATA",
                    severity="INFO", profile=profile, fail_policy="OPEN", ts_event_ms=ts_ev_ms,
                    ts_decision_ms=ts_dec_ms, latency_us=latency_us, inputs_hash=inp_hash, notes={"age_ms": age_ms}
                )

            res = evaluate_defillama_context(
                profile=profile,
                side=side,
                stablecoin_mcap_delta_1d=snap.stablecoin_mcap_delta_1d,
                stablecoin_mcap_delta_7d=snap.stablecoin_mcap_delta_7d,
                btc_dominance_momentum=0.0,
                chain_tvl_delta_1d_pct=snap.chain_tvl_delta_1d_pct,
                dex_volume_spike_z=snap.dex_volume_spike_z,
                fees_revenue_momentum=snap.fees_revenue_momentum,
                tighten_mult=tighten_mult,
                tighten_cap_bps=tighten_cap_bps,
            )

            decision = "ALLOW"
            if res.veto:
                decision = "DENY"
            elif res.tighten_add_bps > 0.0:
                decision = "TIGHTEN"

            latency_us = int((time.monotonic() - t0) * 1_000_000)
            return GateDecisionV1(
                stage="macro", gate=gate_name, decision=decision,
                reason_code=res.veto_reason or "OK",
                severity="WARN" if res.veto else "INFO",
                profile=profile, fail_policy="OPEN",
                ts_event_ms=ts_ev_ms, ts_decision_ms=ts_dec_ms,
                latency_us=latency_us, inputs_hash=inp_hash,
                notes={
                    "flags": res.flags,
                    "tighten_add_bps": res.tighten_add_bps,
                    "risk_score": res.risk_score,
                    "age_ms": age_ms
                }
            )
        except Exception as e:
            log.exception("check_defillama_context failed: %s", e)
            _record_gate_error(gate_name, "error")
            latency_us = int((time.monotonic() - t0) * 1_000_000)
            return GateDecisionV1(
                stage="macro", gate=gate_name, decision="ALLOW", reason_code="ERROR",
                severity="CRITICAL", profile=profile, fail_policy="OPEN",
                ts_event_ms=ts_ev_ms, ts_decision_ms=ts_dec_ms,
                latency_us=latency_us, inputs_hash=inp_hash, notes={"error": str(e)}
            )

    async def check_sentiment_context(
        self,
        ctx: Any,
        kind: str,
        side: str,
        profile: str = "default",
        max_age_ms: int = 86400000,
        tighten_cap_bps: float = 8.0,
    ) -> GateDecisionV1:
        t0 = time.monotonic()
        ts_dec_ms = int(time.time() * 1000)
        ts_ev_ms = _get_ts_ms(ctx)
        sym = str(getattr(ctx, "symbol", "") or "")
        inp_hash = _fast_hash(kind=kind, side=side, sym=sym)
        gate_name = "SentimentContextGate"

        try:
            redis = getattr(ctx, "redis", None)
            sent = await aread_sentiment_context(redis)
            if sent is None:
                latency_us = int((time.monotonic() - t0) * 1_000_000)
                return GateDecisionV1(
                    stage="sentiment", gate=gate_name, decision="ABSTAIN", reason_code="MISSING_DATA",
                    severity="INFO", profile=profile, fail_policy="OPEN", ts_event_ms=ts_ev_ms,
                    ts_decision_ms=ts_dec_ms, latency_us=latency_us, inputs_hash=inp_hash, notes={"msg": "no_data"}
                )

            age_ms = max(0, ts_dec_ms - (sent.ts_ms or 0))
            if age_ms > max_age_ms or sent.quality_status != "OK":
                latency_us = int((time.monotonic() - t0) * 1_000_000)
                return GateDecisionV1(
                    stage="sentiment", gate=gate_name, decision="ABSTAIN", reason_code="STALE_DATA",
                    severity="INFO", profile=profile, fail_policy="OPEN", ts_event_ms=ts_ev_ms,
                    ts_decision_ms=ts_dec_ms, latency_us=latency_us, inputs_hash=inp_hash, notes={"age_ms": age_ms}
                )

            res = evaluate_sentiment_context(
                profile=profile,
                side=side,
                sentiment_regime=sent.sentiment_regime,
                fear_greed_value=sent.fear_greed_value,
                fear_greed_delta_1d=sent.fear_greed_delta_1d,
                fear_greed_delta_7d=sent.fear_greed_delta_7d,
                base_risk_multiplier=sent.sentiment_risk_multiplier,
                tighten_cap_bps=tighten_cap_bps,
            )

            decision = "ALLOW"
            if res.tighten_add_bps > 0.0:
                decision = "TIGHTEN"

            latency_us = int((time.monotonic() - t0) * 1_000_000)
            return GateDecisionV1(
                stage="sentiment", gate=gate_name, decision=decision,
                reason_code="OK", severity="INFO", profile=profile, fail_policy="OPEN",
                ts_event_ms=ts_ev_ms, ts_decision_ms=ts_dec_ms, latency_us=latency_us,
                inputs_hash=inp_hash,
                notes={
                    "flags": res.flags,
                    "tighten_add_bps": res.tighten_add_bps,
                    "risk_multiplier": res.risk_multiplier,
                    "regime": sent.sentiment_regime
                }
            )
        except Exception as e:
            log.exception("check_sentiment_context failed: %s", e)
            _record_gate_error(gate_name, "error")
            latency_us = int((time.monotonic() - t0) * 1_000_000)
            return GateDecisionV1(
                stage="sentiment", gate=gate_name, decision="ALLOW", reason_code="ERROR",
                severity="CRITICAL", profile=profile, fail_policy="OPEN",
                ts_event_ms=ts_ev_ms, ts_decision_ms=ts_dec_ms,
                latency_us=latency_us, inputs_hash=inp_hash, notes={"error": str(e)}
            )

    async def check_crossvenue_context(
        self,
        ctx: Any,
        kind: str,
        side: str,
        profile: str = "default",
        max_age_ms: int = 1000,
        min_agree: float = 0.6,
        max_dislocation_z: float = 2.5,
        max_mid_spread_bps: float = 15.0,
        max_stale_count: int = 2,
        tighten_mult: float = 1.0,
        tighten_cap_bps: float = 8.0,
    ) -> GateDecisionV1:
        t0 = time.monotonic()
        ts_dec_ms = int(time.time() * 1000)
        ts_ev_ms = _get_ts_ms(ctx)
        sym = str(getattr(ctx, "symbol", "") or "")
        inp_hash = _fast_hash(kind=kind, side=side, sym=sym)
        gate_name = "CrossVenueContextGate"

        try:
            redis = getattr(ctx, "redis", None)
            cv = await aread_crossvenue_context(redis, symbol=sym)
            if cv is None:
                latency_us = int((time.monotonic() - t0) * 1_000_000)
                return GateDecisionV1(
                    stage="crossvenue", gate=gate_name, decision="ABSTAIN", reason_code="MISSING_DATA",
                    severity="INFO", profile=profile, fail_policy="OPEN", ts_event_ms=ts_ev_ms,
                    ts_decision_ms=ts_dec_ms, latency_us=latency_us, inputs_hash=inp_hash, notes={"msg": "no_data"}
                )

            age_ms = max(0, ts_dec_ms - (cv.ts_ms or 0))
            if age_ms > max_age_ms:
                latency_us = int((time.monotonic() - t0) * 1_000_000)
                return GateDecisionV1(
                    stage="crossvenue", gate=gate_name, decision="ABSTAIN", reason_code="STALE_DATA",
                    severity="INFO", profile=profile, fail_policy="OPEN", ts_event_ms=ts_ev_ms,
                    ts_decision_ms=ts_dec_ms, latency_us=latency_us, inputs_hash=inp_hash, notes={"age_ms": age_ms}
                )

            res = evaluate_crossvenue_context(
                profile=profile,
                side=side,
                direction_agree=cv.cross_venue_direction_agree,
                trade_imbalance=cv.cross_venue_trade_imbalance,
                dislocation_z=cv.venue_dislocation_z,
                mid_spread_bps=cv.cross_venue_mid_spread_bps,
                stale_count=cv.venue_stale_count,
                min_agree=min_agree,
                max_dislocation_z=max_dislocation_z,
                max_mid_spread_bps=max_mid_spread_bps,
                max_stale_count=max_stale_count,
                tighten_mult=tighten_mult,
                tighten_cap_bps=tighten_cap_bps,
            )

            decision = "ALLOW"
            if res.veto:
                decision = "DENY"
            elif res.tighten_add_bps > 0.0:
                decision = "TIGHTEN"

            latency_us = int((time.monotonic() - t0) * 1_000_000)
            return GateDecisionV1(
                stage="crossvenue", gate=gate_name, decision=decision,
                reason_code=res.veto_reason or "OK",
                severity="WARN" if res.veto else "INFO",
                profile=profile, fail_policy="OPEN",
                ts_event_ms=ts_ev_ms, ts_decision_ms=ts_dec_ms,
                latency_us=latency_us, inputs_hash=inp_hash,
                notes={
                    "flags": res.flags,
                    "tighten_add_bps": res.tighten_add_bps,
                    "age_ms": age_ms
                }
            )
        except Exception as e:
            log.exception("check_crossvenue_context failed: %s", e)
            _record_gate_error(gate_name, "error")
            latency_us = int((time.monotonic() - t0) * 1_000_000)
            return GateDecisionV1(
                stage="crossvenue", gate=gate_name, decision="ALLOW", reason_code="ERROR",
                severity="CRITICAL", profile=profile, fail_policy="OPEN",
                ts_event_ms=ts_ev_ms, ts_decision_ms=ts_dec_ms,
                latency_us=latency_us, inputs_hash=inp_hash, notes={"error": str(e)}
            )

    def check_liquidity_geometry(
        self,
        ctx: Any,
        kind: str,
        profile: str = "default",
        thr_slope: float = 0.0,
        thr_dws: float = 0.0,
        thr_recovery_ms: int = 0,
        tighten_cap_bps: float = 5.0,
        tighten_mult: float = 1.0,
    ) -> GateDecisionV1:
        t0 = time.monotonic()
        ts_dec_ms = int(time.time() * 1000)
        ts_ev_ms = _get_ts_ms(ctx)
        sym = str(getattr(ctx, "symbol", "") or "")
        inp_hash = _fast_hash(kind=kind, sym=sym)
        gate_name = "LiquidityGeometryGate"

        try:
            ind = getattr(ctx, "indicators", {})
            slope_bid = ind.get("book_slope_bid", 0.0) or 0.0
            slope_ask = ind.get("book_slope_ask", 0.0) or 0.0
            dws_bps = ind.get("dws_bps", 0.0) or 0.0
            rec_ms = ind.get("liq_recovery_time_ms", 0) or 0

            res = evaluate_liq_geom(
                profile=profile,
                slope_bid=slope_bid,
                slope_ask=slope_ask,
                dws_bps=dws_bps,
                recovery_ms=rec_ms,
                thr_slope=thr_slope,
                thr_dws=thr_dws,
                thr_recovery_ms=thr_recovery_ms,
                tighten_cap_bps=tighten_cap_bps,
                tighten_mult=tighten_mult,
            )

            decision = "ALLOW"
            if res.veto:
                decision = "DENY"
            elif res.tighten_add_bps > 0.0:
                decision = "TIGHTEN"

            latency_us = int((time.monotonic() - t0) * 1_000_000)
            return GateDecisionV1(
                stage="liquidity", gate=gate_name, decision=decision,
                reason_code=res.veto_reason or "OK",
                severity="WARN" if res.veto else "INFO",
                profile=profile, fail_policy="OPEN",
                ts_event_ms=ts_ev_ms, ts_decision_ms=ts_dec_ms,
                latency_us=latency_us, inputs_hash=inp_hash,
                notes={
                    "flags": res.flags,
                    "tighten_add_bps": res.tighten_add_bps,
                    "slope_min": res.slope_min
                }
            )
        except Exception as e:
            log.exception("check_liquidity_geometry failed: %s", e)
            _record_gate_error(gate_name, "error")
            latency_us = int((time.monotonic() - t0) * 1_000_000)
            return GateDecisionV1(
                stage="liquidity", gate=gate_name, decision="ALLOW", reason_code="ERROR",
                severity="CRITICAL", profile=profile, fail_policy="OPEN",
                ts_event_ms=ts_ev_ms, ts_decision_ms=ts_dec_ms,
                latency_us=latency_us, inputs_hash=inp_hash, notes={"error": str(e)}
            )

    def check_flow_toxicity(
        self,
        ctx: Any,
        kind: str,
        profile: str = "default",
        thr_z: float = 0.0,
        thr_vpin: float = 0.0,
        thr_is: float = 0.0,
        thr_imp: float = 0.0,
        tighten_mult: float = 1.0,
        tighten_cap_bps: float = 10.0,
        veto_without_tca: bool = False,
    ) -> GateDecisionV1:
        t0 = time.monotonic()
        ts_dec_ms = int(time.time() * 1000)
        ts_ev_ms = _get_ts_ms(ctx)
        sym = str(getattr(ctx, "symbol", "") or "")
        inp_hash = _fast_hash(kind=kind, sym=sym)
        gate_name = "FlowToxicityGate"

        try:
            ind = getattr(ctx, "indicators", {})
            ofi_z = ind.get("ofi_norm_z", 0.0) or 0.0
            vpin_cdf = ind.get("vpin_cdf", 0.0) or 0.0
            tca_is = ind.get("tca_is_p95_bps", ind.get("is_p95_bps", 0.0) or 0.0)
            tca_imp = ind.get("tca_perm_impact_p95_bps", ind.get("perm_impact_p95_bps", 0.0) or 0.0)

            res = evaluate_flow_toxicity(
                profile=profile,
                ofi_norm_z=ofi_z,
                thr_ofi_norm_z=thr_z,
                vpin_cdf=vpin_cdf,
                thr_vpin_cdf=thr_vpin,
                tca_is_p95_bps=tca_is,
                tca_perm_impact_p95_bps=tca_imp,
                thr_is_p95_bps=thr_is,
                thr_perm_impact_p95_bps=thr_imp,
                tighten_mult=tighten_mult,
                tighten_cap_bps=tighten_cap_bps,
                veto_without_tca=veto_without_tca,
            )

            decision = "ALLOW"
            if res.veto:
                decision = "DENY"
            elif res.tighten_add_bps > 0.0:
                decision = "TIGHTEN"

            latency_us = int((time.monotonic() - t0) * 1_000_000)
            return GateDecisionV1(
                stage="flow", gate=gate_name, decision=decision,
                reason_code=res.veto_reason or "OK",
                severity="WARN" if res.veto else "INFO",
                profile=profile, fail_policy="OPEN",
                ts_event_ms=ts_ev_ms, ts_decision_ms=ts_dec_ms,
                latency_us=latency_us, inputs_hash=inp_hash,
                notes={
                    "flags": res.flags,
                    "tighten_add_bps": res.tighten_add_bps,
                }
            )
        except Exception as e:
            log.exception("check_flow_toxicity failed: %s", e)
            _record_gate_error(gate_name, "error")
            latency_us = int((time.monotonic() - t0) * 1_000_000)
            return GateDecisionV1(
                stage="flow", gate=gate_name, decision="ALLOW", reason_code="ERROR",
                severity="CRITICAL", profile=profile, fail_policy="OPEN",
                ts_event_ms=ts_ev_ms, ts_decision_ms=ts_dec_ms,
                latency_us=latency_us, inputs_hash=inp_hash, notes={"error": str(e)}
            )

    def check_manipulation_gate(
        self,
        ctx: Any,
        kind: str,
        profile: str = "default",
        thr_qs: float = 0.0,
        thr_lay: float = 0.0,
        thr_otr_z: float = 0.0,
        tighten_mult: float = 1.0,
        tighten_cap_bps: float = 10.0,
    ) -> GateDecisionV1:
        t0 = time.monotonic()
        ts_dec_ms = int(time.time() * 1000)
        ts_ev_ms = _get_ts_ms(ctx)
        sym = str(getattr(ctx, "symbol", "") or "")
        inp_hash = _fast_hash(kind=kind, sym=sym)
        gate_name = "ManipulationGate"

        try:
            import math

            ind = getattr(ctx, "indicators", {})
            qs_score = float(ind.get("quote_stuffing_score", 0.0) or 0.0)
            lay_score = float(ind.get("layering_score", 0.0) or 0.0)
            otr_z = float(ind.get("otr_z", 0.0) or 0.0)

            # P1 FIX: Validate bounded scores [0,1]; NaN → 0; negative → 0; > 1.0 → 1.0
            if math.isnan(qs_score) or qs_score < 0.0:
                qs_score = 0.0
            elif qs_score > 1.0:
                qs_score = 1.0

            if math.isnan(lay_score) or lay_score < 0.0:
                lay_score = 0.0
            elif lay_score > 1.0:
                lay_score = 1.0

            # otr_z is unbounded (z-score); just sanitize NaN
            if math.isnan(otr_z):
                otr_z = 0.0

            # Threshold comparisons
            hit_qs = thr_qs > 0.0 and qs_score >= thr_qs
            hit_lay = thr_lay > 0.0 and lay_score >= thr_lay
            hit_otr = thr_otr_z > 0.0 and otr_z >= thr_otr_z
            hit_any = hit_qs or hit_lay or hit_otr

            decision = "ALLOW"
            reason_code = "OK"
            tighten_add = 0.0
            flags = []
            if hit_qs: flags.append("quote_stuffing")
            if hit_lay: flags.append("layering")
            if hit_otr: flags.append("otr_spike")

            if hit_any:
                if profile in {"strict", "tighten", "hard", "veto"}:
                    # P2 FIX: Balanced scoring - equal weight to all three patterns
                    qs_lay_score = max(qs_score, lay_score)

                    if hit_otr:
                        # Normalize OTR z-score to [0,1] range
                        otr_score = min(1.0, max(0.0, (otr_z - thr_otr_z) / max(thr_otr_z, 1.0)))
                        # Weighted combination: 70% QS/LAY, 30% OTR
                        manip_score = 0.7 * qs_lay_score + 0.3 * otr_score
                    else:
                        manip_score = qs_lay_score

                    if manip_score > 0.0:
                        tighten_add = min(tighten_cap_bps, manip_score * tighten_mult * 3.0)
                        decision = "TIGHTEN"

                if profile in {"hard", "veto"}:
                    decision = "DENY"
                    reason_code = "VETO_QUOTE_STUFFING" if hit_qs else ("VETO_LAYERING" if hit_lay else "VETO_OTR_SPIKE")

            latency_us = int((time.monotonic() - t0) * 1_000_000)
            return GateDecisionV1(
                stage="manipulation", gate=gate_name, decision=decision,
                reason_code=reason_code,
                severity="WARN" if decision == "DENY" else "INFO",
                profile=profile, fail_policy="OPEN",
                ts_event_ms=ts_ev_ms, ts_decision_ms=ts_dec_ms,
                latency_us=latency_us, inputs_hash=inp_hash,
                notes={"flags": flags, "tighten_add_bps": tighten_add}
            )
        except Exception as e:
            log.exception("check_manipulation_gate failed: %s", e)
            _record_gate_error(gate_name, "error")
            latency_us = int((time.monotonic() - t0) * 1_000_000)
            return GateDecisionV1(
                stage="manipulation", gate=gate_name, decision="ALLOW", reason_code="ERROR",
                severity="CRITICAL", profile=profile, fail_policy="OPEN",
                ts_event_ms=ts_ev_ms, ts_decision_ms=ts_dec_ms,
                latency_us=latency_us, inputs_hash=inp_hash, notes={"error": str(e)}
            )

    async def check_exec_health_gate(
        self,
        ctx: Any,
        kind: str,
    ) -> GateDecisionV1:
        t0 = time.monotonic()
        ts_dec_ms = int(time.time() * 1000)
        ts_ev_ms = _get_ts_ms(ctx)
        sym = str(getattr(ctx, "symbol", "") or "")
        inp_hash = _fast_hash(kind=kind, sym=sym)
        gate_name = "ExecHealthGate"

        try:
            redis = getattr(ctx, "redis", None)
            # P6: hard consumer hook
            freeze_snap = await aread_exec_health_auto_freeze(redis=redis, scope="exec_health_gate")
            health_dec = build_exec_health_auto_freeze_decision(scope="exec_health_gate", state=freeze_snap)
            
            veto = health_dec.block
            rc = getattr(health_dec, "reason_code", "OK")

            latency_us = int((time.monotonic() - t0) * 1_000_000)
            return GateDecisionV1(
                stage="execution", gate=gate_name, decision="DENY" if veto else "ALLOW",
                reason_code=rc, severity="CRITICAL" if veto else "INFO",
                profile="hard", fail_policy="OPEN",
                ts_event_ms=ts_ev_ms, ts_decision_ms=ts_dec_ms,
                latency_us=latency_us, inputs_hash=inp_hash,
                notes={"msg": getattr(health_dec, "notes", "")}
            )
        except Exception as e:
            log.exception("check_exec_health_gate failed: %s", e)
            _record_gate_error(gate_name, "error")
            latency_us = int((time.monotonic() - t0) * 1_000_000)
            return GateDecisionV1(
                stage="execution", gate=gate_name, decision="ALLOW", reason_code="ERROR",
                severity="CRITICAL", profile="hard", fail_policy="OPEN",
                ts_event_ms=ts_ev_ms, ts_decision_ms=ts_dec_ms,
                latency_us=latency_us, inputs_hash=inp_hash, notes={"error": str(e)}
            )

    def check_confidence(
        self,
        ctx: Any,
        confidence: float,
        min_conf: float,
    ) -> GateDecisionV1:
        t0 = time.monotonic()
        ts_dec_ms = int(time.time() * 1000)
        ts_ev_ms = _get_ts_ms(ctx)
        sym = str(getattr(ctx, "symbol", "") or "")
        inp_hash = _fast_hash(conf=confidence, min_conf=min_conf, sym=sym)
        gate_name = "ConfidenceGate"

        veto = confidence < min_conf

        latency_us = int((time.monotonic() - t0) * 1_000_000)
        return GateDecisionV1(
            stage="confidence", gate=gate_name, decision="DENY" if veto else "ALLOW",
            reason_code="LOW_CONFIDENCE" if veto else "OK", severity="WARN" if veto else "INFO",
            profile="default", fail_policy="CLOSED",
            ts_event_ms=ts_ev_ms, ts_decision_ms=ts_dec_ms,
            latency_us=latency_us, inputs_hash=inp_hash,
            notes={"val": confidence, "thr": min_conf}
        )

    def check_squeeze_regime(
        self,
        ctx: Any,
        is_squeeze: bool,
    ) -> GateDecisionV1:
        t0 = time.monotonic()
        ts_dec_ms = int(time.time() * 1000)
        ts_ev_ms = _get_ts_ms(ctx)
        sym = str(getattr(ctx, "symbol", "") or "")
        inp_hash = _fast_hash(is_squeeze=is_squeeze, sym=sym)
        gate_name = "RegimeSqueezeGate"

        veto = is_squeeze
        
        latency_us = int((time.monotonic() - t0) * 1_000_000)
        return GateDecisionV1(
            stage="regime", gate=gate_name, decision="DENY" if veto else "ALLOW",
            reason_code="VETO_SQUEEZE" if veto else "OK", severity="WARN" if veto else "INFO",
            profile="hardcoded", fail_policy="CLOSED",
            ts_event_ms=ts_ev_ms, ts_decision_ms=ts_dec_ms,
            latency_us=latency_us, inputs_hash=inp_hash,
            notes={"msg": "Trading disabled in Squeeze regime" if veto else ""}
        )

    async def check_portfolio(
        self,
        ctx: Any,
        *,
        source: str,
        side: str,
        intent_notional: float,
        symbol: str = "",
        kind: str = "",
        profile: str = "default",
    ) -> GateDecisionV1:
        """Check portfolio exposure gate. Returns GateDecisionV1; fail-open if gate not configured."""
        sym = symbol or str(getattr(ctx, "symbol", "") or "")
        ts_ev_ms = _get_ts_ms(ctx)

        if self.portfolio_gate is None:
            return GateDecisionV1(
                stage="portfolio", gate="portfolio_exposure", decision="ALLOW",
                reason_code="PORTFOLIO_GATE_NOT_CONFIGURED", severity="INFO",
                profile=profile, fail_policy="OPEN",
                ts_event_ms=ts_ev_ms, ts_decision_ms=int(time.time() * 1000),
                latency_us=0, inputs_hash="", notes={}
            )

        dec = await self.portfolio_gate.evaluate(
            symbol=sym,
            source=source,
            side=side,
            intent_notional=intent_notional,
            ts_event_ms=ts_ev_ms,
        )
        _record_gate_decision(dec, symbol=sym, kind=kind, profile=profile)
        return dec  # type: ignore


CryptoSignalGates = GateOrchestrator
