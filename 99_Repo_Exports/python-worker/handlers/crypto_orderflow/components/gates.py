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

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Optional Prometheus metrics (fail-open if registry unavailable)
# ---------------------------------------------------------------------------
try:
    from prometheus_client import Counter, Histogram

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
    _GATES_METRICS = True
except Exception:  # pragma: no cover
    _GATES_METRICS = False
    _GATE_DECISIONS_TOTAL = None
    _GATE_SHADOW_DENY_TOTAL = None
    _GATE_TIGHTEN_TOTAL = None
    _GATE_LATENCY_US = None
    _GATES_EVAL_TOTAL = None

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

        self._regime_strict = (os.getenv("REGIME_GATE_STRICT", "0") or "0").strip().lower() in {"1", "true", "yes", "on"}
        def _csv(name: str) -> set[str]:
            v = (os.getenv(name, "") or "").strip().lower()
            return {x.strip() for x in v.split(",") if x.strip()}

        self._regime_breakout_block = _csv("REGIME_GATE_BREAKOUT_BLOCK")
        self._regime_extreme_block = _csv("REGIME_GATE_EXTREME_BLOCK")

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
            max_spread_bps = float(cfg.get("gate_spread_max_bps", 0.0) or 0.0)
            curr_spread_bps = float(indicators.get("liq_spread_bps", indicators.get("spread_bps", 0.0)) or 0.0)

            # 2. Spread Z-Score
            max_spread_z = float(cfg.get("gate_spread_max_z", 0.0) or 0.0)
            curr_spread_z = float(indicators.get("spread_z", 0.0) or 0.0)

            # 3. Book Staleness
            max_stale_ms = int(cfg.get("gate_book_stale_ms", 0) or 0)
            curr_stale_ms = int(indicators.get("book_ts_gap_ms", indicators.get("liq_book_stale_ms", 0)) or 0)

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

    def check_entry_policy(self, ctx: Any, payload: dict) -> GateDecisionV1:
        t0 = time.monotonic()
        ts_dec_ms = int(time.time() * 1000)
        ts_ev_ms = _get_ts_ms(ctx)
        sym = str(getattr(ctx, "symbol", "") or "").strip().upper()
        kind = payload.get("kind", "") or "custom"
        inp_hash = _fast_hash(kind=kind, payload_keys=list(payload.keys()), sym=sym)
        gate_name = "EntryPolicyGate"

        if self._entry_policy is None:
            latency_us = int((time.monotonic() - t0) * 1_000_000)
            return GateDecisionV1(
                stage="entry_policy", gate=gate_name, decision="ABSTAIN", reason_code="OK", severity="INFO",
                profile="default", fail_policy="OPEN", ts_event_ms=ts_ev_ms, ts_decision_ms=ts_dec_ms,
                latency_us=latency_us, inputs_hash=inp_hash, notes={"msg": "no_gate"}
            )
        try:
            res = self._entry_policy.evaluate(ctx=ctx, symbol=sym, kind=kind)
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

    def check_regime_gate(self, ctx: Any, kind: str) -> GateDecisionV1:
        t0 = time.monotonic()
        ts_dec_ms = int(time.time() * 1000)
        ts_ev_ms = _get_ts_ms(ctx)
        sym = str(getattr(ctx, "symbol", "") or "")
        regime = str(getattr(ctx, "market_regime", None) or getattr(ctx, "regime", None) or "")
        inp_hash = _fast_hash(kind=kind, regime=regime, sym=sym)
        gate_name = "StrictRegimeGate"

        if not self._regime_strict:
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

        if r:
            if k == "breakout" and any(b in r for b in self._regime_breakout_block):
                veto = True
                rc = "VETO_REGIME_BREAKOUT_BLOCK"
            if k == "extreme" and any(b in r for b in self._regime_extreme_block):
                veto = True
                rc = "VETO_REGIME_EXTREME_BLOCK"

        latency_us = int((time.monotonic() - t0) * 1_000_000)
        return GateDecisionV1(
            stage="regime", gate=gate_name, decision="DENY" if veto else "ALLOW", reason_code=rc,
            severity="WARN" if veto else "INFO", profile="default", fail_policy="OPEN",
            ts_event_ms=ts_ev_ms, ts_decision_ms=ts_dec_ms, latency_us=latency_us,
            inputs_hash=inp_hash, notes={"regime": r}
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
            
            # 2. Fees-aware Logic (if rocket_v1)
            indicators = getattr(ctx, "indicators", {})
            trail_profile = indicators.get("trail_profile", "")
            if trail_profile == "rocket_v1":
                fees_bps_rt = float(os.getenv("FEES_BPS_RT", "10.0"))
                tp_bps_buffer = float(os.getenv("TP_BPS_BUFFER", "5.0"))
                tp1_share = 0.5 # Default
                
                # Try to get tp1_share from config if possible
                cfg = getattr(ctx, "config", {})
                if cfg and "tp_ratio" in cfg:
                    from services.tp_config import parse_tp_ratio
                    tp_ratios = parse_tp_ratio(cfg["tp_ratio"])
                    if tp_ratios:
                        tp1_share = tp_ratios[0]
                
                rocket_mult = 1.5 # Default
                # ... (could resolve rocket_mult from sym_specs if needed)

                fees_th, _ = fees_aware_min_atr_bps(
                    fees_bps_rt=fees_bps_rt,
                    tp_bps_buffer=tp_bps_buffer,
                    tp1_share=tp1_share,
                    rocket_mult=rocket_mult,
                )
                
                floor_th = notes.get("thr", 0.0)
                unified_th = max(floor_th, fees_th)
                
                atr_bps = notes.get("atr", 0.0)
                if atr_bps == 0:
                     atr_bps = indicators.get("atr_bps_exec", 0.0)

                # Meme Relaxation
                is_meme = _ic.symbol_env_prefix(sym) in ("PEPE", "SHIB", "DOGE", "BONK", "FLOKI", "WIF")  # type: ignore
                effective_th = unified_th
                if is_meme:
                    effective_th *= 0.05
                
                if effective_th > 0 and atr_bps < effective_th:
                    veto = True
                    rc = "VETO_ATR_UNIFIED"
                    notes["unified_th"] = unified_th
                    notes["effective_th"] = effective_th
                    notes["fees_th"] = fees_th
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
                thr_funding_z=thr_funding_z,
                thr_basis_bps=thr_basis_bps,
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
            slope_bid = float(ind.get("book_slope_bid", 0.0) or 0.0)
            slope_ask = float(ind.get("book_slope_ask", 0.0) or 0.0)
            dws_bps = float(ind.get("dws_bps", 0.0) or 0.0)
            rec_ms = int(ind.get("liq_recovery_time_ms", 0) or 0)

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
            ofi_z = float(ind.get("ofi_norm_z", 0.0) or 0.0)
            vpin_cdf = float(ind.get("vpin_cdf", 0.0) or 0.0)
            tca_is = float(ind.get("tca_is_p95_bps", ind.get("is_p95_bps", 0.0)) or 0.0)
            tca_imp = float(ind.get("tca_perm_impact_p95_bps", ind.get("perm_impact_p95_bps", 0.0)) or 0.0)

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
            ind = getattr(ctx, "indicators", {})
            qs_score = float(ind.get("quote_stuffing_score", 0.0) or 0.0)
            lay_score = float(ind.get("layering_score", 0.0) or 0.0)
            otr_z = float(ind.get("otr_z", 0.0) or 0.0)
            
            # Simple policy (matching signal_pipeline inline logic)
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
                    manip_score = max(qs_score, lay_score)
                    if manip_score <= 0.0 and hit_otr:
                        manip_score = min(1.0, max(0.1, (otr_z - thr_otr_z) / max(thr_otr_z, 1.0)))
                    tighten_add = min(tighten_cap_bps, manip_score * tighten_mult * 3.0)
                    if tighten_add > 0:
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
            profile="default", fail_policy="OPEN",
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
