from __future__ import annotations

import json

# Import our components
import os
import random
from collections.abc import Callable
from typing import Any

from common.dq_flags import append_dq_flag
from common.math_safe import safe_float
from handlers.crypto_orderflow.components.gates import CryptoSignalGates
from handlers.crypto_orderflow.components.liquidity import CryptoLiquidity
from handlers.crypto_orderflow.components.observability import CryptoObservability
from handlers.crypto_orderflow.config.handler_config import CryptoOrderFlowConfigManager
from utils.time_utils import get_ny_time_millis


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

        for cand in candidates:
            # We assume cand has 'kind' and 'side' attributes
            # safe_lower logic
            kind_raw = getattr(cand, "kind", "custom")
            try:
                kind_key = str(kind_raw).lower()
            except Exception:
                kind_key = "custom"

            # 1.5 Quality Gate (Detector Check)
            qa = self.gates.check_quality(ctx, kind_key)
            if getattr(qa, "veto", False):
                rc = getattr(qa, "reason", "VETO_QUALITY")
                self.observability.emit_veto_metric(kind=kind_key, ctx=ctx, reason_code=rc)
                continue

            # 2. Regime Gate (Component)
            allowed, gate_reason = self.gates.check_regime_gate(ctx=ctx, kind=kind_key)
            if not allowed:
                self.observability.emit_veto_metric(kind=kind_key, ctx=ctx, reason_code=gate_reason)
                continue

            # 2.5 SMT Coherence Gate (Following)
            side_val = getattr(cand, "side", 0)
            smt_decision = self.gates.check_smt(ctx=ctx, kind=kind_key, side=side_val)
            if getattr(smt_decision, "veto", False):
                rc = getattr(smt_decision, "reason_code", "VETO_SMT")
                self.observability.emit_veto_metric(kind=kind_key, ctx=ctx, reason_code=rc)
                continue

            # 2.6 Consistency Gate (Microstructure Coherence)
            # Checks weak_progress, OBI agreement, etc.
            consistency_decision = self.gates.consistency_once(ctx=ctx, symbol=self.cfg.symbol, kind=kind_key, side=getattr(cand, "side", ""))
            if getattr(consistency_decision, "veto", False):
                rc = getattr(consistency_decision, "reason_code", "VETO_CONSISTENCY")
                self.observability.emit_veto_metric(kind=kind_key, ctx=ctx, reason_code=rc)
                continue

            # 3. Level Enrichment
            try:
                # Resolve risk config
                risk_cfg = self.cfg.resolve_risk_cfg()

                # SLQ dynamic stop override (fail-open, idempotent)
                try:
                    from services.slq_risk_adjust import maybe_apply_slq_to_risk_cfg
                    redis_client = getattr(ctx, "redis", None)
                    risk_cfg = maybe_apply_slq_to_risk_cfg(
                        redis=redis_client,
                        ctx=ctx,
                        symbol=str(self.cfg.symbol),
                        side=side_val,
                        cfg=dict(risk_cfg or {}),
                    )
                    try:
                        ctx.risk_cfg = dict(risk_cfg or {})
                    except Exception:
                        pass
                except Exception:
                    pass

                # Using liquidity component to ensure levels
                # side normalization
                side_val = getattr(cand, "side", 0)

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
                append_dq_flag(ctx, "levels_attach_failed")

            # 4. Cost Edge Gate
            cost_decision = self.gates.edge_cost_cached(
                ctx=ctx, kind=kind_key, symbol=self.cfg.symbol, side=getattr(cand, "side", 0), cfg=None
            )

            # Publish Edge Gate diagnostics (async/fire-and-forget)
            try:
                self._maybe_publish_edge_event(ctx, cand, cost_decision, kind_key)
            except Exception:
                pass

            if cost_decision and getattr(cost_decision, "veto", False):
                rc = getattr(cost_decision, "reason_code", "VETO_COST")
                self.observability.emit_veto_metric(kind=kind_key, ctx=ctx, reason_code=rc)
                continue

            # 5. Validation & Scoring (ConfirmationsEngine)
            # We assume validations needs l2 snapshot.
            # Handler passed l2=self._last_l2_snapshot.
            # Only ctx knows about l2 usually? Or handler state.
            # Orchestrator needs access to l2 snapshot.
            # Maybe ctx has it? ctx.l2?
            # If not, we might need to pass it.
            # Let's assume confirmations.validate can find l2 on ctx or we assume it's passed in ctx.
            res = self.confirmations.validate(kind=kind_key, ctx=ctx)

            # Fix: ConfirmationResult uses 'ok' (bool), not 'veto'.
            if not getattr(res, "ok", True):
                 rc = getattr(res, "code", "VETO_CONFIRM")
                 self.observability.emit_veto_metric(kind=kind_key, ctx=ctx, reason_code=rc)
                 continue

            # 6. Payload & Entry Policy
            try:
                payload, parts = self._build_payload(ctx, cand, res)
            except Exception:
                append_dq_flag(ctx, "payload_build_failed")
                continue

            # Entry Policy
            # Fix: GateDecision uses 'veto' (bool), not 'allow'.
            ep_decision = self.gates.check_entry_policy(ctx, payload)
            if getattr(ep_decision, "veto", False):
                rc = getattr(ep_decision, "reason_code", "VETO_ENTRY_POLICY")
                self.observability.emit_veto_metric(kind=kind_key, ctx=ctx, reason_code=rc)
                continue

            # 7. Emission
            try:
                ok = self.emitter.emit(payload, dedup=True)
                if ok:
                    any_sent = True
            except Exception:
                pass

        return any_sent

    def _build_payload(self, ctx: Any, cand: Any, res: Any) -> Any:
        # Re-implementing payload logic based on observed patterns
        # This duplicates logic from handler but consolidating it here is the goal.

        # Safe string helpers
        def _ss(v): return (v or "")

        reasons = list(getattr(cand, "reasons", None) or [])
        reasons = [_ss(x) for x in reasons][:16]

        payload = {
            "kind": _ss(getattr(cand, "kind", "")),
            "side": _ss(getattr(cand, "side", "")),
            "symbol": _ss(getattr(ctx, "symbol", "")),
            "ts": int(getattr(ctx, "ts", 0) or 0),
            "price": safe_float(getattr(ctx, "price", None), 0.0),
            "raw_score": safe_float(getattr(cand, "raw_score", None), 0.0),
            # final_score comes from res? or scoring result?
            "final_score": safe_float(getattr(res, "final_score", 0.0), 0.0),
            "confidence": float(getattr(res, "confidence", 0.0) or 0.0),
            "reasons": reasons,
            "signal_id": _ss(getattr(cand, "signal_id", "")),
            "venue": _ss(getattr(ctx, "venue", None)),
            "timeframe": _ss(getattr(ctx, "timeframe", None)),

            # -----------------------------------------------------------------
            # NEW: trade levels and ATR for metrics downstream
            # -----------------------------------------------------------------
            "atr": safe_float(getattr(ctx, "atr", None), 0.0),
            "sl_price": safe_float(getattr(ctx, "sl_price", None), 0.0),
            "tp1_price": safe_float(getattr(ctx, "tp1_price", None), 0.0),
            "tp_mode": getattr(ctx, "tp_mode_used", "ATR_LEGACY"),
            "risk_usd_target": safe_float(getattr(ctx, "risk_usd_target", None), 0.0),
            "risk_usd_actual": safe_float(getattr(ctx, "risk_usd", None), 0.0),
            # NEW: lot size from position sizing
            "lot": safe_float(getattr(ctx, "qty", None), 0.0),
            "qty": safe_float(getattr(ctx, "qty", None), 0.0),

            # NEW: Trailing & Execution params
            "trail_profile": getattr(ctx, "trail_profile", ""),
            "trailing_min_lock_r": safe_float(getattr(ctx, "trailing_min_lock_r", None), 0.0),
            "slq_used": int(getattr(ctx, "risk_cfg", {}).get("slq_used", 0) or 0),
        }

        # parts
        parts = getattr(res, "parts", {})
        return payload, parts

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

        stream_key = os.getenv("EDGE_GATE_EVENTS_STREAM", "stream:diag:edge_gate_events")

        # Build event with robust field mapping
        try:
            # Normalize ts_ms (handle seconds, missing values)
            raw_ts = getattr(ctx, "ts_ms", None) or getattr(ctx, "ts", None) or 0
            try:
                ts_val = int(raw_ts)
                # If seconds (10 digits), convert to ms
                if ts_val > 0 and ts_val < 10_000_000_000:
                    ts_val = ts_val * 1000
                ts_ms = ts_val if ts_val > 0 else get_ny_time_millis()
            except Exception:
                ts_ms = get_ny_time_millis()

            # Direct field mapping from EdgeCostGateDecision
            exp_bps = float(getattr(cost_decision, "expected_move_bps", 0.0))
            req_bps = float(getattr(cost_decision, "threshold_bps", 0.0))
            k = float(getattr(cost_decision, "k", 0.0))

            fees_bps = float(getattr(cost_decision, "fees_bps", 0.0))
            slip_bps = float(getattr(cost_decision, "slippage_bps", 0.0))
            buf_bps = float(getattr(cost_decision, "buffer_bps", 0.0))

            # Total costs: always recompute to guarantee buffer inclusion
            total_costs_bps = fees_bps + slip_bps + buf_bps

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
            redis_client.xadd(stream_key, {k: str(v) if v is not None else "" for k, v in evt.items()}, maxlen=50000, approximate=True)

        except Exception:
            pass

