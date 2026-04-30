from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import logging
import os
import time
import json
import math
from typing import Any, Dict, List, Optional, Tuple, Sequence
from types import SimpleNamespace

from services.orderflow.runtime import SymbolRuntime
from services.async_signal_publisher import AsyncSignalPublisher, StreamSink
from services.tp_config import parse_tp_ratio
from core.signal_payload import SignalPayload, StrongGateDecision
from core.of_confirm_engine import OFConfirmEngine

# Imports for publishing logic
from services.pnl_math import calculate_position_size
from services.signal_preprocess import preprocess_signal_for_publish
from core.crypto_signal_formatter import CryptoSignal, CryptoSignalFormatter
from services.outbox.envelope_builder import build_outbox_envelope, dumps_env, build_trace_sidecar_meta_from_ctx
from services.outbox.atomic_outbox import atomic_xadd_async
from common.decision_trace import ensure_trace, trace_gate, trace_enabled

# Metrics
from services.orderflow.metrics import (
    signals_total, strong_gate_veto_total, pre_publish_veto_total, of_session_outcome_total
    , liq_geom_monitor_hit_total, liq_geom_tighten_total, liq_geom_veto_total
    , liq_geom_dws_bps, liq_geom_book_slope_min_usd_per_bps, liq_geom_recovery_time_ms
    , flow_toxic_monitor_hit_total, flow_toxic_tighten_total, flow_toxic_veto_total
    , flow_toxic_ofi_norm_z, flow_toxic_vpin_cdf
    , manip_gate_events_total  # Phase E / P4
)
from services.orderflow.utils import session_utc
from handlers.crypto_orderflow.utils.log_sampler import LogSamplerFactory, sampled_info
from handlers.crypto_orderflow.utils.pre_publish_gates import HardDataQualityGate, RegimeSessionGate

# P5: book sanity + stream integrity gates (pre-publish, fail-open)
from services.orderflow.book_sanity_gate import BookSanityGate
from services.orderflow.stream_integrity_gate import StreamIntegrityGate

_book_sanity_gate = BookSanityGate.from_env()
_stream_integrity_gate = StreamIntegrityGate.from_env()

logger = logging.getLogger("of_signal_pipeline")

class SignalPipeline:
    def __init__(self, publisher: AsyncSignalPublisher, atr_cache: Any):
        """
        :param publisher: AsyncSignalPublisher instance for broadcasting messages.
        :param atr_cache: ATRCache instance (typically shared/global) for level calc.
        """
        self.publisher = publisher
        self.atr_cache = atr_cache
        # OFConfirm engine for validation simulation
        self.of_engine = OFConfirmEngine()
        # Tickers & Streams
        self.cryptoorderflow_signal_stream_template = os.getenv("CRYPTO_ORDERFLOW_SIGNAL_STREAM", "signals:cryptoorderflow:{symbol}")
        self.raw_signal_stream = os.getenv("CRYPTO_RAW_SIGNAL_STREAM", "signals:crypto:raw")
        self.notify_stream = os.getenv("CRYPTO_NOTIFY_STREAM", "notify:telegram")
        self.notify_maxlen = int(os.getenv("CRYPTO_NOTIFY_MAXLEN", "20000"))

        # Confidence score telemetry stream (high-frequency; keep off by default)
        self.conf_scores_publish_enabled = bool(int(os.getenv("CONF_SCORES_PUBLISH_ENABLED", "0") or 0))
        self.conf_scores_stream = os.getenv("CONF_SCORES_STREAM", "signals:confidence:scores")
        self.conf_scores_stream_maxlen = int(os.getenv("CONF_SCORES_STREAM_MAXLEN", "200000") or 200000)
        self.conf_scores_schema_version = int(os.getenv("CONF_SCORES_SCHEMA_VERSION", "1") or 1)
        self.conf_scores_include_evidence_json = bool(int(os.getenv("CONF_SCORES_INCLUDE_EVIDENCE_JSON", "0") or 0))
        self.conf_scores_quarantine_stream = os.getenv("CONF_EVIDENCE_QUARANTINE_STREAM", "signals:confidence:quarantine")
        self.conf_scores_quarantine_maxlen = int(os.getenv("CONF_EVIDENCE_QUARANTINE_MAXLEN", "20000") or 20000)
        
        # Initialize log samplers for signal messages (every 10000th message)
        LogSamplerFactory.get_sampler("SIGNAL_RAW_STREAM", 10000)
        LogSamplerFactory.get_sampler("SIGNAL_PUBLISHED", 10000)

        # Pre-publish gates (fail-open unless enabled by ENV)
        # of_inputs stream: controls for RAM pressure
        self.of_inputs_stream = os.getenv("OF_INPUTS_STREAM", "signals:of:inputs")
        self.of_inputs_publish_enabled = bool(int(os.getenv("OF_INPUTS_PUBLISH_ENABLED", "1") or 1))
        # P1 fix: 100000 → 5000 (при 40KB/entry: 5k * 40KB = 200MB max)
        self.of_inputs_stream_maxlen = int(os.getenv("OF_INPUTS_STREAM_MAXLEN", "5000") or 5000)
        self._hard_dq_gate = HardDataQualityGate.from_env()
        self._rs_gate = RegimeSessionGate.from_env()
        self._rejected_signal_stream = os.getenv("CRYPTO_REJECTED_SIGNAL_STREAM", "signals:crypto:rejected")

        # ------------------------------------------------------------------
        # Decision Snapshot (A2)
        # ------------------------------------------------------------------
        # Purpose: store a compact, joinable decision snapshot for post-trade TCA.
        # - This is NOT a trading signal stream. It is a warm-path input.
        # - Must be fail-open: never block main signal publishing.
        #
        # Contract: Redis Stream `events:decision_snapshot` with field `payload` (JSON).
        # Keys must include: sid, symbol, venue, decision_ts_ms, decision_mid (+ bid/ask).
        self.decision_snapshot_publish_enabled = bool(int(os.getenv("DECISION_SNAPSHOT_PUBLISH_ENABLED", "1") or 1))
        self.decision_snapshot_stream = os.getenv("DECISION_SNAPSHOT_STREAM", "events:decision_snapshot")
        self.decision_snapshot_stream_maxlen = int(os.getenv("DECISION_SNAPSHOT_STREAM_MAXLEN", "200000") or 200000)
        self.decision_snapshot_schema_version = int(os.getenv("DECISION_SNAPSHOT_SCHEMA_VERSION", "1") or 1)

        # ------------------------------------------------------------------
        # Phase C (P2): Liquidity geometry telemetry (optional)
        # ------------------------------------------------------------------
        # To avoid high cardinality, we only emit per-symbol metrics for a small
        # allowlist. All other symbols are aggregated into symbol="__all__".
        raw_syms = str(os.getenv("LIQ_GEOM_METRICS_SYMBOLS", "") or "").strip()
        self._liq_geom_syms_allow = {s.strip().upper() for s in raw_syms.split(",") if s.strip()} if raw_syms else set()

        raw_syms2 = str(os.getenv("FLOW_TOX_METRICS_SYMBOLS", os.getenv("LIQ_GEOM_METRICS_SYMBOLS", "")) or "").strip()
        self._flow_tox_syms_allow = {s.strip().upper() for s in raw_syms2.split(",") if s.strip()} if raw_syms2 else set()

        # TB Labeler Feed (P45 fix): explicitly feed signals:of:inputs
        self.publish_of_inputs = os.getenv("PUBLISH_OF_INPUTS", "1").lower() in {"1", "true", "yes", "on"}
        self.of_inputs_stream = os.getenv("OF_INPUTS_STREAM", "signals:of:inputs")

    @property
    def FEES_BPS_RT(self) -> float:
        return float(os.getenv("FEES_BPS_RT", "10"))     # 10 bps RT (0.05% per side)
    
    @property
    def TP_BPS_BUFFER(self) -> float:
        return float(os.getenv("TP_BPS_BUFFER", "4")) # 4 bps buffer (tightened)

    def _conf_scores_enabled(self) -> bool:
        return bool(self.conf_scores_publish_enabled)

    def _safe_num(self, v: object) -> float | None:
        try:
            f = float(v)  # type: ignore[arg-type]
        except Exception:
            return None
        if not math.isfinite(f):
            return None
        return float(f)

    def _build_conf_evidence_map(self, *, confirmations: list, indicators: dict) -> dict[str, float]:
        """Best-effort numeric evidence_map for `signals:confidence:scores` contract.

        Rules:
        - only numeric values (floats)
        - boolean flags -> 0.0/1.0
        - alias mapping for legacy keys
        """
        out: dict[str, float] = {}

        # Parse confirmations list like ["rsi_agree=1", "div_strength=0.7", ...]
        for item in confirmations or []:
            try:
                s = str(item)
                if "=" not in s:
                    continue
                k, v = s.split("=", 1)
                k = k.strip()
                v = v.strip()
                if not k:
                    continue
                fv = self._safe_num(v)
                if fv is None:
                    continue
                out[k] = float(fv)
            except Exception:
                continue

        # Allow-list a few high-value numeric evidence keys from indicators
        allow = {
            "rsi_agree"
            "div_match"
            "div_strength"
            "sweep"
            "sweep_eqh"
            "sweep_eql"
            "iceberg_strict"
            "ice_strict"
            "reclaim"
            "obi_stable"
            "data_health"
            "spread_bps"
            "book_stale_ms"
        }
        for k in list(allow):
            if k in indicators:
                fv = self._safe_num(indicators.get(k))
                if fv is not None:
                    out[k] = float(fv)

        # market_mode is often a string; encode trend/range as numeric for the evidence_map
        mm = indicators.get("market_mode") or indicators.get("regime")
        if isinstance(mm, str):
            mml = mm.strip().lower()
            if mml in {"trend", "momentum", "breakout"}:
                out.setdefault("market_mode", 1.0)
            elif mml in {"range", "meanrev", "mean_reversion"}:
                out.setdefault("market_mode", 0.0)

        # Aliases / backward-compat
        if "ice_strict" in out and "iceberg_strict" not in out:
            out["iceberg_strict"] = out["ice_strict"]
        if "sweep" in out and ("sweep_eqh" not in out and "sweep_eql" not in out and "sweep_any" not in out):
            out["sweep_any"] = out["sweep"]

        # Drop legacy alias keys from canonical map
        out.pop("ice_strict", None)
        out.pop("sweep", None)

        return out

    def _extract_conf_scores(self, *, signal: dict, indicators: dict) -> tuple[float, float | None]:
        """Return (raw, final)."""
        raw = (
            indicators.get("confidence_raw")
            or indicators.get("confidence_v1")
            or signal.get("confidence_raw")
            or signal.get("confidence")
            or 0.0
        )
        raw_f = self._safe_num(raw) or 0.0

        final = (
            indicators.get("confidence_cal")
            or indicators.get("confidence_final")
            or signal.get("confidence_final")
            or signal.get("confidence")
        )
        final_f = self._safe_num(final) if final is not None else None
        return float(raw_f), float(final_f) if final_f is not None else None

    async def _maybe_publish_confidence_scores(
        self
        *
        symbol: str
        sid: str
        ts_event_ms: int
        signal: dict
        confirmations: list
        indicators: dict
        evidence_dict: dict
    ) -> None:
        if not self._conf_scores_enabled():
            return

        try:
            evidence_map = self._build_conf_evidence_map(confirmations=confirmations, indicators=indicators)
            raw, final = self._extract_conf_scores(signal=signal, indicators=indicators)

            evt = {
                "schema_version": int(self.conf_scores_schema_version)
                "producer": str(os.getenv("SERVICE_NAME", "python-worker"))
                "sid": str(sid)
                "symbol": str(symbol)
                "ts_event_ms": int(ts_event_ms)
                "confidence_raw": float(raw)
                "confidence_final": float(final) if final is not None else None
                "evidence_map": evidence_map
            }
            if self.conf_scores_include_evidence_json:
                # Full evidence (heavy) - keep disabled unless needed.
                evt["evidence_json"] = evidence_dict

            await self.publisher.xadd_json(
                sink=StreamSink(name=self.conf_scores_stream, field="payload", maxlen=self.conf_scores_stream_maxlen)
                payload=evt
                symbol=str(symbol)
            )

        except Exception as e:
            # Best-effort quarantine - never block signal publishing.
            try:
                q = {
                    "ts_event_ms": int(ts_event_ms)
                    "sid": str(sid)
                    "symbol": str(symbol)
                    "error": str(e)
                }
                await self.publisher.xadd_json(
                    sink=StreamSink(name=self.conf_scores_quarantine_stream, field="payload", maxlen=self.conf_scores_quarantine_maxlen)
                    payload=q
                    symbol=str(symbol)
                )
            except Exception:
                pass



    def _build_gate_ctx(self, runtime: SymbolRuntime, signal: Dict[str, Any], sig_ts_ms: int) -> SimpleNamespace:
        """Build a minimal ctx object compatible with pre_publish_gates.* (fail-open)."""
        micro = signal.get("micro") if isinstance(signal.get("micro"), dict) else {}
        indicators = signal.get("indicators") if isinstance(signal.get("indicators"), dict) else {}

        # data-quality flags (prepared by preprocess_signal_for_publish + additional cheap hints)
        flags = []
        if isinstance(signal.get("data_quality_flags"), list):
            flags.extend([str(x) for x in signal.get("data_quality_flags") if x is not None])

        # Additional hints available at publish time
        try:
            book_stale_ms = int(micro.get("book_stale_ms") or 0)
            dq_book_stale_flag_ms = int(os.getenv("DQ_BOOK_STALE_FLAG_MS", "1500") or 1500)
            if book_stale_ms > dq_book_stale_flag_ms:
                flags.append("stale_l2")
        except Exception:
            pass

        try:
            spread_bps = float(micro.get("spread_bps") or 0.0)
            dq_spread_wide_flag_bps = float(os.getenv("DQ_SPREAD_WIDE_FLAG_BPS", "12") or 12.0)
            if spread_bps > dq_spread_wide_flag_bps:
                flags.append("wide_spread")
        except Exception:
            spread_bps = float(getattr(runtime, "last_spread_bps", 0.0) or 0.0)

        # Normalize + dedup
        seen = set()
        dq_flags = []
        for x in flags:
            s = str(x or "").strip().lower()
            if not s or s in seen:
                continue
            seen.add(s)
            dq_flags.append(s)

        # Minimal OF-like object: expose depth_bid_5/ask_5 if runtime has them
        of = SimpleNamespace(
            depth_bid_5=float(getattr(runtime, "last_depth_bid_5", 0.0) or 0.0)
            depth_ask_5=float(getattr(runtime, "last_depth_ask_5", 0.0) or 0.0)
            atr_ts_ms=int(indicators.get("atr_ts_ms") or signal.get("atr_ts_ms") or 0)
            regime=str(indicators.get("regime") or signal.get("regime") or "unknown")
            spread_bps=float(micro.get("spread_bps") or 0.0)
        )

        # Main ctx expected by gates
        ctx = SimpleNamespace(
            ts_event_ms=int(sig_ts_ms)
            ts_ms=int(sig_ts_ms)
            ts=int(sig_ts_ms)
            spread_bps=float(micro.get("spread_bps") or getattr(runtime, "last_spread_bps", 0.0) or 0.0)
            regime=str(indicators.get("regime") or signal.get("regime") or "unknown")
            session=str(signal.get("session") or indicators.get("session") or "na")
            tf=str(signal.get("tf") or indicators.get("tf") or "na")
            venue=str(signal.get("venue") or indicators.get("venue") or "binance")
            touch_is_stale=bool(signal.get("touch_is_stale") or indicators.get("touch_is_stale") or False)
            data_quality_flags=dq_flags
            of=of
            redis=getattr(runtime, "redis_client", None)
        )
        return ctx

    async def publish_signal(self, runtime: SymbolRuntime, signal: Dict[str, Any]) -> None:
        """
        Публикация сигнала в необходимые каналы.
        """

        # Next level:
        #   Optionally route ALL publications through SignalDispatcher outbox
        #   to unify idempotency, per-target retries, and DLQ policy.
        #
        # Safe rollout flags:
        #   CRYPTO_USE_OUTBOX_DISPATCHER=1   -> outbox-only (no direct xadd to notify/raw/audit)
        #   USE_SIGNAL_OUTBOX=1              -> unified shared flag
        #   CRYPTO_SHADOW_OUTBOX=1           -> keep legacy direct publishing + also write to outbox
        use_outbox = (
            os.getenv("CRYPTO_USE_OUTBOX_DISPATCHER", "0").lower() in {"1","true","yes","on"}
            or os.getenv("USE_SIGNAL_OUTBOX", "0").lower() in {"1","true","yes","on"}
        )

        shadow_outbox = os.getenv("CRYPTO_SHADOW_OUTBOX", "0").lower() in {"1","true","yes","on"}
        outbox_stream = os.getenv("SIGNAL_OUTBOX_STREAM", "stream:signals:outbox")
        
        # FEES AWARE GATE CONFIG
        # Modes: "SHADOW" (log only), "ENFORCE" (block signal), "OFF" (disable)
        gate_mode = os.getenv("ATR_GATE_MODE", os.getenv("FEES_AWARE_GATE_MODE", "SHADOW")).upper()

        # ------------------------------------------------------------------
        # Pipeline (explicit stages):
        #   1) extract + normalize primitive inputs (direction/entry/ts/conf)
        #   2) compute levels (sl/tp/lot/atr)
        #   3) build payloads:
        #        - enriched_signal: raw stream (payload)
        #        - audit_payload:   signals:cryptoorderflow:{symbol} (data)
        #        - telegram_payload: notify stream (fields)
        #   4) publish:
        #        - telegram (rate-limited)
        #        - streams via AsyncSignalPublisher (contract normalization + fail-open)
        # ------------------------------------------------------------------
        symbol = runtime.symbol
        direction = str(signal.get("direction") or "").upper().strip()
        cfg = runtime.config
        
        # 1) State initialization (avoid reliance on locals() introspection)
        passed = True
        reason = "SKIP"
        gate_meta: Dict[str, Any] = {}
        
        if direction not in {"LONG", "SHORT"}:
            # FAIL-OPEN: invalid direction should not crash the service.
            logger.warning("⚠️ (%s) publish_signal: invalid direction=%r (skip)", symbol, signal.get("direction"))
            return
            
        # Record total signals
        signals_total.labels(symbol=symbol, handler="crypto_orderflow").inc()
        
        # Outcome: emit (attributed by sig_ts)
        try:
            sig_ts = int(signal.get("tick_ts") or signal.get("ts_ms") or get_ny_time_millis())
            of_session_outcome_total.labels(symbol, session_utc(sig_ts), "emit").inc()
        except Exception:
            pass
        
        try:
            entry = float(signal["entry"])
        except Exception:
            logger.warning("⚠️ (%s) publish_signal: invalid entry=%r (skip)", symbol, signal.get("entry"))
            return
        confirmations = signal.get("confirmations", [])
        indicators = signal.get("indicators") or {}
        # Extract delta values from indicators (where they're actually stored)
        delta = float(indicators.get("delta", 0.0))
        delta_z = float(indicators.get("delta_z", 0.0))
        # Ensure they're also available at top level for backward compatibility
        signal.setdefault("delta", delta)
        signal.setdefault("delta_z", delta_z)
        indicators.setdefault("tick_qty", signal.get("tick_qty"))
        
        # A1 Decision Context Enrichment (hot-path, fail-open)
        #
        # We freeze a minimal "decision snapshot" inside ctx at publish time.
        # This is required for later post-trade TCA joins:
        #   decision_snapshot (sid, decision_ts_ms, decision_mid/bid/ask, ...)  +  fills
        #
        # Important invariants:
        # - decision_ts_ms MUST be epoch-ms and should be deterministic (prefer tick_ts/ts_emit_ms).
        # - Never use time.time() inside the enrichment unless there is no other choice.
        try:
            from services.orderflow.decision_ctx_fields import ensure_decision_ctx_fields
            ensure_decision_ctx_fields(signal, indicators=indicators, runtime=runtime, now_ms=int(sig_ts))
        except Exception as e:
            logger.warning("⚠️ (%s) Failed to enrich A1 decision ctx fields: %s", symbol, e)

        # A2 Decision Snapshot publication (warm-path input, fail-open)
        #
        # This stream is consumed by post-trade jobs (Timescale writer, TCA worker, etc.).
        # It MUST contain joinable keys and be stable across retries.
        if self.decision_snapshot_publish_enabled:
            try:
                from services.orderflow.decision_snapshot import build_decision_snapshot, publish_decision_snapshot
                snap = build_decision_snapshot(
                    signal
                    runtime=runtime
                    indicators=indicators
                    schema_version=int(self.decision_snapshot_schema_version)
                )
                await publish_decision_snapshot(
                    publisher=self.publisher
                    snapshot=snap
                    stream=self.decision_snapshot_stream
                    maxlen=int(self.decision_snapshot_stream_maxlen)
                    symbol=str(symbol)
                )
            except Exception as e:
                # Never block signal publishing.
                logger.warning("⚠️ (%s) decision_snapshot publish failed: %s", symbol, e)

        # --- SHADOW REQUIRE_STRONG_CONFIRMATION (SignalPipeline) ---
        # Note: Enforcement is handled in strategy.py early gate.
        if runtime.config.get("require_strong_confirmation"):
            gate_ok = bool(int(indicators.get("strong_gate_ok", 0) or indicators.get("of_confirm_ok", 0) or 0))
            if not gate_ok:
                # Log veto (sampled)
                logger.info(
                    "🛡️ [GATE-PIPELINE-SHADOW] RequireStrongConfirmation: signal=%s vetoed by strong gate. need=%s have=%s legs=%s"
                    signal.get("signal_id")
                    indicators.get("strong_gate_need")
                    indicators.get("strong_gate_have")
                    indicators.get("strong_gate_legs")
                )
                strong_gate_veto_total.labels(symbol=runtime.symbol, scenario="unknown", reason="require_strong", mode="SHADOW").inc()
                # DO NOT return here - bookkeeping and publishing continue (handled by strategy ENFORCE)

        # ------------------------------------------------------------------
        # 🧩 SPREAD / ILLIQUIDITY GATE (P0)
        # ------------------------------------------------------------------
        # Vetos checks (spread_bps, spread_z, book_stale_ms)
        # 1. Spread BPS
        max_spread_bps = float(cfg.get("gate_spread_max_bps", 0.0) or 0.0)
        curr_spread_bps = float(indicators.get("liq_spread_bps", 0.0) or indicators.get("spread_bps", 0.0) or 0.0)
        
        # 2. Spread Z-Score
        max_spread_z = float(cfg.get("gate_spread_max_z", 0.0) or 0.0)
        curr_spread_z = float(indicators.get("spread_z", 0.0) or 0.0)

        # 3. Book Staleness
        max_stale_ms = int(cfg.get("gate_book_stale_ms", 0) or 0)
        curr_stale_ms = int(indicators.get("book_ts_gap_ms", 0) or 0)
        if curr_stale_ms <= 0:
            curr_stale_ms = int(indicators.get("liq_book_stale_ms", 0) or 0)

        # ------------------------------------------------------------------
        # Phase C (P2): Liquidity geometry & resiliency gate (monitor/tighten/veto)
        # ------------------------------------------------------------------
        # Inputs must already be present in indicators (computed in strategy.py):
        #   - book_slope_bid/ask (USD/bps)
        #   - dws_bps (bps)
        #   - liq_recovery_time_ms (ms)
        #
        # Profiles (ENV: LIQ_GATE_PROFILE, fallback GATE_PROFILE):
        #   - default/soft: annotate only (flags in indicators)
        #   - strict: tighten execution cost proxy (expected_slippage_bps add)
        #   - hard: tighten + veto when thresholds breached
        #
        # Thresholds (0 = disabled):
        #   - LIQ_MIN_BOOK_SLOPE
        #   - LIQ_MAX_DWS_BPS
        #   - LIQ_MAX_RECOVERY_TIME_MS
        liq_profile = str(os.getenv("LIQ_GATE_PROFILE", os.getenv("GATE_PROFILE", "default")) or "default").strip().lower()
        if liq_profile not in {"default", "soft", "strict", "hard"}:
            liq_profile = "default"

        slope_bid = float(indicators.get("book_slope_bid", 0.0) or 0.0)
        slope_ask = float(indicators.get("book_slope_ask", 0.0) or 0.0)
        dws_bps_val = float(indicators.get("dws_bps", 0.0) or 0.0)
        rec_ms = int(indicators.get("liq_recovery_time_ms", 0) or 0)

        thr_slope = float(os.getenv("LIQ_MIN_BOOK_SLOPE", "0") or 0.0)
        thr_dws = float(os.getenv("LIQ_MAX_DWS_BPS", "0") or 0.0)
        thr_rec = int(os.getenv("LIQ_MAX_RECOVERY_TIME_MS", "0") or 0)

        cap = float(os.getenv("LIQ_GEOM_TIGHTEN_ADD_CAP_BPS", "10.0") or 10.0)
        mult = float(os.getenv("LIQ_GEOM_TIGHTEN_ADD_MULT", "1.0") or 1.0)

        try:
            from services.orderflow.liquidity_geom_policy import evaluate_liq_geom
            decg = evaluate_liq_geom(
                profile=liq_profile
                slope_bid=slope_bid
                slope_ask=slope_ask
                dws_bps=dws_bps_val
                recovery_ms=rec_ms
                thr_slope=thr_slope
                thr_dws=thr_dws
                thr_recovery_ms=thr_rec
                tighten_cap_bps=cap
                tighten_mult=mult
            )

            # Always annotate for observability/debugging
            if decg.flags:
                indicators["liq_geom_monitor_hit"] = 1
                indicators["liq_geom_flags"] = ",".join(decg.flags)
            else:
                indicators.setdefault("liq_geom_monitor_hit", 0)
                indicators.setdefault("liq_geom_flags", "")
            indicators["liq_geom_profile"] = liq_profile

            # Optional Prometheus telemetry (bounded symbol cardinality)
            try:
                sym_label = str(symbol).upper()
                if self._liq_geom_syms_allow and sym_label not in self._liq_geom_syms_allow:
                    sym_label = "__all__"
                if dws_bps_val > 0:
                    liq_geom_dws_bps.labels(symbol=sym_label).observe(float(dws_bps_val))
                if decg.slope_min > 0:
                    liq_geom_book_slope_min_usd_per_bps.labels(symbol=sym_label).observe(float(decg.slope_min))
                liq_geom_recovery_time_ms.labels(symbol=sym_label).observe(float(max(0, rec_ms)))
                if decg.flags:
                    liq_geom_monitor_hit_total.labels(symbol=sym_label, profile=liq_profile).inc()
            except Exception:
                pass

            # strict/hard: tighten execution cost proxy (expected_slippage_bps)
            if decg.tighten_add_bps > 0.0:
                exp0 = float(indicators.get("expected_slippage_bps", 0.0) or 0.0)
                indicators["liq_geom_tighten_add_bps"] = float(decg.tighten_add_bps)
                indicators["expected_slippage_bps"] = float(exp0 + float(decg.tighten_add_bps))
                try:
                    sym_label2 = str(symbol).upper()
                    if self._liq_geom_syms_allow and sym_label2 not in self._liq_geom_syms_allow:
                        sym_label2 = "__all__"
                    liq_geom_tighten_total.labels(symbol=sym_label2, profile=liq_profile).inc()
                except Exception:
                    pass

            # hard: veto on breach
            if decg.veto:
                geom_reason = str(decg.veto_reason)
                try:
                    sym_label3 = str(symbol).upper()
                    if self._liq_geom_syms_allow and sym_label3 not in self._liq_geom_syms_allow:
                        sym_label3 = "__all__"
                    liq_geom_veto_total.labels(symbol=sym_label3, reason=geom_reason).inc()
                except Exception:
                    pass
                logger.info(
                    "🛡️ [GATE] Liquidity-Geometry VETO (%s): %s | slope_min=%.1f thr=%.1f dws=%.2f thr=%.2f rec_ms=%d thr=%d"
                    symbol
                    geom_reason
                    float(decg.slope_min)
                    thr_slope
                    dws_bps_val
                    thr_dws
                    rec_ms
                    thr_rec
                )
                strong_gate_veto_total.labels(symbol=symbol, scenario="liq_geom", reason=geom_reason, mode="ENFORCE").inc()
                passed = False
                reason = f"LIQ_GEOM_VETO: {geom_reason}"
                return
        except Exception:
            pass

        # ------------------------------------------------------------------
        # Phase D (P3): Flow toxicity gate (OFI normalized by depth + optional VPIN)
        # ------------------------------------------------------------------
        # Inputs (computed in strategy.py, fail-open):
        #   - ofi_norm_z : robust z-score of (ofi_best_qty*mid)/(notional_1bp_bid+ask)
        #   - vpin_cdf   : optional VPIN-like toxicity proxy in [0..1]
        #
        # Profiles:
        #   - default/soft: annotate only
        #   - strict: tighten expected_slippage_bps
        #   - hard: veto ONLY when flow_toxic AND (TCA-bad OR FLOW_TOX_VETO_WITHOUT_TCA=1)
        #
        # Thresholds:
        #   - FLOW_OFI_NORM_Z_MAX (0 disables)
        #   - FLOW_VPIN_CDF_MAX   (0 disables)
        try:
            from services.orderflow.flow_toxicity import evaluate_flow_toxicity

            flow_profile = str(os.getenv("FLOW_GATE_PROFILE", os.getenv("GATE_PROFILE", "default")) or "default").strip().lower()
            # Allow explicit mode override
            mode_override = str(os.getenv("FLOW_TOXIC_MODE", os.getenv("FLOW_TOX_MODE", "")) or "").strip().lower()
            if mode_override in {"monitor", "tighten", "veto"}:
                flow_profile = mode_override
            if flow_profile not in {"default", "soft", "strict", "hard", "monitor", "tighten", "veto"}:
                flow_profile = "default"

            thr_z = float(os.getenv("FLOW_OFI_NORM_Z_MAX", "0") or 0.0)
            thr_vpin = float(os.getenv("FLOW_VPIN_CDF_MAX", "0") or 0.0)

            cap = float(os.getenv("FLOW_TOX_TIGHTEN_ADD_CAP_BPS", "6.0") or 6.0)
            mult = float(os.getenv("FLOW_TOX_TIGHTEN_ADD_MULT", "1.0") or 1.0)
            veto_wo_tca = bool(int(os.getenv("FLOW_TOX_VETO_WITHOUT_TCA", "0") or 0))

            ofi_z = float(indicators.get("ofi_norm_z", 0.0) or 0.0)
            vpin_cdf = float(indicators.get("vpin_cdf", 0.0) or 0.0)

            # Optional: TCA health inputs (if Phase B is enabled). If missing -> 0.
            tca_is = float(indicators.get("tca_is_p95_bps", indicators.get("is_p95_bps", 0.0)) or 0.0)
            tca_imp = float(indicators.get("tca_perm_impact_p95_bps", indicators.get("perm_impact_p95_bps", 0.0)) or 0.0)
            thr_is = float(os.getenv("EXEC_MAX_IS_P95_BPS", "0") or 0.0)
            thr_imp = float(os.getenv("EXEC_MAX_PERM_IMPACT_P95_BPS", "0") or 0.0)

            decf = evaluate_flow_toxicity(
                profile=flow_profile
                ofi_norm_z=ofi_z
                thr_ofi_norm_z=thr_z
                vpin_cdf=vpin_cdf
                thr_vpin_cdf=thr_vpin
                tca_is_p95_bps=tca_is
                tca_perm_impact_p95_bps=tca_imp
                thr_is_p95_bps=thr_is
                thr_perm_impact_p95_bps=thr_imp
                tighten_mult=mult
                tighten_cap_bps=cap
                veto_without_tca=veto_wo_tca
            )

            # annotate always
            indicators["flow_toxic_profile"] = str(flow_profile)
            indicators["flow_toxic_flags"] = ",".join(decf.flags) if decf.flags else ""
            indicators["flow_toxic_hit"] = 1 if decf.hit else 0

            # Optional Prometheus telemetry (bounded symbol cardinality)
            try:
                sym_label = str(symbol).upper()
                if self._flow_tox_syms_allow and sym_label not in self._flow_tox_syms_allow:
                    sym_label = "__all__"
                flow_toxic_ofi_norm_z.labels(symbol=sym_label).observe(float(ofi_z))
                flow_toxic_vpin_cdf.labels(symbol=sym_label).observe(float(vpin_cdf))
                if decf.hit:
                    flow_toxic_monitor_hit_total.labels(symbol=sym_label, profile=str(flow_profile)).inc()
            except Exception:
                pass

            if decf.tighten_add_bps > 0.0:
                exp0 = float(indicators.get("expected_slippage_bps", 0.0) or 0.0)
                indicators["flow_toxic_tighten_add_bps"] = float(decf.tighten_add_bps)
                indicators["expected_slippage_bps"] = float(exp0 + float(decf.tighten_add_bps))
                try:
                    sym_label2 = str(symbol).upper()
                    if self._flow_tox_syms_allow and sym_label2 not in self._flow_tox_syms_allow:
                        sym_label2 = "__all__"
                    flow_toxic_tighten_total.labels(symbol=sym_label2, profile=str(flow_profile)).inc()
                except Exception:
                    pass

            if decf.veto:
                try:
                    sym_label3 = str(symbol).upper()
                    if self._flow_tox_syms_allow and sym_label3 not in self._flow_tox_syms_allow:
                        sym_label3 = "__all__"
                    flow_toxic_veto_total.labels(symbol=sym_label3, reason=str(decf.veto_reason or "flow_toxic")).inc()
                except Exception:
                    pass
                logger.info(
                    "🛡️ [GATE] FlowToxicity VETO (%s): flags=%s ofi_z=%.2f thr=%.2f vpin_cdf=%.3f thr=%.3f tca_is_p95=%.2f thr=%.2f tca_imp_p95=%.2f thr=%.2f"
                    symbol
                    indicators.get("flow_toxic_flags", "")
                    ofi_z
                    thr_z
                    vpin_cdf
                    thr_vpin
                    tca_is
                    thr_is
                    tca_imp
                    thr_imp
                )
                strong_gate_veto_total.labels(symbol=symbol, scenario="flow_toxic", reason="VETO_FLOW_TOXIC", mode="ENFORCE").inc()
                passed = False
                reason = "VETO_FLOW_TOXIC"
                return
        except Exception:
            pass

        # ------------------------------------------------------------------
        # Phase E / P4: MANIPULATION / MICROSTRUCTURE ABUSE GATE
        # ------------------------------------------------------------------
        # Inputs (injected by strategy.py from runtime.manip/msg_rate):
        #   - quote_stuffing_score  float 0..1
        #   - layering_score        float 0..1
        #   - otr_z                 robust z-score of OTR ratio
        #   - manip_flags           comma-separated code string
        #
        # Mode (ENV: MANIP_GATE_PROFILE, fallback GATE_PROFILE):
        #   - default/soft/monitor: annotate only
        #   - strict/tighten: tighten expected_slippage_bps + annotate
        #   - hard/veto: tighten + veto
        #
        # Thresholds (0 = disabled):
        #   - MANIP_QUOTE_STUFF_SCORE_MAX  (0 = disabled)
        #   - MANIP_LAYERING_SCORE_MAX     (0 = disabled)
        #   - MANIP_OTR_Z_MAX             (0 = disabled)
        try:
            manip_profile = str(os.getenv("MANIP_GATE_PROFILE", os.getenv("GATE_PROFILE", "default")) or "default").strip().lower()
            # Explicit mode override (matches flow_toxicity pattern)
            manip_mode_ov = str(os.getenv("MANIP_MODE", "") or "").strip().lower()
            if manip_mode_ov in {"monitor", "tighten", "veto"}:
                manip_profile = manip_mode_ov
            if manip_profile not in {"default", "soft", "strict", "hard", "monitor", "tighten", "veto"}:
                manip_profile = "default"

            thr_qs = float(os.getenv("MANIP_QUOTE_STUFF_SCORE_MAX", "0") or 0.0)
            thr_lay = float(os.getenv("MANIP_LAYERING_SCORE_MAX", "0") or 0.0)
            thr_otr_z = float(os.getenv("MANIP_OTR_Z_MAX", "0") or 0.0)

            tighten_cap = float(os.getenv("MANIP_TIGHTEN_ADD_CAP_BPS", "6.0") or 6.0)
            tighten_mult = float(os.getenv("MANIP_TIGHTEN_ADD_MULT", "1.0") or 1.0)

            qs_score = float(indicators.get("quote_stuffing_score", 0.0) or 0.0)
            lay_score = float(indicators.get("layering_score", 0.0) or 0.0)
            otr_z_val = float(indicators.get("otr_z", 0.0) or 0.0)
            manip_flags_val = str(indicators.get("manip_flags", "") or "")

            # Evaluate gate flags
            manip_hit_flags = list(manip_flags_val.split(",")) if manip_flags_val else []
            hit_qs = thr_qs > 0.0 and qs_score >= thr_qs
            hit_lay = thr_lay > 0.0 and lay_score >= thr_lay
            hit_otr = thr_otr_z > 0.0 and otr_z_val >= thr_otr_z
            hit_any = hit_qs or hit_lay or hit_otr

            # Annotate always
            indicators["manip_gate_profile"] = manip_profile
            indicators["manip_gate_hit"] = 1 if hit_any else 0

            # Telemetry: monitor hit
            if hit_any and manip_profile in {"default", "soft", "monitor"}:
                reason_parts = []
                if hit_qs: reason_parts.append(f"qs={qs_score:.2f}")
                if hit_lay: reason_parts.append(f"lay={lay_score:.2f}")
                if hit_otr: reason_parts.append(f"otr_z={otr_z_val:.2f}")
                logger.info("🔍 [GATE-MANIP] MONITOR (%s): %s | flags=%s", symbol, " ".join(reason_parts), manip_flags_val)
                try:
                    manip_gate_events_total.labels(symbol=symbol, mode="monitor", reason="ANNOTATE").inc()
                except Exception:
                    pass

            # Tighten in strict/tighten/hard/veto profiles
            if hit_any and manip_profile in {"strict", "tighten", "hard", "veto"}:
                manip_score = max(qs_score, lay_score)
                if manip_score <= 0.0 and hit_otr:
                    manip_score = min(1.0, max(0.1, (otr_z_val - thr_otr_z) / max(thr_otr_z, 1.0)))
                add_bps = float(min(tighten_cap, manip_score * tighten_mult * 3.0))
                if add_bps > 0.0:
                    exp0 = float(indicators.get("expected_slippage_bps", 0.0) or 0.0)
                    indicators["expected_slippage_bps"] = float(exp0 + add_bps)
                    indicators["manip_tighten_add_bps"] = float(add_bps)
                    try:
                        manip_gate_events_total.labels(symbol=symbol, mode="tighten", reason="TIGHTEN").inc()
                    except Exception:
                        pass

            # Veto in hard/veto profiles
            if hit_any and manip_profile in {"hard", "veto"}:
                veto_reason = "VETO_QUOTE_STUFFING" if hit_qs else ("VETO_LAYERING" if hit_lay else "VETO_OTR_SPIKE")
                logger.info(
                    "🛡️ [GATE] Manipulation VETO (%s): %s | qs_score=%.3f lay_score=%.3f otr_z=%.2f flags=%s"
                    symbol, veto_reason, qs_score, lay_score, otr_z_val, manip_flags_val
                )
                try:
                    manip_gate_events_total.labels(symbol=symbol, mode="veto", reason=veto_reason).inc()
                except Exception:
                    pass
                strong_gate_veto_total.labels(symbol=symbol, scenario="manip", reason=veto_reason, mode="ENFORCE").inc()
                passed = False
                reason = f"MANIP_VETO: {veto_reason}"
                return
        except Exception:
            pass

        # ------------------------------------------------------------------
        # P5: Book sanity gate (crossed BBO / NaN depth / negative qty)
        # Default mode: monitor (annotate indicators, never veto unless BOOK_SANITY_MODE=veto)
        # ------------------------------------------------------------------
        try:
            # Populate book_sanity_flags indicator from runtime (set by BookProcessor)
            indicators.setdefault("book_sanity_ok", int(getattr(runtime, "book_sanity_ok", 1) or 1))
            bsf = str(getattr(runtime, "book_sanity_flags", "") or "")
            if bsf:
                indicators.setdefault("book_sanity_flags", bsf)
                # Parse into a list for the gate
                indicators["book_sanity_flags_list"] = [s.strip() for s in bsf.split(",") if s.strip()]
            bsg_dec = _book_sanity_gate.evaluate(indicators=indicators, symbol=str(symbol))
            indicators["book_sanity_gate_veto"] = int(1 if bsg_dec.veto else 0)
            indicators["book_sanity_gate_reason"] = str(bsg_dec.reason_code or "")
            if bsg_dec.veto:
                logger.info("🛡️ [GATE-P5-BOOK-SANITY] VETO (%s): %s | flags=%s", symbol, bsg_dec.reason_code, bsg_dec.flags)
                strong_gate_veto_total.labels(symbol=symbol, scenario="book_sanity", reason=bsg_dec.reason_code, mode="ENFORCE").inc()
                passed = False
                reason = f"P5_BOOK_SANITY: {bsg_dec.reason_code}"
                return
        except Exception:
            pass

        # ------------------------------------------------------------------
        # P5: Stream integrity gate (seq gaps / dup burst / schema drift)
        # Default mode: monitor (annotate, never veto unless STREAM_INTEGRITY_MODE=veto + thresholds set)
        # ------------------------------------------------------------------
        try:
            # Surface P5 integrity fields to indicators (set by TickProcessor / BookProcessor)
            def _si_ema(tracker) -> float:
                try: return float(getattr(tracker, "gap_ema", None).ema or 0.0)
                except Exception: return 0.0
            def _si_gmax(tracker) -> int:
                try: return int(getattr(tracker, "gap_max_window", 0) or 0)
                except Exception: return 0
            def _si_schema(tracker) -> int:
                try: return int(getattr(tracker, "schema_changed_last", 0) or 0)
                except Exception: return 0

            ti = getattr(runtime, "tick_integrity", None)
            bi = getattr(runtime, "book_integrity", None)
            indicators.setdefault("tick_seq_gap_rate_ema", _si_ema(ti))
            indicators.setdefault("tick_seq_max_gap_window", _si_gmax(ti))
            indicators.setdefault("tick_schema_changed", _si_schema(ti))
            indicators.setdefault("book_seq_gap_rate_ema", _si_ema(bi))
            indicators.setdefault("book_seq_max_gap_window", _si_gmax(bi))

            sig_dec = _stream_integrity_gate.evaluate(indicators=indicators, symbol=str(symbol))
            indicators["stream_integrity_gate_veto"] = int(1 if sig_dec.veto else 0)
            indicators["stream_integrity_gate_reason"] = str(sig_dec.reason_code or "")
            if sig_dec.veto:
                logger.info("🛡️ [GATE-P5-STREAM-INTEGRITY] VETO (%s): %s | flags=%s", symbol, sig_dec.reason_code, sig_dec.flags)
                strong_gate_veto_total.labels(symbol=symbol, scenario="stream_integrity", reason=sig_dec.reason_code, mode="ENFORCE").inc()
                passed = False
                reason = f"P5_STREAM_INTEGRITY: {sig_dec.reason_code}"
                return
        except Exception:
            pass

        spread_veto = False
        spread_reason = ""

        if max_spread_bps > 0 and curr_spread_bps > max_spread_bps:
            spread_veto = True
            spread_reason = f"spread_bps={curr_spread_bps:.2f} > {max_spread_bps}"
        elif max_spread_z > 0 and curr_spread_z > max_spread_z:
            spread_veto = True
            spread_reason = f"spread_z={curr_spread_z:.2f} > {max_spread_z}"
        elif max_stale_ms > 0 and curr_stale_ms > max_stale_ms:
            spread_veto = True
            spread_reason = f"book_stale_ms={curr_stale_ms} > {max_stale_ms}"

        if spread_veto:
            logger.info("🛡️ [GATE] Spread/Liquidity VETO (%s): %s", symbol, spread_reason)
            strong_gate_veto_total.labels(symbol=symbol, scenario="spread", reason=spread_reason, mode="ENFORCE").inc()
            passed = False
            reason = f"SPREAD_VETO: {spread_reason}"
            # STRICT ENFORCEMENT
            return

        # Log Strong Gate outcome if present (sample every 10000th message)
        if indicators.get("strong_gate_scn"):
             strong_gate_sampler = LogSamplerFactory.get_sampler("STRONG_GATE", 10000)
             if strong_gate_sampler.should_log(f"strong_gate_{symbol}"):
                 logger.info(
                     "🛡️ [GATE] Strong Gate: scn=%s have=%s need=%s"
                     indicators.get("strong_gate_scn")
                     indicators.get("strong_gate_have")
                     indicators.get("strong_gate_need")
                 )


        # ---- trail_profile ----
        # cfg already initialized
        trail_profile = signal.get("trail_profile") or cfg.get("trail_profile") or "rocket_v1"

        # ---- ts normalization (epoch ms best-effort) ----
        # We keep multiple mirrors because older downstream components differ:
        #   - some read `ts`, others `timestamp`, newer contract expects `ts_ms`.
        def _ts_ms() -> int:
            v = signal.get("tick_ts") or signal.get("generated_at") or signal.get("ts_ms") or signal.get("ts")
            try:
                iv = int(float(v))
                return iv if iv > 0 else get_ny_time_millis()
            except Exception:
                return get_ny_time_millis()

        ts_ms = _ts_ms()

        # Calculate actual ATR that will be used (including fallbacks)
        sl, tp_levels, lot, atr = self._calculate_levels(runtime, entry, direction, indicators, trail_profile=trail_profile)
        # ------------------------------------------------------------------
        # Construction Phase using Typed SignalPayload
        # ------------------------------------------------------------------
        
        # 1. Gate Decision (if available in indicators)
        gate_decision = None
        if indicators.get("strong_gate_ok") is not None:
             gate_decision = StrongGateDecision(
                 ok=bool(int(indicators.get("strong_gate_ok", 0)))
                 scenario=str(indicators.get("strong_gate_scn", "na"))
                 need=int(indicators.get("strong_gate_need", 0))
                 have=int(indicators.get("strong_gate_have", 0))
                 a=int(indicators.get("strong_gate_legs", {}).get("A", 0) if isinstance(indicators.get("strong_gate_legs"), dict) else 0)
                 b=int(indicators.get("strong_gate_legs", {}).get("B", 0) if isinstance(indicators.get("strong_gate_legs"), dict) else 0)
                 c=int(indicators.get("strong_gate_legs", {}).get("C", 0) if isinstance(indicators.get("strong_gate_legs"), dict) else 0)
                 reason="gate_decision"
                 legs=indicators.get("strong_gate_legs") if isinstance(indicators.get("strong_gate_legs"), dict) else None
             )

        # 2. Confidence Parts
        conf_parts = indicators.get("confidence_breakdown")

        # 3. Create Typed Payload
        sig_payload = SignalPayload(
            confirmations={c.split("=")[0]: c.split("=")[1] for c in confirmations if "=" in c}
            indicators=indicators
            gate=gate_decision
            confidence_parts=conf_parts
            rejection_reason=reason
            ts_ms=ts_ms
            symbol=symbol
            signal_id=str(signal.get("signal_id", ""))
        )
        
        # Export back to dict for legacy compatibility
        # (This essentially enriches the raw payload with structured data)
        evidence_dict = sig_payload.to_dict()

        # Build final stream payload (legacy structure + new evidence)
        payload = {
            "signal_id": str(signal.get("signal_id", ""))
            "symbol": runtime.symbol
            "direction": direction
            "entry": float(entry)
            "sl": float(sl)
            "tp_levels": [float(x) for x in tp_levels]
            "lot": float(lot)
            "atr": float(atr)
            "confidence": float(signal.get("confidence", 0.0) or 0.0), # Will be re-calculated or passed
            "reason": str(signal.get("reason", "unknown"))
            "ts_ms": int(ts_ms)
            "generated_at": int(ts_ms)
            "written_at": get_ny_time_millis()
            "evidence": evidence_dict, # <--- NEW FIELD
            
            # --- Fields kept for backward compatibility with raw consumers ---
            "delta": delta
            "delta_z": delta_z
            "tick_qty": indicators.get("tick_qty")
            "confirmations": confirmations
            "indicators": indicators
        }
        
        
        # Optional: publish compact confidence score event to high-frequency stream
        await self._maybe_publish_confidence_scores(
            symbol=str(symbol)
            sid=str(payload.get("signal_id", ""))
            ts_event_ms=int(payload.get("ts_ms", 0))
            signal=signal
            confirmations=confirmations
            indicators=indicators
            evidence_dict=evidence_dict
        )
        # ------------------------------------------------------------------
        # ✅ FIX "BROKEN CHAIN": expose ATR-floor tier selection into indicators
        # so raw stream audit + unified gate see correct atr_floor_th_bps.
        # ------------------------------------------------------------------
        try:
            from core.atr_floor_policy import compute_atr_bps_threshold

            rg = str(getattr(runtime, "last_regime", "na") or "na").lower()

            # Current executed ATR in bps (always useful for audits)
            atr_bps_exec = 0.0
            try:
                if float(entry) > 0 and float(atr) > 0:
                    atr_bps_exec = float(10000.0 * (float(atr) / float(entry)))
            except Exception:
                atr_bps_exec = 0.0
            indicators["atr_bps_exec"] = float(atr_bps_exec)

            # Pull floors (prefer calibrated/dynamic; fallback to config)
            t0 = float(runtime.dynamic_cfg.get("atr_floor_t0_bps", cfg.get("atr_floor_t0_bps", 0.0)) or 0.0)
            t1 = float(runtime.dynamic_cfg.get("atr_floor_t1_bps", cfg.get("atr_floor_t1_bps", 0.0)) or 0.0)
            t2 = float(runtime.dynamic_cfg.get("atr_floor_t2_bps", cfg.get("atr_floor_t2_bps", 0.0)) or 0.0)

            tier, picked, floor_th = compute_atr_bps_threshold(regime=rg, cfg=cfg, t0=t0, t1=t1, t2=t2)

            indicators["atr_floor_t0_bps"] = float(t0)
            indicators["atr_floor_t1_bps"] = float(t1)
            indicators["atr_floor_t2_bps"] = float(t2)
            indicators["atr_floor_tier"] = int(tier)
            indicators["atr_floor_picked_bps"] = float(picked)
            indicators["atr_floor_th_bps"] = float(floor_th)
            indicators["atr_floor_rg"] = str(rg)
            indicators["atr_floor_ready"] = int(runtime.dynamic_cfg.get("atr_calib_ready", 0) or 0)
            indicators["atr_floor_src"] = str(runtime.dynamic_cfg.get("atr_bps_src", "na") or "na")
            indicators["atr_floor_n"] = int(runtime.dynamic_cfg.get("atr_bps_n", 0) or 0)

            # Keep legacy mirror used by some earlier logic
            indicators["atr_bps_th"] = float(floor_th)
        except Exception:
            pass

        # Optional: also expose fees-aware threshold for audits even if gate not enforced
        try:
            from core.fees_aware_policy import fees_aware_min_atr_bps

            # tp1_share derived from TP_RATIO (env) or config snapshot
            tp_ratios = parse_tp_ratio(cfg.get("tp_ratio"))
            tp1_share_actual = float(tp_ratios[0] if tp_ratios else 0.5)
            rocket_mult = float(self._get_rocket_multiplier(runtime.symbol) or 0.0)
            fees_th, fees_meta = fees_aware_min_atr_bps(
                fees_bps_rt=float(self.FEES_BPS_RT)
                tp_bps_buffer=float(self.TP_BPS_BUFFER)
                tp1_share=tp1_share_actual
                rocket_mult=rocket_mult
            )
            indicators["atr_fees_th_bps"] = float(fees_th)
            indicators["atr_fees_tp1_share"] = float(tp1_share_actual)
            indicators["atr_fees_rocket_mult"] = float(rocket_mult)
        except Exception:
            pass

        # Unified threshold numbers into indicators (debug-only; gate uses same values below)
        try:
            floor_th = float(indicators.get("atr_floor_th_bps", 0.0) or 0.0)
            fees_th = float(indicators.get("atr_fees_th_bps", 0.0) or 0.0)
            unified_th = float(max(floor_th, fees_th))
            indicators["atr_unified_th_bps"] = float(unified_th)
            indicators["atr_gate_dominant"] = ("fees" if fees_th >= floor_th else "floor") if unified_th > 0 else "na"
        except Exception:
            pass


        # ------------------------------------------------------------------
        # 🚦 UNIFIED ATR GATE (ATR-floor tiers + fees-aware margin for rocket_v1)
        # ------------------------------------------------------------------
        if gate_mode in {"SHADOW", "ENFORCE"} and trail_profile == "rocket_v1":
            try:
                from core.atr_floor_policy import compute_atr_bps_threshold
                from core.fees_aware_policy import fees_aware_min_atr_bps
                
                atr_bps_exec = (float(atr) / float(entry)) * 10000.0 if entry > 0 else 0.0
                
                # 1) ATR-floor threshold (tier-by-regime)
                rg = str(getattr(runtime, "last_regime", "na") or "na").lower()
                # Prefer computed floor_th_bps (fixed chain); fallback to runtime.dynamic_cfg if needed
                atr_floor_th = float(
                    indicators.get("atr_floor_th_bps", 0.0)
                    or indicators.get("atr_bps_th", 0.0)
                    or runtime.dynamic_cfg.get("atr_bps_th", 0.0)
                    or 0.0
                )
                
                if not (atr_floor_th > 0):
                    # fallback: recompute from floors
                    t0 = float(runtime.dynamic_cfg.get("atr_floor_t0_bps", runtime.config.get("atr_floor_t0_bps", 0.0)) or 0.0)
                    t1 = float(runtime.dynamic_cfg.get("atr_floor_t1_bps", runtime.config.get("atr_floor_t1_bps", 0.0)) or 0.0)
                    t2 = float(runtime.dynamic_cfg.get("atr_floor_t2_bps", runtime.config.get("atr_floor_t2_bps", 0.0)) or 0.0)
                    _, _, _th = compute_atr_bps_threshold(regime=rg, cfg=runtime.config, t0=t0, t1=t1, t2=t2)
                    atr_floor_th = float(_th)
                
                # 2) Fees-aware threshold
                # cfg already initialized at the top of publish_signal
                tp_ratios = parse_tp_ratio(cfg.get("tp_ratio"))
                tp1_share_actual = float(tp_ratios[0] if tp_ratios else 0.5)
                rocket_mult = float(self._get_rocket_multiplier(runtime.symbol) or 0.0)
                fees_th, _ = fees_aware_min_atr_bps(
                    fees_bps_rt=float(self.FEES_BPS_RT)
                    tp_bps_buffer=float(self.TP_BPS_BUFFER)
                    tp1_share=tp1_share_actual
                    rocket_mult=rocket_mult
                )
                
                unified_th = float(max(atr_floor_th, fees_th))
                dominant = "fees" if fees_th >= atr_floor_th else "floor"
                
                # EXPERT RELAXATION (2026-01-30):
                # For meme coins, we relax the ATR gate significantly (5% floor)
                # to ensure they enter the virtual tracking system for calibration reports.
                from core.instrument_config import symbol_env_prefix
                is_meme = symbol_env_prefix(symbol) in ("PEPE", "SHIB", "DOGE", "BONK", "FLOKI", "WIF")
                
                effective_th = unified_th
                if is_meme:
                    effective_th *= 0.05 # 95% discount for calibration
                
                veto = bool(effective_th > 0 and atr_bps_exec < effective_th)
                if is_meme and not veto and effective_th < unified_th:
                     logger.info("✅ [ATR-GATE] (%s) RELAXED PASS: atr_bps=%.2f passed via relaxation (th=%.2f -> relaxed_th=%.2f)", 
                                 symbol, atr_bps_exec, unified_th, effective_th)

                if veto:
                    # [AUDIT-ONLY] Unified Pipeline Gate is now strictly passive.
                    # Substituted by "Early Gate" in strategy.py (load shedder).
                    msg_mode = "[AUDIT-ONLY]" if gate_mode == "SHADOW" else "[GATE-ENFORCE]"
                    
                    if gate_mode in {"SHADOW", "ENFORCE"}:
                         logger.info("ℹ️ %s ATR unified VETO triggered (%s): atr_bps=%.2f < th=%.2f (relaxed_th=%.2f) | %s"
                                      msg_mode, dominant, atr_bps_exec, unified_th, effective_th, symbol)

                    indicators["gate_shadow_veto"] = True
                    indicators["gate_reason"] = "LOW_ATR_UNIFIED"
                    # Keep numeric trail for downstream audit
                    indicators["atr_floor_th_bps"] = float(atr_floor_th)
                    indicators["atr_fees_th_bps"] = float(fees_th)
                    indicators["atr_unified_th_bps"] = float(unified_th)
                    indicators["atr_gate_dominant"] = str(dominant)
                    
                    if gate_mode == "ENFORCE":
                        strong_gate_veto_total.labels(symbol=symbol, scenario="atr_unified", reason="low_atr", mode="ENFORCE").inc()
                        # Strict enforcement
                        passed = False
                        reason = f"ATR_VETO: {dominant} {atr_bps_exec:.2f} < {unified_th:.2f}"
                        return
            except Exception:
                pass

        # Логируем для отладки с правильным ATR (sample every 10000th message)
        calc_levels_sampler = LogSamplerFactory.get_sampler("CALC_LEVELS", 10000)
        if calc_levels_sampler.should_log(f"calc_levels_{symbol}"):
            logger.info("🔍 Calculating levels for %s: trail_profile=%s, entry=%.8f, atr=%.8f", symbol, trail_profile, entry, atr)
        
        # ------------------------------------------------------------------
        # TP1 calculation using parametrizable mult
        # ------------------------------------------------------------------
        if trail_profile == "rocket_v1" and tp_levels:
            # Safe mult resolution: meta > symbol-override > global-default
            rocket_mult = float(gate_meta.get("mult") or self._get_rocket_multiplier(symbol))
            
            expected_tp1 = entry + (atr * rocket_mult) if str(direction or "").upper() == "LONG" else entry - (atr * rocket_mult)
            actual_tp1 = float(tp_levels[0])
            diff = abs(expected_tp1 - actual_tp1)
            
            # tick-aware tolerance
            ts = getattr(runtime, "tick_size", None)
            if ts is None:
                ts = getattr(getattr(runtime, "ctx", None), "tick_size", None)
            if ts is None:
                ts = getattr(getattr(runtime, "spec", None), "tick_size", None)
            tick_size = float(ts) if ts and float(ts) > 0 else 0.01  # last-resort

            tol = max(2.0 * tick_size, 1e-12)
            hard_tol = 3.0 * tol
            
            if diff > hard_tol:
                 logger.warning(
                     "⚠️ TP1 mismatch: calc=%.4f vs levels=%.4f (diff=%.6f > %.6f) symbol=%s"
                     expected_tp1, actual_tp1, diff, hard_tol, symbol
                 )
                 # Force correct TP1 if deviation is significant
                 if str(direction or "").upper() == "LONG":
                     tp_levels[0] = float(entry + atr * rocket_mult)
                 else:
                     tp_levels[0] = float(entry - atr * rocket_mult)
        
        mix_dict = self._build_mix_dict(delta, delta_z, indicators, confirmations)

        confidence = signal.get("confidence")
        if confidence is None:
            confidence = indicators.get("confidence")
        if confidence is None:
            confidence = 0.3
        try:
            confidence = float(confidence)
        except (TypeError, ValueError):
            confidence = 0.3
        confidence = max(0.0, min(1.0, confidence))

        # ------------------------------------------------------------------
        # Stage: build enriched_signal (raw stream payload)
        # ------------------------------------------------------------------
        enriched_signal = dict(signal)
        enriched_signal.update(
            {
                "strategy": enriched_signal.get("strategy", "cryptoorderflow")
                "source": "CryptoOrderFlow"
                "tf": enriched_signal.get("tf", "tick")
                "symbol": symbol
                "direction": direction,         # legacy
                "side": direction,              # normalized mirror for contract
                "entry": entry
                "sl": sl
                "tp_levels": tp_levels
                "lot": lot
                "atr": atr
                "timestamp": ts_ms,             # legacy mirror
                "ts": ts_ms,                    # common downstream
                "ts_ms": ts_ms,                 # contract
                "trail_after_tp1": self._normalize_trailing_flag(enriched_signal.get("trail_after_tp1"), symbol)
                "trail_profile": trail_profile
                # Full configuration snapshot for reproducibility
                "config_snapshot": {
                    "config": cfg
                    "calibrated_specs": getattr(runtime, "calibrated_specs", {})
                    "indicators": indicators
                    "runtime_meta": {
                        "spec_update_ts_ms": getattr(runtime, "spec_update_ts_ms", 0)
                        "gate_meta": enriched_signal.get("gate_meta", {})
                    }
                }
                # Evidence package for downstream analysis
                "evidence": {
                    "obi_stable_secs": float(indicators.get("obi_stable_secs", 0.0) or 0.0)
                    "obi_stability_score": float(indicators.get("obi_stability_score", 0.0) or 0.0)
                    "strong_gate_legs": int(indicators.get("strong_gate_legs", 0) or 0)
                    "strong_gate_scn": str(indicators.get("strong_gate_scn", "") or "")
                    "weak_recent_cnt": int(indicators.get("weak_recent_cnt", 0) or 0)
                    "weak_recent_frac": float(indicators.get("weak_recent_frac", 0.0) or 0.0)
                }
            }
        )

        # IDs: Crypto pipeline uses signal["signal_id"] as primary id. Mirror into sid for other consumers.
        try:
            sid = str(signal.get("signal_id") or enriched_signal.get("signal_id") or "").strip()
            if not sid:
                sid = f"signal:{symbol}:cryptoorderflow:{ts_ms}"
                enriched_signal["signal_id"] = sid
            enriched_signal["sid"] = sid
        except Exception:
            pass

        # Contract normalization (FAIL-OPEN)
        preprocess_signal_for_publish(enriched_signal, symbol=str(symbol), source="CryptoOrderFlow", logger=logger)

        # ------------------------------------------------------------------
        # PRE-PUBLISH GATES (Hard Data Quality + Regime/Session)
        # Эти гейты должны отсечь «отравленные» сигналы (stale touch/ATR, gaps, low depth, burst flip, drift).
        # По умолчанию они отключены (enabled=False в from_env), так что безопасно внедрять wiring.
        # ------------------------------------------------------------------
        veto_dec = None
        try:
            kind = str(indicators.get("kind") or signal.get("kind") or "")
            pp_ctx = SimpleNamespace(
                ts_event_ms=int(enriched_signal.get("ts_ms") or 0)
                ts=int(enriched_signal.get("ts_ms") or 0)
                data_quality_flags=enriched_signal.get("data_quality_flags") or {}
                atr_ts_ms=int(enriched_signal.get("atr_ts_ms") or 0)
                touch_is_stale=bool(enriched_signal.get("touch_is_stale") or False)
                l2_is_stale=bool(enriched_signal.get("l2_is_stale") or False)
                spread_bps=float(enriched_signal.get("spread_bps") or indicators.get("spread_bps") or 0.0)
                depth_bid_20=float(indicators.get("depth_bid_20") or 0.0)
                depth_ask_20=float(indicators.get("depth_ask_20") or 0.0)
                regime=str(indicators.get("liq_regime") or indicators.get("regime") or signal.get("liq_regime") or "")
                of=SimpleNamespace(
                    depth_bid_5=float(indicators.get("depth_bid_5") or 0.0)
                    depth_ask_5=float(indicators.get("depth_ask_5") or 0.0)
                    burst_flip_ratio=float(indicators.get("burst_flip_ratio") or indicators.get("burst_flip_ratio_60s") or 0.0)
                )
            )

            for _gate in (self._hard_dq_gate, self._rs_gate):
                if _gate is None:
                    continue
                try:
                    dec = _gate.evaluate(pp_ctx, symbol=str(symbol), kind=kind)
                except Exception:
                    continue
                if getattr(dec, "apply", False) and getattr(dec, "veto", False):
                    veto_dec = dec
                    break
        except Exception:
            veto_dec = None

        if veto_dec is not None:
            # Metrics
            try:
                pre_publish_veto_total.labels(
                    symbol=str(symbol), gate=str(veto_dec.gate), reason=str(veto_dec.reason_code)
                ).inc()
            except Exception:
                pass

            # Annotate payloads
            enriched_signal["pre_publish_veto"] = True
            enriched_signal["pre_publish_gate"] = str(veto_dec.gate)
            enriched_signal["pre_publish_reason"] = str(veto_dec.reason_code)
            if getattr(veto_dec, "notes", None):
                enriched_signal["pre_publish_notes"] = veto_dec.notes

            # Send to rejected stream for triage
            if self.publisher and self.publisher.r:
                try:
                    await self.publisher.r.xadd(
                        self._rejected_signal_stream
                        fields={
                            "symbol": str(symbol)
                            "gate": str(veto_dec.gate)
                            "reason": str(veto_dec.reason_code)
                            "ts_ms": str(int(enriched_signal.get("ts_ms") or 0))
                            "payload": json.dumps(enriched_signal, ensure_ascii=False)
                        }
                        maxlen=200000
                    )
                except Exception:
                    pass

            # Still emit audit record (deterministic) but stop before trade/notify sinks
            try:
                signal_stream = self.cryptoorderflow_signal_stream_template.format(symbol=symbol)
                audit_payload = {
                    "sid": enriched_signal.get("sid") or enriched_signal.get("signal_id") or ""
                    "signal_id": enriched_signal.get("signal_id") or ""
                    "symbol": symbol
                    "side": enriched_signal.get("side") or direction
                    "entry": entry
                    "sl": sl
                    "tp_levels": tp_levels
                    "lot": lot
                    "source": "CryptoOrderFlow"
                    "reason": signal.get("reason") or "delta_spike"
                    "confidence": confidence
                    "confidence01": confidence
                    "confidence_pct": confidence * 100.0
                    "atr": atr
                    "ts": ts_ms
                    "ts_ms": ts_ms
                    "pre_publish_veto": True
                    "pre_publish_gate": str(veto_dec.gate)
                    "pre_publish_reason": str(veto_dec.reason_code)
                    "indicators": indicators
                    "strategy": "cryptoorderflow"
                    "tf": "tick"
                }
                preprocess_signal_for_publish(audit_payload, symbol=str(symbol), source="CryptoOrderFlow", logger=logger)
                await self.publisher.xadd_json(
                    sink=StreamSink(name=str(signal_stream), field="data", maxlen=1000)
                    payload=audit_payload
                    symbol=str(symbol)
                )
            except Exception:
                pass

            try:
                await self._push_virtual_to_binance_queue(
                    sid=enriched_signal.get("sid") or enriched_signal.get("signal_id") or signal.get("signal_id") or ""
                    symbol=symbol
                    direction=direction
                    entry=entry
                    sl=sl
                    tp_levels=tp_levels
                    lot=lot
                    ts_ms=ts_ms
                    confidence=confidence
                    enriched_signal=enriched_signal
                    indicators=indicators
                    is_rejected_signal=True
                )
            except Exception as e:
                logger.warning("⚠️ Failed to push vetoed virtual to binance: %s", e)
            return

        # ==== Пересчитываем размер позиции по риску (гарантия лимита 5% депозита) ====
        # P2: Liquidity Scaling for Risk
        # 0.5 <= scale <= 2.0. We penalize if scale < 0.8.
        liq_scale = float(indicators.get("liquidity_scale", 1.0) or 1.0)
        risk_factor = 1.0
        if liq_scale < 0.8:
            # Linear ramp: scale=0.5 -> factor=0.5; scale=0.8 -> factor=1.0
            # factor = 0.5 + (scale - 0.5) * (0.5/0.3) = 0.5 + (scale-0.5)*1.666
            # Simplified: just clamp(scale, 0.5, 1.0) effectively?
            # User rec: "decrease lot/leverage if scale < 0.8".
            # Let's map 0.5..0.8 to 0.5..1.0
            risk_factor = max(0.5, min(1.0, liq_scale / 0.8))
            logger.info("⚠️ [RISK] (%s) Liquidity Scale %.2f < 0.8 -> Risk Factor %.2f", symbol, liq_scale, risk_factor)
        
        # Pull base risk from env or indicators
        base_risk_pct = float(os.getenv("RISK_PERCENT", "5.0"))
        if 0 < base_risk_pct < 0.5: base_risk_pct *= 100.0 # Sanity handle 0.05
        
        effective_risk_pct = base_risk_pct * risk_factor

        lot_risk, position_size_usd, deposit, leverage = calculate_position_size(
            symbol=symbol
            entry_price=entry
            sl_price=sl
            side=str(direction or "").upper()
            risk_percent=effective_risk_pct
        )
        lot = lot_risk
        if lot <= 0:
            logger.warning("🚫 [VETO] (%s) Risk veto: lot=0 (sl_floor/fee_risk). entry=%.8f sl=%.8f", symbol, entry, sl)
            return

        # ✅ Correct enriched_signal with risk-based lot and margin params
        enriched_signal["lot"] = lot
        enriched_signal["position_size_usd"] = position_size_usd
        enriched_signal["deposit"] = deposit
        enriched_signal["leverage"] = leverage

        # Determine validation status based on OFConfirm result
        # OFConfirm result is stored in indicators["of_confirm_ok"] from strategy.py
        of_confirm_ok = indicators.get("of_confirm_ok")
        of_confirm_reason = indicators.get("strong_gate_reason", "unknown")

        if of_confirm_ok == 1:
            validation_status = "passed"
            validation_reason = f"OFConfirm passed ({of_confirm_reason})"
        elif of_confirm_ok == 0:
            validation_status = "failed"
            validation_reason = f"OFConfirm failed: {indicators.get('of_confirm', {}).get('reason', of_confirm_reason)}"
        else:
            # of_confirm_ok not set or OFConfirm was not evaluated
            validation_status = "bypassed"
            validation_reason = "OFConfirm not evaluated"

        enriched_signal["validation_status"] = validation_status
        enriched_signal["validation_reason"] = validation_reason

        # --- SHADOW MODE: mark main signal as virtual if validation failed or shadowed
        gate_mode = str(indicators.get("of_gate_mode") or "").upper()
        if (gate_mode == "SHADOW" and validation_status == "failed") or indicators.get("gate_shadow_veto"):
            enriched_signal["is_virtual"] = 1
            if indicators.get("gate_shadow_veto"):
                enriched_signal["validation_status"] = "failed"
                enriched_signal["validation_reason"] = indicators.get("gate_reason", "SHADOW_VETO")
        # ---



        crypto_signal = CryptoSignal(
            sid=signal["signal_id"]
            symbol=symbol
            side=str(direction or "").upper()
            entry=entry
            sl=sl
            tp_levels=tp_levels
            lot=lot
            position_size_usd=position_size_usd
            deposit=deposit
            leverage=leverage
            atr=atr
            confidence=confidence
            ts=int(signal.get("tick_ts") or signal.get("generated_at"))
            source="CryptoOrderFlow"
            reason_mix=mix_dict
            confirmations=confirmations
            indicators=indicators
            trail_profile=trail_profile
            trail_after_tp1=self._normalize_trailing_flag(enriched_signal.get("trail_after_tp1"), symbol)
            config_params=signal.get("config_params") or {"strong_gate_ok": signal.get("indicators", {}).get("strong_gate_ok", 0)}
            validation_status=enriched_signal.get("validation_status")
            validation_reason=enriched_signal.get("validation_reason")
        )

        # Определяем, является ли сигнал слабым (Не проходит Gate и уверенность < 70%)
        strong_gate_ok = crypto_signal.config_params.get("strong_gate_ok", 0) if crypto_signal.config_params else 0
        is_weak = (int(strong_gate_ok) != 1 and confidence < 0.70)
        
        if is_weak:
            telegram_payload = None
            logger.info("🚫 [TELEGRAM] (%s) Signal is WEAK (strong_ok=%s, conf=%.2f). Skipping notify.", symbol, strong_gate_ok, confidence)
        else:
            telegram_payload = {
                "text": CryptoSignalFormatter.format_telegram_message(crypto_signal)
                "symbol": symbol
                "direction": crypto_signal.side
                "entry": f"{entry:.2f}"
                "stop": f"{sl:.2f}"
                "tp": ",".join(f"{tp:.2f}" for tp in tp_levels)
                "source": crypto_signal.source
                "reason": signal.get("reason") or "delta_spike"
                "timestamp": str(crypto_signal.ts)
            }

        # Build outbox envelope (dispatcher will apply notify gating itself).
        try:
            signal_stream = self.cryptoorderflow_signal_stream_template.format(symbol=symbol)
            audit_payload = {
                "sid": crypto_signal.sid
                "signal_id": crypto_signal.sid,   # canonical mirror
                "symbol": symbol
                "side": crypto_signal.side
                "entry": entry
                "sl": sl
                "tp_levels": tp_levels
                "lot": lot
                "source": "CryptoOrderFlow"
                "reason": signal.get("reason") or "delta_spike"
                "confidence": confidence
                "confidence01": confidence
                "confidence_pct": confidence * 100.0
                "atr": atr
                "ts": ts_ms
                "ts_ms": ts_ms
                "trail_after_tp1": self._normalize_trailing_flag(enriched_signal.get("trail_after_tp1"), symbol)
                "trail_profile": enriched_signal.get("trail_profile", "rocket_v1")
                "indicators": indicators
                "strategy": "cryptoorderflow"
                "tf": "tick"
            }

            env = build_outbox_envelope(
                sid=crypto_signal.sid
                symbol=symbol
                kind="crypto_orderflow"
                notify_payload=telegram_payload
                audit_payload={"payload": json.dumps(enriched_signal, ensure_ascii=False)}
                signal_stream_payload={"data": json.dumps(audit_payload, ensure_ascii=False)}
                audit_stream=self.raw_signal_stream
                signal_stream=signal_stream
            )
            
            logger.info(f"DEBUG_OUTBOX_ENV: keys={list(env.keys())} targets={list(env.get('targets', {}).keys()) if 'targets' in env else 'MISSING'}")

            # ✅ VALIDATION: Ensure envelope structure is correct (audit_payload/meta must not be on top level)
            if "audit_payload" in env or "meta" not in env or "targets" not in env:
                logger.error(f"❌ ({symbol}) Invalid envelope structure: audit_payload on top level or missing required fields")
                logger.error(f"   env keys: {list(env.keys())}")
                logger.error(f"   targets keys: {list(env.get('targets', {}).keys()) if 'targets' in env else 'MISSING'}")
                # Don't publish malformed envelope
                return

            env_json = dumps_env(env)
        except Exception as err:
            logger.error(f"❌ ({symbol}) Error building outbox envelope: {err}", exc_info=True)
            env_json = ""

        # Outbox path:
        #   - outbox-only: no direct publishing (dispatcher does it)
        #   - shadow: publish legacy + outbox (audit/compare during rollout)
        if env_json and (use_outbox or shadow_outbox):
            # Atomic outbox write + meta sidecar (DecisionTrace full)
            meta_obj = None
            try:
                # минимальный ctx только для trace (чтобы не тянуть весь объект)
                ctx_min = SimpleNamespace()
                setattr(ctx_min, "ts_ms", get_ny_time_millis())
                # если symbol/kind доступны в этой области — проставьте:
                try:
                    setattr(ctx_min, "symbol", str(env.get("symbol") or env.get("sym") or ""))
                    setattr(ctx_min, "kind", str(env.get("kind") or ""))
                except Exception:
                    pass
                if trace_enabled():
                    ensure_trace(ctx_min, sid=str(sid))
                    # detector stage timing (минимально)
                    trace_gate(ctx_min, stage="detector", name="service_emit", passed=True, veto=False, reason_code="OK", duration_ms=0.0)
                    meta_obj = build_trace_sidecar_meta_from_ctx(ctx=ctx_min, sid=str(sid))
            except Exception:
                meta_obj = None

            # env_json уже готов (как раньше). Пишем его как payload_obj.
            payload_obj = env  # dict envelope
            await atomic_xadd_async(
                self.publisher.r
                stream_key=str(outbox_stream)
                signal_id=str(sid)
                payload_obj=payload_obj
                kind=str(env.get("kind") or "")
                symbol=str(env.get("symbol") or "")
                ts=str(env.get("ts_ms") or "")
                meta_obj=meta_obj
            )
            
            if use_outbox:
                # Send notify even in outbox mode
                notify_enabled = True  # Force enable Telegram notifications
                if notify_enabled and self.publisher.r and telegram_payload:
                    logger.info("📱 [TELEGRAM] (%s) Attempting to send notify: publisher=%s, payload=%s", symbol, self.publisher.r is not None, telegram_payload is not None)
                    try:
                        notify_signal_every_n = int(os.getenv("CRYPTO_NOTIFY_SIGNAL_EVERY_N", os.getenv("NOTIFY_SIGNAL_EVERY_N", "1")))
                        msg_id = int(ts_ms)
                        counter_value = int(msg_id)
                        if counter_value % notify_signal_every_n == 0:
                            await self.publisher.r.xadd(
                                self.notify_stream
                                fields=telegram_payload
                                maxlen=20000
                            )
                            logger.info("✅ [TELEGRAM] (%s) Sent notify to %s", symbol, self.notify_stream)
                        else:
                            logger.debug("⏭️ [TELEGRAM] (%s) Skipped notify (rate limit)", symbol)
                    except Exception as exc:
                        logger.warning("⚠️ [TELEGRAM] (%s) Failed to send notify: %s", symbol, exc)
                elif not telegram_payload:
                    logger.debug("⏭️ [TELEGRAM] (%s) Skipped notify: payload=None (likely WEAK signal)", symbol)
                else:
                    logger.warning("❌ [TELEGRAM] (%s) Cannot send notify: enabled=%s, publisher=%s, payload=%s", symbol, notify_enabled, self.publisher.r is not None, telegram_payload is not None)

                # Also send to raw stream for ExecutionGateService compatibility
                pub = self.publisher
                try:
                    await pub.xadd_json(
                        sink=StreamSink(name=str(self.raw_signal_stream), field="payload", maxlen=100000)
                        payload=enriched_signal
                        symbol=str(symbol)
                    )
                    sampled_info(logger, "SIGNAL_RAW_STREAM", "📤 [SIGNAL] (%s) Also sent to %s for ExecutionGateService", symbol, self.raw_signal_stream)
                except Exception as e:
                    logger.warning("⚠️ [SIGNAL] (%s) Failed to send to raw stream: %s", symbol, e)

                # Feed TB Labeler (P45 fix) - Direct Path (copied here for outbox mode)
                if self.publish_of_inputs and self.of_inputs_stream and self.of_inputs_publish_enabled:
                    try:
                        await pub.xadd_json(
                            sink=StreamSink(name=str(self.of_inputs_stream), field="payload", maxlen=self.of_inputs_stream_maxlen)
                            payload=enriched_signal
                            symbol=str(symbol)
                        )
                    except Exception as e:
                        logger.warning("⚠️ [SIGNAL] (%s) Failed to send to of_inputs_stream: %s", symbol, e)

                # Stop here if we rely purely on outbox
                sampled_info(logger, "SIGNAL_PUBLISHED", "🚀 [SIGNAL] (%s) %s P=%s Published via Atomic Outbox: ID=%s", symbol, direction, entry, sid)
                await self._push_virtual_to_binance_queue(
                    sid=sid, symbol=symbol, direction=direction
                    entry=entry, sl=sl, tp_levels=tp_levels, lot=lot
                    ts_ms=ts_ms, confidence=confidence
                    enriched_signal=enriched_signal, indicators=indicators
                )
                return

        # ------------------------------------------------------------------
        # DIRECT PUBLISHING (Failback / Mixed Mode)
        # ------------------------------------------------------------------
        
        # 1) Telegram Notify
        notify_enabled = True  # Send all signals but with validation status
        if notify_enabled and self.publisher.r and telegram_payload:
             # Rate limiting implemented via modulo check (simple but effective for flood control)
             notify_signal_every_n = int(os.getenv("CRYPTO_NOTIFY_SIGNAL_EVERY_N", os.getenv("NOTIFY_SIGNAL_EVERY_N", "1")))
             msg_id = int(ts_ms)
             counter_value = int(msg_id) # Proxy monotonic
             
             try:
                # We reuse AsyncSignalPublisher to push to notify stream? No, it's a specific format.
                # AsyncSignalPublisher writes structured JSON. Notify expects specific fields.
                # We'll use self.publisher.r (Redis client) directly.
                 if counter_value % notify_signal_every_n == 0:
                     await self.publisher.r.xadd(
                         self.notify_stream
                         fields=telegram_payload
                         maxlen=20000
                     )
             except Exception as exc:
                 logger.warning("⚠️ (%s) Не удалось опубликовать в %s: %s", symbol, self.notify_stream, exc)
        
        # 2) Raw Stream via AsyncSignalPublisher
        pub = self.publisher
        try:
             # We create a new instance? No, reuse 'pub'
             # Note: logic in strategy created a new AsyncSignalPublisher, but we have one injected.
             # We should use the injected one.
             await pub.xadd_json(
                 sink=StreamSink(name=str(self.raw_signal_stream), field="payload", maxlen=100000)
                 payload=enriched_signal
                 symbol=str(symbol)
             )
        except Exception:
             pass

        # Feed TB Labeler (P45 fix) - Direct Path
        if self.publish_of_inputs and self.of_inputs_stream and self.of_inputs_publish_enabled:
            try:
                await pub.xadd_json(
                    sink=StreamSink(name=str(self.of_inputs_stream), field="payload", maxlen=self.of_inputs_stream_maxlen)
                    payload=enriched_signal
                    symbol=str(symbol)
                )
            except Exception:
                pass

        # 3) Audit Payload via AsyncSignalPublisher
        preprocess_signal_for_publish(audit_payload, symbol=str(symbol), source="CryptoOrderFlow", logger=logger)
        await pub.xadd_json(
            sink=StreamSink(name=str(signal_stream), field="data", maxlen=1000)
            payload=audit_payload
            symbol=str(symbol)
        )

        # ------------------------------------------------------------------
        # 4) Bookkeeping (Deterministic signal audit/cooldown)
        # Moved to the end to ensure it only updates on actual successful emit path.
        # ------------------------------------------------------------------
        try:
            # Update last emission state
            runtime.last_signal_ts = ts_ms
            runtime.last_emit_ts_ms = ts_ms
            runtime.last_emit_dir = direction
            
            # Record pressure event
            runtime.pressure.record_emit(ts_ms)
            
            # Backward compat for SMT logic: update last_of_strong_ts_ms if strong confirmation passed
            ok = int(indicators.get("of_confirm_ok", 0) or indicators.get("strong_gate_ok", 0) or 0)
            if ok == 1:
                runtime.last_of_strong_ts_ms = ts_ms
                runtime.last_of_dir = direction

            # Persist last strong metadata (useful for audits)
            runtime.last_of_confirm_score = float(indicators.get("of_confirm_score", 0.0) or 0.0)
            runtime.last_strong_gate_have = int(indicators.get("strong_gate_have", 0) or 0)
            runtime.last_strong_gate_need = int(indicators.get("strong_gate_need", 0) or 0)
            runtime.last_strong_gate_scn = str(indicators.get("strong_gate_scn", "") or "")

        except Exception as e:
            logger.debug("⚠️ publish_signal final bookkeeping error: %s", e)

        # Push virtual copy to Binance executor (direct publish path)
        await self._push_virtual_to_binance_queue(
            sid=sid, symbol=symbol, direction=direction
            entry=entry, sl=sl, tp_levels=tp_levels, lot=lot
            ts_ms=ts_ms, confidence=confidence
            enriched_signal=enriched_signal, indicators=indicators
        )

    async def _push_virtual_to_binance_queue(
        self
        *
        sid: str
        symbol: str
        direction: str
        entry: float
        sl: float
        tp_levels: List[float]
        lot: float
        ts_ms: int
        confidence: float
        enriched_signal: Dict[str, Any]
        indicators: Dict[str, Any]
        is_rejected_signal: bool = False
    ) -> None:
        """Push failed/rejected orderflow signals to orders:queue:binance as is_virtual=1.

        Controlled by BINANCE_VIRTUAL_ORDERS_ENABLED=1 (default: disabled).
        Only pushes if validation_status="failed" or is_rejected_signal=True AND confidence is high.
        BinanceExecutor reads is_virtual=1 and routes to demo/testnet client.
        """
        if not os.getenv("BINANCE_VIRTUAL_ORDERS_ENABLED", "0").lower() in {"1", "true", "yes", "on"}:
            return
        if not self.publisher or not self.publisher.r:
            return

        validation_status = str(enriched_signal.get("validation_status") or "").lower()
        if not is_rejected_signal and validation_status != "failed":
            return

        min_conf_pct = float(os.getenv("CRYPTO_SIGNAL_MIN_CONF", os.getenv("SIGNAL_MIN_CONF", "70")))
        if 0 < min_conf_pct <= 1:
            min_conf_pct *= 100.0
        if confidence < (min_conf_pct / 100.0):
            # Debug skipped conditionally so as not to spam
            return

        try:
            binance_queue = os.getenv("ORDERS_QUEUE_BINANCE", "orders:queue:binance")
            order_payload = {
                "action": "open"
                "sid": str(sid)
                "symbol": str(symbol)
                "side": str(direction)
                "qty": float(lot)
                "type": "MARKET"
                "entry": float(entry)
                "sl": float(sl)
                "tp_levels": [float(x) for x in (tp_levels or [])]
                "is_virtual": 1
                "source": str(enriched_signal.get("source") or "CryptoOrderFlow")
                "strategy": str(enriched_signal.get("strategy") or "cryptoorderflow")
                "confidence": float(confidence)
                "confidence_pct": float(confidence) * 100.0
                "ts_ms": int(ts_ms)
                "trail_after_tp1": bool(enriched_signal.get("trail_after_tp1", False))
                "trail_profile": str(enriched_signal.get("trail_profile") or "rocket_v1")
                "atr": float(indicators.get("atr", 0.0) or 0.0)
                "validation_status": "failed" if is_rejected_signal else str(enriched_signal.get("validation_status") or "")
            }
            await self.publisher.r.rpush(
                binance_queue
                json.dumps(order_payload, ensure_ascii=False)
            )
            logger.info(
                "🚀 [BINANCE-VIRTUAL] (%s) Order pushed to %s sid=%s side=%s entry=%.2f conf=%.0f%%"
                symbol, binance_queue, sid, direction, entry, confidence * 100.0
            )
        except Exception as exc:
            logger.warning("⚠️ [BINANCE-VIRTUAL] (%s) Failed to push to binance queue: %s", symbol, exc)



    async def send_telegram_report(self, text: str, source: str = "report", symbol: str = "") -> None:
        """Send arbitrary report text to Telegram via notify stream (type=report)."""
        try:
            ts_ms = str(get_ny_time_millis())
            fields = {
                "type": "report"
                "text": str(text or "")
                "source": str(source or "report")
                "symbol": str(symbol or "")
                "ts_ms": ts_ms
            }
            # notify_worker.py handles type=report with priority
            await self.publisher.r.xadd(
                self.notify_stream
                fields=fields
                maxlen=int(getattr(self, "notify_maxlen", 20000) or 20000)
                approximate=True
            )
            logger.info("📱 [TELEGRAM-REPORT] sent: source=%s symbol=%s", fields["source"], fields["symbol"])
        except Exception as exc:
            logger.warning("⚠️ [TELEGRAM-REPORT] failed: %s", exc)

    def _get_rocket_multiplier(self, symbol: str) -> float:
        """
        Возвращает множитель для TP1 в профиле rocket_v1.
        Ищет в ENV: ROCKET_TP1_ATR_MULT_{SYMBOL} (напр. ROCKET_TP1_ATR_MULT_BTCUSDT)
        Fallback: ROCKET_TP1_ATR_MULT (дефолт 0.78)
        
        ✅ NEW: Добавлен clamp(0.5..10.0) и логгирование некорректных значений.
        """
        env_var = f"ROCKET_TP1_ATR_MULT_{symbol.upper()}"
        val = os.getenv(env_var)
        source = env_var
        
        if not val:
            val = os.getenv("ROCKET_TP1_ATR_MULT", "0.78")
            source = "ROCKET_TP1_ATR_MULT"
            
        try:
            m = float(val)
        except (ValueError, TypeError):
            logger.warning("⚠️ Некорректное значение множителя %s=%r. Используем дефолт 0.78", source, val)
            return 0.78
            
        # Clamp 0.5 .. 10.0
        if m < 0.5 or m > 10.0:
            logger.warning("⚠️ Множитель %s=%.2f вне диапазона (0.5..10.0). Применяем clamp.", source, m)
            m = max(0.5, min(10.0, m))
            
        return m
    
    def _sigmoid_abs(self, x: float, k: float = 1.0) -> float:
        """
        Sigmoid activation for absolute value:
        Maps [0, inf) -> [0, 1)
        """
        ax = abs(x)
        return 1.0 - (1.0 / (1.0 + k * ax))

    def _build_mix_dict(
        self
        delta: float
        delta_z: float
        indicators: Optional[Dict[str, Any]]
        confirmations: Sequence[str]
    ) -> Dict[str, float]:
        mix: Dict[str, float] = {}

        # ✅ FIX: delta_z can be 0.0 (valid z-score), don't check for None only
        if delta_z is not None:
            mix["p_delta"] = self._sigmoid_abs(delta_z, k=0.5)
            mix["p_speed"] = abs(delta_z)
        # ✅ FIX: delta can be 0.0 (valid delta), don't check for None only
        if delta is not None:
            mix["delta"] = abs(delta)
        if indicators:
            if "obi" in indicators:
                mix["p_cluster"] = float(indicators.get("obi"))
            if "confidence" in indicators:
                mix["confidence"] = float(indicators.get("confidence"))

        if confirmations:
            mix["confirmations_count"] = float(len(confirmations))

        return mix

    def _normalize_trailing_flag(self, value: Any, symbol: Optional[str] = None) -> bool:
        """
        Возвращает финальный флаг трейлинга.
        """
        explicit_flag: Optional[bool] = None
        if value is not None:
            try:
                if isinstance(value, str):
                    explicit_flag = value.lower() in ("1", "true", "yes", "on")
                else:
                    explicit_flag = bool(value)
            except Exception:
                explicit_flag = False

        # Глобальный флаг FORCE_TRAIL_AFTER_TP1 мы берем из env
        # Но в SignalPipeline у нас нет прямого доступа к self.force_trail_after_tp1 как в сервисе
        # Предполагаем, что он передается или читаем из env каждый раз (это не критично)
        force_trail = os.getenv("FORCE_TRAIL_AFTER_TP1", "0").lower() in ("1", "true", "yes", "on")
        
        # Spec trailing is harder without runtime.spec access easily, but let's assume default false for now
        # or rely on runtime.calibrated_specs if available.
        # Actually logic in Strategy used `self._env_bool` which reads env.
        
        if explicit_flag is not None:
            return explicit_flag
        
        return force_trail

    def _calculate_levels(
        self
        runtime: SymbolRuntime
        entry: float
        side: str
        indicators: Dict[str, Any]
        trail_profile: Optional[str] = None
    ) -> Tuple[float, List[float], float, float]:
        cfg = runtime.config
        atr = float(indicators.get("atr", 0.0) or 0.0)
        atr_ts_ms = 0
        # Use canonical TF resolver (single source of truth)
        atr_tf = runtime.get_atr_tf_selected()
        indicators["atr_tf_used"] = atr_tf
        # Prefer cache + sanity selection when atr not provided by signal
        if atr <= 0:
            try:
                # Deterministic-ish "now" for age calculation:
                # prefer signal ts_ms if present; else wall time.
                nm = 0
                try:
                    nm = int(indicators.get("ts_ms", 0) or indicators.get("tick_ts", 0) or indicators.get("generated_at", 0) or 0)
                except Exception:
                    nm = 0
                prefer_src = ""
                try:
                    # check if we have a robust enough preference
                    if int(runtime.dynamic_cfg.get("atr_src_ready", 0) or 0) == 1:
                        prefer_src = str(runtime.dynamic_cfg.get("atr_src_pref", "") or "")
                except Exception:
                    prefer_src = ""
                
                # Use injected ATR cache
                if self.atr_cache:
                    atr, atr_meta = self.atr_cache.get_with_meta(symbol=runtime.symbol, timeframe=atr_tf, now_ms=(nm if nm > 0 else None), prefer_src=prefer_src)
                    atr = float(atr or 0.0)
                    if isinstance(atr_meta, dict):
                        indicators["atr_src"] = str(atr_meta.get("src") or atr_meta.get("source") or "na")
                        indicators["atr_ts_ms"] = int(atr_meta.get("ts_ms", 0) or 0)
                        indicators["atr_age_ms"] = int(atr_meta.get("age_ms", 0) or 0)
                        indicators["atr_consistency"] = float(atr_meta.get("consistency", 1.0) or 1.0)
                        indicators["atr_cons_ok"] = int(atr_meta.get("cons_ok", 1) or 1)
                        indicators["atr_candidates_n"] = int(atr_meta.get("candidates_n", 0) or 0)
                        atr_ts_ms = int(atr_meta.get("ts_ms", 0) or 0)
                        if prefer_src:
                            indicators["atr_src_prefer"] = str(prefer_src)
            except Exception:

                atr = 0.0

        # Always expose atr_bps_exec for unified gates/debug
        try:
            if float(entry) > 0 and float(atr) > 0:
                indicators["atr_bps_exec"] = float(10000.0 * (float(atr) / float(entry)))
        except Exception:
            pass

        # Final ATR fallback (absolute last resort)
        if atr <= 0:
            symbol_fallbacks = {
                "BTCUSDT": 30.0
                "ETHUSDT": 4.0
                "BNBUSDT": 0.5
                "SOLUSDT": 0.3
            }
            atr = symbol_fallbacks.get(runtime.symbol, entry * 0.0003)
            indicators["atr_src"] = "fallback-symbol"
            indicators["atr_sanity_reason"] = "no_valid_atr_found"
            indicators["atr_sanity_ok"] = 1

        lot = indicators.get("lot")
        if lot is None:
            lot = indicators.get("tick_qty") or indicators.get("delta") or 1.0
            lot = max(float(lot), cfg.get("min_lot", 0.01))

        def rr_levels(rr_str: str) -> List[float]:
            try:
                return [float(x.strip()) for x in rr_str.split(",") if x.strip()]
            except Exception:
                return [1.3, 2.0, 2.7]

        if str(cfg.get("stop_mode", "ATR")).upper() == "ATR":
            stop_dist = atr * cfg.get("stop_atr_mult", 0.6)
        elif str(cfg.get("stop_mode", "ATR")).upper() == "PCT":
            stop_dist = entry * cfg.get("stop_pct", 0.2) / 100
        else:
            stop_dist = cfg.get("stop_points", 1.0)

        # Проверяем, используется ли профиль rocket_v1
        if not trail_profile:
            # Пытаемся получить из конфигурации или индикаторов
            trail_profile = cfg.get("trail_profile") or indicators.get("trail_profile") or cfg.get("default_trail_profile", "rocket_v1")
        
        # Override SL multiplier with calibrated value if available
        # Only valid for ATR mode and rocket_v1 (or generic trailing if desired)
        if trail_profile == "rocket_v1" and str(cfg.get("stop_mode", "ATR")).upper() == "ATR":
            try:
                calib_dist = runtime.calibrated_specs.get("trailing", {}).get("tp1_offset_atr")
                if calib_dist is not None:
                    try:
                        calib_mult = float(calib_dist)
                        if calib_mult > 0:
                            stop_dist = atr * calib_mult
                            logger.debug("🎯 Using calibrated SL mult=%.2f for %s (dist=%.2f)", calib_mult, runtime.symbol, stop_dist)
                    except ValueError:
                        pass
            except Exception:
                pass

        # ------------------------------------------------------------------
        # LIQMAP risk diagnostic: would we need to WIDEN SL to be beyond an
        # adverse liquidation cluster? (Risk discipline: widening => hard veto.)
        #
        # We do NOT change SL here. We only compute diagnostics/flags so that
        # publish_signal() can optionally hard-veto the trade before publishing.
        # ------------------------------------------------------------------
        try:
            base_sl_bps = (float(stop_dist) / float(entry)) * 10000.0 if float(entry) > 0.0 else 0.0
            indicators['liqmap_sl_base_bps'] = float(base_sl_bps)

            side_u = str(side).upper()
            if side_u == 'LONG':
                reco_bps = float(indicators.get('liqmap_sl_reco_bps_long') or indicators.get('liqmap_sl_reco_bps') or 0.0)
            else:
                reco_bps = float(indicators.get('liqmap_sl_reco_bps_short') or indicators.get('liqmap_sl_reco_bps') or 0.0)

            indicators['liqmap_sl_reco_bps'] = float(reco_bps)

            if base_sl_bps > 0.0 and reco_bps > 0.0:
                ratio = float(reco_bps) / float(base_sl_bps)
                indicators['liqmap_sl_widen_ratio'] = float(ratio)
                cap = float(os.getenv('LIQMAP_SL_WIDEN_CAP', '1.25') or 1.25)
                indicators['liqmap_sl_widen_needed'] = 1 if ratio > cap else 0
            else:
                indicators['liqmap_sl_widen_ratio'] = 0.0
                indicators['liqmap_sl_widen_needed'] = 0
        except Exception:
            pass

        # Для rocket_v1: TP1 = MULT * ATR, остальные TP через RR
        rocket_mult = self._get_rocket_multiplier(runtime.symbol)
        is_rocket_v1 = (trail_profile == "rocket_v1")
        
        # Логируем для отладки (sample every 10000th message)
        if is_rocket_v1:
            tp1_dist = atr * rocket_mult
            rocket_v1_sampler = LogSamplerFactory.get_sampler("ROCKET_V1", 10000)
            if rocket_v1_sampler.should_log(f"rocket_v1_{runtime.symbol}"):
                logger.info("🎯 rocket_v1 detected in _calculate_levels: symbol=%s, atr=%.2f, mult=%.2f, tp1_dist=%.2f", 
                           runtime.symbol, atr, rocket_mult, tp1_dist)
        else:
            logger.debug("_calculate_levels: trail_profile=%s (not rocket_v1), will use RR method", trail_profile)
        
        if side.upper() == "LONG":
            sl = entry - stop_dist
            tp1_dist = atr * rocket_mult if is_rocket_v1 else stop_dist * rr_levels(cfg.get("tp_rr", "1.3,2.0,2.7"))[0]
            
            # Base calculation
            if is_rocket_v1:
                # TP1 = mult ATR
                tp1 = entry + tp1_dist
                rr_list = rr_levels(cfg.get("tp_rr", "1.3,2.0,2.7"))
                
                # Default RR-based potential TPs
                tp2_potential = entry + stop_dist * (rr_list[1] if len(rr_list) > 1 else 2.0)
                tp3_potential = entry + stop_dist * (rr_list[2] if len(rr_list) > 2 else 2.7)
                
                # Enforce monotonicity: TP2/TP3 must be significantly further than TP1
                # If TP1 is very far (high rocket_mult), scale TP2/TP3 relative to TP1
                # Strategy: TP2 >= max(RR_based, TP1_dist * 1.5)
                #           TP3 >= max(RR_based, TP1_dist * 2.0)
                tp2_dist = max(tp2_potential - entry, tp1_dist * 1.5)
                tp3_dist = max(tp3_potential - entry, tp1_dist * 2.0)
                
                tp2 = entry + tp2_dist
                tp3 = entry + tp3_dist
                
                tps = [tp1, tp2, tp3]
                logger.debug(
                    "✅ rocket_v1 LONG adjusted: TP1=%.2f (%.2f ATR), TP2=%.2f (%.2f ATR), TP3=%.2f (%.2f ATR)"
                    tp1, tp1_dist/atr, tp2, tp2_dist/atr, tp3, tp3_dist/atr
                )
            else:
                # Standard RR logic
                tps = [entry + stop_dist * rr for rr in rr_levels(cfg.get("tp_rr", "1.3,2.0,2.7"))]

        else: # SHORT
            sl = entry + stop_dist
            tp1_dist = atr * rocket_mult if is_rocket_v1 else stop_dist * rr_levels(cfg.get("tp_rr", "1.3,2.0,2.7"))[0]
            
            if is_rocket_v1:
                # TP1 = mult ATR
                tp1 = entry - tp1_dist
                rr_list = rr_levels(cfg.get("tp_rr", "1.3,2.0,2.7"))
                
                # Default RR-based potential TPs
                tp2_potential = entry - stop_dist * (rr_list[1] if len(rr_list) > 1 else 2.0)
                tp3_potential = entry - stop_dist * (rr_list[2] if len(rr_list) > 2 else 2.7)
                
                # Enforce monotonicity for SHORT (distances are positive)
                # Strategy: TP2_dist >= max(RR_dist, TP1_dist * 1.5)
                tp2_dist = max(entry - tp2_potential, tp1_dist * 1.5)
                tp3_dist = max(entry - tp3_potential, tp1_dist * 2.0)
                
                tp2 = entry - tp2_dist
                tp3 = entry - tp3_dist
                
                tps = [tp1, tp2, tp3]
                logger.debug(
                    "✅ rocket_v1 SHORT adjusted: TP1=%.2f (%.2f ATR), TP2=%.2f (%.2f ATR), TP3=%.2f (%.2f ATR)"
                    tp1, tp1_dist/atr, tp2, tp2_dist/atr, tp3, tp3_dist/atr
                )
            else:
                # Standard RR logic
                tps = [entry - stop_dist * rr for rr in rr_levels(cfg.get("tp_rr", "1.3,2.0,2.7"))]
        
        # FINAL SAFETY: Sort TPs by distance from entry to guarantee order 1 < 2 < 3
        # abs(tp - entry) makes it direction-agnostic
        tps.sort(key=lambda x: abs(x - entry))

        return sl, tps, float(lot), float(atr)

    async def send_telegram_report(self, text: str, source: str, symbol: str) -> None:
        """
        Отправляет телеграм отчет в notify stream.

        :param text: Текст отчета
        :param source: Источник отчета
        :param symbol: Символ/инструмент
        """
        try:
            await self.publisher.r.xadd(
                self.notify_stream
                fields={"type": "report", "text": text, "source": source, "symbol": symbol, "ts_ms": str(get_ny_time_millis())}
                maxlen=self.notify_maxlen
                approximate=True
            )
            logger.info("📱 [TELEGRAM-REPORT] Sent report for %s from %s", symbol, source)
        except Exception as exc:
            logger.warning("⚠️ [TELEGRAM-REPORT] Failed to send report for %s: %s", symbol, exc)
