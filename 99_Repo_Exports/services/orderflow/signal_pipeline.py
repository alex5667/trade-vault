from __future__ import annotations

import logging
import os
import time
import json
from typing import Any, Dict, List, Optional, Tuple, Sequence
from types import SimpleNamespace

from services.orderflow.runtime import SymbolRuntime
from services.async_signal_publisher import AsyncSignalPublisher, StreamSink
from services.orderflow.configuration import _ensure_list_levels
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
)
from services.orderflow.utils import session_utc
from handlers.crypto_orderflow.utils.log_sampler import LogSamplerFactory, sampled_info
from handlers.crypto_orderflow.utils.pre_publish_gates import HardDataQualityGate, RegimeSessionGate

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
        
        # Initialize log samplers for signal messages (every 10000th message)
        LogSamplerFactory.get_sampler("SIGNAL_RAW_STREAM", 10000)
        LogSamplerFactory.get_sampler("SIGNAL_PUBLISHED", 10000)

        # Pre-publish gates (fail-open unless enabled by ENV)
        self._hard_dq_gate = HardDataQualityGate.from_env()
        self._rs_gate = RegimeSessionGate.from_env()
        self._rejected_signal_stream = os.getenv("CRYPTO_REJECTED_SIGNAL_STREAM", "signals:crypto:rejected")

        # Sequential counter for deterministic Telegram notify rate-limiting
        self._notify_counter: int = 0

    @property
    def FEES_BPS_RT(self) -> float:
        return float(os.getenv("FEES_BPS_RT", "10"))     # 10 bps RT (0.05% per side)
    
    @property
    def TP_BPS_BUFFER(self) -> float:
        return float(os.getenv("TP_BPS_BUFFER", "4")) # 4 bps buffer (tightened)

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
            depth_bid_5=float(getattr(runtime, "last_depth_bid_5", 0.0) or 0.0),
            depth_ask_5=float(getattr(runtime, "last_depth_ask_5", 0.0) or 0.0),
            atr_ts_ms=int(indicators.get("atr_ts_ms") or signal.get("atr_ts_ms") or 0),
            regime=str(indicators.get("regime") or signal.get("regime") or "unknown"),
            spread_bps=float(micro.get("spread_bps") or 0.0),
        )

        # Main ctx expected by gates
        ctx = SimpleNamespace(
            ts_event_ms=int(sig_ts_ms),
            ts_ms=int(sig_ts_ms),
            ts=int(sig_ts_ms),
            spread_bps=float(micro.get("spread_bps") or getattr(runtime, "last_spread_bps", 0.0) or 0.0),
            regime=str(indicators.get("regime") or signal.get("regime") or "unknown"),
            session=str(signal.get("session") or indicators.get("session") or "na"),
            tf=str(signal.get("tf") or indicators.get("tf") or "na"),
            venue=str(signal.get("venue") or indicators.get("venue") or "binance"),
            touch_is_stale=bool(signal.get("touch_is_stale") or indicators.get("touch_is_stale") or False),
            data_quality_flags=dq_flags,
            of=of,
            redis=getattr(runtime, "redis_client", None),
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
            sig_ts = int(signal.get("tick_ts") or signal.get("ts_ms") or (time.time() * 1000))
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
        
        # --- SHADOW REQUIRE_STRONG_CONFIRMATION (SignalPipeline) ---
        # Note: Enforcement is handled in strategy.py early gate.
        if runtime.config.get("require_strong_confirmation"):
            gate_ok = bool(int(indicators.get("strong_gate_ok", 0) or indicators.get("of_confirm_ok", 0) or 0))
            if not gate_ok:
                # Log veto (sampled)
                logger.info(
                    "🛡️ [GATE-PIPELINE-SHADOW] RequireStrongConfirmation: signal=%s vetoed by strong gate. need=%s have=%s legs=%s",
                    signal.get("signal_id"),
                    indicators.get("strong_gate_need"),
                    indicators.get("strong_gate_have"),
                    indicators.get("strong_gate_legs"),
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
                     "🛡️ [GATE] Strong Gate: scn=%s have=%s need=%s",
                     indicators.get("strong_gate_scn"),
                     indicators.get("strong_gate_have"),
                     indicators.get("strong_gate_need"),
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
                return iv if iv > 0 else int(time.time() * 1000)
            except Exception:
                return int(time.time() * 1000)

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
                 ok=bool(int(indicators.get("strong_gate_ok", 0))),
                 scenario=str(indicators.get("strong_gate_scn", "na")),
                 need=int(indicators.get("strong_gate_need", 0)),
                 have=int(indicators.get("strong_gate_have", 0)),
                 a=int(indicators.get("strong_gate_legs", {}).get("A", 0) if isinstance(indicators.get("strong_gate_legs"), dict) else 0),
                 b=int(indicators.get("strong_gate_legs", {}).get("B", 0) if isinstance(indicators.get("strong_gate_legs"), dict) else 0),
                 c=int(indicators.get("strong_gate_legs", {}).get("C", 0) if isinstance(indicators.get("strong_gate_legs"), dict) else 0),
                 reason="gate_decision",
                 legs=indicators.get("strong_gate_legs") if isinstance(indicators.get("strong_gate_legs"), dict) else None
             )

        # 2. Confidence Parts
        conf_parts = indicators.get("confidence_breakdown")

        # 3. Create Typed Payload
        sig_payload = SignalPayload(
            confirmations={c.split("=")[0]: c.split("=")[1] for c in confirmations if "=" in c},
            indicators=indicators,
            gate=gate_decision,
            confidence_parts=conf_parts,
            rejection_reason=reason,
            ts_ms=ts_ms,
            symbol=symbol,
            signal_id=str(signal.get("signal_id", "")),
        )
        
        # Export back to dict for legacy compatibility
        # (This essentially enriches the raw payload with structured data)
        evidence_dict = sig_payload.to_dict()

        # Build final stream payload (legacy structure + new evidence)
        payload = {
            "signal_id": str(signal.get("signal_id", "")),
            "symbol": runtime.symbol,
            "direction": direction,
            "entry": float(entry),
            "sl": float(sl),
            "tp_levels": [float(x) for x in tp_levels],
            "lot": float(lot),
            "atr": float(atr),
            "confidence": float(signal.get("confidence", 0.0) or 0.0), # Will be re-calculated or passed
            "reason": str(signal.get("reason", "unknown")),
            "ts_ms": int(ts_ms),
            "generated_at": int(ts_ms),
            "written_at": int(time.time() * 1000),
            "evidence": evidence_dict, # <--- NEW FIELD
            
            # --- Fields kept for backward compatibility with raw consumers ---
            "delta": delta,
            "delta_z": delta_z,
            "tick_qty": indicators.get("tick_qty"),
            "confirmations": confirmations,
            "indicators": indicators,
        }
        
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
                fees_bps_rt=float(self.FEES_BPS_RT),
                tp_bps_buffer=float(self.TP_BPS_BUFFER),
                tp1_share=tp1_share_actual,
                rocket_mult=rocket_mult,
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
                    fees_bps_rt=float(self.FEES_BPS_RT),
                    tp_bps_buffer=float(self.TP_BPS_BUFFER),
                    tp1_share=tp1_share_actual,
                    rocket_mult=rocket_mult,
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
                         logger.info("ℹ️ %s ATR unified VETO triggered (%s): atr_bps=%.2f < th=%.2f (relaxed_th=%.2f) | %s",
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
                     "⚠️ TP1 mismatch: calc=%.4f vs levels=%.4f (diff=%.6f > %.6f) symbol=%s",
                     expected_tp1, actual_tp1, diff, hard_tol, symbol
                 )
                 # Force correct TP1 if deviation is significant
                 if str(direction or "").upper() == "LONG":
                     tp_levels[0] = float(entry + atr * rocket_mult)
                 else:
                     tp_levels[0] = float(entry - atr * rocket_mult)
        
        # Ensure direction is available in indicators for p_cluster normalization
        indicators["direction"] = str(direction or "").upper()
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
                "strategy": enriched_signal.get("strategy", "cryptoorderflow"),
                "source": "CryptoOrderFlow",
                "tf": enriched_signal.get("tf", "tick"),
                "symbol": symbol,
                "direction": direction,         # legacy
                "side": direction,              # normalized mirror for contract
                "entry": entry,
                "sl": sl,
                "tp_levels": tp_levels,
                "lot": lot,
                "atr": atr,
                "timestamp": ts_ms,             # legacy mirror
                "ts": ts_ms,                    # common downstream
                "ts_ms": ts_ms,                 # contract
                "trail_after_tp1": self._normalize_trailing_flag(enriched_signal.get("trail_after_tp1"), symbol),
                "trail_profile": trail_profile,
                # Full configuration snapshot for reproducibility
                "config_snapshot": {
                    "config": cfg,
                    "calibrated_specs": getattr(runtime, "calibrated_specs", {}),
                    "indicators": indicators,
                    "runtime_meta": {
                        "spec_update_ts_ms": getattr(runtime, "spec_update_ts_ms", 0),
                        "gate_meta": enriched_signal.get("gate_meta", {}),
                    }
                },
                # Evidence package for downstream analysis
                "evidence": {
                    "obi_stable_secs": float(indicators.get("obi_stable_secs", 0.0) or 0.0),
                    "obi_stability_score": float(indicators.get("obi_stability_score", 0.0) or 0.0),
                    "strong_gate_legs": int(indicators.get("strong_gate_legs", 0) or 0),
                    "strong_gate_scn": str(indicators.get("strong_gate_scn", "") or ""),
                    "weak_recent_cnt": int(indicators.get("weak_recent_cnt", 0) or 0),
                    "weak_recent_frac": float(indicators.get("weak_recent_frac", 0.0) or 0.0),
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
                ts_event_ms=int(enriched_signal.get("ts_ms") or 0),
                ts=int(enriched_signal.get("ts_ms") or 0),
                data_quality_flags=enriched_signal.get("data_quality_flags") or {},
                atr_ts_ms=int(enriched_signal.get("atr_ts_ms") or 0),
                touch_is_stale=bool(enriched_signal.get("touch_is_stale") or False),
                l2_is_stale=bool(enriched_signal.get("l2_is_stale") or False),
                spread_bps=float(enriched_signal.get("spread_bps") or indicators.get("spread_bps") or 0.0),
                depth_bid_20=float(indicators.get("depth_bid_20") or 0.0),
                depth_ask_20=float(indicators.get("depth_ask_20") or 0.0),
                regime=str(indicators.get("liq_regime") or indicators.get("regime") or signal.get("liq_regime") or ""),
                of=SimpleNamespace(
                    depth_bid_5=float(indicators.get("depth_bid_5") or 0.0),
                    depth_ask_5=float(indicators.get("depth_ask_5") or 0.0),
                    burst_flip_ratio=float(indicators.get("burst_flip_ratio") or indicators.get("burst_flip_ratio_60s") or 0.0),
                ),
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
                    symbol=str(symbol), gate=str(getattr(veto_dec, "gate", "UnknownGate")), reason=str(veto_dec.reason_code)
                ).inc()
            except Exception:
                pass

            # Annotate payloads
            enriched_signal["pre_publish_veto"] = True
            enriched_signal["pre_publish_gate"] = str(getattr(veto_dec, "gate", "UnknownGate"))
            enriched_signal["pre_publish_reason"] = str(veto_dec.reason_code)
            if getattr(veto_dec, "notes", None):
                enriched_signal["pre_publish_notes"] = veto_dec.notes

            # Send to rejected stream for triage
            if self.publisher and self.publisher.r:
                try:
                    await self.publisher.r.xadd(
                        self._rejected_signal_stream,
                        fields={
                            "symbol": str(symbol),
                            "gate": str(getattr(veto_dec, "gate", "UnknownGate")),
                            "reason": str(veto_dec.reason_code),
                            "ts_ms": str(int(enriched_signal.get("ts_ms") or 0)),
                            "payload": json.dumps(enriched_signal, ensure_ascii=False),
                        },
                        maxlen=200000,
                    )
                except Exception:
                    pass

            # Still emit audit record (deterministic) but stop before trade/notify sinks
            try:
                signal_stream = self.cryptoorderflow_signal_stream_template.format(symbol=symbol)
                audit_payload = {
                    "sid": enriched_signal.get("sid") or enriched_signal.get("signal_id") or "",
                    "signal_id": enriched_signal.get("signal_id") or "",
                    "symbol": symbol,
                    "side": enriched_signal.get("side") or direction,
                    "entry": entry,
                    "sl": sl,
                    "tp_levels": tp_levels,
                    "lot": lot,
                    "source": "CryptoOrderFlow",
                    "reason": signal.get("reason") or "delta_spike",
                    "confidence": confidence,
                    "confidence01": confidence,
                    "confidence_pct": confidence * 100.0,
                    "atr": atr,
                    "ts": ts_ms,
                    "ts_ms": ts_ms,
                    "pre_publish_veto": True,
                    "pre_publish_gate": str(getattr(veto_dec, "gate", "UnknownGate")),
                    "pre_publish_reason": str(veto_dec.reason_code),
                    "indicators": indicators,
                    "strategy": "cryptoorderflow",
                    "tf": "tick",
                }
                preprocess_signal_for_publish(audit_payload, symbol=str(symbol), source="CryptoOrderFlow", logger=logger)
                await self.publisher.xadd_json(
                    sink=StreamSink(name=str(signal_stream), field="data", maxlen=1000),
                    payload=audit_payload,
                    symbol=str(symbol),
                )
            except Exception:
                pass
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
            symbol=symbol,
            entry_price=entry,
            sl_price=sl,
            side=str(direction or "").upper(),
            risk_percent=effective_risk_pct,
        )
        lot = lot_risk

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

        await self._maybe_publish_paper_shadow(
            symbol=symbol,
            direction=str(direction or "").upper(),
            entry=entry,
            sl=sl,
            tp_levels=tp_levels,
            lot=lot,
            ts_ms=ts_ms,
            confidence=confidence,
            indicators=indicators,
            enriched_signal=enriched_signal,
        )

        crypto_signal = CryptoSignal(
            sid=signal["signal_id"],
            symbol=symbol,
            side=str(direction or "").upper(),
            entry=entry,
            sl=sl,
            tp_levels=tp_levels,
            lot=lot,
            position_size_usd=position_size_usd,
            deposit=deposit,
            leverage=leverage,
            atr=atr,
            confidence=confidence,
            ts=int(signal.get("tick_ts") or signal.get("generated_at")),
            source="CryptoOrderFlow",
            reason_mix=mix_dict,
            confirmations=confirmations,
            indicators=indicators,
            trail_profile=trail_profile,
            trail_after_tp1=self._normalize_trailing_flag(enriched_signal.get("trail_after_tp1"), symbol),
            config_params=signal.get("config_params") or {"strong_gate_ok": signal.get("indicators", {}).get("strong_gate_ok", 0)},
            validation_status=enriched_signal.get("validation_status"),
            validation_reason=enriched_signal.get("validation_reason"),
        )

        telegram_payload = {
            "text": CryptoSignalFormatter.format_telegram_message(crypto_signal),
            "symbol": symbol,
            "direction": crypto_signal.side,
            "entry": f"{entry:.2f}",
            "stop": f"{sl:.2f}",
            "tp": ",".join(f"{tp:.2f}" for tp in tp_levels),
            "source": crypto_signal.source,
            "reason": signal.get("reason") or "delta_spike",
            "timestamp": str(crypto_signal.ts),
        }

        # Build outbox envelope (dispatcher will apply notify gating itself).
        try:
            signal_stream = self.cryptoorderflow_signal_stream_template.format(symbol=symbol)
            audit_payload = {
                "sid": crypto_signal.sid,
                "signal_id": crypto_signal.sid,   # canonical mirror
                "symbol": symbol,
                "side": crypto_signal.side,
                "entry": entry,
                "sl": sl,
                "tp_levels": tp_levels,
                "lot": lot,
                "source": "CryptoOrderFlow",
                "reason": signal.get("reason") or "delta_spike",
                "confidence": confidence,
                "confidence01": confidence,
                "confidence_pct": confidence * 100.0,
                "atr": atr,
                "ts": ts_ms,
                "ts_ms": ts_ms,
                "trail_after_tp1": self._normalize_trailing_flag(enriched_signal.get("trail_after_tp1"), symbol),
                "trail_profile": enriched_signal.get("trail_profile", "rocket_v1"),
                "indicators": indicators,
                "strategy": "cryptoorderflow",
                "tf": "tick",
            }

            env = build_outbox_envelope(
                sid=crypto_signal.sid,
                symbol=symbol,
                kind="crypto_orderflow",
                notify_payload=telegram_payload,
                audit_payload={"payload": json.dumps(enriched_signal, ensure_ascii=False)},
                signal_stream_payload={"data": json.dumps(audit_payload, ensure_ascii=False)},
                audit_stream=self.raw_signal_stream,
                signal_stream=signal_stream,
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
                setattr(ctx_min, "ts_ms", int(time.time() * 1000))
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
                self.publisher.r,
                stream_key=str(outbox_stream),
                signal_id=str(sid),
                payload_obj=payload_obj,
                kind=str(env.get("kind") or ""),
                symbol=str(env.get("symbol") or ""),
                ts=str(env.get("ts_ms") or ""),
                meta_obj=meta_obj,
            )
            
            if use_outbox:
                # Send notify even in outbox mode
                notify_enabled = True  # Force enable Telegram notifications
                logger.info("📱 [TELEGRAM] (%s) Attempting to send notify: publisher=%s, payload=%s", symbol, self.publisher.r is not None, telegram_payload is not None)
                if notify_enabled and self.publisher.r and telegram_payload:
                    try:
                        notify_signal_every_n = max(1, int(os.getenv("CRYPTO_NOTIFY_SIGNAL_EVERY_N", "1")))
                        self._notify_counter += 1
                        if self._notify_counter % notify_signal_every_n == 0:
                            await self.publisher.r.xadd(
                                self.notify_stream,
                                fields=telegram_payload,
                                maxlen=20000,
                            )
                            logger.info("✅ [TELEGRAM] (%s) Sent notify to %s (counter=%d)", symbol, self.notify_stream, self._notify_counter)
                        else:
                            logger.debug("⏭️ [TELEGRAM] (%s) Skipped notify (rate limit %d/%d)", symbol, self._notify_counter, notify_signal_every_n)
                    except Exception as exc:
                        logger.warning("⚠️ [TELEGRAM] (%s) Failed to send notify: %s", symbol, exc)
                else:
                    logger.warning("❌ [TELEGRAM] (%s) Cannot send notify: enabled=%s, publisher=%s, payload=%s", symbol, notify_enabled, self.publisher.r is not None, telegram_payload is not None)

                # Also send to raw stream for ExecutionGateService compatibility
                pub = self.publisher
                try:
                    await pub.xadd_json(
                        sink=StreamSink(name=str(self.raw_signal_stream), field="payload", maxlen=100000),
                        payload=enriched_signal,
                        symbol=str(symbol),
                    )
                    sampled_info(logger, "SIGNAL_RAW_STREAM", "📤 [SIGNAL] (%s) Also sent to %s for ExecutionGateService", symbol, self.raw_signal_stream)
                except Exception as e:
                    logger.warning("⚠️ [SIGNAL] (%s) Failed to send to raw stream: %s", symbol, e)

                # Stop here if we rely purely on outbox
                sampled_info(logger, "SIGNAL_PUBLISHED", "🚀 [SIGNAL] (%s) %s P=%s Published via Atomic Outbox: ID=%s", symbol, direction, entry, sid)
                return

        # ------------------------------------------------------------------
        # DIRECT PUBLISHING (Failback / Mixed Mode)
        # ------------------------------------------------------------------
        
        # 1) Telegram Notify
        notify_enabled = True  # Send all signals but with validation status
        if notify_enabled and self.publisher.r:
             # Rate limiting implemented via modulo check (simple but effective for flood control)
             notify_signal_every_n = max(1, int(os.getenv("CRYPTO_NOTIFY_SIGNAL_EVERY_N", "1")))
             self._notify_counter += 1
             
             try:
                # We reuse AsyncSignalPublisher to push to notify stream? No, it's a specific format.
                # AsyncSignalPublisher writes structured JSON. Notify expects specific fields.
                # We'll use self.publisher.r (Redis client) directly.
                 if self._notify_counter % notify_signal_every_n == 0:
                     await self.publisher.r.xadd(
                         self.notify_stream,
                         fields=telegram_payload,
                         maxlen=20000,
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
                 sink=StreamSink(name=str(self.raw_signal_stream), field="payload", maxlen=100000),
                 payload=enriched_signal,
                 symbol=str(symbol),
             )
        except Exception:
             pass

        # 3) Audit Payload via AsyncSignalPublisher
        preprocess_signal_for_publish(audit_payload, symbol=str(symbol), source="CryptoOrderFlow", logger=logger)
        await pub.xadd_json(
            sink=StreamSink(name=str(signal_stream), field="data", maxlen=1000),
            payload=audit_payload,
            symbol=str(symbol),
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

    async def _maybe_publish_paper_shadow(
        self,
        *,
        symbol: str,
        direction: str,
        entry: float,
        sl: float,
        tp_levels: List[float],
        lot: float,
        ts_ms: int,
        confidence: float,
        indicators: Dict[str, Any],
        enriched_signal: Dict[str, Any],
    ) -> None:
        enabled = os.getenv("CRYPTO_PAPER_SHADOW_ENABLED", "0").lower() in {"1", "true", "yes", "on"}
        if not enabled:
            return

        min_conf_pct = float(os.getenv("CRYPTO_SIGNAL_MIN_CONF", "70"))
        if 0 < min_conf_pct <= 1:
            min_conf_pct *= 100.0
        if confidence < (min_conf_pct / 100.0):
            return

        gate_mode = str(indicators.get("of_gate_mode") or "").upper()
        validation_status = str(enriched_signal.get("validation_status") or "").lower()
        if gate_mode != "SHADOW" or validation_status != "failed":
            return

        if not self.publisher or not self.publisher.r:
            logger.warning("⚠️ [PAPER] (%s) Publisher not ready; skipping paper order", symbol)
            return

        paper_stream = os.getenv("PAPER_ORDERS_STREAM", "paper:orders")
        maxlen = int(os.getenv("PAPER_ORDERS_MAXLEN", "20000"))
        payload = {
            "sid": str(enriched_signal.get("sid") or enriched_signal.get("signal_id") or ""),
            "symbol": symbol,
            "side": direction,
            "entry": float(entry),
            "sl": float(sl),
            "tp_levels": [float(x) for x in (tp_levels or [])],
            "lot": float(lot),
            "ts_ms": int(ts_ms),
            "is_virtual": 1,
            "shadow": 1,
            "source": str(enriched_signal.get("source") or "CryptoOrderFlow"),
            "strategy": str(enriched_signal.get("strategy") or "cryptoorderflow"),
            "tf": str(enriched_signal.get("tf") or "tick"),
            "confidence": float(confidence),
            "confidence_pct": float(confidence) * 100.0,
            "validation_status": str(enriched_signal.get("validation_status") or ""),
            "validation_reason": str(enriched_signal.get("validation_reason") or ""),
            "of_gate_mode": gate_mode,
            "strong_gate_shadow_veto": int(indicators.get("strong_gate_shadow_veto") or 0),
        }
        try:
            await self.publisher.r.xadd(
                paper_stream,
                {"data": json.dumps(payload, ensure_ascii=False)},
                maxlen=maxlen,
            )
            logger.info("🧾 [PAPER] (%s) Shadow signal routed to %s (sid=%s)", symbol, paper_stream, payload.get("sid"))
        except Exception as exc:
            logger.warning("⚠️ [PAPER] (%s) Failed to publish paper order: %s", symbol, exc)

    async def send_telegram_report(self, text: str, source: str = "report", symbol: str = "") -> None:
        """Send arbitrary report text to Telegram via notify stream (type=report)."""
        try:
            ts_ms = str(int(time.time() * 1000))
            fields = {
                "type": "report",
                "text": str(text or ""),
                "source": str(source or "report"),
                "symbol": str(symbol or ""),
                "ts_ms": ts_ms,
            }
            # notify_worker.py handles type=report with priority
            await self.publisher.r.xadd(
                self.notify_stream,
                fields=fields,
                maxlen=int(getattr(self, "notify_maxlen", 20000) or 20000),
                approximate=True,
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
        import math
        ax = abs(x)
        return 1.0 - (1.0 / (1.0 + k * ax))

    def _build_mix_dict(
        self,
        delta: float,
        delta_z: float,
        indicators: Optional[Dict[str, Any]],
        confirmations: Sequence[str],
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
                raw_obi = float(indicators.get("obi", 0.0) or 0.0)
                # Convert raw OBI [-1..+1] to directional probability [0..1]:
                # For LONG: positive OBI (bid > ask) is confirming → high p_cluster
                # For SHORT: negative OBI (ask > bid) is confirming → high p_cluster
                sig_dir = str(indicators.get("direction", "") or "").upper()
                if sig_dir == "SHORT":
                    raw_obi = -raw_obi
                mix["p_cluster"] = max(0.0, min(1.0, raw_obi))
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
        self,
        runtime: SymbolRuntime,
        entry: float,
        side: str,
        indicators: Dict[str, Any],
        trail_profile: Optional[str] = None,
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
                "BTCUSDT": 30.0,
                "ETHUSDT": 4.0,
                "BNBUSDT": 0.5,
                "SOLUSDT": 0.3,
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
                    "✅ rocket_v1 LONG adjusted: TP1=%.2f (%.2f ATR), TP2=%.2f (%.2f ATR), TP3=%.2f (%.2f ATR)",
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
                    "✅ rocket_v1 SHORT adjusted: TP1=%.2f (%.2f ATR), TP2=%.2f (%.2f ATR), TP3=%.2f (%.2f ATR)",
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
                self.notify_stream,
                fields={"type": "report", "text": text, "source": source, "symbol": symbol, "ts_ms": str(int(time.time()*1000))},
                maxlen=self.notify_maxlen,
                approximate=True,
            )
            logger.info("📱 [TELEGRAM-REPORT] Sent report for %s from %s", symbol, source)
        except Exception as exc:
            logger.warning("⚠️ [TELEGRAM-REPORT] Failed to send report for %s: %s", symbol, exc)
