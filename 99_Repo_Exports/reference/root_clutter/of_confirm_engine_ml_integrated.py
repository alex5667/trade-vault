from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any, Dict, Optional, Tuple
import math

from core.book_evidence import compute_obi_flags, compute_iceberg_flags, compute_ofi_flags
from core.of_evidence import compute_sweep_recent, compute_reclaim_recent, compute_absorption_flags
from core.strong_of_gate import eval_reversal, eval_continuation, hidden_trend_dir
from core.absorption_level_score import compute_absorption_level_score
from core.fp_edge_evidence import compute_fp_edge_absorb
from core.scenario_v4 import classify_v4
from core.of_confirm_contract import OFConfirmV3, pack_bits
from core.cfg_merge import merged_cfg
from core.strong_need_policy import compute_strong_need_same_tick
from services.cancellation_spike_gate import CancellationSpikeGate
from services.ml_confirm_gate_dynamic_v7_schema_v2 import MLConfirmGateDynamicV7SchemaV2
from common.metrics_stage import veto_total, dist


def _clamp01(x: float) -> float:
    try:
        if x < 0.0: return 0.0
        if x > 1.0: return 1.0
        return float(x)
    except Exception:
        return 0.0


def _f(x: Any, d: float = 0.0) -> float:
    try:
        v = float(x)
        return v if math.isfinite(v) else d
    except Exception:
        return d


def _i(x: Any, d: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return d


@dataclass
class OFConfirm:
    """ Obsolete v2 contract, replaced by OFConfirmV3 """
    version: int
    ts_ms: int
    symbol: str
    tf: str
    direction: str               # LONG/SHORT
    scenario: str                # reversal/continuation/none
    ok: int                      # 1/0
    have: int
    need: int
    score: float                 # 0..1
    evidence: Dict[str, Any]
    contrib: Dict[str, float]    # score contributions per key

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class OFConfirmEngine:
    # Use a high bit to avoid clashing with existing gate bits.
    GATE_BIT_CANCEL_SPIKE = 1 << 28

    def __init__(self, version: int = 3, cancel_gate: Optional[CancellationSpikeGate] = None, ml_gate: Optional[MLConfirmGateDynamicV7SchemaV2] = None) -> None:
        self.version = int(version)
        self._cancel_spike_gate = cancel_gate # will lazy init in build() if None
        self._ml_gate = ml_gate  # lazy init in build() if None

    def build(
        self,
        *,
        symbol: str,
        tf: str,
        direction: str,
        tick_ts_ms: int,
        price: float,
        delta_z: float,
        runtime: Any,
        cfg: Dict[str, Any],
        indicators: Dict[str, Any],
        absorption: Optional[Dict[str, Any]] = None,
    ) -> Tuple[Optional[OFConfirmV3], Optional[Any]]:
        """
        Returns:
          (of_confirm, gate_decision)

        Centralizes evidence computation, scenario evaluation, and continuous scoring.
        """
        now_ts = tick_ts_ms if tick_ts_ms > 0 else _i(indicators.get("now_ts_ms", 0), 0)
        
        # --- Book evidence (OBI/Iceberg) ---
        obi_dir_ok, obi_stable, obi_stable_secs, obi_val = compute_obi_flags(
            direction=direction,
            now_ts_ms=now_ts,
            last_event=getattr(runtime, "last_obi_event", None),
            cfg=cfg,
            indicators=indicators,
        )
        iceberg_dir_ok, iceberg_strict, iceberg_refresh, iceberg_duration = compute_iceberg_flags(
            direction=direction,
            price=float(price),
            now_ts_ms=now_ts,
            last_event=getattr(runtime, "last_iceberg_event", None),
            cfg=cfg,
            indicators=indicators,
        )

        # A1: OFI as first-class evidence (not only strategy telemetry)
        ofi_dir_ok, ofi_stable, ofi_stable_secs, ofi_val, ofi_z, ofi_stability_score = compute_ofi_flags(
            direction=direction,
            now_ts_ms=now_ts,
            last_event=getattr(runtime, "last_ofi_event", None),
            cfg=cfg,
            indicators=indicators,
        )

        # --- Book health gate for book-based evidences (OBI/Iceberg) ---
        try:
            book_ok = int(indicators.get("book_health_ok", 1) or 1)
        except Exception:
            book_ok = 1
        
        # --- Data health gate (stricter than book_ok) ---
        # If overall data_health is low, we fail-closed ONLY for evidences that depend on book/time.
        try:
            dh = float(indicators.get("data_health", 1.0) or 1.0)
        except Exception:
            dh = 1.0
        dh_min = float(cfg.get("data_health_min_for_book_evidence", 0.70))
        if dh < dh_min:
            book_ok = 0
            indicators["data_health_veto_book_evidence"] = 1
        
        if book_ok == 0:
            # Do not allow these evidences to contribute to StrongGate B/C components
            obi_dir_ok, obi_stable, obi_stable_secs, obi_val = False, False, 0.0, 0.0
            iceberg_dir_ok, iceberg_strict, iceberg_refresh, iceberg_duration = False, False, 0, 0.0
            ofi_dir_ok, ofi_stable, ofi_stable_secs, ofi_val, ofi_z, ofi_stability_score = (
                False,
                False,
                0.0,
                0.0,
                0.0,
                0.0,
            )
            indicators["book_health_veto_book_evidence"] = 1
            # Keep indicators consistent for explainability
            indicators["ofi_dir_ok"] = 0
            indicators["ofi_stable"] = 0
            indicators["ofi"] = 0.0
            indicators["ofi_z"] = 0.0
            indicators["ofi_stable_secs"] = 0.0
            indicators["ofi_stability_score"] = 0.0
            indicators["ofi_age_ms"] = -1

        # --- Sweep/Reclaim evidence (staleness-gated) ---
        sweep_recent = compute_sweep_recent(
            now_ts_ms=now_ts,
            last_sweep=getattr(runtime, "last_sweep", None),
            cfg=cfg,
            indicators=indicators,
        )
        reclaim_recent, reclaim_hold_bars = compute_reclaim_recent(
            direction=direction,
            now_ts_ms=now_ts,
            last_reclaim=getattr(runtime, "last_reclaim", None),
            cfg=cfg,
            indicators=indicators,
        )

        # --- Absorption ---
        abs_ok, abs_vol = compute_absorption_flags(
            direction=direction,
            absorption=absorption,
            cfg=cfg,
            indicators=indicators,
        )

        # --- Weak progress (computed on bar_close) ---
        wp_any = bool(getattr(getattr(runtime, "last_wp", None), "weak_any", False))
        indicators["weak_progress"] = 1 if wp_any else 0

        # --- A2: FP edge absorption (anti-fake-impulse) ---
        fp_edge_ok, fp_edge_strength, fp_edge_rng, fp_edge_bias = compute_fp_edge_absorb(
            direction=direction,
            now_ts_ms=int(now_ts),
            last_edge=getattr(runtime, "last_fp_edge", None),
            cfg=cfg,
            indicators=indicators,
        )

        # --- Scenario selection v4 (B1 + B2: saw/chop + vol/news proxies) ---
        try:
            cancel_meta = {}  # Anti-spoof meta if available
            if hasattr(self, "_cancel_spike_gate") and self._cancel_spike_gate:
                 # It's tricky because cancel gate runs later in this function...
                 # Ideally we should run cancel gate earlier or reuse stored state.
                 # For now, we rely on runtime to possibly have previous state or check local logic.
                 # Actually, classify_v4 expects 'cancel_meta' which is an output of cancel_gate.
                 # In this engine, cancel_gate runs at the end. 
                 # To support scenario_v4 properly, we need input from cancel_gate or assume 0 for now
                 # and let the later stage veto if needed.
                 # However, 'saw_chop_spoof_proxy' scenario NEEDS this.
                 pass
            
            # For correctness of scenario classification utilizing cancel_meta, we might need to 
            # run a lightweight check or use last tick's.
            # But let's assume standard inputs for now.
            
            # We need to construct cancel_meta from indicators if available same-tick?
            # Or use empty for first pass.
            cm = {
                "ready": _i(indicators.get("cancel_spike_ready", 0)),
                "veto_kind": str(indicators.get("cancel_spike_reason", "")),
            }
            
            # Needed for vol_shock
            liq_regime = str(getattr(runtime, "liq_regime", "na") or "na")
            liq_score = _f(getattr(runtime, "liq_score", 0.0), 0.0)
            
            # Needed for exec risk
            spread_bps = _f(indicators.get("spread_bps", indicators.get("liq_spread_bps", 0.0)), 0.0)
            slip_bps = _f(indicators.get("expected_slippage_bps", 0.0), 0.0)
            exec_risk_bps = float(max(0.0, spread_bps + slip_bps))
            
            sc_v4 = classify_v4(
                sweep_recent=sweep_recent,
                trend_dir=trend_dir, # We need trend_dir logic first?
                pressure_hi=False, # Will be computed later, using placeholders implies order mismatch
                churn_hi=False,
                exec_risk_bps=exec_risk_bps,
                liq_regime=liq_regime,
                liq_score=liq_score,
                cancel_meta=cm,
                cfg=cfg,
            )
            # Re-compute trend_dir properly for continuation fallback?
            # Actually valid point: trend_dir logic is needed for classify_v4 if base is continuation.
            # Let's keep the trend logic block but remove legacy scenario assignment.
            pass
        except Exception:
            sc_v4 = None

        # Re-implementing trend logic for V4
        scenario = "reversal" if sweep_recent else "continuation"
        fallback_reason = "unknown"
        trend_dir = None
        
        if scenario == "continuation":
            # Best practice: if CVD is quarantined, ignore hidden divergence (avoid false trend from broken baseline)
            cvd_q = int(indicators.get("cvd_quarantine_active", 0) or 0)
            div = None if cvd_q == 1 else getattr(runtime, "last_div", None)
            if cvd_q == 1:
                indicators["hidden_div_ignored"] = 1
            trend_dir = hidden_trend_dir(getattr(div, "kind", None) if div else None)
            
            # FAILBACK: If no hidden divergence, use REGIME as trend definition (Trend Following)
            if trend_dir is None:
                 rg = str(getattr(runtime, "last_regime", "na") or "na").lower()
                 if "bull" in rg: 
                     trend_dir = "LONG"
                 elif "bear" in rg: 
                     trend_dir = "SHORT"
            else:
                 indicators["hidden_div_used"] = 1

        # Now run V4 with correct trend
        # We need pressure/churn for V4 too. They are computed later in original code.
        # We must pull them UP.
        dyn = getattr(runtime, "dynamic_cfg", {}) or {}
        cfg2 = merged_cfg(cfg, dyn)
        
        try:
             pressure_hi = bool(getattr(runtime, "pressure").is_pressure_hi(int(now_ts), float(cfg2.get("pressure_hi_per_min", 4.0))))
        except: pressure_hi = False
        
        try:
             churn_hi = bool(int(getattr(runtime, "book_churn_hi", 0) or 0))
        except: churn_hi = False
        
        # Exec risk
        spread_bps = _f(indicators.get("spread_bps", indicators.get("liq_spread_bps", 0.0)), 0.0)
        slip_bps = _f(indicators.get("expected_slippage_bps", 0.0), 0.0)
        exec_risk_bps = float(max(0.0, spread_bps + slip_bps))
        
        sc_v4 = classify_v4(
            sweep_recent=sweep_recent,
            trend_dir=trend_dir, 
            pressure_hi=pressure_hi,
            churn_hi=churn_hi,
            exec_risk_bps=exec_risk_bps,
            liq_regime=str(getattr(runtime, "liq_regime", "na") or "na"),
            liq_score=_f(getattr(runtime, "liq_score", 0.0), 0.0),
            cancel_meta={"ready": 0}, # approximated early
            cfg=cfg2,
        )
        
        scenario = sc_v4.base
        # Override scenario if wrapper detected special proxy
        if sc_v4.id not in ("reversal_sweep", "continuation_trend", "range_meanrev"):
             # If special scenario (vol_shock, saw_chop), does it force a specific base logic?
             # Usually they imply 'range' or 'special' logic, but for strong_gate policies we map them to base.
             pass
             
        dec = None
        
        if trend_dir is None and scenario == "continuation":
            scenario = "none"
            fallback_reason = "no_sweep_and_no_trend"
            try:
                 indicators["of_debug_fail"] = f"no_trend:regime={getattr(runtime, 'last_regime', 'na')}"
            except Exception: 
                 pass

        # --- Absorption-on-level (v2) from last microbar footprint + external confirms ---
        abs_lvl_ok = False
        abs_lvl_score = 0.0
        abs_lvl_bias = "NONE"
        abs_lvl_dir_match = False
        bar = None
        
        try:
            if bool(int(cfg.get("abs_lvl_enable", 1))):
                bar = getattr(runtime, "last_bar", None)
                if bar is not None and bool(getattr(bar, "fp_enabled", False)):
                    abs_lvl = compute_absorption_level_score(
                        bar=bar,
                        direction=direction,
                        delta_z=float(delta_z),
                        weak_progress=bool(wp_any),
                        iceberg_strict=bool(iceberg_strict),
                        reclaim_recent=bool(reclaim_recent),
                        cfg=cfg,
                    )
                    abs_lvl_ok = bool(abs_lvl.ok)
                    abs_lvl_score = float(abs_lvl.score)
                    abs_lvl_bias = str(abs_lvl.bias)
                    abs_lvl_dir_match = bool(abs_lvl.dir_match)

                    indicators["abs_lvl_ok"] = int(abs_lvl_ok)
                    indicators["abs_lvl_score"] = abs_lvl_score
                    indicators["abs_lvl_bias"] = abs_lvl_bias
                    indicators["abs_lvl_ladder"] = int(abs_lvl.ladder_len)
                    indicators["abs_lvl_poc_edge"] = int(abs_lvl.poc_on_edge)
                    indicators["abs_lvl_eff"] = float(abs_lvl.eff_delta)
                    # indicators["abs_lvl_parts"] = abs_lvl.parts
        except Exception:
            pass

        # --- Strong gate decision (need is scenario-dependent, can be escalated same-tick) ---
        # Merge dynamic cfg (runtime.dynamic_cfg) into local cfg view.
        dyn = getattr(runtime, "dynamic_cfg", {}) or {}
        cfg2 = merged_cfg(cfg, dyn)

        # pressure_hi, churn_hi computed above for V4, reuse?
        pass

        nd = compute_strong_need_same_tick(
            scenario=str(scenario),
            pressure_hi=pressure_hi,
            churn_hi=churn_hi,
            regime=str(regime),
            unstable=bool(unstable),
            cfg=cfg2,
        )
        # Apply need overrides into cfg2 for eval_* (same-tick)
        # Apply need overrides into cfg2 for eval_* (same-tick)
        cfg2["strong_need_reversal"] = int(nd.need_rev)
        cfg2["strong_need_continuation"] = int(nd.need_cont)
        
        # Effective flags (Legs + A2)
        ofi_leg = bool(ofi_dir_ok and ofi_stable)
        micro_stable = bool(obi_stable or ofi_leg)  # OFI substitution
        abs_lvl_ok_eff = bool(abs_lvl_ok or fp_edge_ok) # FP Edge substitution

        if scenario == "reversal":
            dec = eval_reversal(
                direction=direction,
                delta_z=float(delta_z),
                weak_progress=wp_any,
                sweep_recent=True,
                reclaim_recent=reclaim_recent,
                obi_stable=micro_stable,
                iceberg_strict=iceberg_strict,
                abs_lvl_ok=abs_lvl_ok_eff,
                cfg=cfg2,
            )
        elif scenario == "continuation" and trend_dir is not None:
            # continuation context (countertrend absorption observed) is maintained in runtime
            now_ts_for_cont = now_ts
            cont_ts = int(getattr(runtime, "cont_ctx_ts_ms", 0) or 0)
            cont_valid = int(cfg2.get("cont_ctx_valid_ms", 120_000))
            cont_ctx_recent = (cont_ts > 0 and 0 <= now_ts_for_cont - cont_ts <= cont_valid)

            # hidden ctx recent
            cvd_q = int(indicators.get("cvd_quarantine_active", 0) or 0)
            div = None if cvd_q == 1 else getattr(runtime, "last_div", None)
            if cvd_q == 1:
                indicators["hidden_div_ignored"] = 1
            hidden_ms = int(cfg2.get("hidden_ctx_valid_ms", 120_000))
            div_ts = int(getattr(div, "ts_ms", now_ts_for_cont))
            hidden_ctx_recent = (div is not None and 0 <= now_ts_for_cont - div_ts <= hidden_ms)

            dec = eval_continuation(
                direction=direction,
                trend_dir=trend_dir,
                hidden_ctx_recent=hidden_ctx_recent,
                iceberg_strict=iceberg_strict,
                obi_stable=micro_stable,
                cont_ctx_recent=cont_ctx_recent,
                abs_lvl_ok=abs_lvl_ok_eff,
                cfg=cfg2,
            )

        # Attach need escalation diagnostics
        if dec is not None and sc_v4:
             # Attach V4 reason
             setattr(dec, "scenario_v4", sc_v4.id)
             setattr(dec, "scenario_reason", sc_v4.reason)

        # --- Score (0..1) ---
        contrib: Dict[str, float] = {}
        score = 0.0
        # Z contribution
        z_abs = abs(float(delta_z))
        z_ref = _f(cfg.get("score_z_ref", 3.0), 3.0)
        contrib["z"] = _clamp01(z_abs / max(1e-9, z_ref)) * _f(cfg.get("w_z", 0.30), 0.30)
        score += contrib["z"]
        # Weak progress
        contrib["weak_progress"] = (1.0 if wp_any else 0.0) * _f(cfg.get("w_wp", 0.15), 0.15)
        score += contrib["weak_progress"]
        # Reclaim
        contrib["reclaim"] = (1.0 if reclaim_recent else 0.0) * _f(cfg.get("w_reclaim", 0.20), 0.20)
        score += contrib["reclaim"]
        # OBI stable
        contrib["obi_stable"] = (1.0 if obi_stable else 0.0) * _f(cfg.get("w_obi", 0.15), 0.15)
        score += contrib["obi_stable"]
        # Iceberg strict
        contrib["iceberg_strict"] = (1.0 if iceberg_strict else 0.0) * _f(cfg.get("w_ice", 0.15), 0.15)
        score += contrib["iceberg_strict"]
        # Optional absorption
        contrib["absorption"] = (1.0 if abs_ok else 0.0) * _f(cfg.get("w_abs", 0.05), 0.05)
        score += contrib["absorption"]
        
        # A3: Execution Risk Penalty
        # If not already computed (we did for v4), re-fetch
        risk_ref = _f(cfg.get("exec_risk_ref_bps", 10.0), 10.0)
        risk_w = _f(cfg.get("w_exec_risk", 0.25), 0.25)
        
        exec_risk_norm = _clamp01(exec_risk_bps / max(1e-9, risk_ref))
        contrib["exec_risk"] = -1.0 * exec_risk_norm * risk_w
        score += contrib["exec_risk"]
        
        score = _clamp01(score)

        ok = 0
        have = 0
        need = 0
        if dec is not None:
            ok = 1 if bool(dec.ok) else 0
            have = int(dec.have)
            need = int(dec.need)

        # Score threshold (double filter)
        score_min = _f(cfg.get("of_score_min", 0.65), 0.65)
        if ok == 1 and score < score_min:
             # Logic: if score is too low, we can veto even if 2-of-3 passed (optional but recommended)
             # But we only do this if it's not shadow mode in the caller. 
             # We'll just return ok=0 and let the service decide.
             ok = 0
             # Optional: log if we vetoed by score
        # ------------------------------------------------------------------
        # Cancellation Spike gate (L3-lite anti-pulling / anti-spoof proxy)
        # ------------------------------------------------------------------
        gate_reason = ""
        gate_meta = {}
        gate_vetoed = False
        ok_pre_gate = int(ok)
        try:
            if not hasattr(self, "_cancel_spike_gate") or getattr(self, "_cancel_spike_gate") is None:
                self._cancel_spike_gate = CancellationSpikeGate()

            # prefer explicit keys from indicators
            c_bid = _f(indicators.get("cancel_bid_rate_ema", 0.0), 0.0)
            c_ask = _f(indicators.get("cancel_ask_rate_ema", 0.0), 0.0)
            t_buy = _f(indicators.get("taker_buy_rate_ema", 0.0), 0.0)
            t_sell = _f(indicators.get("taker_sell_rate_ema", 0.0), 0.0)

            # bucket monotonicity or bar_id
            b_id = indicators.get("bucket_id", indicators.get("bar_id"))
            if b_id is None and bar is not None:
                b_id = getattr(bar, "id", None)

            gd = self._cancel_spike_gate.check(
                symbol=str(symbol),
                direction=str(direction),
                cancel_bid_rate_ema=float(c_bid),
                cancel_ask_rate_ema=float(c_ask),
                taker_buy_rate_ema=float(t_buy),
                taker_sell_rate_ema=float(t_sell),
                bucket_id=int(b_id) if b_id is not None else None,
                cfg2=cfg2,
            )
            gate_reason = str(gd.reason)
            gate_meta = dict(getattr(gd, "meta", {}) or {})

            # Attach diagnostics
            indicators["cancel_spike_reason"] = gate_reason
            indicators["cancel_spike_ready"] = int(gate_meta.get("ready", 0))

            if (not bool(gd.allow)) and ok_pre_gate == 1:
                ok = 0
                gate_vetoed = True
                try:
                    if dec is not None:
                        # Mark as gate bit
                        setattr(dec, "gate_bits", int(getattr(dec, "gate_bits", 0)) | self.GATE_BIT_CANCEL_SPIKE)
                except Exception:
                    pass
                veto_total(runtime, reason_code=gate_reason, kind="cancel_spike", symbol=str(symbol))

            # Distributions
            try:
                dist(runtime, "cancel_spike_ratio_support", float(gate_meta.get("ratio_support", 0.0)),
                     kind="cancel_spike", symbol=str(symbol), side=str(gate_meta.get("support_side", "")), dir=str(gate_meta.get("direction", "")))
                dist(runtime, "cancel_spike_z_support", float(gate_meta.get("z_support", 0.0)),
                     kind="cancel_spike", symbol=str(symbol), side=str(gate_meta.get("support_side", "")), dir=str(gate_meta.get("direction", "")))
            except Exception:
                pass
        except Exception:
            pass

        final_reason = str(getattr(dec, "reason", fallback_reason))
        if dec is not None:
             final_reason = f"{final_reason}({have}/{need})"

        if gate_vetoed and gate_reason:
            final_reason = f"{gate_reason}(veto)"

        evidence = {
            "delta_z": float(delta_z),
            "weak_progress": int(wp_any),
            "sweep": int(sweep_recent),
            "reclaim": int(reclaim_recent),
            "reclaim_hold_bars": int(reclaim_hold_bars),
            "obi_dir_ok": int(obi_dir_ok),
            "obi": float(obi_val),
            "obi_stable": int(obi_stable),
            "obi_stable_secs": float(obi_stable_secs),
            "iceberg_dir_ok": int(iceberg_dir_ok),
            "iceberg_refresh": int(iceberg_refresh),
            "iceberg_duration": float(iceberg_duration),
            "iceberg_strict": int(iceberg_strict),
            "absorption": int(abs_ok),
            "absorption_volume": float(abs_vol),
            "obi_age_ms": int(indicators.get("obi_age_ms", -1)),
            "iceberg_age_ms": int(indicators.get("iceberg_age_ms", -1)),
            "sweep_age_ms": int(indicators.get("sweep_age_ms", -1)),
            "reclaim_age_ms": int(indicators.get("reclaim_age_ms", -1)),
            "abs_lvl_ok": int(abs_lvl_ok),
            "abs_lvl_score": float(abs_lvl_score),
            "abs_lvl_bias": str(abs_lvl_bias),
            "abs_lvl_dir_match": int(abs_lvl_dir_match),
            "fp_move_bp": float(getattr(bar, "fp_move_bp", 0.0) if bar else 0.0),
            "fp_eff_quote": float(getattr(bar, "fp_eff_quote", 0.0) if bar else 0.0),
            "fp_quote_delta": float(getattr(bar, "fp_quote_delta", 0.0) if bar else 0.0),

            # --- L3-lite diagnostics ---
            "cancel_bid_rate_ema": float(_f(indicators.get("cancel_bid_rate_ema", 0.0), 0.0)),
            "cancel_ask_rate_ema": float(_f(indicators.get("cancel_ask_rate_ema", 0.0), 0.0)),
            "taker_buy_rate_ema": float(_f(indicators.get("taker_buy_rate_ema", 0.0), 0.0)),
            "taker_sell_rate_ema": float(_f(indicators.get("taker_sell_rate_ema", 0.0), 0.0)),
            "cancel_spike_veto": int(gate_vetoed),
            "cancel_spike_ready": int(gate_meta.get("ready", 0) if isinstance(gate_meta, dict) else 0),
            "cancel_spike_ratio_support": float(gate_meta.get("ratio_support", 0.0) if isinstance(gate_meta, dict) else 0.0),
            "cancel_spike_z_support": float(gate_meta.get("z_support", 0.0) if isinstance(gate_meta, dict) else 0.0),
        }

        
        # --- ML Confirm (Step C1/D/4) ---
        # Runs after hard vetoes and after heuristic have/need/score are computed.
        # Modes:
        #   OFF: no influence
        #   SHADOW: compute p_edge and log, but do not block
        #   ENFORCE: if heuristic ok==1 and p_edge<p_min -> block (ok=0)
        # Dynamic ML gate supports hot-reload from Redis cfg:ml_confirm
        # and canary enforcement via enforce_share/enforce_symbols
        try:
            if self._ml_gate is None:
                self._ml_gate = MLConfirmGateDynamicV7SchemaV2.from_env()
            
            # Extract sid from indicators if available (for canary routing)
            sid = str(indicators.get("sid", indicators.get("signal_id", "")) or "")
            
            # Prefer scenario_v4 for ML bucketization when dec.scenario is legacy (reversal/continuation)
            # This ensures ML v10.4 util_mh always gets v4 scenario for correct bucket selection and util_floor_by_bucket
            ml_scenario = str(getattr(dec, "scenario", "") if dec else "") or str(scenario)

            # If legacy scenario, try to use indicators["scenario_v4"] (set by engine / strategy)
            if ml_scenario.lower() in ("reversal", "continuation", "none"):
                sv4 = ""
                try:
                    sv4 = str(indicators.get("scenario_v4", "") or "")
                except Exception:
                    sv4 = ""
                if sv4:
                    ml_scenario = sv4
                # Fallback: use computed sc_v4.id if available (from line 287)
                elif sc_v4 and hasattr(sc_v4, "id") and sc_v4.id and sc_v4.id.lower() not in ("reversal", "continuation", "none", "reversal_sweep", "continuation_trend"):
                    ml_scenario = sc_v4.id
                # Also check dec.scenario_v4 if it was set (from line 419)
                elif dec and hasattr(dec, "scenario_v4"):
                    sv4_dec = str(getattr(dec, "scenario_v4", "") or "")
                    if sv4_dec and sv4_dec.lower() not in ("reversal", "continuation", "none", "reversal_sweep", "continuation_trend"):
                        ml_scenario = sv4_dec

            # Ensure scenario_v4 is in indicators for ML feature extraction
            indicators_with_v4 = dict(indicators, delta_z=float(delta_z))
            scenario_v4_value = ""
            if sc_v4 and hasattr(sc_v4, "id"):
                scenario_v4_value = str(sc_v4.id)
            elif dec and hasattr(dec, "scenario_v4"):
                scenario_v4_value = str(getattr(dec, "scenario_v4", "") or "")
            if scenario_v4_value:
                indicators_with_v4["scenario_v4"] = scenario_v4_value
            
            ml_dec = self._ml_gate.check(
                sid=sid,
                symbol=str(symbol),
                ts_ms=int(now_ts),
                direction=str(direction),
                scenario=str(ml_scenario),
                indicators=indicators_with_v4,
                rule_score=float(score),
                rule_have=int(have),
                rule_need=int(need),
                cancel_spike_veto=int(gate_vetoed),
                ok_rule=int(ok),
            )
            evidence["ml"] = ml_dec.to_dict()

            # ENFORCE blocks only when heuristic ok==1
            if str(ml_dec.mode).upper() == "ENFORCE" and int(ok) == 1 and not bool(ml_dec.allow):
                ok = 0
                if str(getattr(ml_dec, "kind", "")).lower().startswith("util_mh"):
                    final_reason = (
                        f"ml_block(score={getattr(ml_dec,'score',ml_dec.p_edge):.3f}"
                        f"<floor={getattr(ml_dec,'floor',ml_dec.p_min):.3f},h={getattr(ml_dec,'best_h_ms',0)})|"
                        + str(final_reason)
                    )
                else:
                    final_reason = f"ml_block(p={ml_dec.p_edge:.3f}<thr={ml_dec.p_min:.3f})|" + str(final_reason)
        except Exception as _e:
            # last-resort safety: do not crash confirm engine
            evidence["ml"] = {"mode": "ERR", "error": str(_e)[:200]}
        
        ofc = OFConfirmV3(
            v=3,
            symbol=str(symbol),
            ts_ms=int(now_ts),
            direction=str(direction),
            scenario=str(getattr(dec, "scenario", scenario) if dec else scenario),
            ok=int(ok),
            score=float(score),
            have=int(have),
            need=int(need),
            gate_bits=int(getattr(dec, "gate_bits", 0)),
            reason=str(final_reason),
            evidence=evidence,
            contrib=contrib,
        )
        return ofc, dec