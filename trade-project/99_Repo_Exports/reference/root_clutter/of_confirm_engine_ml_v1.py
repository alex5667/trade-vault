from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Any, Dict, Optional, Tuple
import math

from core.book_evidence import compute_obi_flags, compute_iceberg_flags
from core.of_evidence import compute_sweep_recent, compute_reclaim_recent, compute_absorption_flags
from core.strong_of_gate import eval_reversal, eval_continuation, hidden_trend_dir
from core.absorption_level_score import compute_absorption_level_score
from core.of_confirm_contract import OFConfirmV3, pack_bits
from core.cfg_merge import merged_cfg
from core.strong_need_policy import compute_strong_need_same_tick
from services.cancellation_spike_gate import CancellationSpikeGate
from common.metrics_stage import veto_total, dist
from services.cancellation_spike_gate import CancellationSpikeGate


from services.ml_confirm_gate import MLConfirmGate

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

    def __init__(self, version: int = 3, cancel_gate: Optional[CancellationSpikeGate] = None, ml_gate: Optional[MLConfirmGate] = None) -> None:
        self.version = int(version)
        self._cancel_spike_gate = cancel_gate # will lazy init in build() if None
        # ML gate: OFF/SHADOW/ENFORCE controlled by env; safe to always construct.
        self._ml_gate = ml_gate or MLConfirmGate.from_env()

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
            indicators["book_health_veto_book_evidence"] = 1

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

        # --- Scenario selection ---
        scenario = "reversal" if sweep_recent else "continuation"
        dec = None
        fallback_reason = "unknown"

        # Continuation needs a trend direction (from hidden divergence kind if available)
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

            if trend_dir is None:
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

        # Determine regime / instability / pressure / churn same-tick inputs
        try:
            regime = str(getattr(runtime, "last_regime", "na") or "na")
        except Exception:
            regime = "na"
        try:
            unstable = bool(int(dyn.get("abs_lvl_th_unstable", 0) or 0))
        except Exception:
            unstable = False
        # pressure_hi recorded by runtime.pressure
        try:
            pressure_hi = bool(getattr(runtime, "pressure").is_pressure_hi(int(now_ts), float(cfg2.get("pressure_hi_per_min", 4.0))))
        except Exception:
            pressure_hi = False
        # churn_hi from runtime
        try:
            churn_hi = bool(int(getattr(runtime, "book_churn_hi", 0) or 0))
        except Exception:
            churn_hi = False

        nd = compute_strong_need_same_tick(
            scenario=str(scenario),
            pressure_hi=pressure_hi,
            churn_hi=churn_hi,
            regime=str(regime),
            unstable=bool(unstable),
            cfg=cfg2,
        )
        # Apply need overrides into cfg2 for eval_* (same-tick)
        cfg2["strong_need_reversal"] = int(nd.need_rev)
        cfg2["strong_need_continuation"] = int(nd.need_cont)
        # We don't store it back to cfg2 as a key used by eval_*, but we keep for audit if needed

        if scenario == "reversal":
            dec = eval_reversal(
                direction=direction,
                delta_z=float(delta_z),
                weak_progress=wp_any,
                sweep_recent=True,
                reclaim_recent=reclaim_recent,
                obi_stable=obi_stable,
                iceberg_strict=iceberg_strict,
                abs_lvl_ok=abs_lvl_ok,
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
                obi_stable=obi_stable,
                cont_ctx_recent=cont_ctx_recent,
                abs_lvl_ok=abs_lvl_ok,
                cfg=cfg2,
            )

        # Attach need escalation diagnostics
        try:
            if dec is not None:
                setattr(dec, "need_reason", str(nd.reason))
        except Exception:
            pass

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

        # ------------------------------------------------------------------
        # ML confirm gate (Step C/D/4): after hard vetoes, before final decision.
        # Modes:
        #   OFF    -> no effect
        #   SHADOW -> attach p_edge but never block
        #   ENFORCE-> require p_edge >= threshold (fail policy applied inside MLConfirmGate)
        # ------------------------------------------------------------------
        try:
            ml = getattr(self, "_ml_gate", None)
            if ml is not None:
                # Build X strictly from decision-time information (no leakage).
                X_ml: Dict[str, Any] = {}
                X_ml["scenario_v4"] = str(getattr(dec, "scenario", scenario) if dec else scenario)
                X_ml["direction"] = str(direction)
                X_ml["have"] = int(have)
                X_ml["need"] = int(need)
                X_ml["score"] = float(score)
                # Evidence is already explainable; reuse as features.
                for k, v in evidence.items():
                    X_ml[f"ev.{k}"] = v
                # Also pass selected indicators (execution / health).
                for k in ("spread_bps", "expected_slippage_bps", "exec_risk_norm", "data_health", "book_health_ok"):
                    if k in indicators:
                        X_ml[k] = indicators.get(k)
                sid = str(indicators.get("sid", "") or "")
                rg = str(regime or "")
                md = ml.check(X=X_ml, symbol=str(symbol), ts_ms=int(now_ts), sid=sid, regime_group=rg)
                evidence["ml_mode"] = str(md.mode)
                evidence["ml_p_edge"] = float(md.p_edge)
                evidence["ml_threshold"] = float(md.threshold)
                evidence["ml_allow"] = int(md.allow)
                evidence["ml_model_ver"] = str(md.model_version)
                evidence["ml_fail_reason"] = str(md.fail_reason)
                evidence["ml_top_features"] = str(md.top_features)
                # In ENFORCE mode, MLDecision.allow may be False (including fail-closed).
                if int(ok) == 1 and (not bool(md.allow)) and str(md.mode).upper() == "ENFORCE":
                    ok = 0
                    final_reason = f"ml_block(p={md.p_edge:.2f}<thr={md.threshold:.2f})"
        except Exception:
            pass

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